import asyncio
import json
import time
from typing import Any, Dict, Optional, TypedDict

import redis.asyncio as redis

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


PAYLOAD_VERSION = 1


class QueuePayload(TypedDict, total=False):
    """队列消息的强类型定义，便于后续演进与校验。"""

    version: int
    object_name: str
    presigned_url: str
    timestamp: float
    service_name: str
    clip_time: str
    camera_id: str
    algorithm_capability: str
    area2d_pt: str


class RedisQueueService:
    """
    简单的 Redis List 任务队列：
    - watcher 将视频切片推入 List
    - 后台 consumer 使用 BLPOP 阻塞获取任务
    """

    def __init__(self, url: Optional[str] = None, queue_name: Optional[str] = None):
        self.url = url or settings.REDIS_URL
        self.queue_name = queue_name or settings.REDIS_QUEUE_NAME
        self.client = redis.from_url(self.url, encoding="utf-8", decode_responses=True)

    async def init(self):
        """启动时做一次 PING，确保 Redis 可用。"""
        await self.client.ping()
        logger.info("RedisQueueService connected to %s queue=%s", self.url, self.queue_name)

    def _queue_key(self) -> str:
        return self.queue_name

    async def enqueue_minio_video(
        self,
        object_name: str,
        presigned_url: str,
        *,
        service_name: Optional[str] = None,
        clip_time: Optional[str] = None,
        camera_id: Optional[str] = None,
        algorithm_capability: Optional[str] = None,
        area2d_pt: Optional[str] = None,
        retries: int = 0,
    ):
        payload: QueuePayload = {
            "version": PAYLOAD_VERSION,
            "object_name": object_name,
            "presigned_url": presigned_url,
            "timestamp": time.time(),
            "service_name": service_name or "",
            "clip_time": clip_time or "",
            "camera_id": camera_id or "",
            "algorithm_capability": algorithm_capability or "",
            "area2d_pt": area2d_pt or "",
            "retries": retries,
        }
        key = self._queue_key()
        await self.client.rpush(key, json.dumps(payload))
        logger.debug("入队 MinIO 对象 %s key=%s", object_name, key)

    async def dequeue_video(self, timeout: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        阻塞式获取任务；timeout=0 表示永不超时，None 则使用配置的默认值。
        如果任务携带的 timestamp 超出存储期，会自动丢弃并继续等待下一条。
        """
        expire_seconds = settings.EXPIRE_DAY * 86400
        pop_timeout = timeout if timeout is not None else settings.REDIS_POP_TIMEOUT
        key = self._queue_key()
        backoff = 0.5
        while True:
            try:
                item = await self.client.brpop(key, timeout=pop_timeout)
            except asyncio.CancelledError:
                return None
            except Exception as exc:
                logger.warning("Redis BRPOP 失败 key=%s error=%s", key, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5)
                continue

            backoff = 0.5
            if not item:
                if timeout is not None:
                    return None
                continue
            _, data = item
            try:
                payload = json.loads(data)
            except json.JSONDecodeError:
                logger.warning("无法解析 Redis 任务：%s", data)
                continue
            if not self._validate_payload(payload):
                continue

            created_ts = payload.get("timestamp")
            if created_ts:
                try:
                    age = time.time() - float(created_ts)
                except (TypeError, ValueError):
                    age = 0
                if age > expire_seconds:
                    logger.warning(
                        "⏰ Redis 任务已超过保存期，丢弃: %s (age=%.0fs > expire=%ds)",
                        payload.get("object_name"),
                        age,
                        expire_seconds,
                    )
                    continue
            return payload

    async def enqueue_dead_letter(self, payload: Dict[str, Any], camera_id: Optional[str] = None):
        """写入死信队列，按摄像头拆分。"""
        base = self._queue_key()
        key = f"{base}:dlq"
        if camera_id:
            key = f"{key}:{camera_id}"
        await self.client.rpush(key, json.dumps(payload))
        await self.client.expire(key, settings.EXPIRE_DAY * 86400)
        logger.warning("写入死信队列 key=%s object=%s reason=%s", key, payload.get("object_name"), payload.get("error"))

    async def requeue(self, payload: Dict[str, Any], delay_seconds: int = 0):
        """失败重试：可选延迟后重新入队，重试计数+1。"""
        retries = int(payload.get("retries", 0)) + 1
        payload["retries"] = retries
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)
        try:
            key = self._queue_key()
            await self.client.rpush(key, json.dumps(payload))
            logger.info("重试入队 object=%s retries=%s key=%s", payload.get("object_name"), retries, key)
        except Exception as exc:
            logger.exception("重试入队失败 payload=%s error=%s", payload, exc)

    async def close(self):
        await self.client.close()

    def _validate_payload(self, payload: Dict[str, Any]) -> bool:
        """基础校验：版本、必需字段存在且类型大致正确。"""
        if payload.get("version") != PAYLOAD_VERSION:
            logger.warning("收到不匹配的 payload 版本：%s data=%s", payload.get("version"), payload)
            return False
        required = ["object_name", "presigned_url", "timestamp"]
        for field in required:
            if field not in payload:
                logger.warning("缺少必需字段 %s data=%s", field, payload)
                return False
        return True

    # Metrics hook placeholders（可与监控系统对接）
    def record_enqueue(self, payload: QueuePayload):
        pass

    def record_dequeue(self, payload: QueuePayload, dropped: bool = False):
        pass

    def record_dlq(self, payload: Dict[str, Any]):
        pass
