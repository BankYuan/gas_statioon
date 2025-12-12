import json
import time
from typing import Any, Dict, Optional

import redis.asyncio as redis

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


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

    async def enqueue_video(self, video_path: str):
        raise NotImplementedError("Use enqueue_minio_video to push MinIO info")

    async def enqueue_minio_video(
        self,
        object_name: str,
        presigned_url: str,
        *,
        service_name: Optional[str] = None,
        clip_time: Optional[str] = None,
    ):
        payload = json.dumps(
            {
                "object_name": object_name,
                "presigned_url": presigned_url,
                "timestamp": time.time(),
                "service_name": service_name,
                "clip_time": clip_time,
            }
        )
        await self.client.rpush(self.queue_name, payload)
        # 队列中的元数据与 MinIO 对象生命周期一致，持续刷新 TTL，避免积压
        await self.client.expire(self.queue_name, settings.EXPIRE_DAY * 86400)
        logger.debug("入队 MinIO 对象 %s", object_name)

    async def dequeue_video(self, timeout: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        阻塞式获取任务。timeout=0 表示永不超时，None 则使用配置的默认值。
        如果任务携带的 timestamp 超出存储期，会自动丢弃并继续等待下一条。
        """
        expire_seconds = settings.EXPIRE_DAY * 86400
        pop_timeout = timeout if timeout is not None else settings.REDIS_POP_TIMEOUT
        while True:
            item = await self.client.blpop(self.queue_name, timeout=pop_timeout)
            if not item:
                return None
            _, data = item
            try:
                payload = json.loads(data)
            except json.JSONDecodeError:
                logger.warning("无法解析 Redis 任务：%s", data)
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

    async def close(self):
        await self.client.close()
