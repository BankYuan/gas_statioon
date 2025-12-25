import asyncio
import os
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from typing import Optional, Set, List

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from app.config import settings
from app.utils.logger import get_logger
from app.database import async_session
from app.models.camera import Camera
from sqlalchemy import select
from app.service.algorithm_registry import AlgorithmTask
import contextlib
import threading

logger = get_logger(__name__)


class TTLCache:
    """线程安全的 TTL + 简单容量限制缓存。"""

    def __init__(self, ttl_seconds: int, max_size: int = 1024):
        self.ttl = ttl_seconds
        self.max_size = max_size
        self._data: dict = {}
        self._lock = asyncio.Lock()
        self._last_prune = time.time()
        self._prune_task: Optional[asyncio.Task] = None
        self._closed = False

    async def get(self, key):
        async with self._lock:
            entry = self._data.get(key)
            if not entry:
                return None
            value, ts = entry
            if time.time() - ts > self.ttl:
                self._data.pop(key, None)
                return None
            return value

    async def set(self, key, value):
        async with self._lock:
            if len(self._data) >= self.max_size:
                # 简单 FIFO 淘汰最旧的
                oldest_key = next(iter(self._data))
                self._data.pop(oldest_key, None)
            self._data[key] = (value, time.time())
            self._prune_locked()
            # 确保有后台定期清理任务
            if (self._prune_task is None or self._prune_task.done()) and not self._closed:
                self._prune_task = asyncio.create_task(self._prune_loop())

    async def clear(self, key):
        async with self._lock:
            self._data.pop(key, None)

    def _prune_locked(self):
        now = time.time()
        # 降低频率，避免每次 set 都全量遍历
        if now - self._last_prune < 5:
            return
        expired = [k for k, (_, ts) in self._data.items() if now - ts > self.ttl]
        for k in expired:
            self._data.pop(k, None)
        self._last_prune = now

    async def _prune_loop(self):
        try:
            while True:
                await asyncio.sleep(30)
                async with self._lock:
                    self._prune_locked()
        except asyncio.CancelledError:
            return

    async def close(self):
        """取消后台清理任务，释放资源。"""
        self._closed = True
        if self._prune_task and not self._prune_task.done():
            self._prune_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._prune_task


_capability_cache = TTLCache(ttl_seconds=300, max_size=2048)
_camera_area_cache = TTLCache(ttl_seconds=300, max_size=2048)


def _normalize_camera_id(camera_id: Optional[str]) -> Optional[str]:
    if not camera_id:
        return None
    return camera_id.replace("camera_", "", 1) if camera_id.startswith("camera_") else camera_id


def _extract_camera_id_from_path(path: str) -> Optional[str]:
    parts = Path(path).parts
    for part in reversed(parts):
        if part.startswith("camera_"):
            return part
    return None


async def _get_camera_capability(camera_id: Optional[str]) -> Optional[str]:
    camera_id = _normalize_camera_id(camera_id)
    if not camera_id:
        return None

    cached = await _capability_cache.get(camera_id)
    if cached is not None:
        return cached

    try:
        async with async_session() as session:
            result = await session.execute(
                select(Camera.algorithm_capability, Camera.algorithm_enabled).where(Camera.camera_id == camera_id)
            )
            row = result.first()
            if not row:
                return None
            capability, enabled = row[0], row[1]
            value = capability if enabled else "disabled"
            await _capability_cache.set(camera_id, value)
            return value
    except Exception:
        logger.exception("查询摄像头算法能力失败 camera_id=%s", camera_id)
        return None


async def _get_camera_area(camera_id: Optional[str]) -> Optional[str]:
    camera_id = _normalize_camera_id(camera_id)
    if not camera_id:
        return None

    cached = await _camera_area_cache.get(camera_id)
    if cached is not None:
        return cached

    try:
        async with async_session() as session:
            result = await session.execute(
                select(Camera.area2d_pt).where(Camera.camera_id == camera_id)
            )
            area = result.scalar_one_or_none()
            if area:
                await _camera_area_cache.set(camera_id, area)
            return area
    except Exception:
        logger.exception("查询摄像头区域失败 camera_id=%s", camera_id)
        return None


# ============================
# 上传单文件到 MinIO
# ============================
async def upload_to_minio(video_path: str, minio_service):
    """上传单个视频到 MinIO，返回对象名与预签名 URL，并删除本地文件。"""
    rel_path = os.path.relpath(video_path, settings.LOCAL_VIDEO_PATH)
    object_name = f"videos/{rel_path.replace(os.sep, '/')}"

    loop = asyncio.get_running_loop()
    logger.info("📦 正在上传文件到 MinIO: %s", object_name)
    try:
        await loop.run_in_executor(
            None,
            lambda: minio_service.upload_file(
                bucket=settings.MINIO_BUCKET,
                object_name=object_name,
                file_path=video_path,
                expire_days=settings.EXPIRE_DAY,
            )
        )
        presigned_url = minio_service.client.presigned_get_object(
            settings.MINIO_BUCKET,
            object_name,
            expires=timedelta(days=settings.EXPIRE_DAY)
        )
    except Exception as exc:
        logger.exception("上传或生成预签名 URL 失败：%s", exc)
        raise
    finally:
        if os.path.exists(video_path):
            await loop.run_in_executor(None, os.remove, video_path)

    logger.info("✅ 上传完成: %s", object_name)
    return {"object_name": object_name, "presigned_url": presigned_url}


def build_task_payload(upload_info: dict, capability: Optional[str], area2d_pt: Optional[str], clip_time: str, camera_id: Optional[str]):
    """根据算法能力构造入队 payload，预留多算法扩展。"""
    return {
        "object_name": upload_info["object_name"],
        "presigned_url": upload_info["presigned_url"],
        "service_name": settings.BEHAVIOR_SERVICE_NAME,
        "clip_time": clip_time,
        "camera_id": camera_id,
        "algorithm_capability": capability,
        "area2d_pt": area2d_pt,
    }


# ============================
async def process_and_enqueue(
    video_path: str,
    minio_service,
    semaphore: asyncio.Semaphore,
    redis_service,
):
    """上传文件并将 MinIO 元数据入队（生产者侧）。"""
    try:
        logger.info("🔥 即将处理文件 → %s", video_path)

        await semaphore.acquire()
        try:
            upload_info = await upload_to_minio(video_path, minio_service)
        finally:
            semaphore.release()

        clip_time = Path(upload_info["object_name"]).stem
        camera_id = _extract_camera_id_from_path(upload_info["object_name"])
        capability = await _get_camera_capability(camera_id)
        area2d_pt = await _get_camera_area(camera_id)
        if capability == "disabled":
            logger.info("🚫 摄像头算法已停用，跳过入队 camera=%s file=%s", camera_id, video_path)
            return

        payload = build_task_payload(upload_info, capability, area2d_pt, clip_time, camera_id)
        await redis_service.enqueue_minio_video(**payload)
        logger.info("✅ 已将 MinIO 对象入队：%s", upload_info["object_name"])

    except Exception as e:
        logger.exception("❌ 处理失败 %s: %s", video_path, e)

# ============================
# Watchdog 文件事件（上传+入队 MinIO 信息）
# ============================
class VideoHandler(FileSystemEventHandler):
    """处理本地新生成的视频文件，同时监听创建和重命名事件"""

    def __init__(self, loop, minio_service, redis_service, semaphore):
        self.loop = loop
        self.minio_service = minio_service
        self.redis_service = redis_service
        self.semaphore = semaphore
        self._processing: Set[str] = set()
        self._processing_lock = threading.Lock()
        self._stabilize_checks = 3
        self._stabilize_interval = 0.5

    def _is_temp_name(self, filename: str) -> bool:
        lower = filename.lower()
        name_wo_ext, _ = os.path.splitext(lower)
        return (
            lower.startswith(".")
            or "tmp" in name_wo_ext
            or "temp" in name_wo_ext
            or lower.endswith((".tmp", ".temp", ".partial"))
        )

    def _should_process(self, path: str) -> bool:
        if not path:
            return False
        if not os.path.exists(path) or os.path.isdir(path):
            return False
        filename = os.path.basename(path)
        if self._is_temp_name(filename):
            return False
        if not filename.lower().endswith(".mp4"):
            return False
        return True

    def _process_path(self, path: str):
        """校验路径并异步等待文件稳定后入队"""
        if not self._should_process(path):
            logger.debug("🚫 过滤临时/非视频/不存在文件: %s", path)
            return

        abs_path = os.path.abspath(path)
        if abs_path in self._processing:
            with self._processing_lock:
                if abs_path in self._processing:
                    logger.debug("⚠️ 文件已经在上传队列中，忽略重复事件: %s", abs_path)
                    return
                self._processing.add(abs_path)

        def _task():
            asyncio.create_task(self._wait_ready_and_enqueue(abs_path))

        self.loop.call_soon_threadsafe(_task)

    async def _wait_ready_and_enqueue(self, path: str):
        try:
            ready = await self._wait_until_stable(path)
            if not ready:
                logger.warning("⚠️ 文件始终未稳定，放弃入队: %s", path)
                return
            await process_and_enqueue(
                path,
                self.minio_service,
                self.semaphore,
                self.redis_service,
            )
        finally:
            with self._processing_lock:
                self._processing.discard(path)

    async def _wait_until_stable(self, path: str) -> bool:
        prev_size = None
        for _ in range(self._stabilize_checks):
            try:
                size = os.path.getsize(path)
            except OSError:
                return False
            if size <= 0:
                await asyncio.sleep(self._stabilize_interval)
                continue
            if prev_size is not None and size == prev_size:
                logger.debug("📁 文件大小稳定，准备上传 %s size=%s", path, size)
                return True
            prev_size = size
            await asyncio.sleep(self._stabilize_interval)
        logger.warning("⚠️ 文件大小一直变化，判定为不稳定 %s", path)
        return False

    def on_created(self, event):
        """捕获文件创建事件（通常是临时文件创建，会被过滤）"""
        if not event.is_directory:
            logger.debug("on_created 触发: %s", event.src_path)
            self._process_path(event.src_path)

    def on_moved(self, event):
        """
        捕获文件重命名事件。
        这是从临时文件 (.xxx.mp4) 重命名到最终文件 (xxx.mp4) 时触发的关键事件。
        """
        if not event.is_directory:
            logger.debug("on_moved 触发: %s -> %s", event.src_path, event.dest_path)
            self._process_path(event.dest_path)

# ============================
# Watchdog Runner 封装
# ============================
class WatchdogRunner:
    """封装 Watchdog 的生命周期，便于统一管理."""

    def __init__(self, loop, minio_service, redis_service):
        self.loop = loop
        self.minio_service = minio_service
        self.redis_service = redis_service
        self.semaphore = asyncio.Semaphore(settings.UPLOAD_CONCURRENCY)
        self.handler = VideoHandler(loop, minio_service, redis_service, self.semaphore)
        self.observer = Observer()
        self._running = False

    async def start(self):
        self.observer.schedule(self.handler, path=settings.LOCAL_VIDEO_PATH, recursive=True)
        self.observer.start()
        self._running = True
        logger.info("📡 正在监听视频目录：%s", settings.LOCAL_VIDEO_PATH)
        return self.observer

    async def stop(self):
        if not self._running:
            return
        self._running = False
        self.observer.stop()
        await asyncio.to_thread(self.observer.join)
        logger.info("Watchdog 已关闭")

# ============================
# Queue Consumer 封装
# ============================
class QueueConsumer:
    """封装 Redis 队列消费的生命周期和并发管理。"""

    def __init__(self, redis_service, minio_service, behavior_service, event_processor=None, algorithm_registry=None):
        self.redis_service = redis_service
        self.minio_service = minio_service
        self.behavior_service = behavior_service
        self.event_processor = event_processor
        self.algorithm_registry = algorithm_registry
        self.task_queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.running = asyncio.Event()
        self.running.set()
        self.dispatcher_task: Optional[asyncio.Task] = None
        self.worker_tasks: List[asyncio.Task] = []

    async def start(self):
        self.dispatcher_task = asyncio.create_task(self._dispatcher(), name="redis-dispatcher")
        self.worker_tasks = [
            asyncio.create_task(self._worker(i), name=f"redis-worker-{i}")
            for i in range(settings.UPLOAD_CONCURRENCY)
        ]

    async def wait(self):
        if self.dispatcher_task:
            await self.dispatcher_task

    async def stop(self):
        if not self.running.is_set():
            return
        self.running.clear()
        if self.dispatcher_task:
            self.dispatcher_task.cancel()
        for t in self.worker_tasks:
            t.cancel()
        await asyncio.gather(*(self.worker_tasks + ([self.dispatcher_task] if self.dispatcher_task else [])), return_exceptions=True)
        logger.info("Redis 任务消费者已停止")

    async def run_forever(self):
        await self.start()
        try:
            await self.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def _dispatcher(self):
        try:
            while self.running.is_set():
                task = await self.redis_service.dequeue_video(timeout=1)
                if not task:
                    await asyncio.sleep(0.1)
                    continue
                await self.task_queue.put(task)
        except asyncio.CancelledError:
            logger.info("dispatcher 被取消")
        except Exception:
            logger.exception("dispatcher 异常退出")

    async def _worker(self, worker_id: int):
        try:
            while self.running.is_set():
                try:
                    task = await asyncio.wait_for(self.task_queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                try:
                    await _process_queue_task(
                        task,
                        worker_id,
                        self.minio_service,
                        self.behavior_service,
                        self.event_processor,
                        self.algorithm_registry,
                        self.redis_service,
                    )
                finally:
                    self.task_queue.task_done()
        except asyncio.CancelledError:
            logger.info("worker-%s 被取消", worker_id)
        except Exception:
            logger.exception("worker-%s 异常退出", worker_id)

# ============================
# Redis 队列消费者（手动启动）
# ============================
async def redis_consumer(redis_service, minio_service, behavior_service, event_processor=None, algorithm_registry=None):
    """兼容旧入口：启动 QueueConsumer。"""
    consumer = QueueConsumer(redis_service, minio_service, behavior_service, event_processor, algorithm_registry)
    await consumer.start()
    try:
        await consumer.wait()
    finally:
        await consumer.stop()


async def _process_queue_task(
    task,
    worker_id: int,
    minio_service,
    behavior_service,
    event_processor,
    algorithm_registry,
    redis_service=None,
):
    """下载队列任务对应的对象并调用算法/后处理。"""
    object_name = task.get("object_name")
    presigned_url = task.get("presigned_url")

    if not object_name or not presigned_url:
        logger.warning("worker-%s 收到无效任务：%s", worker_id, task)
        return

    logger.info("🛰️ worker-%s 消费 MinIO 任务：%s camera=%s", worker_id, object_name, task.get("camera_id"))
    camera_id = task.get("camera_id") or next((part for part in Path(object_name).parts if part.startswith("camera_")), None)
    clip_time = task.get("clip_time")
    service_name = task.get("service_name")
    capability, area2d_pt = await _resolve_task_context(task, camera_id)
    if capability == "disabled":
        logger.info("worker-%s 跳过算法处理（已停用） camera=%s object=%s", worker_id, camera_id, object_name)
        return

    tmp_path = await _download_object(minio_service, object_name)
    if not tmp_path:
        if redis_service:
            await _send_to_dlq(redis_service, task, camera_id, "download_failed")
        return

    try:
        result = None
        handler = behavior_service
        if algorithm_registry is not None:
            handler = algorithm_registry.get(capability, default=behavior_service)
        task_ctx = AlgorithmTask(
            video_path=tmp_path,
            camera_id=camera_id,
            object_name=object_name,
            clip_time=clip_time,
            service_name=service_name,
            algorithm_capability=capability,
            area2d_pt=area2d_pt,
            extra=task,
        )
        if handler is not None:
            if hasattr(handler, "handle"):
                result = await handler.handle(task_ctx)
            elif callable(getattr(handler, "process_clip", None)):
                result = await handler.process_clip(
                    tmp_path,
                    camera_id=camera_id,
                    object_name=object_name,
                    clip_time=clip_time,
                    service_name=service_name,
                )
            else:
                logger.warning("handler 无法调用 capability=%s handler=%s", capability, handler)
        # 只有行为识别流程才需要事件后处理；检查完成标记以保持兼容
        if isinstance(result, dict) and result.get("complete") and event_processor is not None:
            await event_processor.process_event(result)
            logger.info("事件后处理完成 camera=%s", result.get("camera_id"))
    except Exception as proc_exc:
        logger.exception("行为识别失败 %s: %s", tmp_path, proc_exc)
        if redis_service:
            # 简单重试一次，超出则进入 DLQ
            retries = int(task.get("retries", 0))
            if retries < 1:
                await redis_service.requeue(task, delay_seconds=1)
            else:
                await _send_to_dlq(redis_service, task, camera_id, "process_failed", str(proc_exc))
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            logger.warning("清理临时文件失败 tmp=%s", tmp_path)


# ============================
# 启动 Watchdog 监听（生产：上传+入队 MinIO 信息）
# ============================
async def start_watching(loop, minio_service, redis_service):
    """启动 Watchdog 监听本地目录并上传入队。"""
    runner = WatchdogRunner(loop, minio_service, redis_service)
    await runner.start()
    return runner


async def _resolve_task_context(task: dict, camera_id: Optional[str]):
    """获取最新的能力/区域配置，降级使用任务内字段。"""
    capability = task.get("algorithm_capability")
    area2d_pt = task.get("area2d_pt")
    if camera_id:
        try:
            latest_cap = await _get_camera_capability(camera_id)
            if latest_cap:
                capability = latest_cap
        except Exception:
            logger.warning("获取 camera capability 失败，使用任务内配置", exc_info=True)
        if not area2d_pt:
            try:
                area2d_pt = await _get_camera_area(camera_id)
            except Exception:
                logger.warning("获取 camera area 失败，使用任务内配置", exc_info=True)
    return capability, area2d_pt


async def shutdown_caches():
    """取消缓存后台任务，避免资源泄漏。"""
    await _capability_cache.close()
    await _camera_area_cache.close()


async def _download_object(minio_service, object_name: str) -> Optional[str]:
    """下载 MinIO 对象到临时文件，失败返回 None。"""
    suffix = Path(object_name).suffix or ".mp4"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp_path = tmp.name
    try:
        await asyncio.to_thread(
            minio_service.client.fget_object,
            settings.MINIO_BUCKET,
            object_name,
            tmp_path,
        )
        return tmp_path
    except Exception as dl_exc:
        logger.exception("下载 MinIO 对象失败 object=%s: %s", object_name, dl_exc)
        try:
            os.remove(tmp_path)
        except OSError:
            logger.warning("清理下载失败的临时文件失败 tmp=%s", tmp_path)
        return None


async def _send_to_dlq(redis_service, task: dict, camera_id: Optional[str], reason: str, error: Optional[str] = None):
    """发送到死信队列，附加错误信息。"""
    payload = dict(task)
    payload["error"] = reason
    if error:
        payload["error_detail"] = error
    await redis_service.enqueue_dead_letter(payload, camera_id=camera_id)


# ============================
# 手动启动消费者
# ============================
async def start_consumer(minio_service, behavior_service, redis_service, event_processor=None, algorithm_registry=None):
    """创建并启动 Redis 消费者任务。"""
    consumer = QueueConsumer(redis_service, minio_service, behavior_service, event_processor, algorithm_registry)
    return asyncio.create_task(consumer.run_forever(), name="redis-consumer-main")
