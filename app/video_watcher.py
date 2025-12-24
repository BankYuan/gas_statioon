import asyncio
import os
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from typing import Optional, Set

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from app.config import settings
from app.utils.logger import get_logger
from app.database import async_session
from app.models.camera import Camera
from sqlalchemy import select
from app.service.algorithm_registry import AlgorithmTask

logger = get_logger(__name__)

# 简单的摄像头能力缓存，减少频繁查询数据库
_capability_cache = {}
_CAPABILITY_TTL_SECONDS = 300
_camera_area_cache = {}
_CAMERA_AREA_TTL_SECONDS = 300


def _normalize_camera_id(camera_id: Optional[str]) -> Optional[str]:
    if not camera_id:
        return None
    return camera_id.replace("camera_", "", 1) if camera_id.startswith("camera_") else camera_id


def _extract_camera_id_from_path(path: str) -> Optional[str]:
    parts = Path(path).parts
    for part in parts:
        if part.startswith("camera_"):
            return part
    return None


async def _get_camera_capability(camera_id: Optional[str]) -> Optional[str]:
    camera_id = _normalize_camera_id(camera_id)
    if not camera_id:
        return None

    cached = _capability_cache.get(camera_id)
    now = time.time()
    if cached and now - cached[1] < _CAPABILITY_TTL_SECONDS:
        return cached[0]

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
            _capability_cache[camera_id] = (value, now)
            return value
    except Exception:
        logger.exception("查询摄像头算法能力失败 camera_id=%s", camera_id)
        return None


async def _get_camera_area(camera_id: Optional[str]) -> Optional[str]:
    camera_id = _normalize_camera_id(camera_id)
    if not camera_id:
        return None

    cached = _camera_area_cache.get(camera_id)
    now = time.time()
    if cached and now - cached[1] < _CAMERA_AREA_TTL_SECONDS:
        return cached[0]

    try:
        async with async_session() as session:
            result = await session.execute(
                select(Camera.area2d_pt).where(Camera.camera_id == camera_id)
            )
            area = result.scalar_one_or_none()
            if area:
                _camera_area_cache[camera_id] = (area, now)
            return area
    except Exception:
        logger.exception("查询摄像头区域失败 camera_id=%s", camera_id)
        return None


# ============================
# 上传单文件到 MinIO
# ============================
async def handle_video(video_path: str, minio_service):
    """上传单个视频到 MinIO，生成预签名 URL，并删除本地文件。"""
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

    logger.debug("presigned url: %s", presigned_url)
    logger.info("✅ 上传完成: %s", object_name)

    if os.path.exists(video_path):
        await loop.run_in_executor(None, os.remove, video_path)
        logger.info("🗑️ 已删除本地视频：%s", video_path)

    return {
        "object_name": object_name,
        "presigned_url": presigned_url,
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
            upload_info = await handle_video(video_path, minio_service)
        finally:
            semaphore.release()

        clip_time = Path(upload_info["object_name"]).stem
        camera_id = _extract_camera_id_from_path(upload_info["object_name"])
        capability = await _get_camera_capability(camera_id)
        area2d_pt = await _get_camera_area(camera_id)
        if capability == "disabled":
            logger.info("🚫 摄像头算法已停用，跳过入队 camera=%s file=%s", camera_id, video_path)
            return
        
        #这里以后需要根据算法能力的不同决定哪些信息入队，目前仅有行为识别算法
        await redis_service.enqueue_minio_video(
            object_name=upload_info["object_name"],
            presigned_url=upload_info["presigned_url"],
            service_name=settings.BEHAVIOR_SERVICE_NAME,
            clip_time=clip_time,
            camera_id=camera_id,
            algorithm_capability=capability,
            area2d_pt=area2d_pt,
        )
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
        self._stabilize_checks = 3
        self._stabilize_interval = 0.5

    def _is_temp_name(self, filename: str) -> bool:
        lower = filename.lower()
        name_wo_ext, _ = os.path.splitext(lower)
        return (
            filename.startswith(".")
            or lower.endswith(".tmp")
            or lower.endswith(".temp")
            or ".tmp" in lower
            or ".temp" in lower
            or "tmp" in name_wo_ext
            or "temp" in name_wo_ext
            or lower.endswith(".partial")
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
# Redis 队列消费者（手动启动）
# ============================
async def redis_consumer(redis_service, minio_service, behavior_service, event_processor=None, algorithm_registry=None):
    """Redis 队列消费者：dispatcher + worker 池并发处理任务。"""
    logger.info("🚀 Redis 任务消费者已启动（dispatcher + worker pool）")

    task_queue: asyncio.Queue = asyncio.Queue(maxsize=100)

    async def worker(worker_id: int):
        try:
            while True:
                task = await task_queue.get()
                try:
                    await _process_queue_task(
                        task,
                        worker_id,
                        minio_service,
                        behavior_service,
                        event_processor,
                        algorithm_registry,
                    )
                finally:
                    task_queue.task_done()
        except asyncio.CancelledError:
            logger.info("worker-%s 被取消", worker_id)
            raise

    async def dispatcher():
        try:
            while True:
                task = await redis_service.dequeue_video()
                if not task:
                    continue
                await task_queue.put(task)
        except asyncio.CancelledError:
            logger.info("dispatcher 被取消")
            raise

    workers = [
        asyncio.create_task(worker(i), name=f"redis-worker-{i}")
        for i in range(settings.UPLOAD_CONCURRENCY)
    ]
    dispatcher_task = asyncio.create_task(dispatcher(), name="redis-dispatcher")

    try:
        await asyncio.gather(dispatcher_task, *workers)
    except asyncio.CancelledError:
        dispatcher_task.cancel()
        for w in workers:
            w.cancel()
        await asyncio.gather(dispatcher_task, *workers, return_exceptions=True)
        logger.info("Redis 任务消费者被取消，准备退出")
        raise
    except Exception:
        logger.exception("Redis 任务消费者异常退出")
        dispatcher_task.cancel()
        for w in workers:
            w.cancel()
        await asyncio.gather(dispatcher_task, *workers, return_exceptions=True)
        raise


async def _process_queue_task(
    task,
    worker_id: int,
    minio_service,
    behavior_service,
    event_processor,
    algorithm_registry,
):
    """下载队列任务对应的对象并调用算法/后处理。"""
    object_name = task.get("object_name")
    presigned_url = task.get("presigned_url")

    if not object_name or not presigned_url:
        logger.warning("worker-%s 收到无效任务：%s", worker_id, task)
        return

    logger.info("🛰️ worker-%s 消费 MinIO 任务：%s", worker_id, object_name)
    camera_id = task.get("camera_id") or next((part for part in Path(object_name).parts if part.startswith("camera_")), None)
    clip_time = task.get("clip_time")
    service_name = task.get("service_name")
    capability = task.get("algorithm_capability")
    # 总是查一次最新配置，避免入队后被停用仍然消费
    if camera_id:
        latest_cap = await _get_camera_capability(camera_id)
        if latest_cap:
            capability = latest_cap
    if capability == "disabled":
        logger.info("worker-%s 跳过算法处理（已停用） camera=%s object=%s", worker_id, camera_id, object_name)
        return
    area2d_pt = task.get("area2d_pt")
    if not area2d_pt and camera_id:
        area2d_pt = await _get_camera_area(camera_id)

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
    except Exception as dl_exc:
        logger.exception("下载 MinIO 对象失败 %s: %s", object_name, dl_exc)
        try:
            os.remove(tmp_path)
        except OSError:
            pass
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
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


# ============================
# 启动 Watchdog 监听（生产：上传+入队 MinIO 信息）
# ============================
async def start_watching(loop, minio_service, redis_service):
    """启动 Watchdog 监听本地目录并上传入队。"""
    semaphore = asyncio.Semaphore(settings.UPLOAD_CONCURRENCY)
    handler = VideoHandler(loop, minio_service, redis_service, semaphore)
    observer = Observer()

    observer.schedule(handler, path=settings.LOCAL_VIDEO_PATH, recursive=True)
    observer.start()

    logger.info("📡 正在监听视频目录：%s", settings.LOCAL_VIDEO_PATH)

    return observer


# ============================
# 手动启动消费者
# ============================
async def start_consumer(minio_service, behavior_service, redis_service, event_processor=None, algorithm_registry=None):
    """创建并启动 Redis 消费者任务。"""
    return asyncio.create_task(
        redis_consumer(
            redis_service,
            minio_service,
            behavior_service,
            event_processor,
            algorithm_registry,
        )
    )
