import asyncio
import os
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


# ============================
# 上传单文件到 MinIO
# ============================
async def handle_video(video_path: str, minio_service):
    """上传文件到 MinIO，生成预签名 URL，并删除本地文件。"""
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
    """
    生产阶段：上传到 MinIO → 删除本地文件 → 将 MinIO 信息入 Redis。
    """
    try:
        logger.info("🔥 即将处理文件 → %s", video_path)

        await semaphore.acquire()
        try:
            upload_info = await handle_video(video_path, minio_service)
        finally:
            semaphore.release()

        await redis_service.enqueue_minio_video(
            object_name=upload_info["object_name"],
            presigned_url=upload_info["presigned_url"],
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

    def _process_path(self, path):
        """通用文件过滤和处理逻辑"""
        filename = os.path.basename(path)

        if (not os.path.isdir(path)
                and path.lower().endswith(".mp4")
                and not filename.startswith('.')):

            self.loop.call_soon_threadsafe(
                lambda: asyncio.create_task(
                    process_and_enqueue(
                        path,
                        self.minio_service,
                        self.semaphore,
                        self.redis_service,
                    )
                )
            )
        else:
            logger.debug("🚫 过滤临时文件/非视频文件: %s", path)

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
async def redis_consumer(redis_service, minio_service, behavior_service):
    """
    阻塞等待 Redis 队列的 MinIO 对象任务。
    需按需手动启动：下载 MinIO 对象到本地 → 行为识别/拼接。
    """
    logger.info("🚀 Redis 任务消费者已启动")
    try:
        while True:
            task = await redis_service.dequeue_video()
            if not task:
                continue

            object_name = task.get("object_name")
            presigned_url = task.get("presigned_url")

            if not object_name or not presigned_url:
                logger.warning("收到无效任务：%s", task)
                continue

            logger.info("🛰️ 消费 MinIO 任务：%s", object_name)
            camera_id = next((part for part in Path(object_name).parts if part.startswith("camera_")), None)

            # 下载 MinIO 对象到本地临时文件
            suffix = Path(object_name).suffix or ".mp4"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp_path = tmp.name
                print("temp_path:", tmp_path)
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
                continue

            try:
                if behavior_service is not None:
                    await behavior_service.process_clip(tmp_path, camera_id=camera_id)
            except Exception as proc_exc:
                logger.exception("行为识别/拼接失败 %s: %s", tmp_path, proc_exc)
            finally:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
    except asyncio.CancelledError:
        logger.info("Redis 任务消费者被取消，准备退出")
        raise
    except Exception:
        logger.exception("Redis 任务消费者异常退出")
        raise


# ============================
# 启动 Watchdog 监听（生产：上传+入队 MinIO 信息）
# ============================
async def start_watching(loop, minio_service, redis_service):
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
async def start_consumer(minio_service, behavior_service, redis_service):
    consumer_task = asyncio.create_task(
        redis_consumer(redis_service, minio_service, behavior_service)
    )
    return consumer_task
