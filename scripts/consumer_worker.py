import asyncio
import os
import signal
import sys
from pathlib import Path

# PyArmor runtime support
runtime_dir = Path(__file__).resolve().parent.parent / "pyarmor_runtime_000000"
if runtime_dir.exists() and str(runtime_dir) not in sys.path:
    sys.path.insert(0, str(runtime_dir))

# 确保可以找到项目内的 app 包
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import settings
from app.service.redis_queue_service import RedisQueueService
from app.service.minio_service import MinioService
from app.service.behavior_service import BehaviorService
from app.service.event_post_processor import EventPostProcessor
from app.service.algorithm_registry import AlgorithmRegistry
from app.utils.logger import init_logging, get_logger
from app.video_watcher import start_consumer
from verify_license import verify_license
from app.utils.license_guard import LicenseGuard


def _ensure_license():
    valid, message = verify_license(settings.LICENSE_PATH)
    if not valid:
        raise RuntimeError(f"License verification failed: {message}")


async def main():
    init_logging(settings.LOG_LEVEL)
    logger = get_logger(__name__)
    _ensure_license()

    redis_service = RedisQueueService()
    minio_service = MinioService()
    behavior_service = BehaviorService(redis_client=redis_service.client)
    event_processor = EventPostProcessor(minio_service, redis_service.client)
    algorithm_registry = AlgorithmRegistry()
    algorithm_registry.register("behavior", behavior_service)
    algorithm_registry.register("default", behavior_service)

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, minio_service.init)
    await redis_service.init()

    stop_event = asyncio.Event()
    license_guard = LicenseGuard(interval_seconds=3600)
    await license_guard.start()

    def _stop():
        logger.info("收到停止信号，准备关闭消费者")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            # 部分平台不支持信号处理，忽略
            pass

    consumer_task = await start_consumer(
        minio_service,
        behavior_service,
        redis_service,
        event_processor,
        algorithm_registry,
    )
    logger.info("Redis 消费者已启动")

    await stop_event.wait()

    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        logger.info("消费者任务已取消")

    await redis_service.close()
    await license_guard.stop()
    logger.info("消费者已退出")


if __name__ == "__main__":
    asyncio.run(main())
