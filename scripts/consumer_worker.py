import asyncio
import os
import signal
import sys
from pathlib import Path
from typing import Optional, Dict, Any

# PyArmor runtime support
runtime_dir = Path(__file__).resolve().parent.parent / "pyarmor_runtime_000000"
if runtime_dir.exists() and str(runtime_dir) not in sys.path:
    sys.path.insert(0, str(runtime_dir))
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

logger = get_logger(__name__)

# ------------------------
# License
# ------------------------
class LicenseError(RuntimeError):
    pass

def ensure_license():
    valid, message = verify_license(settings.LICENSE_PATH)
    if not valid:
        raise LicenseError(f"License verification failed: {message}")

# ------------------------
# 重试辅助
# ------------------------
async def retry(coro, retries=3, interval=5, desc="operation"):
    for attempt in range(1, retries + 1):
        try:
            return await coro()
        except Exception as exc:
            logger.warning(f"{desc} 第 {attempt}/{retries} 次失败: {exc}")
            if attempt == retries:
                logger.error(f"{desc} 超过最大重试次数，抛出异常")
                raise
            await asyncio.sleep(interval)

# ------------------------
# 初始化服务
# ------------------------
async def init_services() -> Dict[str, Any]:
    redis_service = RedisQueueService()
    minio_service = MinioService()
    behavior_service = BehaviorService(redis_client=redis_service.client)
    event_processor = EventPostProcessor(minio_service, redis_service.client)
    algorithm_registry = AlgorithmRegistry()
    algorithm_registry.register("behavior", behavior_service)
    algorithm_registry.register("default", behavior_service)
    license_guard = LicenseGuard(interval_seconds=3600)

    loop = asyncio.get_running_loop()
    # 重试初始化 MinIO / Redis / LicenseGuard
    await retry(lambda: loop.run_in_executor(None, minio_service.init), desc="MinIO init")
    await retry(redis_service.init, desc="Redis init")
    await retry(license_guard.start, desc="LicenseGuard start")

    return {
        "redis_service": redis_service,
        "minio_service": minio_service,
        "behavior_service": behavior_service,
        "event_processor": event_processor,
        "algorithm_registry": algorithm_registry,
        "license_guard": license_guard,
    }

# ------------------------
# 启动消费者
# ------------------------
async def start_consumer_loop(services, stop_event: asyncio.Event):
    async def run_consumer():
        consumer_task = await start_consumer(
            services["minio_service"],
            services["behavior_service"],
            services["redis_service"],
            services["event_processor"],
            services["algorithm_registry"],
        )
        logger.info("Redis 消费者已启动")
        await stop_event.wait()
        logger.info("收到停止信号，准备关闭消费者")
        consumer_task.cancel()
        try:
            await asyncio.wait_for(consumer_task, timeout=5)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            logger.warning("消费者未能在超时时间内退出")
        try:
            await consumer_task
        except asyncio.CancelledError:
            logger.info("消费者任务已取消")

    await retry(run_consumer, retries=3, interval=5, desc="启动消费者")

# ------------------------
# 信号处理
# ------------------------
def setup_signal_handlers(stop_event: asyncio.Event):
    loop = asyncio.get_running_loop()
    def _stop():
        logger.info("收到系统信号，触发退出")
        stop_event.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            # 回退到同步 signal.signal 以支持 Windows 等平台
            signal.signal(
                sig,
                lambda s, f: loop.call_soon_threadsafe(_stop),
            )
            logger.warning(f"平台不支持 loop.add_signal_handler，已降级为同步信号处理: {sig}")

# ------------------------
# 服务关闭
# ------------------------
async def shutdown_services(redis_service=None, license_guard=None):
    if redis_service:
        await redis_service.close()
    if license_guard:
        await license_guard.stop()

# ------------------------
# Main
# ------------------------
async def main():
    init_logging(settings.LOG_LEVEL)
    ensure_license()

    stop_event = asyncio.Event()
    setup_signal_handlers(stop_event)

    services = await init_services()

    try:
        await start_consumer_loop(services, stop_event)
    finally:
        await shutdown_services(
            redis_service=services.get("redis_service"),
            license_guard=services.get("license_guard")
        )
        logger.info("消费者已退出")

if __name__ == "__main__":
    asyncio.run(main())
