# =========================================================
# ⚠️ 日志必须最早初始化（保证任何阶段崩溃都有日志）
# =========================================================
import logging
from app.utils.logger import init_logging

# 使用默认 INFO，不依赖 settings / FastAPI
if not logging.getLogger().handlers:
    init_logging()

# =========================================================
# 标准库 & 三方库
# =========================================================
import asyncio
import sys
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Optional, List, Callable, Awaitable

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# =========================================================
# 业务模块
# =========================================================
from app.routers.camera_api import router as camera_router
from app.routers.snapshot_api import router as snapshot_router
from app.routers.record_api import router as record_router

from app.video_watcher import start_watching
from app.service.minio_service import MinioService
from app.service.zlm_service import ZLMService
from app.service.camera_service import CameraService
from app.service.behavior_service import BehaviorService
from app.service.redis_queue_service import RedisQueueService
from app.service.algorithm_registry import AlgorithmRegistry
from app.video_watcher import shutdown_caches
from app.database import async_session

from app.utils.logger import get_logger
from app.config import settings
from verify_license import verify_license
from app.utils.license_guard import LicenseGuard

logger = get_logger(__name__)

# =========================================================
# PyArmor Runtime 注入
# =========================================================
runtime_dir = Path(__file__).resolve().parent.parent / "pyarmor_runtime_000000"
if runtime_dir.exists() and str(runtime_dir) not in sys.path:
    sys.path.insert(0, str(runtime_dir))

# =========================================================
# 自定义异常
# =========================================================
class LicenseError(Exception):
    """License 校验失败"""

# =========================================================
# License 校验
# =========================================================
def ensure_license() -> None:
    valid, message = verify_license(settings.LICENSE_PATH)
    if not valid:
        raise LicenseError(message)

# =========================================================
# 工具：安全取消 Task
# =========================================================
async def cancel_task(
    task: Optional[asyncio.Task],
    timeout: int = 5,
):
    if not task or task.done():
        return

    task.cancel()
    try:
        await asyncio.wait_for(task, timeout=timeout)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        logger.warning("后台任务未能在超时时间内退出")
    except Exception:
        logger.exception("后台任务异常退出")

# =========================================================
# Lifespan（10 分版核心）
# =========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 生命周期（生产级）：
    - Startup：分阶段初始化
    - Shutdown：统一、可控、逆序释放
    """

    loop = asyncio.get_running_loop()

    # 所有 shutdown 行为统一登记
    shutdown_hooks: List[Callable[[], Awaitable[None]]] = []

    async def run_shutdown_hook(hook: Callable[[], Awaitable[None]]):
        try:
            await hook()
        except Exception:
            logger.exception("资源关闭失败")

    try:
        # =================================================
        # Startup
        # =================================================
        logger.info("🚀 Video Platform Service starting...")

        # 1️⃣ License（失败立即终止）
        ensure_license()

        # 2️⃣ MinIO（同步 SDK → executor）
        app.state.minio_service = MinioService()
        await loop.run_in_executor(None, app.state.minio_service.init)

        # 3️⃣ Redis
        app.state.redis_service = RedisQueueService()
        await app.state.redis_service.init()
        shutdown_hooks.append(app.state.redis_service.close)

        # 4️⃣ 基础服务
        app.state.zlm_service = ZLMService()
        shutdown_hooks.append(app.state.zlm_service.close)
        app.state.camera_service = CameraService()

        # 5️⃣ 行为 / 算法注册
        app.state.behavior_service = BehaviorService(
            redis_client=app.state.redis_service.client
        )
        shutdown_hooks.append(app.state.behavior_service.close)

        registry = AlgorithmRegistry()
        registry.register("behavior", app.state.behavior_service)
        registry.register("default", app.state.behavior_service)
        app.state.algorithm_registry = registry

        # 6️⃣ License 守护
        app.state.license_guard = LicenseGuard(interval_seconds=3600)
        await app.state.license_guard.start()
        shutdown_hooks.append(app.state.license_guard.stop)

        # 7️⃣ 文件监听 Watchdog
        app.state.observer = await start_watching(
            loop,
            app.state.minio_service,
            app.state.redis_service,
        )

        async def stop_observer():
            try:
                await app.state.observer.stop()
            except Exception:
                logger.exception("关闭 Watchdog 失败")

        shutdown_hooks.append(stop_observer)

        # 8️⃣ 定时同步 ZLM → DB
        app.state.zlm_sync_task = asyncio.create_task(sync_zlm_loop())
        async def stop_zlm_sync():
            await cancel_task(app.state.zlm_sync_task)
        shutdown_hooks.append(stop_zlm_sync)

        # 9️⃣ 关闭本地缓存清理任务
        shutdown_hooks.append(shutdown_caches)

        logger.info("✅ Video Platform Service started successfully")
        yield

    except Exception:
        logger.exception("❌ Application startup failed")
        raise

    finally:
        # =================================================
        # Shutdown（逆序释放）
        # =================================================
        logger.info("🛑 Shutting down Video Platform Service...")

        for hook in reversed(shutdown_hooks):
            await run_shutdown_hook(hook)

        logger.info("👋 Video Platform Service shutdown complete")

# =========================================================
# FastAPI App
# =========================================================
app_ = FastAPI(
    title="Video Platform Service",
    version="1.0.0",
    lifespan=lifespan,
)

# =========================================================
# Middleware
# =========================================================
app_.add_middleware(
    CORSMiddleware,
    allow_origins=getattr(settings, "CORS_ORIGINS", ["*"]),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# Routers
# =========================================================
app_.include_router(camera_router, prefix="/camera", tags=["Camera"])
app_.include_router(snapshot_router, prefix="/snapshot", tags=["Snapshot"])
app_.include_router(record_router, prefix="/record", tags=["Record"])

# =========================================================
# Health Check
# =========================================================
@app_.get("/", tags=["Health"])
async def health_check():
    return {
        "status": "online",
        "service": "Video Platform API",
        "version": "1.0.0",
    }


# 定时同步 ZLM → DB
async def sync_zlm_loop(interval_seconds: int = 60):
    while True:
        try:
            async with async_session() as db:
                zlm_list = await app_.state.zlm_service.get_media_list()
                await app_.state.camera_service.sync_with_zlm(db, zlm_list)
        except Exception:
            logger.exception("定时同步 ZLM 失败")
        await asyncio.sleep(interval_seconds)
