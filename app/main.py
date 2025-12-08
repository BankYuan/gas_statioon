import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers.camera_api import router as camera_router
from app.routers.snapshot_api import router as snapshot_router
from app.routers.record_api import router as record_router
from app.video_watcher import start_watching, start_consumer
from app.service.minio_service import MinioService
from app.service.zlm_service import ZLMService
from app.service.camera_service import CameraService
from app.service.behavior_service import BehaviorService
from app.service.redis_queue_service import RedisQueueService
from app.utils.logger import init_logging, get_logger
from app.config import settings as _settings

app_ = FastAPI(title="Video Platform Service", version="1.0.0")

# CORS 设置（开发环境用）
app_.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app_.include_router(camera_router, prefix="/camera", tags=["Camera"])
app_.include_router(snapshot_router, prefix="/snapshot", tags=["Snapshot"])
app_.include_router(record_router, prefix="/record", tags=["Record"])

@app_.get("/")
async def root():
    return {"message": "Video Platform API is running!"}


@app_.on_event("startup")
async def startup_event():
    loop = asyncio.get_running_loop()
    # 初始化日志（尽早）
    init_logging(_settings.LOG_LEVEL)
    logger = get_logger(__name__)

    # 创建并初始化单例服务（MinIO、ZLM、CameraService）
    app_.state.minio_service = MinioService()
    # MinIO init 可能会进行网络 I/O，放到线程池执行以避免阻塞事件循环
    await loop.run_in_executor(None, app_.state.minio_service.init)

    app_.state.zlm_service = ZLMService()
    app_.state.camera_service = CameraService()
    app_.state.behavior_service = BehaviorService()
    app_.state.redis_service = RedisQueueService()
    await app_.state.redis_service.init()

    logger.info("Services initialized, starting file watcher")

    # 启动视频目录监听（生产：上传到 MinIO 并入队 MinIO 信息）
    observer = await start_watching(
        loop,
        app_.state.minio_service,
        app_.state.redis_service,
    )
    app_.state.observer = observer
    app_.state.consumer_task = None  # 消费需要时再启动


@app_.on_event("shutdown")
async def shutdown_event():
    logger = get_logger(__name__)
    observer = getattr(app_.state, "observer", None)
    consumer_task = getattr(app_.state, "consumer_task", None)
    redis_service = getattr(app_.state, "redis_service", None)

    logger.info("🛑 停止 Watchdog 监听...")
    if observer:
        observer.stop()
        observer.join()   # 等待线程退出
        logger.info("Watchdog 已关闭")

    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            logger.info("Redis 任务消费者已取消")

    if redis_service:
        await redis_service.close()

