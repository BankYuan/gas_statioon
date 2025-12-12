# app/config.py
from typing import Optional

from pydantic_settings import BaseSettings  # Pydantic Settings 统一配置入口

class Settings(BaseSettings):
    # ==== ZLMediaKit（拉流/切片） ====
    ZLM_API_BASE: str = "http://127.0.0.1:8080/index/api"
    ZLM_SECRET: str = "J2Gbvg3593Wj3lMDYmXG4xZLlUni16FM"
    MP4_SAVE_PATH: str = "./save_data/mp4"
    MP4_MAX_SECOND: str = "60"
    SNAPSHOT_SAVE_PATH: str = "./save_data/snapshot"

    # ==== MinIO 上传 + Watchdog ====
    MINIO_ENDPOINT: str = "127.0.0.1:19000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET: str = "camera-video"
    LOCAL_VIDEO_PATH: str = "/mnt/data1/Gas_station_video/mp4"
    EXPIRE_DAY: int = 1   # 259200 秒
    UPLOAD_CONCURRENCY: int = 4  # Watchdog 上传/消费者并发
    LOG_LEVEL: str = "DEBUG"  # 默认日志级别

    # ==== 数据库 ====
    DATABASE_URL: str = "postgresql+asyncpg://video_user:123456@127.0.0.1:15432/video_db"

    # ==== 行为识别 + 事件后处理 ====
    BEHAVIOR_SERVICE_URL: str = "http://127.0.0.1:2005/service"  # 算法服务 HTTP 地址
    BEHAVIOR_SERVICE_NAME: str = "cdfuelsight"  # 请求体中的 service_name
    BEHAVIOR_CACHE_DIR: str = "./save_data/behavior_cache"  # 历史兼容目录
    BEHAVIOR_MAX_CONCAT_CLIPS: int = 20  # 单次事件允许累积的最大切片数（默认上限 20 段）
    BEHAVIOR_MAX_DURATION_SECONDS: int = 0  # 若为 0 则不按秒数限制，完全由切片数量控制
    BEHAVIOR_REQUEST_TIMEOUT: int = 120  # 调用算法接口的超时时间
    BEHAVIOR_REQUEST_RETRIES: int = 3  # 调用算法接口失败时重试次数
    BEHAVIOR_REQUEST_RETRY_BACKOFF: float = 1.5  # 重试退避基数（seconds）
    BEHAVIOR_EVENT_BUCKET: str = "camera-event-video"  # 拼接结果存放的 MinIO 桶
    BEHAVIOR_EVENT_PREFIX: str = "events"  # 事件视频对象前缀
    BEHAVIOR_EVENT_URL_EXPIRE: int = 86400  # 事件视频预签名 URL 的有效期（秒）
    BEHAVIOR_EVENT_MAX_RETRIES: int = 3  # 事件后处理（拼接/上传/入库）重试次数
    BEHAVIOR_EVENT_RETRY_BACKOFF: float = 2.0  # 后处理失败后退避基数
    BEHAVIOR_EVENT_DOWNLOAD_RETRIES: int = 3  # 下载单个切片的重试次数
    BEHAVIOR_EVENT_FAILURE_QUEUE: str = "behavior:event_failures"  # 失败事件落表队列
    BEHAVIOR_FAILURE_TTL_SECONDS: int = 604800  # 失败/历史事件在 Redis 中的保留时长（7 天）
    BEHAVIOR_EVENT_NOTIFY_URL: Optional[str] = None  # 完整事件回调 URL（可为空，表示不上报）
    BEHAVIOR_SESSION_REDIS_KEY: str = "behavior:sessions"  # Redis 中保存 session 的键前缀
    BEHAVIOR_SESSION_TTL_SECONDS: int = 3600  # Session 在 Redis 中的 TTL

    # ==== Redis 任务队列 ====
    REDIS_URL: str = "redis://127.0.0.1:6378/0"
    REDIS_QUEUE_NAME: str = "video_tasks"
    REDIS_POP_TIMEOUT: int = 5

    class Config:
        env_file = ".env"

settings = Settings()
