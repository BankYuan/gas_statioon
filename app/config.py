# app/config.py
from pydantic_settings import BaseSettings # NEW

class Settings(BaseSettings):
    # ZLMediaKit
    ZLM_API_BASE: str = "http://127.0.0.1:8080/index/api"
    ZLM_SECRET: str = "J2Gbvg3593Wj3lMDYmXG4xZLlUni16FM"
    MP4_SAVE_PATH: str = "./save_data/mp4"
    MP4_MAX_SECOND: str = "60"
    SNAPSHOT_SAVE_PATH: str = "./save_data/snapshot"

    # MinIO
    MINIO_ENDPOINT: str = "127.0.0.1:19000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET: str = "camera-video"
    LOCAL_VIDEO_PATH: str = "/mnt/data1/Gas_station_video/mp4"
    EXPIRE_DAY: int = 1   # 259200 秒
    # 并发限制
    UPLOAD_CONCURRENCY: int = 4
    # 日志级别
    LOG_LEVEL: str = "INFO"

    # DB
    DATABASE_URL: str = "postgresql+asyncpg://video_user:123456@127.0.0.1:15432/video_db"

    # 行为识别
    BEHAVIOR_SERVICE_URL: str = "http://127.0.0.1:2005/service"
    BEHAVIOR_SERVICE_NAME: str = "cdfuelsight"
    BEHAVIOR_CACHE_DIR: str = "./save_data/behavior_cache"
    BEHAVIOR_MAX_CONCAT_CLIPS: int = 5
    BEHAVIOR_MAX_DURATION_SECONDS: int = 900

    # Redis 任务队列
    REDIS_URL: str = "redis://127.0.0.1:6378/0"
    REDIS_QUEUE_NAME: str = "video_tasks"
    REDIS_POP_TIMEOUT: int = 5

    class Config:
        env_file = ".env"

settings = Settings()
