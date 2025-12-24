# app/config.py
from typing import Optional

from pydantic_settings import BaseSettings  # Pydantic Settings 统一配置入口

class Settings(BaseSettings):
    LICENSE_PATH: str = "./license.json"
    LICENSE_CURRENT_IMAGE_ID: str = ""  # 当前运行镜像标识（通过 .env 注入；为空则仅依赖 license 内的 image_id）
    LICENSE_EXPECTED_CONTAINER_ID: str = ""  # 可选：预期容器 ID（如无需校验可留空）
    CLEANUP_MAX_AGE_HOURS: int = 24  # 本地切片清理阈值（小时）
    API_TOKEN: str = "d017965da5c68ae134d7f113b68e6353"

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
    LOG_LEVEL: str = "INFO"  # 默认日志级别

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
    BEHAVIOR_EVENT_FRAME_BUCKET: str = "camera-event-frames"  # 开始/结束帧存放桶
    BEHAVIOR_EVENT_FRAME_URL_EXPIRE: int = 86400  # 帧图预签名 URL 有效期（秒）
    # 事件分类开关与阈值（默认切片 60s 场景）
    BEHAVIOR_ENABLE_LIFT_ONLY: bool = True       # 提枪未挂枪是否输出事件
    BEHAVIOR_ENABLE_SHORT_HANG: bool = True      # 短时挂枪是否输出事件
    BEHAVIOR_ENABLE_HANG_ONLY: bool = True       # 未提枪直接挂枪是否输出事件
    BEHAVIOR_MAX_WAIT_SECONDS: int = 180         # 提枪后最长等待挂枪时长（秒），超时视为异常中断
    BEHAVIOR_SHORT_HANG_THRESHOLD_SECONDS: int = 5  # 提挂总时长低于此阈值视为误操作（短时挂枪）
    BEHAVIOR_HANG_ONLY_BACKTRACK_SEGMENTS: int = 5  # 挂枪异常时回溯的前置切片数（含当前最多 5 段）
    BEHAVIOR_HISTORY_PER_CAMERA: bool = True     # 历史/失败记录按摄像头分 key，便于大规模场景隔离
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
    REDIS_QUEUE_NAME: str = "video_tasks"  # 默认队列名前缀，按摄像头拆分时将附加 camera_id
    REDIS_POP_TIMEOUT: int = 5

    class Config:
        env_file = ".env"

settings = Settings()
