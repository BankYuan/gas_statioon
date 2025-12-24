import logging
import os
from pathlib import Path


def init_logging(level: str | None = None):
    """初始化全局 logging 配置。"""
    log_level = (level or os.environ.get("LOG_LEVEL") or "INFO").upper()

    numeric_level = getattr(logging, log_level, logging.INFO)

    root = logging.getLogger()
    root.setLevel(numeric_level)

    fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    formatter = logging.Formatter(fmt)

    # 控制台输出：仅在不存在 StreamHandler 时添加，避免重复
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root.addHandler(stream_handler)

    # 文件输出（可选，通过 LOG_FILE 环境变量指定），无论是否已有其他 handler 都尝试添加
    log_file = os.environ.get("LOG_FILE")
    if log_file and not any(isinstance(h, logging.FileHandler) for h in root.handlers):
        try:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)
        except Exception as exc:
            root.error("无法创建日志文件 %s: %s", log_file, exc)


def get_logger(name: str):
    return logging.getLogger(name)
