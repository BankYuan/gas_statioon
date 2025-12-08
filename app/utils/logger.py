import logging
import os


def init_logging(level: str | None = None):
    """初始化全局 logging 配置。"""
    log_level = (level or os.environ.get("LOG_LEVEL") or "INFO").upper()

    numeric_level = getattr(logging, log_level, logging.INFO)

    root = logging.getLogger()
    root.setLevel(numeric_level)

    if not root.handlers:
        handler = logging.StreamHandler()
        fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        root.addHandler(handler)


def get_logger(name: str):
    return logging.getLogger(name)
