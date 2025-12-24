#!/usr/bin/env python3
"""
清理本地积压的摄像头切片。

ZLMediaKit 会先将 MP4 写入 settings.LOCAL_VIDEO_PATH，若上传/消费者停机，
该目录会不断膨胀。本脚本按文件的 mtime 清理超过阈值的文件，并可选清空空目录。
"""

import argparse
import os
import sys
import time
from pathlib import Path

from app.config import settings
from app.utils.logger import init_logging, get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="清理 settings.LOCAL_VIDEO_PATH 中过期的本地切片",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--max-age-hours",
        type=float,
        default=settings.CLEANUP_MAX_AGE_HOURS,
        help="保留文件的最大时间（小时），超过后将被删除（默认 24h）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅输出将删除的文件，不真正删除",
    )
    parser.add_argument(
        "--remove-empty-dirs",
        action="store_true",
        help="删除空目录（在文件删除后执行）",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=settings.LOG_LEVEL,
        help="日志级别",
    )
    return parser.parse_args()


def cleanup_files(root: Path, max_age_seconds: float, dry_run: bool) -> int:
    now = time.time()
    count = 0
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            mtime = path.stat().st_mtime
        except OSError as exc:
            logger.warning("无法获取文件时间 %s error=%s", path, exc)
            continue
        age = now - mtime
        if age < max_age_seconds:
            continue
        count += 1
        if dry_run:
            logger.info("[DryRun] 将删除 %s (%.1f h)", path, age / 3600)
            continue
        try:
            path.unlink()
            logger.info("已删除 %s (%.1f h)", path, age / 3600)
        except OSError as exc:
            logger.error("删除文件失败 %s error=%s", path, exc)
    return count


def cleanup_empty_dirs(root: Path, dry_run: bool) -> int:
    removed = 0
    for path in sorted(root.rglob("*"), reverse=True):
        if not path.is_dir():
            continue
        if any(path.iterdir()):
            continue
        removed += 1
        if dry_run:
            logger.info("[DryRun] 将删除空目录 %s", path)
            continue
        try:
            path.rmdir()
            logger.info("已删除空目录 %s", path)
        except OSError as exc:
            logger.warning("删除空目录失败 %s error=%s", path, exc)
    return removed


def main():
    args = parse_args()
    init_logging(args.log_level)

    root = Path(settings.LOCAL_VIDEO_PATH)
    if not root.exists():
        logger.error("目录不存在：%s", root)
        sys.exit(1)

    logger.info(
        "开始清理目录=%s max_age_hours=%.1f dry_run=%s",
        root,
        args.max_age_hours,
        args.dry_run,
    )

    deleted = cleanup_files(root, args.max_age_hours * 3600, args.dry_run)
    logger.info("过期文件处理完成 count=%d", deleted)

    if args.remove_empty_dirs:
        removed = cleanup_empty_dirs(root, args.dry_run)
        logger.info("空目录处理完成 count=%d", removed)


if __name__ == "__main__":
    main()
