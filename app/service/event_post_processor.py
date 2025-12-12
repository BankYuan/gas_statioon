import asyncio
import json
import os
import shutil
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.config import settings
from app.database import async_session
from app.models.behavior_event import BehaviorEvent
from app.utils.logger import get_logger

logger = get_logger(__name__)


class EventPostProcessor:
    def __init__(self, minio_service, redis_client=None):
        self.minio_service = minio_service
        self.redis_client = redis_client
        self.max_attempts = max(1, settings.BEHAVIOR_EVENT_MAX_RETRIES)
        self.retry_backoff = max(0.5, settings.BEHAVIOR_EVENT_RETRY_BACKOFF)
        self.download_retries = max(1, settings.BEHAVIOR_EVENT_DOWNLOAD_RETRIES)

    async def process_event(self, event_result: Dict[str, Any]):
        segments = event_result.get("segments") or []
        if not segments:
            return
        camera_id = event_result.get("camera_id", "unknown")
        start_time = segments[0].get("clip_time")
        end_time = segments[-1].get("clip_time")
        logger.info(
            "事件后处理：准备拼接 camera=%s segments=%d start=%s end=%s",
            camera_id,
            len(segments),
            start_time,
            end_time,
        )

        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                await self._process_once(camera_id, start_time, end_time, segments)
                return
            except Exception as exc:
                last_exc = exc
                logger.exception(
                    "事件后处理失败 attempt=%s/%s camera=%s error=%s",
                    attempt,
                    self.max_attempts,
                    camera_id,
                    exc,
                )
                if attempt < self.max_attempts:
                    await asyncio.sleep(self.retry_backoff * attempt)
        await self._record_failure(event_result, last_exc)

    async def _process_once(
        self,
        camera_id: str,
        start_time: Optional[str],
        end_time: Optional[str],
        segments: List[Dict[str, Any]],
    ):
        tmp_dir = tempfile.mkdtemp(prefix="behavior-event-")
        try:
            local_files = await self._download_segments(segments, tmp_dir)
            if not local_files:
                raise RuntimeError("no local segments downloaded")
            logger.debug("事件后处理：完成片段下载 files=%s", local_files)
            output_path = os.path.join(tmp_dir, f"{uuid.uuid4().hex}.mp4")
            await self._concat_videos(local_files, output_path)
            logger.debug("事件后处理：拼接输出 %s", output_path)
            video_object, video_url = await self._upload_event_video(camera_id, output_path)
            await self._save_event_record(
                camera_id=camera_id,
                start_clip_time=start_time,
                end_clip_time=end_time,
                video_object=video_object,
                video_url=video_url,
                segments=segments,
            )
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    async def _download_segments(self, segments: List[Dict[str, Any]], tmp_dir: str) -> List[str]:
        local_paths: List[str] = []
        for idx, seg in enumerate(segments):
            object_name = seg.get("object_name")
            if not object_name:
                raise ValueError("segment missing object_name")
            local_path = os.path.join(tmp_dir, f"{idx}_{Path(object_name).name}")
            await self._download_with_retry(object_name, local_path)
            local_paths.append(local_path)
        return local_paths

    async def _download_with_retry(self, object_name: str, local_path: str):
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.download_retries + 1):
            try:
                await asyncio.to_thread(
                    self.minio_service.client.fget_object,
                    settings.MINIO_BUCKET,
                    object_name,
                    local_path,
                )
                return
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "下载事件片段失败 attempt=%s/%s object=%s error=%s",
                    attempt,
                    self.download_retries,
                    object_name,
                    exc,
                )
                if attempt < self.download_retries:
                    await asyncio.sleep(self.retry_backoff)
        raise RuntimeError(f"download segment failed: {object_name}") from last_exc

    async def _concat_videos(self, files: List[str], output_path: str):
        if len(files) == 1:
            shutil.copy2(files[0], output_path)
            return

        list_file = f"{output_path}.txt"
        list_content = "".join(f"file '{Path(path).resolve()}'\n" for path in files)
        Path(list_file).write_text(list_content, encoding="utf-8")

        cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            list_file,
            "-c",
            "copy",
            output_path,
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.error(
                "事件拼接失败 code=%s stdout=%s stderr=%s",
                proc.returncode,
                stdout.decode(errors="ignore"),
                stderr.decode(errors="ignore"),
            )
            raise RuntimeError("ffmpeg concat failed")
        try:
            os.remove(list_file)
        except OSError:
            pass

    async def _upload_event_video(self, camera_id: str, file_path: str):
        object_name = f"{settings.BEHAVIOR_EVENT_PREFIX}/{camera_id}/{int(time.time())}.mp4"
        await asyncio.to_thread(
            self.minio_service.upload_file,
            settings.BEHAVIOR_EVENT_BUCKET,
            object_name,
            file_path,
        )
        expires = settings.BEHAVIOR_EVENT_URL_EXPIRE
        if isinstance(expires, (int, float)):
            from datetime import timedelta

            expires = timedelta(seconds=int(expires))

        presigned_url = await asyncio.to_thread(
            self.minio_service.client.presigned_get_object,
            settings.BEHAVIOR_EVENT_BUCKET,
            object_name,
            expires=expires,
        )
        return object_name, presigned_url

    async def _save_event_record(
        self,
        *,
        camera_id: str,
        start_clip_time: str,
        end_clip_time: str,
        video_object: str,
        video_url: str,
        segments: List[Dict[str, Any]],
    ):
        async with async_session() as session:
            session.add(
                BehaviorEvent(
                    camera_id=camera_id,
                    start_clip_time=start_clip_time,
                    end_clip_time=end_clip_time,
                    video_object=video_object,
                    video_url=video_url,
                    segments=segments,
                )
            )
            await session.commit()

        logger.info(
            "事件已写入数据库 camera=%s video=%s start=%s end=%s",
            camera_id,
            video_object,
            start_clip_time,
            end_clip_time,
        )

    async def _record_failure(self, event_result: Dict[str, Any], exc: Optional[Exception]):
        payload = {
            "error": str(exc) if exc else "unknown",
            "event": event_result,
            "timestamp": time.time(),
        }
        data = json.dumps(payload, ensure_ascii=False, default=str)
        if not self.redis_client:
            logger.error("事件后处理失败，未写入失败队列：%s", data)
            return
        try:
            await self.redis_client.lpush(settings.BEHAVIOR_EVENT_FAILURE_QUEUE, data)
            logger.error(
                "事件后处理失败，已写入失败队列=%s camera=%s",
                settings.BEHAVIOR_EVENT_FAILURE_QUEUE,
                event_result.get("camera_id"),
            )
        except Exception:
            logger.exception("写入事件失败队列失败 payload=%s", data)
