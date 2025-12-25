import asyncio
import json
import os
import shutil
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

import httpx

from app.config import settings
from app.database import async_session
from app.models.behavior_event import BehaviorEvent
from app.utils.logger import get_logger

logger = get_logger(__name__)

# 标签归一化，用于匹配提枪/挂枪动作
_LIFT_LABELS = {"提油枪"}
_HANG_LABELS = {"挂油枪"}
_LIFT_LABELS_NORM = {lbl.strip().lower() for lbl in _LIFT_LABELS}
_HANG_LABELS_NORM = {lbl.strip().lower() for lbl in _HANG_LABELS}


@dataclass
class ProcessResult:
    """事件后处理结果封装，便于统一传递。"""
    video_object: str
    video_url: str
    frames: Dict[str, Optional[str]]

class EventPostProcessor:
    def __init__(self, minio_service, redis_client=None):
        self.minio_service = minio_service
        self.redis_client = redis_client
        self.max_attempts = max(1, settings.BEHAVIOR_EVENT_MAX_RETRIES)
        self.retry_backoff = max(0.5, settings.BEHAVIOR_EVENT_RETRY_BACKOFF)
        self.download_retries = max(1, settings.BEHAVIOR_EVENT_DOWNLOAD_RETRIES)
        self.history_key_prefix = settings.BEHAVIOR_EVENT_FAILURE_QUEUE.rstrip(":")
        self.history_ttl = getattr(settings, "BEHAVIOR_FAILURE_TTL_SECONDS", 604800)

    async def process_event(self, event_result: Dict[str, Any]):
        """
        事件后处理入口：
        - 下载切片并拼接成完整视频
        - 截取首尾帧（提挂帧或异常帧），上传到帧桶
        - 生成回调/历史所需的 URL 字段
        """
        segments = event_result.get("segments") or []
        if not segments:
            return
        camera_id = event_result.get("camera_id", "unknown")
        start_time = segments[0].get("clip_time")
        end_time = segments[-1].get("clip_time")
        reason = event_result.get("reason", "ok")
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
                result = await self._process_once(
                    camera_id, start_time, end_time, segments, reason
                )
                await self._record_success(
                    camera_id=camera_id,
                    reason=reason,
                    result=result,
                )
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
        reason: str,
    ) -> ProcessResult:
        """
        单次后处理：下载→拼接→截帧→上传视频与帧。
        """
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
            segment_files = list(zip(segments, local_files))
            frames = await self._extract_and_upload_frames(
                camera_id,
                segment_files,
                reason=reason,
                tmp_dir=tmp_dir,
            )
            await self._save_event_record(
                camera_id=camera_id,
                start_clip_time=start_time,
                end_clip_time=end_time,
                video_object=video_object,
                video_url=video_url,
                segments=segments,
            )
            return ProcessResult(video_object=video_object, video_url=video_url, frames=frames)
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
        """ffmpeg concat 包装，失败按退避重试。"""
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                await self._concat_videos_once(files, output_path)
                return
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "事件拼接失败 attempt=%s/%s error=%s",
                    attempt,
                    self.max_attempts,
                    exc,
                )
                if attempt < self.max_attempts:
                    await asyncio.sleep(self.retry_backoff * attempt)
        raise RuntimeError(f"ffmpeg concat failed after retries: {output_path}") from last_exc

    async def _concat_videos_once(self, files: List[str], output_path: str):
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
            "-loglevel",
            "error",
            output_path,
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            logger.warning("ffmpeg concat 超时，被强制终止 output=%s", output_path)
            raise RuntimeError("ffmpeg concat timeout")
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

    async def _extract_and_upload_frames(self, camera_id: str, segment_files: List[Tuple[Dict[str, Any], str]], reason: str, tmp_dir: str):
        """
        按动作时间截帧：
        - 提枪帧：提枪动作的 start/end_time
        - 挂枪帧：挂枪动作的 start/end_time
        - 若缺失对应动作则对应帧留空字符串
        """
        if not segment_files:
            return {
                "start_lift_frame_url": "",
                "end_lift_frame_url": "",
                "start_hang_frame_url": "",
                "end_hang_frame_url": "",
            }

        times = self._collect_action_times(segment_files)
        lift_frames = await self._extract_lift_frames(camera_id, times, tmp_dir)
        hang_frames = await self._extract_hang_frames(camera_id, times, reason, tmp_dir)
        return {**lift_frames, **hang_frames}

    @classmethod
    def _collect_action_times(cls, segment_files: List[Tuple[Dict[str, Any], str]]) -> Dict[str, Optional[Tuple[str, float]]]:
        """提取提/挂枪动作对应的 (文件, 时间戳) 元组。"""
        lift_start = lift_end = hang_start = hang_end = None
        for segment, file_path in segment_files:
            actions = segment.get("actions") or []
            for act in actions:
                label = cls._normalize_label(act.get("label_name"))
                if label in _LIFT_LABELS_NORM:
                    if lift_start is None:
                        lift_start = (file_path, float(act.get("start_time", 0)))
                    lift_end = (file_path, float(act.get("end_time", 0)))
                if label in _HANG_LABELS_NORM:
                    if hang_start is None:
                        hang_start = (file_path, float(act.get("start_time", 0)))
                    hang_end = (file_path, float(act.get("end_time", 0)))
        return {
            "lift_start": lift_start,
            "lift_end": lift_end,
            "hang_start": hang_start,
            "hang_end": hang_end,
        }

    async def _extract_lift_frames(self, camera_id: str, times: Dict[str, Optional[Tuple[str, float]]], tmp_dir: str) -> Dict[str, str]:
        """截取提枪开始/结束帧。"""
        urls = {"start_lift_frame_url": "", "end_lift_frame_url": ""}
        if times.get("lift_start"):
            urls["start_lift_frame_url"] = await self._extract_frame_with_retry(
                camera_id, times["lift_start"], "lift_start", tmp_dir
            )
        if times.get("lift_end"):
            urls["end_lift_frame_url"] = await self._extract_frame_with_retry(
                camera_id, times["lift_end"], "lift_end", tmp_dir
            )
        return urls

    async def _extract_hang_frames(self, camera_id: str, times: Dict[str, Optional[Tuple[str, float]]], reason: str, tmp_dir: str) -> Dict[str, str]:
        """截取挂枪开始/结束帧（lift_only 场景跳过）。"""
        urls = {"start_hang_frame_url": "", "end_hang_frame_url": ""}
        if reason == "lift_only":
            return urls
        if times.get("hang_start"):
            urls["start_hang_frame_url"] = await self._extract_frame_with_retry(
                camera_id, times["hang_start"], "hang_start", tmp_dir
            )
        if times.get("hang_end"):
            urls["end_hang_frame_url"] = await self._extract_frame_with_retry(
                camera_id, times["hang_end"], "hang_end", tmp_dir
            )
        return urls

    async def _extract_frame_with_retry(
        self,
        camera_id: str,
        frame_info: Tuple[str, float],
        kind: str,
        tmp_dir: str,
    ) -> str:
        """为单个帧创建独立临时文件并带重试截帧+上传。"""
        src, seconds = frame_info
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_attempts + 1):
            with tempfile.NamedTemporaryFile(prefix=f"{kind}-", suffix=".jpg", dir=tmp_dir, delete=False) as tmp_file:
                tmp_path = Path(tmp_file.name)
            try:
                await self._extract_frame_at(src, tmp_path, seconds)
                return await self._upload_frame(camera_id, tmp_path, kind)
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "截帧失败 attempt=%s/%s kind=%s src=%s seconds=%s error=%s",
                    attempt,
                    self.max_attempts,
                    kind,
                    src,
                    seconds,
                    exc,
                )
                if attempt < self.max_attempts:
                    await asyncio.sleep(self.retry_backoff * attempt)
            finally:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
        raise RuntimeError(f"extract frame failed after retries kind={kind} src={src}") from last_exc

    async def _extract_frame(self, src: str, dst: Path, last: bool = False):
        # 兼容原接口，保留但不再使用 last 分支
        await self._extract_frame_at(src, dst, 0 if not last else None)

    async def _extract_frame_at(self, src: str, dst: Path, seconds: Optional[float]):
        # 精确到时间点取帧，若 seconds 为 None 则取末尾 0.5 秒
        args = ["ffmpeg", "-y"]
        if seconds is None:
            args += ["-sseof", "-0.5"]
        else:
            args += ["-ss", str(max(0, seconds))]
        args += ["-i", src, "-vframes", "1", "-q:v", "2", str(dst)]
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            logger.warning("ffmpeg 截帧超时，被强制终止 src=%s seconds=%s", src, seconds)
            raise RuntimeError("extract frame timeout")
        if proc.returncode != 0:
            logger.warning(
                "截帧失败 src=%s seconds=%s code=%s stderr=%s",
                src,
                seconds,
                proc.returncode,
                stderr.decode(errors="ignore"),
            )
            raise RuntimeError("extract frame failed")

    async def _upload_frame(self, camera_id: str, file_path: Path, kind: str):
        object_name = f"frames/{camera_id}/{int(time.time())}_{kind}.jpg"
        await asyncio.to_thread(
            self.minio_service.upload_file,
            settings.BEHAVIOR_EVENT_FRAME_BUCKET,
            object_name,
            str(file_path),
            content_type="image/jpeg",
        )
        expires = settings.BEHAVIOR_EVENT_FRAME_URL_EXPIRE
        if isinstance(expires, (int, float)):
            from datetime import timedelta

            expires = timedelta(seconds=int(expires))

        presigned_url = await asyncio.to_thread(
            self.minio_service.client.presigned_get_object,
            settings.BEHAVIOR_EVENT_FRAME_BUCKET,
            object_name,
            expires=expires,
        )
        return presigned_url

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
        status = "postprocess_failed"
        payload = {
            "error": str(exc) if exc else "unknown",
            "event": event_result,
            "timestamp": time.time(),
            "status": status,
        }
        data = json.dumps(payload, ensure_ascii=False, default=str)
        if not self.redis_client:
            logger.error("事件后处理失败，未写入失败队列：%s", data)
            return
        try:
            redis_key = self._history_key(status, camera_id=event_result.get("camera_id"))
            await self.redis_client.lpush(redis_key, data)
            await self.redis_client.expire(redis_key, self.history_ttl)
            logger.error(
                "事件后处理失败，已写入失败队列=%s camera=%s",
                redis_key,
                event_result.get("camera_id"),
            )
        except Exception:
            logger.exception("写入事件失败队列失败 payload=%s", data)

    async def _record_success(
        self,
        *,
        camera_id: str,
        reason: str,
        result: ProcessResult,
    ):
        await self._record_history(
            camera_id,
            "complete",
            reason,
            result.video_url,
            result.frames,
        )
        notify_ok = await self._notify_complete_event(
            camera_id,
            reason,
            result.video_url,
            result.frames,
        )
        if not notify_ok:
            logger.warning(
                "事件通知仍失败 camera=%s video=%s，已记录 notify_failed",
                camera_id,
                result.video_object,
            )

    async def _record_history(
        self,
        camera_id: str,
        status: str,
        reason: str,
        video_url: str,
        frames: Dict[str, Optional[str]],
    ):
        if not self.redis_client:
            return
        payload = {
            "camera_id": camera_id,
            "status": status,
            "reason": reason,
            "video_url": video_url,
            "start_lift_frame_url": frames.get("start_lift_frame_url") or "",
            "end_lift_frame_url": frames.get("end_lift_frame_url") or "",
            "start_hang_frame_url": frames.get("start_hang_frame_url") or "",
            "end_hang_frame_url": frames.get("end_hang_frame_url") or "",
            "timestamp": time.time(),
        }
        data = json.dumps(payload, ensure_ascii=False, default=str)
        try:
            redis_key = self._history_key(status, camera_id=camera_id)
            await self.redis_client.lpush(redis_key, data)
            await self.redis_client.expire(redis_key, self.history_ttl)
            logger.info("事件历史已记录 camera=%s status=%s redis_key=%s", camera_id, status, redis_key)
        except Exception:
            logger.exception("写入事件历史失败 camera=%s", camera_id)

    async def _notify_complete_event(
        self,
        camera_id: str,
        reason: str,
        video_url: str,
        frames: Dict[str, Optional[str]],
    ) -> bool:
        notify_url = settings.BEHAVIOR_EVENT_NOTIFY_URL
        if not notify_url:
            return True
        payload = {
            "camera_id": camera_id,
            "reason": reason,
            "video_url": video_url,
            "start_lift_frame_url": frames.get("start_lift_frame_url") or "",
            "end_lift_frame_url": frames.get("end_lift_frame_url") or "",
            "start_hang_frame_url": frames.get("start_hang_frame_url") or "",
            "end_hang_frame_url": frames.get("end_hang_frame_url") or "",
        }
        last_exc: Optional[Exception] = None
        for attempt in range(1, settings.BEHAVIOR_EVENT_MAX_RETRIES + 1):
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.post(notify_url, json=payload)
                    resp.raise_for_status()
                logger.info("事件通知已发送 camera=%s attempt=%s status=%s", camera_id, attempt, resp.status_code)
                return True
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "事件通知发送失败 camera=%s attempt=%s/%s error=%s",
                    camera_id,
                    attempt,
                    settings.BEHAVIOR_EVENT_MAX_RETRIES,
                    exc,
                )
                await asyncio.sleep(self.retry_backoff * attempt)
        await self._record_history(
            camera_id,
            "notify_failed",
            reason,
            video_url,
            frames,
        )
        if last_exc:
            logger.error("事件通知彻底失败 camera=%s error=%s", camera_id, last_exc)
        return False

    def _history_key(self, status: str, camera_id: Optional[str] = None) -> str:
        prefix = self.history_key_prefix or settings.BEHAVIOR_EVENT_FAILURE_QUEUE
        if getattr(settings, "BEHAVIOR_HISTORY_PER_CAMERA", False) and camera_id:
            return f"{prefix}:{status}:{camera_id}"
        return f"{prefix}:{status}"

    @staticmethod
    def _normalize_label(label: Optional[str]) -> str:
        return (label or "").strip().lower()
