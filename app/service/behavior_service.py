import asyncio
import logging
import os
import shutil
import time
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ActionResult:
    nozzle_no: Optional[int]
    license_plate_number: Optional[str]
    start_time: float
    end_time: float
    label_id: int
    label_name: str
    classify_score: float
    iou_score: float

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ActionResult":
        return cls(
            nozzle_no=data.get("nozzle_no"),
            license_plate_number=data.get("license_plate_number"),
            start_time=float(data.get("start_time", 0)),
            end_time=float(data.get("end_time", 0)),
            label_id=int(data.get("label_id", 0)),
            label_name=data.get("label_name") or data.get("Label_name") or "",
            classify_score=float(data.get("classify_score", 0)),
            iou_score=float(data.get("iou_score", 0)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ClipEntry:
    """Single clip cached for行为识别."""

    original_path: str
    cached_path: str
    created_at: float = field(default_factory=lambda: time.time())


@dataclass
class ClipSession:
    clips: List[ClipEntry] = field(default_factory=list)
    concat_path: Optional[str] = None
    last_actions: List[ActionResult] = field(default_factory=list)
    last_raw_response: Optional[Dict[str, Any]] = None

    def to_summary(self) -> Dict[str, Any]:
        return {
            "clips": [clip.cached_path for clip in self.clips],
            "concat_path": self.concat_path,
            "last_actions": [a.to_dict() for a in self.last_actions],
        }


class BehaviorService:
    """
    管理“行为识别”完整流程：
    1. 缓存每分钟切片
    2. 调用算法服务做识别
    3. 根据结果判断是否需要拼接，再次识别
    """

    TARGET_LIFT_LABELS = {"提油枪"}
    TARGET_HANG_LABELS = {"挂油枪"}

    def __init__(self):
        self.service_url = settings.BEHAVIOR_SERVICE_URL
        self.service_name = settings.BEHAVIOR_SERVICE_NAME
        # 使用绝对路径，避免 ffmpeg concat 相对路径拼接错误
        self.cache_dir = Path(settings.BEHAVIOR_CACHE_DIR).resolve()
        self.max_clips = settings.BEHAVIOR_MAX_CONCAT_CLIPS
        self.duration_limit = settings.BEHAVIOR_MAX_DURATION_SECONDS
        self.sessions: Dict[str, ClipSession] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._lift_labels = {self._normalize_label(label) for label in self.TARGET_LIFT_LABELS}
        self._hang_labels = {self._normalize_label(label) for label in self.TARGET_HANG_LABELS}
        self._known_labels = self._lift_labels | self._hang_labels
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_lock(self, camera_id: str) -> asyncio.Lock:
        if camera_id not in self._locks:
            self._locks[camera_id] = asyncio.Lock()
        return self._locks[camera_id]

    @staticmethod
    def _normalize_label(label: Optional[str]) -> str:
        return (label or "").strip().lower()

    @staticmethod
    def _extract_camera_id(video_path: str) -> str:
        parts = Path(video_path).parts
        print("6666666666", parts)
        candidates = [p for p in parts if p.startswith("camera_")]
        if not candidates:
            return "unknown"
        return candidates[-1]

    async def process_clip(self, video_path: str, camera_id: Optional[str] = None) -> Dict[str, Any]:
        """
        引擎入口：传入一个切片，内部决定是否拼接并重新识别。
        返回结果包含识别状态、拼接文件等信息。
        """
        if not os.path.exists(video_path):
            raise FileNotFoundError(f"Video not found: {video_path}")

        camera_id = camera_id or self._extract_camera_id(video_path)
        lock = self._get_lock(camera_id)

        async with lock:
            session = self.sessions.setdefault(camera_id, ClipSession())
            cached_path = await self._cache_clip(camera_id, video_path)
            entry = ClipEntry(original_path=video_path, cached_path=cached_path)
            session.clips.append(entry)
            logger.info("行为识别：摄像头 %s 新切片 %s", camera_id, cached_path)

            combined_path = await self._ensure_combined_video(camera_id, session)
            actions, raw_resp = await self._call_recognition(combined_path)
            session.last_actions = actions
            session.last_raw_response = raw_resp

            complete, reason = self._is_complete(actions)
            logger.info(
                "行为识别：摄像头 %s 状态 complete=%s reason=%s clips=%d",
                camera_id,
                complete,
                reason,
                len(session.clips),
            )

            result = {
                "camera_id": camera_id,
                "complete": complete,
                "reason": reason,
                "final_video": combined_path if complete else None,
                "pending_clips": [clip.cached_path for clip in session.clips],
                "actions": [a.to_dict() for a in actions],
                "raw": raw_resp,
            }

            if complete:
                await self._finalize_session(camera_id, session, keep_path=combined_path)
            else:
                session.concat_path = combined_path if len(session.clips) > 1 else None
                await self._apply_clip_limit(camera_id, session)

            return result

    async def _apply_clip_limit(self, camera_id: str, session: ClipSession):
        if len(session.clips) <= self.max_clips:
            return

        # 防止无限堆叠，丢弃最旧的一个切片
        dropped = session.clips.pop(0)
        try:
            os.remove(dropped.cached_path)
        except OSError:
            pass
        logger.warning(
            "行为识别：摄像头 %s 待拼接切片超限，丢弃 %s，当前保留 %d 个",
            camera_id,
            dropped.cached_path,
            len(session.clips),
        )

    async def _finalize_session(self, camera_id: str, session: ClipSession, keep_path: str):
        """
        识别完成后清理缓存，保留最终拼接的视频文件。
        """
        for clip in session.clips:
            if clip.cached_path == keep_path:
                continue
            try:
                os.remove(clip.cached_path)
            except OSError:
                pass

        session.clips.clear()
        session.concat_path = None
        session.last_actions = []
        session.last_raw_response = None
        # 用新的 session 接收下一次事件
        self.sessions[camera_id] = ClipSession()

    async def _ensure_combined_video(self, camera_id: str, session: ClipSession) -> str:
        if len(session.clips) == 1:
            return session.clips[0].cached_path

        cached_paths = [clip.cached_path for clip in session.clips]
        return await self._concat_videos(camera_id, cached_paths)

    async def _cache_clip(self, camera_id: str, video_path: str) -> str:
        dst_dir = self.cache_dir / camera_id
        dst_dir.mkdir(parents=True, exist_ok=True)
        filename = os.path.basename(video_path)
        unique_name = f"{int(time.time())}_{uuid.uuid4().hex}_{filename}"
        dst_path = (dst_dir / unique_name).resolve()

        def _copy():
            shutil.copy2(video_path, dst_path)

        await asyncio.to_thread(_copy)
        return str(dst_path)

    async def _concat_videos(self, camera_id: str, clip_paths: List[str]) -> str:
        concat_dir = (self.cache_dir / camera_id / "concat").resolve()
        concat_dir.mkdir(parents=True, exist_ok=True)
        output_name = f"{int(time.time())}_{uuid.uuid4().hex}.mp4"
        output_path = concat_dir / output_name

        list_file = concat_dir / f"{output_name}.txt"
        # ffmpeg concat 需要绝对路径，避免相对路径被重复拼接
        file_content = "".join(f"file '{Path(path).resolve()}'\n" for path in clip_paths)

        def _write_list():
            with open(list_file, "w", encoding="utf-8") as fp:
                fp.write(file_content)

        await asyncio.to_thread(_write_list)

        cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(list_file),
            "-c",
            "copy",
            str(output_path),
        ]

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            if process.returncode != 0:
                logger.error(
                    "ffmpeg 拼接失败 code=%s stdout=%s stderr=%s",
                    process.returncode,
                    stdout.decode(errors="ignore"),
                    stderr.decode(errors="ignore"),
                )
                raise RuntimeError(f"ffmpeg concat failed: {stderr.decode(errors='ignore')}")
        finally:
            try:
                os.remove(list_file)
            except OSError:
                pass

        return str(output_path)

    async def _call_recognition(self, video_path: str) -> Tuple[List[ActionResult], Dict[str, Any]]:
        payload = {
            "service_name": self.service_name,
            "params": {
                "video": [video_path],
            },
        }
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(self.service_url, json=payload)
            resp.raise_for_status()
            data = resp.json()

        actions = [ActionResult.from_dict(item) for item in data.get("data", [])]
        return actions, data

    def _is_complete(self, actions: List[ActionResult]) -> Tuple[bool, str]:
        if not actions:
            return False, "empty"

        lifts = [
            a for a in actions
            if self._normalize_label(a.label_name) in self._lift_labels
        ]
        hangs = [
            a for a in actions
            if self._normalize_label(a.label_name) in self._hang_labels
        ]

        if logger.isEnabledFor(logging.DEBUG):
            for action in actions:
                normalized = self._normalize_label(action.label_name)
                if normalized and normalized not in self._known_labels:
                    logger.debug("行为识别：收到未知动作标签 %s", action.label_name)

        if not lifts and not hangs:
            return False, "no_target_action"
        if not lifts:
            return False, "missing_lift"
        if not hangs:
            return False, "missing_hang"

        # 取最早提枪、最早挂枪判断顺序
        lifts_sorted = sorted(lifts, key=lambda a: a.start_time)
        hangs_sorted = sorted(hangs, key=lambda a: a.start_time)

        last_lift_end = max(a.end_time for a in lifts_sorted)
        first_hang_start = hangs_sorted[0].start_time
        if last_lift_end > first_hang_start:
            return False, "order_error"

        total_duration = max(a.end_time for a in hangs_sorted) - lifts_sorted[0].start_time
        if self.duration_limit and total_duration > self.duration_limit:
            return False, "duration_exceeded"

        return True, "ok"

    def dump_sessions(self) -> List[Dict[str, Any]]:
        """提供当前所有摄像头的缓存状态，便于 API 查询。"""
        snapshot: List[Dict[str, Any]] = []
        for camera_id, session in self.sessions.items():
            if not session.clips and not session.last_actions:
                continue
            snapshot.append(
                {
                    "camera_id": camera_id,
                    **session.to_summary(),
                }
            )
        return snapshot
