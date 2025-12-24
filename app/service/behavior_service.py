import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx
from collections import deque

from app.config import settings
from app.service.algorithm_registry import AlgorithmTask
from app.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ActionResult:
    """单个算法动作识别结果。"""
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
class SegmentResult:
    """一次切片识别返回，包含原始 MinIO 对象与动作列表。"""
    object_name: str
    actions: List[ActionResult]
    clip_time: Optional[str] = None
    created_at: float = field(default_factory=lambda: time.time())

    def to_summary(self) -> Dict[str, Any]:
        return {
            "object_name": self.object_name,
            "clip_time": self.clip_time,
            "created_at": self.created_at,
            "actions": [a.to_dict() for a in self.actions],
        }

    def to_dict(self) -> Dict[str, Any]:
        return self.to_summary()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SegmentResult":
        actions = [ActionResult.from_dict(item) for item in data.get("actions", [])]
        return cls(
            object_name=data.get("object_name") or "",
            actions=actions,
            clip_time=data.get("clip_time"),
            created_at=float(data.get("created_at", time.time())),
        )


@dataclass
class ClipSession:
    """摄像头级别的状态机，负责跟踪“提枪→挂枪”事件拼接。"""
    pending_segments: List[SegmentResult] = field(default_factory=list)
    start_action: Optional[ActionResult] = None
    start_abs_ts: Optional[float] = None
    last_raw_response: Optional[Dict[str, Any]] = None

    def reset(self):
        self.pending_segments.clear()
        self.start_action = None
        self.start_abs_ts = None

    def to_summary(self) -> Dict[str, Any]:
        return self.to_dict()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pending_segments": [seg.to_dict() for seg in self.pending_segments],
            "start_action": self.start_action.to_dict() if self.start_action else None,
            "start_abs_ts": self.start_abs_ts,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ClipSession":
        pending = [SegmentResult.from_dict(item) for item in data.get("pending_segments", [])]
        start_data = data.get("start_action")
        start_action = ActionResult.from_dict(start_data) if start_data else None
        return cls(
            pending_segments=pending,
            start_action=start_action,
            start_abs_ts=data.get("start_abs_ts"),
        )


class BehaviorService:
    """
    行为识别调度与拼接的核心服务：
    - 每段切片调用算法，按摄像头串行，避免乱序。
    - 依据提枪/挂枪标签拼接完整事件，支持异常分类。
    - 会话状态与异常历史写入 Redis，便于恢复与排查。
    """

    TARGET_LIFT_LABELS = {"提油枪"}
    TARGET_HANG_LABELS = {"挂油枪"}

    def __init__(self, redis_client=None):
        self.service_url = settings.BEHAVIOR_SERVICE_URL
        self.service_name = settings.BEHAVIOR_SERVICE_NAME
        self.max_segments = settings.BEHAVIOR_MAX_CONCAT_CLIPS
        self.request_retries = max(1, settings.BEHAVIOR_REQUEST_RETRIES)
        self.retry_backoff = max(0.5, settings.BEHAVIOR_REQUEST_RETRY_BACKOFF)
        self.abs_time_epsilon = 1e-3
        self.sessions: Dict[str, ClipSession] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._lift_labels = {self._normalize_label(label) for label in self.TARGET_LIFT_LABELS}
        self._hang_labels = {self._normalize_label(label) for label in self.TARGET_HANG_LABELS}
        self._known_labels = self._lift_labels | self._hang_labels
        self.redis_client = redis_client
        self._http_client = httpx.AsyncClient(timeout=settings.BEHAVIOR_REQUEST_TIMEOUT)
        self.session_key_prefix = settings.BEHAVIOR_SESSION_REDIS_KEY.rstrip(":")
        ttl_candidate = int(settings.BEHAVIOR_SESSION_TTL_SECONDS)
        self.session_ttl = ttl_candidate or settings.EXPIRE_DAY * 86400
        self.history_key_prefix = settings.BEHAVIOR_EVENT_FAILURE_QUEUE.rstrip(":")
        self.history_ttl = getattr(settings, "BEHAVIOR_FAILURE_TTL_SECONDS", 604800)
        self.enable_lift_only = bool(getattr(settings, "BEHAVIOR_ENABLE_LIFT_ONLY", True))
        self.enable_short_hang = bool(getattr(settings, "BEHAVIOR_ENABLE_SHORT_HANG", True))
        self.enable_hang_only = bool(getattr(settings, "BEHAVIOR_ENABLE_HANG_ONLY", True))
        self.max_wait_seconds = max(0, int(getattr(settings, "BEHAVIOR_MAX_WAIT_SECONDS", 0)))
        self.short_hang_threshold = max(
            0, int(getattr(settings, "BEHAVIOR_SHORT_HANG_THRESHOLD_SECONDS", 0))
        )
        self.hang_only_backtrack = max(1, int(getattr(settings, "BEHAVIOR_HANG_ONLY_BACKTRACK_SEGMENTS", 5)))
        self._recent_segments: Dict[str, deque] = {}

    def _get_lock(self, camera_id: str) -> asyncio.Lock:
        if camera_id not in self._locks:
            self._locks[camera_id] = asyncio.Lock()
        return self._locks[camera_id]

    def set_redis_client(self, redis_client):
        self.redis_client = redis_client

    @staticmethod
    def _normalize_label(label: Optional[str]) -> str:
        return (label or "").strip().lower()

    @staticmethod
    def _extract_camera_id(video_path: str) -> str:
        parts = os.path.normpath(video_path).split(os.sep)
        candidates = [p for p in parts if p.startswith("camera_")]
        return candidates[-1] if candidates else "unknown"

    async def process_clip(
        self,
        video_path: str,
        camera_id: Optional[str] = None,
        *,
        object_name: Optional[str] = None,
        clip_time: Optional[str] = None,
        service_name: Optional[str] = None,
        area2d_pt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        单段切片处理入口：
        - 调用算法服务拿到动作列表
        - 更新摄像头会话，判断是否形成完整/异常事件
        - 返回当前状态或完成事件摘要
        """
        if not os.path.exists(video_path):
            raise FileNotFoundError(f"Video not found: {video_path}")

        camera_id = camera_id or self._extract_camera_id(video_path)
        lock = self._get_lock(camera_id)
        recent = self._recent_segments.setdefault(
            camera_id, deque(maxlen=self.hang_only_backtrack)
        )

        async with lock:
            session = await self._get_session(camera_id)
            actions, raw_resp = await self._call_recognition(
                video_path,
                service_name or self.service_name,
                area2d_pt=area2d_pt,
            )
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "行为识别结果 camera=%s clip=%s actions=%s",
                    camera_id,
                    object_name or video_path,
                    [
                        {
                            "label": a.label_name,
                            "start": a.start_time,
                            "end": a.end_time,
                        }
                        for a in actions
                    ],
                )
            session.last_raw_response = raw_resp

            segment = SegmentResult(
                object_name=object_name or video_path,
                actions=actions,
                clip_time=clip_time,
            )
            recent.append(segment)

            complete, reason, event_segments, cleared_snapshot = self._update_session(
                camera_id, session, segment
            )
            pending = [seg.object_name for seg in session.pending_segments]

            result = {
                "camera_id": camera_id,
                "complete": complete,
                "reason": reason,
                "pending_clips": pending,
                "actions": [a.to_dict() for a in actions],
                "segments": [seg.to_summary() for seg in (event_segments or [])],
                "raw": raw_resp,
            }
            if complete:
                logger.info(
                    "行为识别：camera=%s 事件完成 reason=%s segments=%s",
                    camera_id,
                    reason,
                    result["segments"],
                )
            else:
                logger.info(
                    "行为识别：camera=%s session 中间状态 reason=%s pending=%s",
                    camera_id,
                    reason,
                    [seg.to_summary() for seg in session.pending_segments],
                )
            if cleared_snapshot:
                await self._record_session_history(camera_id, "cleared", reason, cleared_snapshot)
            await self._persist_session(camera_id, session)
            return result

    def _update_session(
        self,
        camera_id: str,
        session: ClipSession,
        segment: SegmentResult,
    ) -> Tuple[bool, str, Optional[List[SegmentResult]], Optional[List[SegmentResult]]]:
        """
        核心状态机：
        - 识别提枪/挂枪并更新会话
        - 分类返回 ok/short_hang/lift_only/hang_only 等事件
        - 保护段数/时间上限，必要时清空会话
        """
        segment_ts = self._clip_timestamp(segment.clip_time)
        actions_sorted = sorted(segment.actions, key=lambda a: a.start_time)
        lifts = [a for a in actions_sorted if self._normalize_label(a.label_name) in self._lift_labels]
        hangs = [a for a in actions_sorted if self._normalize_label(a.label_name) in self._hang_labels]

        if logger.isEnabledFor(logging.DEBUG):
            for action in actions_sorted:
                normalized = self._normalize_label(action.label_name)
                if normalized and normalized not in self._known_labels:
                    logger.debug("行为识别：收到未知动作标签 %s", action.label_name)

        event_segments: Optional[List[SegmentResult]] = None
        cleared_snapshot: Optional[List[SegmentResult]] = None
        reason = "waiting_lift" if session.start_action is None else "waiting_hang"

        def start_new_event(lift_action: ActionResult):
            session.start_action = lift_action
            session.start_abs_ts = segment_ts
            session.pending_segments = [segment]
            logger.debug(
                "行为识别：摄像头事件开始 lift=%s abs_ts=%s clip=%s",
                lift_action.start_time,
                session.start_abs_ts,
                segment.object_name,
            )

        def append_segment():
            if not session.pending_segments or session.pending_segments[-1] is not segment:
                session.pending_segments.append(segment)

        def _has_hang(seg: SegmentResult) -> bool:
            return any(self._normalize_label(a.label_name) in self._hang_labels for a in seg.actions)
        def _has_lift(seg: SegmentResult) -> bool:
            return any(self._normalize_label(a.label_name) in self._lift_labels for a in seg.actions)

        if session.start_action is None:
            if lifts:
                start_new_event(lifts[0])
                reason = "waiting_hang"
            elif hangs and self.enable_hang_only:
                history = self._recent_segments.get(camera_id) or deque(maxlen=self.hang_only_backtrack)
                # 仅当最近窗口内不存在提枪动作时，才认为是挂枪异常
                if len(history) >= self.hang_only_backtrack and not any(_has_lift(seg) for seg in history):
                    filtered = [seg for seg in history if _has_hang(seg)]
                    # 如果过滤后为空，至少包含当前段
                    event_segments = filtered if filtered else [segment]
                    reason = "hang_only"
        else:
            append_segment()
            logger.debug(
                "行为识别：继续累积切片 camera_start=%s seg=%s pending=%d",
                session.start_abs_ts,
                segment.object_name,
                len(session.pending_segments),
            )
            if lifts:
                start_new_event(lifts[-1])
                reason = "waiting_hang"

        def _calc_duration(h: ActionResult) -> Optional[float]:
            if session.start_abs_ts is not None and segment_ts is not None:
                return segment_ts - session.start_abs_ts
            return h.start_time - session.start_action.start_time if session.start_action else None

        if session.start_action and hangs:
            valid_hangs: List[ActionResult] = []
            for h in hangs:
                if session.start_abs_ts is not None and segment_ts is not None:
                    diff = segment_ts - session.start_abs_ts
                    if diff < -self.abs_time_epsilon:
                        continue
                    if abs(diff) <= self.abs_time_epsilon and h.start_time < session.start_action.start_time:
                        continue
                    valid_hangs.append(h)
                    continue
                if h.start_time < session.start_action.start_time:
                    continue
                valid_hangs.append(h)
            if valid_hangs:
                append_segment()
                segment_count = len(session.pending_segments)
                if self.max_segments and segment_count > self.max_segments:
                    reason = "max_segments_reached"
                    logger.warning(
                        "行为识别：事件拼接段数超限 count=%d limit=%d",
                        segment_count,
                        self.max_segments,
                    )
                    cleared_snapshot = list(session.pending_segments)
                    session.reset()
                else:
                    duration = _calc_duration(valid_hangs[0])
                    if (
                        self.short_hang_threshold
                        and duration is not None
                        and duration < self.short_hang_threshold
                        and self.enable_short_hang
                    ):
                        event_segments = list(session.pending_segments)
                        session.reset()
                        reason = "short_hang"
                        logger.info(
                            "行为识别：短时挂枪 duration=%.3f segs=%d",
                            duration,
                            len(event_segments),
                        )
                    else:
                        event_segments = list(session.pending_segments)
                        session.reset()
                        reason = "ok"
                        logger.info(
                            "行为识别：提挂完成 segments=%d objects=%s",
                            segment_count,
                            [seg.object_name for seg in event_segments],
                        )

        if session.start_action and not event_segments:
            # 提枪后等待挂枪的超时/超段数处理
            wait_expired = False
            if (
                self.max_wait_seconds
                and segment_ts is not None
                and session.start_abs_ts is not None
                and (segment_ts - session.start_abs_ts) > self.max_wait_seconds
            ):
                wait_expired = True
            if self.max_segments and len(session.pending_segments) > self.max_segments:
                wait_expired = True

            if wait_expired:
                if self.enable_lift_only:
                    event_segments = list(session.pending_segments)
                    reason = "lift_only"
                    logger.info(
                        "行为识别：提枪未挂枪超时 segments=%d objects=%s",
                        len(event_segments),
                        [seg.object_name for seg in event_segments],
                    )
                else:
                    cleared_snapshot = list(session.pending_segments)
                    reason = "max_segments_reached"
                session.reset()

        if not session.start_action and not event_segments:
            session.pending_segments.clear()

        # 事件结束或清空后清理回溯窗口，避免跨事件混入旧片段
        if event_segments or cleared_snapshot:
            self._recent_segments.pop(camera_id, None)

        if event_segments:
            logger.info(
                "行为识别：完整事件 camera_session=%s reason=%s segments=%s",
                session.start_abs_ts,
                reason,
                [seg.to_summary() for seg in event_segments],
            )
        return bool(event_segments), reason, event_segments, cleared_snapshot

    async def _call_recognition(
        self,
        video_path: str,
        service_name: str,
        area2d_pt: Optional[str] = None,
    ) -> Tuple[List[ActionResult], Dict[str, Any]]:
        payload = {
            "service_name": service_name,
            "params": {
                "video": [video_path],
            },
        }
        if area2d_pt:
            payload["params"]["area2D_pt"] = [area2d_pt]
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.request_retries + 1):
            try:
                resp = await self._http_client.post(self.service_url, json=payload)
                resp.raise_for_status()
                data = resp.json()
                actions = [ActionResult.from_dict(item) for item in data.get("data", [])]
                logger.debug(
                    "行为识别：请求成功 attempt=%s len(actions)=%d",
                    attempt,
                    len(actions),
                )
                return actions, data
            except (httpx.HTTPError, ValueError) as exc:
                last_exc = exc
                logger.warning(
                    "行为识别请求失败 attempt=%s/%s camera_video=%s reason=%s",
                    attempt,
                    self.request_retries,
                    video_path,
                    exc,
                )
                if attempt < self.request_retries:
                    await asyncio.sleep(self.retry_backoff * attempt)
        raise RuntimeError("behavior recognition request failed") from last_exc

    def dump_sessions(self) -> List[Dict[str, Any]]:
        snapshot: List[Dict[str, Any]] = []
        for camera_id, session in self.sessions.items():
            if not session.pending_segments and not session.start_action:
                continue
            snapshot.append(
                {
                    "camera_id": camera_id,
                    **session.to_summary(),
                }
            )
        return snapshot

    async def _get_session(self, camera_id: str) -> ClipSession:
        session = self.sessions.get(camera_id)
        if session is not None:
            return session
        session = await self._load_session(camera_id)
        self.sessions[camera_id] = session
        return session

    async def _load_session(self, camera_id: str) -> ClipSession:
        if not self.redis_client:
            return ClipSession()
        redis_key = self._session_key(camera_id)
        try:
            data = await self.redis_client.get(redis_key)
        except Exception:
            logger.exception("读取 Redis session 失败 camera=%s", camera_id)
            return ClipSession()
        if not data:
            return ClipSession()
        try:
            payload = json.loads(data)
            return ClipSession.from_dict(payload)
        except Exception:
            logger.exception("解析 Redis session 失败 camera=%s", camera_id)
            return ClipSession()

    async def _persist_session(self, camera_id: str, session: ClipSession):
        if not self.redis_client:
            return
        redis_key = self._session_key(camera_id)
        if not self._session_has_state(session):
            try:
                await self.redis_client.delete(redis_key)
                logger.debug("行为识别：清理会话状态 camera=%s redis_key=%s", camera_id, redis_key)
            except Exception:
                logger.exception("清理 Redis session 失败 camera=%s", camera_id)
            return

        payload = json.dumps(session.to_dict(), ensure_ascii=False)
        try:
            await self.redis_client.set(redis_key, payload, ex=self.session_ttl)
            logger.debug(
                "行为识别：写入会话状态 camera=%s redis_key=%s pending=%d start=%s",
                camera_id,
                redis_key,
                len(session.pending_segments),
                bool(session.start_action),
            )
        except Exception:
            logger.exception("写入 Redis session 失败 camera=%s", camera_id)

    @staticmethod
    def _session_has_state(session: ClipSession) -> bool:
        return bool(session.start_action or session.pending_segments)

    def _session_key(self, camera_id: str) -> str:
        return f"{self.session_key_prefix}:{camera_id}"

    @staticmethod
    def _clip_timestamp(value: Optional[str]) -> Optional[float]:
        """将切片命名中的时间（如 2025-12-09-17-10-06-413）解析为 epoch 秒。"""
        if not value:
            return None
        parts = value.split("-")
        try:
            if len(parts) >= 7:
                year, month, day, hour, minute, second = map(int, parts[:6])
                millis = int(parts[6])
                base = datetime(year, month, day, hour, minute, second)
                dt = base + timedelta(milliseconds=millis)
            else:
                dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            return dt.timestamp()
        except Exception:
            logger.debug("无法解析 clip_time：%s", value)
            return None

    async def _record_session_history(
        self,
        camera_id: str,
        status: str,
        reason: str,
        segments: List[SegmentResult],
    ):
        if not self.redis_client or not segments:
            return
        payload = {
            "camera_id": camera_id,
            "status": status,
            "reason": reason,
            "segments": [seg.to_summary() for seg in segments],
            "timestamp": time.time(),
        }
        data = json.dumps(payload, ensure_ascii=False)
        try:
            redis_key = self._history_key(status, camera_id=camera_id)
            await self.redis_client.lpush(redis_key, data)
            await self.redis_client.expire(redis_key, self.history_ttl)
            logger.info(
                "行为识别：记录事件历史 camera=%s status=%s redis_key=%s",
                camera_id,
                status,
                redis_key,
            )
        except Exception:
            logger.exception("记录事件历史失败 camera=%s", camera_id)

    def _history_key(self, status: str, camera_id: Optional[str] = None) -> str:
        prefix = self.history_key_prefix or settings.BEHAVIOR_EVENT_FAILURE_QUEUE
        if getattr(settings, "BEHAVIOR_HISTORY_PER_CAMERA", False) and camera_id:
            return f"{prefix}:{status}:{camera_id}"
        return f"{prefix}:{status}"

    async def handle(self, task: AlgorithmTask) -> Dict[str, Any]:
        """
        适配 AlgorithmRegistry：用统一 task 调用行为识别。
        """
        return await self.process_clip(
            task.video_path,
            camera_id=task.camera_id,
            object_name=task.object_name,
            clip_time=task.clip_time,
            service_name=task.service_name,
            area2d_pt=task.area2d_pt,
        )
