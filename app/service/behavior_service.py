import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.config import settings
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
    管理“行为识别”流程：
    1. 每段切片单独调用算法
    2. 根据动作数据拼接提枪→挂枪事件
    3. 超过最大段数仍无结果时自动重置
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
        self.history_key = settings.BEHAVIOR_EVENT_FAILURE_QUEUE
        self.history_ttl = getattr(settings, "BEHAVIOR_FAILURE_TTL_SECONDS", 604800)

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
    ) -> Dict[str, Any]:
        if not os.path.exists(video_path):
            raise FileNotFoundError(f"Video not found: {video_path}")

        camera_id = camera_id or self._extract_camera_id(video_path)
        lock = self._get_lock(camera_id)

        async with lock:
            session = await self._get_session(camera_id)
            actions, raw_resp = await self._call_recognition(
                video_path,
                service_name or self.service_name,
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

            complete, reason, event_segments, cleared_snapshot = self._update_session(session, segment)
            pending = [seg.object_name for seg in session.pending_segments]

            result = {
                "camera_id": camera_id,
                "complete": complete,
                "reason": reason,
                "final_video": None,
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
            if complete and event_segments:
                await self._record_event_history(camera_id, "complete", reason, event_segments)
                await self._notify_complete_event(result)
            elif cleared_snapshot:
                await self._record_event_history(camera_id, "cleared", reason, cleared_snapshot)
            await self._persist_session(camera_id, session)
            return result

    def _update_session(
        self,
        session: ClipSession,
        segment: SegmentResult,
    ) -> Tuple[bool, str, Optional[List[SegmentResult]], Optional[List[SegmentResult]]]:
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

        if session.start_action is None:
            if lifts:
                start_new_event(lifts[0])
                reason = "waiting_hang"
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
                    event_segments = list(session.pending_segments)
                    session.reset()
                    reason = "ok"
                    logger.info(
                        "行为识别：提挂完成 segments=%d objects=%s",
                        segment_count,
                        [seg.object_name for seg in event_segments],
                    )

        if (
            session.start_action
            and self.max_segments
            and len(session.pending_segments) > self.max_segments
        ):
            reason = "max_segments_reached"
            logger.warning(
                "行为识别：切片累计超过上限 camera_ts=%s pending=%d limit=%d",
                session.start_abs_ts,
                len(session.pending_segments),
                self.max_segments,
            )
            cleared_snapshot = list(session.pending_segments)
            session.reset()

        if not session.start_action and not event_segments:
            session.pending_segments.clear()

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
    ) -> Tuple[List[ActionResult], Dict[str, Any]]:
        payload = {
            "service_name": service_name,
            "params": {
                "video": [video_path],
            },
        }
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

    async def _record_event_history(
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
            await self.redis_client.lpush(self.history_key, data)
            await self.redis_client.expire(self.history_key, self.history_ttl)
        except Exception:
            logger.exception("记录事件历史失败 camera=%s", camera_id)

    async def _notify_complete_event(self, event_payload: Dict[str, Any]):
        """将完整事件推送到外部系统（如未配置 URL 则直接返回）。"""
        if not settings.BEHAVIOR_EVENT_NOTIFY_URL:
            return
        payload = {
            "camera_id": event_payload.get("camera_id"),
            "reason": event_payload.get("reason"),
            "segments": event_payload.get("segments"),
            "pending_clips": event_payload.get("pending_clips"),
            "raw": event_payload.get("raw"),
        }
        try:
            logger.info(
                "行为识别：模拟推送事件到 %s payload=%s",
                settings.BEHAVIOR_EVENT_NOTIFY_URL,
                payload,
            )
            # TODO: 实际对接时可启用 HTTP 请求
            # async with httpx.AsyncClient(timeout=10) as client:
            #     await client.post(settings.BEHAVIOR_EVENT_NOTIFY_URL, json=payload)
        except Exception:
            logger.exception("行为识别：推送事件失败 camera=%s", payload.get("camera_id"))
