from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class AlgorithmTask:
    """
    统一传递给算法处理器的输入上下文。
    各处理器自行决定如何使用/忽略字段，返回结构可自由定义。
    """

    video_path: str
    camera_id: Optional[str] = None
    object_name: Optional[str] = None
    clip_time: Optional[str] = None
    service_name: Optional[str] = None
    algorithm_capability: Optional[str] = None
    area2d_pt: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None


class AlgorithmRegistry:
    """
    以摄像头的 algorithm_capability 为 key 注册/获取算法处理器。
    处理器需提供 `handle(task: AlgorithmTask)` 协程，返回任意结构。
    """

    def __init__(self):
        self._handlers: Dict[str, Any] = {}

    def register(self, capability: str, handler: Any):
        if not capability:
            return
        self._handlers[capability] = handler

    def get(self, capability: Optional[str], default: Optional[Any] = None) -> Optional[Any]:
        if capability and capability in self._handlers:
            return self._handlers[capability]
        return self._handlers.get("default") or default

    def available(self):
        return list(self._handlers.keys())
