# app/routers/record_api.py

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

router = APIRouter(prefix="/record", tags=["Record"])


def get_behavior_service(request: Request):
    svc = getattr(request.app.state, "behavior_service", None)
    if svc is None:
        raise HTTPException(status_code=500, detail="Behavior service not initialized")
    return svc


class BehaviorProcessRequest(BaseModel):
    video_path: str = Field(..., description="本地 mp4 文件的绝对路径")


class BehaviorProcessResponse(BaseModel):
    camera_id: str
    complete: bool
    reason: str
    final_video: Optional[str]
    pending_clips: List[str]
    actions: List[dict]


@router.post("/behavior/process", response_model=BehaviorProcessResponse)
async def process_behavior_video(
    payload: BehaviorProcessRequest,
    behavior_service=Depends(get_behavior_service),
):
    """
    手动触发一次行为识别流程。
    用于调试：提交一个切片路径，内部会自动拼接、重跑识别。
    """
    try:
        result = await behavior_service.process_clip(payload.video_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Behavior service failed: {exc}")

    return BehaviorProcessResponse(
        camera_id=result.get("camera_id", ""),
        complete=bool(result.get("complete")),
        reason=str(result.get("reason")),
        final_video=result.get("final_video"),
        pending_clips=result.get("pending_clips", []),
        actions=result.get("actions", []),
    )


@router.get("/behavior/sessions")
async def list_behavior_sessions(behavior_service=Depends(get_behavior_service)):
    """
    查询当前仍在等待拼接/识别的切片队列。
    """
    return behavior_service.dump_sessions()
