# app/schemas/camera.py
from pydantic import BaseModel
from typing import Optional


class CameraBase(BaseModel):
    name: str
    rtsp_url: str
    location: Optional[str] = None


class CameraCreate(BaseModel):
    name: str
    rtsp_url: str

    resolution: Optional[str] = "HD"
    codec: Optional[str] = "H264"
    fps: Optional[str] = "25fps"
    clip_duration: Optional[str] = "60"

    # 可选，如果你希望前端提交，否则后端自动生成
    app: Optional[str] = "live"
    stream: Optional[str] = None
    camera_id: Optional[str] = None

class CameraOut(BaseModel):
    id: int
    name: str
    rtsp_url: str
    stream_key: str

    class Config:
        orm_mode = True

class CameraResponse(BaseModel):
    id: int
    camera_id: str
    name: str
    rtsp_url: str
    resolution: str | None
    codec: str | None
    fps: str | None
    is_pulled: bool
    clip_duration: str | None
    proxy_url: str | None
    app: str
    stream: str | None
    stream_key: str | None

    class Config:
        orm_mode = True  # ⚠ 关键，允许从 SQLAlchemy ORM 转换
