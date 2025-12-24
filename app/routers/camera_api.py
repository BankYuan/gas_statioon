# app/routers/camera_api.py
from typing import List
from fastapi import APIRouter, Depends, HTTPException, Request, Header
from sqlalchemy.ext.asyncio import AsyncSession
from app.config import settings
import os
from datetime import datetime
from app.database import get_db
from app.schemas.camera import CameraCreate, CameraResponse
from app.models.camera import Camera

router = APIRouter(prefix="/camera", tags=["Camera"])


def verify_api_token(x_api_token: str = Header(None)):
    required = settings.API_TOKEN
    if required and x_api_token != required:
        raise HTTPException(status_code=401, detail="Invalid API token")


def get_minio_service(request: Request):
    return request.app.state.minio_service


def get_zlm_service(request: Request):
    return request.app.state.zlm_service


def get_camera_service(request: Request):
    return request.app.state.camera_service


@router.post("/add", response_model=CameraResponse, dependencies=[Depends(verify_api_token)])
async def add_camera(data: CameraCreate, db: AsyncSession = Depends(get_db), camera_service=Depends(get_camera_service)):
    # print(data)
    camera = await camera_service.add_camera(db, data)
    return camera



@router.get("/list", response_model=List[CameraResponse])
async def list_cameras(db: AsyncSession = Depends(get_db), camera_service=Depends(get_camera_service)):
    cameras = await camera_service.list_cameras(db)
    return cameras


@router.get("/{camera_id}", response_model=CameraResponse)
async def get_camera(camera_id: str, db: AsyncSession = Depends(get_db), camera_service=Depends(get_camera_service)):
    """获取摄像头详情"""
    camera = await camera_service.get_camera(db, camera_id)
    if not camera:
        raise HTTPException(404, "Camera not found")
    return camera


@router.delete("/{camera_id}")
async def delete_camera(camera_id: str, db: AsyncSession = Depends(get_db), camera_service=Depends(get_camera_service)):
    """删除摄像头"""
    ok = await camera_service.delete_camera(db, camera_id)
    if not ok:
        raise HTTPException(404, "Camera not found")
    return {"message": "Camera deleted"}


# -----------------------------
#      ZLM 绑定业务部分
# -----------------------------

@router.post("/{camera_id}/start", dependencies=[Depends(verify_api_token)])
async def start_stream(camera_id: str, db: AsyncSession = Depends(get_db), camera_service=Depends(get_camera_service), zlm_service=Depends(get_zlm_service)):
    """
    为摄像头开启 ZLM 拉流
    """
    # 1. 获取摄像头信息
    camera = await camera_service.get_camera(db, camera_id)
    if not camera:
        raise HTTPException(status_code=404, detail="Camera not found")

    # 2. 调用 ZLM 拉流
    stream_name = f"camera_{camera_id}"  # 可自定义规则
    result = await zlm_service.add_stream_proxy(
        vhost="__defaultVhost__",
        app="live",
        stream=stream_name,
        url=camera.rtsp_url,
        enable_mp4=True,
    )

    # 3. 判断拉流是否成功
    if result.get("code") != 0:
        raise HTTPException(500, detail=f"Start stream failed: {result}")

    stream_key = result.get("data", {}).get("key", "")

    # 4. 更新数据库字段
    camera.stream = stream_name
    camera.stream_key = stream_key
    camera.app = "live"
    camera.is_pulled = True

    # settings.ZLM_API_BASE 通常含 /index/api，构造播放地址时剔除 api 路径
    root = settings.ZLM_API_BASE.split('/index')[0]
    camera.proxy_url = f"{root}/live/{stream_name}/index.m3u8"

    db.add(camera)
    await db.commit()
    await db.refresh(camera)

    return {"message": "Stream started", "camera": {
        "id": camera.id,
        "stream": camera.stream,
        "is_pulled": camera.is_pulled
    }}


@router.post("/{camera_id}/stop", dependencies=[Depends(verify_api_token)])
async def stop_stream(camera_id: str, db: AsyncSession = Depends(get_db), camera_service=Depends(get_camera_service), zlm_service=Depends(get_zlm_service)):
    # 查询摄像头
    camera = await camera_service.get_camera(db, camera_id)
    if not camera:
        raise HTTPException(404, "Camera not found")

    # 必须用 stream_key
    if not camera.stream_key:
        raise HTTPException(400, "Camera stream_key is empty, cannot stop stream")

    result = await zlm_service.delete_stream(camera.stream_key)

    if result.get("code") != 0:
        raise HTTPException(500, f"Stop stream failed: {result}")

    # 更新数据库
    camera.is_pulled = False
    camera.stream = ""
    camera.stream_key = ""
    camera.proxy_url = ""

    db.add(camera)
    await db.commit()
    await db.refresh(camera)

    return {"message": "Stream stopped", "camera": {
        "id": camera.id,
        "is_pulled": camera.is_pulled,
        "stream": camera.stream,
        "stream_key": camera.stream_key,
        "proxy_url": camera.proxy_url
    }}

@router.get("/{camera_id}/snapshot")
async def snapshot_camera(camera_id: str, db: AsyncSession = Depends(get_db), zlm_service=Depends(get_zlm_service), camera_service=Depends(get_camera_service)):
    """
    为摄像头获取截图
    """
    # 1. 查询摄像头信息
    camera = await camera_service.get_camera(db, camera_id)
    if not camera:
        raise HTTPException(404, "Camera not found")

    # 2. 构造本地保存路径
    filename = f"{camera_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}.jpg"
    output_path = os.path.join(settings.SNAPSHOT_SAVE_PATH, filename)
    os.makedirs(settings.SNAPSHOT_SAVE_PATH, exist_ok=True)

    try:
        saved_path = await zlm_service.get_snapshot(camera.rtsp_url, output_path)
    except Exception as e:
        raise HTTPException(500, f"Snapshot failed: {e}")

    return {"message": "Snapshot success", "file_path": saved_path, "file_name": filename}

@router.get("/{camera_id}/status")
async def get_stream_status(camera_id: str, db: AsyncSession = Depends(get_db), camera_service=Depends(get_camera_service), zlm_service=Depends(get_zlm_service)):
    """
    查询摄像头流是否在线
    """
    # 1. 查询摄像头信息
    camera = await camera_service.get_camera(db, camera_id)
    if not camera:
        raise HTTPException(404, "Camera not found")

    # 2. 获取 app 和 stream
    app = camera.app or "live"
    stream = camera.stream
    if not stream:
        raise HTTPException(400, "Camera stream is empty, not started yet")

    # 3. 调用 service 层检查流是否在线（使用注入的 zlm_service）
    try:
        online = await zlm_service.is_stream_online(app, stream)
    except Exception as e:
        raise HTTPException(500, f"Check stream status failed: {e}")

    return {"camera_id": camera_id, "app": app, "stream": stream, "online": online}

@router.post("/sync")
async def sync_camera(db: AsyncSession = Depends(get_db), zlm_service=Depends(get_zlm_service), camera_service=Depends(get_camera_service)):
    zlm_list = await zlm_service.get_media_list()
    result = await camera_service.sync_with_zlm(db, zlm_list)
    return result


@router.post("/{camera_id}/disable", dependencies=[Depends(verify_api_token)])
async def disable_algorithm(camera_id: str, db: AsyncSession = Depends(get_db), camera_service=Depends(get_camera_service)):
    """停用该摄像头的算法服务"""
    ok = await camera_service.disable_algorithm(db, camera_id)
    if not ok:
        raise HTTPException(404, "Camera not found")
    return {"message": "Algorithm disabled", "camera_id": camera_id}



