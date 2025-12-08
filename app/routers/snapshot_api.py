# app/routers/snapshot_api.py

from fastapi import APIRouter, HTTPException, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.config import settings
import os
from datetime import datetime

router = APIRouter(prefix="/snapshot", tags=["Snapshot"])


def get_minio_service(request: Request):
    return request.app.state.minio_service


def get_zlm_service(request: Request):
    return request.app.state.zlm_service


def get_camera_service(request: Request):
    return request.app.state.camera_service


@router.get("/{camera_id}")
async def snapshot_camera(
    camera_id: str,
    db: AsyncSession = Depends(get_db),
    request: Request = None,
    minio_service=Depends(get_minio_service),
    zlm_service=Depends(get_zlm_service),
    camera_service=Depends(get_camera_service),
):
    """
    截取摄像头图像 → 上传 MinIO → 返回预签名 URL
    """
    camera = await camera_service.get_camera(db, camera_id)
    if not camera:
        raise HTTPException(404, "Camera not found")

    # 保存本地快照
    filename = f"{camera_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}.jpg"
    output_path = os.path.join(settings.SNAPSHOT_SAVE_PATH, filename)
    os.makedirs(settings.SNAPSHOT_SAVE_PATH, exist_ok=True)

    try:
        # 调用 async 的 zlm snapshot
        await zlm_service.get_snapshot(camera.rtsp_url, output_path)
    except Exception as e:
        raise HTTPException(500, f"Snapshot failed: {e}")

    # 上传到 MinIO（同步客户端，放到线程池）
    object_name = f"snapshot/{filename}"
    loop = __import__("asyncio").get_running_loop()
    try:
        await loop.run_in_executor(
            None,
            lambda: minio_service.upload_file(
                bucket=settings.MINIO_BUCKET,
                object_name=object_name,
                file_path=output_path,
            ),
        )

        # 生成预签名 URL
        presigned = await loop.run_in_executor(
            None,
            lambda: minio_service.get_presigned_url(settings.MINIO_BUCKET, object_name, expires=settings.EXPIRE_SECONDS),
        )

    finally:
        # 清理本地文件
        if os.path.exists(output_path):
            await loop.run_in_executor(None, os.remove, output_path)

    return {"camera_id": camera_id, "snapshot_url": presigned}
