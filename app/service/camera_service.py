# app/services/camera_service.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete

from app.models.camera import Camera
from app.schemas.camera import CameraCreate

class CameraService:

    async def add_camera(self, db: AsyncSession, camera: CameraCreate):

        db_obj = Camera(
            name=camera.name,
            rtsp_url=camera.rtsp_url,
            resolution=camera.resolution,
            codec=camera.codec,
            fps=camera.fps,
            clip_duration=camera.clip_duration,
            camera_id=camera.camera_id,
            algorithm_capability=camera.algorithm_capability,
            area2d_pt=camera.area2d_pt,
            algorithm_enabled=camera.algorithm_enabled,

            # 默认信息
            app="live",
            stream="",
            stream_key="",
            is_pulled=False,
        )

        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)

        return db_obj

    async def list_cameras(self, db: AsyncSession):
        "获取摄像头信息"
        result = await db.execute(select(Camera))
        return result.scalars().all()

    async def get_camera(self, db: AsyncSession, camera_id: str) -> Camera | None:
        """获取摄像头详情"""
        result = await db.execute(select(Camera).where(Camera.camera_id == camera_id))
        return result.scalars().first()

    async def delete_camera(self, db: AsyncSession, camera_id: str) -> bool:
        """删除摄像头"""
        result = await db.execute(select(Camera).where(Camera.camera_id == camera_id))
        camera = result.scalars().first()
        if not camera:
            return False
        await db.delete(camera)
        await db.commit()
        return True

    async def sync_with_zlm(self, db: AsyncSession, zlm_list: list):
        """
        根据 ZLM 返回的 media 列表同步数据库：
        - 如果 ZLM 无流，则清空数据库
        - 如果 ZLM 多，新增
        - 如果 ZLM 少，删除
        """

        # ZLM 的 key 列表： ["app/stream", ...]
        zlm_keys = {f"{m['app']}/{m['stream']}" for m in zlm_list}
        # print(zlm_keys,"dddddd")

        # 查询 DB 所有摄像头
        res = await db.execute(select(Camera))
        db_cameras = res.scalars().all()

        # 数据库里现有的 key
        db_keys = {f"{cam.app}/{cam.stream}" for cam in db_cameras}

        # ---------- 1. ZLM 无流 → 清空数据库 ----------
        if not zlm_keys:
            # 安全策略：不自动清空，以免误删。返回提示由上层决策。
            return {"action": "noop", "reason": "ZLM has no streams"}

        # ---------- 2. 新增缺少的摄像头 ----------
        missing = zlm_keys - db_keys

        for key in missing:
            app, stream = key.split("/")  # app=live, stream=camera_5101...

            # --- 从 ZLM media_list 中找到对应这条 stream 的记录 ---
            media = next((m for m in zlm_list if m["app"] == app and m["stream"] == stream), None)
            if not media:
                continue

            origin_rtsp = media.get("originUrl") or ""  # ZLM 返回原始 RTSP
            # 若已有同 camera_id 的记录，做更新而非重复插入
            existing = next((c for c in db_cameras if c.camera_id == stream.replace("camera_", "")), None)
            if existing:
                existing.rtsp_url = origin_rtsp or existing.rtsp_url
                db.add(existing)
                continue

            # 从 stream 提取真正的 camera_id
            # stream 格式是 camera_51018500451327700007
            if stream.startswith("camera_"):
                camera_id = stream.replace("camera_", "")
            else:
                camera_id = stream  # 兜底

            new_camera = Camera(
                name=f"摄像头 {camera_id}",
                camera_id=camera_id,
                rtsp_url=origin_rtsp,
                resolution="HD",
                codec="H265",
                fps="25",
                algorithm_capability="unknown",
                area2d_pt=None,
                algorithm_enabled=True,
                is_pulled=True,
                proxy_url="",  # 可为空
                app=app,
                stream=stream,
                stream_key="__defaultVhost__/live/"+f"{stream}",
                clip_duration="10"
            )

            db.add(new_camera)

        # ---------- 3. 删除多余的摄像头 ----------
        extra = db_keys - zlm_keys
        if extra:
            # 批量删除多余的摄像头
            app_stream_pairs = [tuple(key.split("/")) for key in extra]
            for app, stream in app_stream_pairs:
                await db.execute(
                    delete(Camera)
                    .where(Camera.app == app)
                    .where(Camera.stream == stream)
                )

        await db.commit()

        return {
            "added": list(missing),
            "removed": list(extra)
        }

    async def disable_algorithm(self, db: AsyncSession, camera_id: str) -> bool:
        """停用摄像头算法服务"""
        result = await db.execute(select(Camera).where(Camera.camera_id == camera_id))
        camera = result.scalars().first()
        if not camera:
            return False
        camera.algorithm_enabled = False
        db.add(camera)
        await db.commit()
        await db.refresh(camera)
        return True


camera_service = CameraService()
