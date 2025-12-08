# scripts/test_crud.py
import asyncio
from sqlalchemy import text
from app.database import async_session
from app.models.camera import Camera

async def test():
    async with async_session() as session:
        # 插入测试摄像头
        cam = Camera(name="测试摄像头", rtsp_url="rtsp://127.0.0.1/test")
        session.add(cam)
        await session.commit()
        print(f"插入摄像头 ID: {cam.id}")

        # 查询摄像头
        result = await session.execute(text("SELECT * FROM cameras"))
        cameras = result.fetchall()
        print("摄像头列表:", cameras)

asyncio.run(test())
