# scripts/test_db.py
import asyncio
from app.database import engine
from app.models.camera import Base

async def init():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("数据库表初始化完成！")

asyncio.run(init())
