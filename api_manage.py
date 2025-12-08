from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
import httpx
import os
import uuid

app = FastAPI(title="ZLMediaKit 管理服务")

ZLM_URL = "http://127.0.0.1:8080"


# -----------------------------
# 添加流代理 addStreamProxy
# -----------------------------
@app.get("/add_stream_proxy")
async def add_stream_proxy(
    secret: str = Query(...),
    vhost: str = Query("__defaultVhost__"),
    app_: str = Query("live"),
    stream: str = Query("test"),
    url: str = Query(...),
    enable_mp4: bool = Query(False),
    mp4_save_path: str = Query("./save_mp4"),
    mp4_max_second: int = Query(300),
):
    api = f"{ZLM_URL}/index/api/addStreamProxy"

    params = {
        "secret": secret,
        "vhost": vhost,
        "app": app_,
        "stream": stream,
        "url": url,
        "enable_mp4": str(enable_mp4),
        "mp4_save_path": mp4_save_path,
        "mp4_max_second": mp4_max_second,
    }

    async with httpx.AsyncClient() as client:
        resp = await client.get(api, params=params)

    return JSONResponse(resp.json())


# -----------------------------
# 截图 getSnap
# -----------------------------
@app.get("/snap")
async def get_snap(
    secret: str = Query(...),
    url: str = Query(...),
    timeout_sec: int = Query(10),
    expire_sec: int = Query(1),
):
    api = f"{ZLM_URL}/index/api/getSnap"

    params = {
        "secret": secret,
        "url": url,
        "timeout_sec": timeout_sec,
        "expire_sec": expire_sec,
    }

    # 保存临时文件
    save_path = f"/tmp/snapshot_{uuid.uuid4().hex}.jpg"

    async with httpx.AsyncClient() as client:
        resp = await client.get(api, params=params)

    # ZLM 返回的是图片二进制
    with open(save_path, "wb") as f:
        f.write(resp.content)

    return FileResponse(save_path, media_type="image/jpeg")

# --------------------------------------------------------
# 2) 删除流代理 delStreamProxy
# --------------------------------------------------------
@app.get("/del_stream_proxy")
async def del_stream_proxy(
    secret: str = Query(...),
    key: str = Query(...),
):
    """
    key 示例：
        __defaultVhost__/proxy/0
    """
    api = f"{ZLM_URL}/index/api/delStreamProxy"

    params = {
        "secret": secret,
        "key": key,
    }

    async with httpx.AsyncClient() as client:
        resp = await client.get(api, params=params)

    return JSONResponse(resp.json())





