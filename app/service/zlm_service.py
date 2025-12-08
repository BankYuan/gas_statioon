# app/services/zlm_service.py

import httpx
from app.config import settings
import os
from datetime import datetime
from app.utils.logger import get_logger

logger = get_logger(__name__)

class ZLMService:
    """
    ZLM API 调用封装：
    - addStreamProxy：拉取 RTSP → ZLM → 转成多种协议
    - getSnap：截图
    - delStreamProxy：删除拉流代理
    """

    def __init__(self):
        self.base_url = settings.ZLM_API_BASE   # http://127.0.0.1:8080
        self.secret = settings.ZLM_SECRET       # API 密钥

    # -------------------------------
    # 1. 拉流代理 addStreamProxy
    # -------------------------------
    async def add_stream_proxy(self, vhost, app, stream, url, enable_mp4=True):
        output_path = os.path.join(settings.MP4_SAVE_PATH, stream)
        os.makedirs(output_path, exist_ok=True)

        params = {
            "secret": self.secret,
            "vhost": vhost,
            "app": app,
            "stream": stream,
            "url": url,
            "enable_mp4": str(enable_mp4).lower(),
            "mp4_save_path": output_path,
            "mp4_max_second": settings.MP4_MAX_SECOND,
        }

        api = f"{self.base_url}/addStreamProxy"
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(api, params=params)
            # print(resp.status_code)
            # print(resp.text)  # 打印原始响应
            # print(resp.headers)
            return resp.json()

    # -------------------------------
    # 2. 截图 getSnap
    # -------------------------------
    async def get_snapshot(self, rtsp_url, output_path: str):
        params = {
            "secret": self.secret,
            "url": rtsp_url,
            "timeout_sec": 10,
            "expire_sec": 1,
        }

        api = f"{self.base_url}/getSnap"

        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(api, params=params)

            if response.status_code != 200:
                raise RuntimeError(f"截图失败: {response.text}")

            # 保存到本地
            with open(output_path, "wb") as f:
                f.write(response.content)

        return output_path

    # -----------------------
    # 3.查询流是否在线
    # -----------------------
    async def is_stream_online(self, app: str, stream: str):
        api = f"{self.base_url}/getMediaList"
        params = {
            "secret": self.secret,
            "vhost": "__defaultVhost__",
            "app": app,
            "stream": stream,
        }

        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(api, params=params)
            r.raise_for_status()
            data = r.json()

        streams = data.get("data", [])
        return len(streams) > 0

    # -------------------------------
    # 4. 删除流 delStreamProxy
    # -------------------------------
    async def delete_stream(self, key: str):
        """
        删除 ZLM 拉流
        key: addStreamProxy 返回的 key
        """
        params = {
            "secret": self.secret,
            "key": key,
        }

        api = f"{self.base_url}/delStreamProxy"

        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(api, params=params)
            # 避免 JSONDecodeError
            try:
                return resp.json()
            except Exception:
                # 打印调试信息
                logger.error("ZLM delete_stream resp: %s %s", resp.status_code, resp.text)
                return {"code": -1, "msg": f"invalid json: {resp.text}"}

    # -----------------------
    # 截图（上传 MinIO）
    # -----------------------
    # snapshot_to_minio 已移除，使用 get_snapshot + MinioService 在调用端完成上传。
    async def get_media_list(self):
        url = f"{settings.ZLM_API_BASE}/getMediaList"

        params = {
            "secret": settings.ZLM_SECRET
        }

        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()

        data = r.json()
        # print(data)
        return data.get("data", [])



# 全局实例
zlm_service = ZLMService()
