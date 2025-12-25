import os
from typing import Any, Dict, List, Optional

import httpx

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class ZLMAPIError(RuntimeError):
    """ZLM API 调用失败。"""


class ZLMService:
    """
    ZLM API 调用封装：
    - addStreamProxy：拉取 RTSP → ZLM → 转成多种协议
    - getSnap：截图
    - delStreamProxy：删除拉流代理
    """

    def __init__(self, http_client: Optional[httpx.AsyncClient] = None):
        self.base_url = settings.ZLM_API_BASE   # http://127.0.0.1:8080
        self.secret = settings.ZLM_SECRET       # API 密钥
        self._client = http_client or httpx.AsyncClient(timeout=20, base_url=self.base_url)
        self._owns_client = http_client is None
        self._closed = False

    async def _request(
        self,
        path: str,
        params: Dict[str, Any],
        *,
        timeout: Optional[int] = None,
        expect_json: bool = True,
        method: str = "GET",
    ) -> Any:
        """统一请求封装，返回 JSON dict 或二进制，失败抛 ZLMAPIError。"""
        client_params = {"params": params}
        if timeout is not None:
            client_params["timeout"] = timeout
        logger.debug("ZLM request path=%s params=%s", path, params)
        resp = await self._client.request(method, path, **client_params)
        try:
            resp.raise_for_status()
            if expect_json:
                data = resp.json()
                # 约定 code!=0 视为失败
                if isinstance(data, dict) and "code" in data and data.get("code") not in (0, "0", None):
                    raise ZLMAPIError(f"path={path} code={data.get('code')} msg={data}")
                logger.debug("ZLM response path=%s code=%s", path, data.get("code") if isinstance(data, dict) else None)
                return data
            return resp.content
        except Exception as exc:
            logger.error("ZLM request failed path=%s status=%s body=%s error=%s", path, getattr(resp, "status_code", None), getattr(resp, "text", None), exc)
            raise ZLMAPIError(str(exc)) from exc

    # -------------------------------
    # 1. 拉流代理 addStreamProxy
    # -------------------------------
    async def add_stream_proxy(self, vhost: str, app: str, stream: str, url: str, enable_mp4: bool = True) -> Dict[str, Any]:
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
        return await self._request("/addStreamProxy", params)

    # -------------------------------
    # 2. 截图 getSnap
    # -------------------------------
    async def get_snapshot(self, rtsp_url: str, output_path: str) -> str:
        params = {
            "secret": self.secret,
            "url": rtsp_url,
            "timeout_sec": 10,
            "expire_sec": 1,
        }
        content = await self._request("/getSnap", params, expect_json=False, timeout=20)
        with open(output_path, "wb") as f:
            f.write(content)
        logger.info("ZLM 截图完成 output=%s", output_path)
        return output_path

    # -----------------------
    # 3.查询流是否在线
    # -----------------------
    async def is_stream_online(self, app: str, stream: str) -> bool:
        params = {
            "secret": self.secret,
            "vhost": "__defaultVhost__",
            "app": app,
            "stream": stream,
        }
        data = await self._request("/getMediaList", params, timeout=10)
        streams = data.get("data", [])
        return len(streams) > 0

    # -------------------------------
    # 4. 删除流 delStreamProxy
    # -------------------------------
    async def delete_stream(self, key: str) -> Dict[str, Any]:
        """
        删除 ZLM 拉流
        key: addStreamProxy 返回的 key
        """
        params = {
            "secret": self.secret,
            "key": key,
        }
        return await self._request("/delStreamProxy", params, timeout=20)

    # -----------------------
    # 截图（上传 MinIO）
    # -----------------------
    async def get_media_list(self) -> List[Dict[str, Any]]:
        params = {
            "secret": settings.ZLM_SECRET
        }
        data = await self._request("/getMediaList", params, timeout=10)
        return data.get("data", [])

    async def close(self):
        if self._owns_client and not self._closed:
            self._closed = True
            await self._client.aclose()


# 全局实例
zlm_service = ZLMService()
