import asyncio
import threading
import time
from typing import Optional

from app.config import settings
from app.utils.logger import get_logger
from verify_license import verify_license


class LicenseGuard:
    def __init__(self, interval_seconds: float = 3600.0):
        self.interval = max(interval_seconds, 60.0)
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self.logger = get_logger(__name__)

    async def start(self):
        if self._task is None or self._task.done():
            self._stop_event.clear()
            self._task = asyncio.create_task(self._run())

    async def stop(self):
        if self._task:
            self._stop_event.set()
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self):
        while not self._stop_event.is_set():
            await asyncio.sleep(self.interval)
            valid, message = verify_license(settings.LICENSE_PATH)
            if not valid:
                self.logger.error("License 失效：%s，准备停止服务", message)
                raise RuntimeError(f"License expired or invalid: {message}")
            else:
                self.logger.debug("License 守护检查通过")
