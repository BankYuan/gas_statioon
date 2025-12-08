import asyncio
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from app.service.video_queue_service import producer

LOCAL_VIDEO_PATH = "/mnt/data1/Gas_station_video/mp4"

class VideoHandler(FileSystemEventHandler):
    """监听新增视频文件"""
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".mp4"):
            asyncio.get_event_loop().create_task(producer(event.src_path))

def start_observer():
    observer = Observer()
    observer.schedule(VideoHandler(), path=LOCAL_VIDEO_PATH, recursive=True)
    observer.start()
    from app.utils.logger import get_logger

    logger = get_logger(__name__)
    logger.info("Watching for new videos in %s", LOCAL_VIDEO_PATH)
