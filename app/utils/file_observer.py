import asyncio
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from app.service.video_queue_service import producer
from app.service.video_queue_service import queue

LOCAL_VIDEO_PATH = "/mnt/data1/Gas_station_video/mp4"

class VideoHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".mp4"):
            asyncio.get_event_loop().call_soon_threadsafe(queue.put_nowait, event.src_path)

def start_observer():
    observer = Observer()
    observer.schedule(VideoHandler(), path=LOCAL_VIDEO_PATH, recursive=True)
    observer.start()
    from app.utils.logger import get_logger

    logger = get_logger(__name__)
    logger.info("Watching for new videos in %s", LOCAL_VIDEO_PATH)
