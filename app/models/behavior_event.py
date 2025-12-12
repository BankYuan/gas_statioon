from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, JSON, String

from app.models.camera import Base


class BehaviorEvent(Base):
    __tablename__ = "behavior_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    camera_id = Column(String(50), index=True, nullable=False)
    start_clip_time = Column(String(64))
    end_clip_time = Column(String(64))
    video_object = Column(String(512), nullable=False)
    video_url = Column(String(1024))
    segments = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
