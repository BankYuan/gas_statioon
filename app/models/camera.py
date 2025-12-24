# app/models/camera.py
from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Camera(Base):
    __tablename__ = "cameras"

    # --- 基本信息 ---
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String(100), nullable=False)      # 摄像头名称（如：入口1号摄像头）
    camera_id = Column(String(50), unique=True)     # 摄像头编号（如：51018500451327700007）

    # --- 源流信息（原始RTSP） ---
    rtsp_url = Column(String(1000), nullable=False) # 摄像头原始RTSP地址
    resolution = Column(String(20), default="HD")   # 分辨率：HD / SD / 1080P / 720P
    codec = Column(String(20), default="H264")      # H264 / H265
    fps = Column(String(20), default=25)               # 帧率
    algorithm_capability = Column(String(100), nullable=False, default="unknown")  # 算法能力标签，创建摄像头时必填
    area2d_pt = Column(String(2000), nullable=True)   # 行为识别区域点位配置（字符串/JSON）
    algorithm_enabled = Column(Boolean, default=True)  # 是否启用算法服务

    # --- 是否正在拉流 ---
    is_pulled = Column(Boolean, default=False)      # 是否已成功 addStreamProxy

    # --- 截图/视频片段配置 ---
    clip_duration = Column(String(20), default="10")     # 视频截断时长（秒）

    # --- ZLMediaKit 代理信息 ---
    proxy_url = Column(String(1000))                # 拉流后的播放地址 (ZLM 返回)
    app = Column(String(50), default="live")        # 一般固定 live
    stream = Column(String(200))                    # 流命名（你自己生成）
    stream_key = Column(String(200))                # ZLM 返回的内部 stream_key

