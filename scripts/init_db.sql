-- scripts/init_db.sql
-- 摄像头表定义：覆盖摄像头基础信息、算法能力、拉流与切片配置。
CREATE TABLE IF NOT EXISTS cameras (
    id SERIAL PRIMARY KEY,                               -- 自增主键
    name VARCHAR(100) NOT NULL,                          -- 摄像头名称
    camera_id VARCHAR(50) UNIQUE NOT NULL,               -- 业务摄像头编号
    rtsp_url VARCHAR(1000) NOT NULL,                     -- 原始 RTSP 地址
    resolution VARCHAR(20) DEFAULT 'HD',                 -- 分辨率描述
    codec VARCHAR(20) DEFAULT 'H264',                    -- 编码格式
    fps VARCHAR(20) DEFAULT '25',                        -- 帧率
    algorithm_capability VARCHAR(100) NOT NULL DEFAULT 'unknown', -- 算法能力标签
    area2d_pt VARCHAR(2000),                             -- 区域点位配置（JSON/字符串）
    algorithm_enabled BOOLEAN DEFAULT TRUE,              -- 算法是否启用
    is_pulled BOOLEAN DEFAULT FALSE,                     -- 是否已拉流
    clip_duration VARCHAR(20) DEFAULT '10',              -- 切片时长（秒）
    proxy_url VARCHAR(1000),                             -- 拉流后的播放地址
    app VARCHAR(50) DEFAULT 'live',                      -- ZLM app
    stream VARCHAR(200),                                 -- ZLM stream 名称
    stream_key VARCHAR(200)                              -- ZLM 返回的 stream_key
);

-- 唯一索引：camera_id
CREATE UNIQUE INDEX IF NOT EXISTS idx_cameras_camera_id ON cameras (camera_id);
-- 组合索引：app + stream，便于查询流
CREATE INDEX IF NOT EXISTS idx_cameras_app_stream ON cameras (app, stream);
