-- scripts/init_db.sql
CREATE TABLE IF NOT EXISTS cameras (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    camera_id VARCHAR(50) UNIQUE NOT NULL,
    rtsp_url VARCHAR(1000) NOT NULL,
    resolution VARCHAR(20) DEFAULT 'HD',
    codec VARCHAR(20) DEFAULT 'H264',
    fps VARCHAR(20) DEFAULT '25',
    is_pulled BOOLEAN DEFAULT FALSE,
    clip_duration VARCHAR(20) DEFAULT '10',
    proxy_url VARCHAR(1000),
    app VARCHAR(50) DEFAULT 'live',
    stream VARCHAR(200),
    stream_key VARCHAR(200)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_cameras_camera_id ON cameras (camera_id);
CREATE INDEX IF NOT EXISTS idx_cameras_app_stream ON cameras (app, stream);
