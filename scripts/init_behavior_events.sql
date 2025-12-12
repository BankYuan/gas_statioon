CREATE TABLE IF NOT EXISTS behavior_events (
    id SERIAL PRIMARY KEY,
    camera_id VARCHAR(50) NOT NULL,
    start_clip_time VARCHAR(64),
    end_clip_time VARCHAR(64),
    video_object VARCHAR(512) NOT NULL,
    video_url VARCHAR(1024),
    segments JSONB NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_behavior_events_camera ON behavior_events(camera_id);
