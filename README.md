# Gas Station Video Platform

基于 FastAPI 的加油站视频处理平台，串联 ZLMediaKit（拉流/切片）、MinIO（存储）、Redis（任务队列 & Session）、行为识别服务与 PostgreSQL。系统监听摄像头切片、上传到对象存储、并对提枪/挂枪事件进行识别、拼接与回传。

---

## 1. 功能组件

- **摄像头与快照管理**：`/camera`、`/snapshot` API 完成摄像头 CRUD、ZLM 控制、抓图上传。
- **本地 Watchdog**：实时监听 `settings.LOCAL_VIDEO_PATH`，过滤临时文件后上传到 MinIO，并将对象信息写入 Redis 队列。
- **行为识别管线**：`BehaviorService` 对每段切片调用算法服务，凭动作数据拼接提枪→挂枪事件；`EventPostProcessor` 拼接视频、上传事件桶并写入数据库。
- **会话持久化 & 历史追溯**：每个摄像头的 Session 保存在 Redis `behavior:sessions:{camera_id}`；完成或清空的事件追加到 `behavior:event_failures`，默认保留 7 天。
- **事件通知**：预留 `BEHAVIOR_EVENT_NOTIFY_URL`，可对接外部 HTTP 接口推送识别结果（当前默认打印日志）。
- **任务队列**：自研 `RedisQueueService` + dispatcher + worker pool，实现多摄像头并行、同摄像头串行的消费模型。
- **外部集成**：封装 MinIO 生命周期管理、ZLM API 操作、PostgreSQL ORM。

---

## 2. 系统流程

```
Camera RTSP
  │  (ZLMediaKit 切片)
  ▼
本地 MP4 ── Watchdog (video_watcher)
  │ 上传至 MinIO camera-video
  └─> Redis video_tasks（包含 clip_time / service_name）

Redis Dispatcher（BLPOP）
  └─> asyncio.Queue
       └─ Workers：
            1. 下载 MinIO 切片到临时文件
            2. BehaviorService.process_clip
                 - 单段调用算法 → SegmentResult
                 - per-camera Session（asyncio.Lock + Redis）拼接事件
                 - 中间/完整事件写入 behavior:event_failures（保留 7 天）
            3. 事件完成 → EventPostProcessor
                 - 下载并 concat 视频
                 - 上传 camera-event-video 桶
                 - 写入 behavior_events 表
                 - 可选：通知 BEHAVIOR_EVENT_NOTIFY_URL
```

---

## 3. 目录速览

| 路径 | 说明 |
| --- | --- |
| `app/main.py` | FastAPI 入口，初始化服务并注册路由 |
| `app/routers/*.py` | 摄像头、快照、行为识别 API |
| `app/service/` | MinIO、ZLM、行为识别、事件处理、Redis 队列等核心服务 |
| `app/video_watcher.py` | Watchdog + MinIO 上传 + Redis 入队/消费 |
| `scripts/consumer_worker.py` | 独立运行的 Redis 队列消费者 |
| `scripts/init_db.sql` | `cameras` 表建表脚本 |
| `scripts/init_behavior_events.sql` | `behavior_events` 表建表脚本 |
| `api_manage.py` | 调试 ZLM API 的辅助服务 |

---

## 4. 环境准备

1. **Python**：建议 3.11+，准备虚拟环境。
2. **依赖服务**（均可容器化部署）  
   MinIO `127.0.0.1:19000`、ZLMediaKit `http://127.0.0.1:8080/index/api`、PostgreSQL `127.0.0.1:15432`、Redis `127.0.0.1:6378`、行为识别服务 `settings.BEHAVIOR_SERVICE_URL`。
3. **安装依赖**
   ```bash
   pip install fastapi uvicorn[standard] sqlalchemy[asyncio] asyncpg \
              httpx redis watchdog minio pydantic-settings python-dotenv
   ```
4. **配置**：创建 `.env` 并覆写 `app/config.py` 默认值（如 `MINIO_ENDPOINT`、`REDIS_URL`、`BEHAVIOR_SESSION_TTL_SECONDS` 等）。
5. **数据库初始化**
   ```bash
   psql -h 127.0.0.1 -p 15432 -U video_user -d video_db -f scripts/init_db.sql
   psql -h 127.0.0.1 -p 15432 -U video_user -d video_db -f scripts/init_behavior_events.sql
   ```
6. **MinIO 桶**：创建 `camera-video`（切片）与 `camera-event-video`（事件）并配置生命周期。
7. **本地目录**：确保 `settings.LOCAL_VIDEO_PATH` 存在，ZLMediaKit 正在输出切片。
8. **生命周期脚本**：`python scripts/minio_lifecycleconfig.py`，为带 `expire_days` 标签的对象注入过期策略，让 MinIO/Redis 一致清理。

---

## 5. 启动方式

### 5.1 FastAPI 主服务
```bash
uvicorn app.main:app_ --host 0.0.0.0 --port 8000 --reload
```
启动后自动初始化 MinIO/ZLM/Redis/行为识别服务实例，并监听 `LOCAL_VIDEO_PATH`。常用 API：`/` 健康检查、`/camera/*` 管理摄像头、`/snapshot/{camera_id}` 截图上传、`/record/behavior/process` 手动触发行为识别。

### 5.2 Redis 消费者（必跑）
```bash
python scripts/consumer_worker.py
```
消费 `video_tasks` 队列、下载对象并调用 `BehaviorService`。支持 `Ctrl+C` 优雅退出。生产环境建议用 systemd/supervisor 管理，避免队列过期。

---

## 6. 数据存储与查看

| 类型 | 内容 | 查看方式（使用默认配置即可在当前服务器执行） |
| --- | --- | --- |
| Redis `video_tasks` | 待消费任务 | `redis-cli -h 127.0.0.1 -p 6378 LRANGE video_tasks 0 -1` |
| Redis `behavior:sessions` | 各摄像头 Session | `redis-cli -h 127.0.0.1 -p 6378 HKEYS behavior:sessions`<br>`redis-cli -h 127.0.0.1 -p 6378 HGET behavior:sessions camera_51018500451327700007` |
| Redis `behavior:event_failures` | 完成/清空事件历史（7 天） | `redis-cli -h 127.0.0.1 -p 6378 LRANGE behavior:event_failures 0 20` |
| PostgreSQL `cameras`、`behavior_events` | 摄像头/事件记录 | `psql -h 127.0.0.1 -p 15432 -U video_user -d video_db` 然后 `SELECT * FROM cameras LIMIT 5;`、`SELECT id,camera_id,start_clip_time,end_clip_time,video_object FROM behavior_events ORDER BY id DESC LIMIT 10;` |
| MinIO `camera-video` | Watchdog 上传的原始切片 | 控制台 http://127.0.0.1:19000 或 `mc alias set local http://127.0.0.1:19000 minioadmin minioadmin`，再 `mc ls local/camera-video/videos/` |
| MinIO `camera-event-video` | 事件拼接结果 | `mc ls local/camera-event-video/events/` |

---

## 7. 常用运维命令

- **检查生命周期配置**：`python scripts/minio_lifecycleconfig.py`
- **调试 ZLM 接口**：`uvicorn api_manage:app --reload` 后访问 `/add_stream_proxy`、`/getSnap` 等
- **监控队列长度**：`redis-cli -h 127.0.0.1 -p 6378 LLEN video_tasks`
- **下载/查看事件视频**：`mc cp local/camera-event-video/events/<camera>/<ts>.mp4 ./`

---

## 8. 开发提示

- 日志由 `app/utils/logger.py` 统一初始化，`LOG_LEVEL=DEBUG` 可输出 Watchdog/行为识别的细节日志。
- `BehaviorService` 只负责动作拼接，事件完成后交由 `EventPostProcessor` 下载、拼接、上传、写库；可通过自定义 `BEHAVIOR_EVENT_NOTIFY_URL` 对接告警系统。
- `behavior:event_failures` 里的历史记录可被后台任务消费落库，保证追溯。
- Watchdog `Observer` 会在 FastAPI 关闭事件中停止，如需独立运行可手动调用 `start_watching`。

---

## 9. 后续计划

- 提供 `requirements.txt` / Poetry / docker-compose，降低部署成本。
- 完善 `scripts/cron_snapshot.py`、`api_manage.py` 等辅助脚本说明。
- 增补自动化测试与集成测试。

欢迎根据实际业务需求扩展识别逻辑、回传接口或拆分模块。***
