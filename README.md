# Gas Station Video Platform

基于 FastAPI 的加油站视频处理平台，串联 ZLMediaKit（拉流/切片）、MinIO（存储）、Redis（任务队列 & Session）、行为识别服务与 PostgreSQL。系统监听摄像头切片、上传到对象存储、并对提枪/挂枪事件进行识别、拼接与回传。

---

## 1. 功能组件

- **摄像头与快照管理**：`/camera`、`/snapshot` API 完成摄像头 CRUD、ZLM 控制、抓图上传。
- **本地 Watchdog**：实时监听 `settings.LOCAL_VIDEO_PATH`，过滤临时文件后上传到 MinIO，并将对象信息写入 Redis 队列。
- **行为识别管线**：`BehaviorService` 对每段切片调用算法服务，凭动作数据拼接提枪→挂枪事件；`EventPostProcessor` 拼接视频、上传事件桶并写入数据库。
- **会话持久化 & 历史追溯**：每个摄像头的 Session 保存在 Redis `behavior:sessions:{camera_id}`；完成/清空/通知失败/后处理失败事件分别写入 `behavior:event_failures:<status>`，默认保留 7 天，便于按状态排查。
- **事件通知**：配置 `BEHAVIOR_EVENT_NOTIFY_URL` 后，将以 HTTP POST 推送事件结果，并带指数退避重试；失败会写入 Redis 历史队列以便补偿。
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

Redis Dispatcher（单协程 BLPOP 出队）
  ├─ 将任务投递到 asyncio.Queue（容量可控，负责“一个出队，多 worker 消费”）
  └─ 并发 Workers（数量由配置指定，多线程/协程并发）：
        1. 从本地缓存/MinIO 下载切片到临时文件
        2. 调用 BehaviorService.process_clip
             - 单段调用算法 → SegmentResult
             - 按摄像头加锁（asyncio.Lock）+ Redis Session 续命，实现同摄像头串行、不同摄像头并行
             - 根据结果更新 Redis `behavior:sessions:*`，并在完成/清空时写入 `behavior:event_failures:<status>`
        3. 一旦判定提枪→挂枪完整 → 交给 EventPostProcessor
             - 顺序下载并 concat 视频（ffmpeg）
             - 上传到 camera-event-video 桶，并写入 PostgreSQL `behavior_events`
             - 调用 BEHAVIOR_EVENT_NOTIFY_URL（带重试），失败则写入 `behavior:event_failures:notify_failed`
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
3. **安装依赖**（已整理在 `requirements.txt`）
   ```bash
   pip install -r requirements.txt
   ```
   其中涵盖 FastAPI、SQLAlchemy、MinIO、Redis、Watchdog、PyCryptodome 等核心组件，便于一键安装。
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
| Redis `video_tasks` | 待消费任务（Watchdog 上传新切片时会追加） | `redis-cli -h 127.0.0.1 -p 6378 LRANGE video_tasks 0 -1` |
| Redis `behavior:sessions` | 各摄像头 Session（哈希或独立 key，默认 `behavior:sessions:{camera_id}`；只有累积到切片时才有值） | `redis-cli -h 127.0.0.1 -p 6378 HKEYS behavior:sessions`<br>`redis-cli -h 127.0.0.1 -p 6378 HGET behavior:sessions camera_51018500451327700007`<br>`redis-cli -h 127.0.0.1 -p 6378 GET behavior:sessions:camera_51018500451327700007` |
| Redis `behavior:event_failures:*` | 完整/清空/通知失败/拼接失败事件历史（7 天；`complete` / `cleared` / `notify_failed` / `postprocess_failed`）<br>只有触发相应动作时才会出现记录 | - 完整事件：`redis-cli -h 127.0.0.1 -p 6378 LRANGE behavior:event_failures:complete 0 20`（当提枪→挂枪流程走完时生成）<br>- 清空快照：`redis-cli -h 127.0.0.1 -p 6378 LRANGE behavior:event_failures:cleared 0 20`（事件因段数超限被放弃时生成）<br>- 通知失败：`redis-cli -h 127.0.0.1 -p 6378 LRANGE behavior:event_failures:notify_failed 0 20`（HTTP 通知多次重试仍失败）<br>- 后处理失败：`redis-cli -h 127.0.0.1 -p 6378 LRANGE behavior:event_failures:postprocess_failed 0 20`（拼接/上传/写库出错且用尽重试） |
| PostgreSQL `cameras`、`behavior_events` | 摄像头/事件记录（`behavior_events` 在后处理成功后才有新行） | `psql -h 127.0.0.1 -p 15432 -U video_user -d video_db` 然后 `SELECT * FROM cameras LIMIT 5;`、`SELECT id,camera_id,start_clip_time,end_clip_time,video_object FROM behavior_events ORDER BY id DESC LIMIT 10;` |
| MinIO `camera-video` | Watchdog 上传的原始切片（只有 ZLM 持续输出时才有新增） | 控制台 http://127.0.0.1:19000 或 `mc alias set local http://127.0.0.1:19000 minioadmin minioadmin`，再 `mc ls local/camera-video/videos/` |
| MinIO `camera-event-video` | 事件拼接结果（事件完成且后处理成功后新增） | `mc ls local/camera-event-video/events/` |

---

## 7. 常用运维命令

### 7.1 队列 / 存储
- **检查生命周期配置**：`python scripts/minio_lifecycleconfig.py`
- **监控 Redis 队列长度**：`redis-cli -h 127.0.0.1 -p 6378 LLEN video_tasks`
- **下载事件视频**：`mc cp local/camera-event-video/events/<camera>/<ts>.mp4 ./`

### 7.2 摄像头管理（FastAPI）
| 目的 | 命令 | 说明 / 输出示例 |
| --- | --- | --- |
| 新增摄像头 | ```bash<br>curl -X POST http://127.0.0.1:8000/camera/add \ <br>  -H "Content-Type: application/json" \ <br>  -d '{"name":"入口1","rtsp_url":"rtsp://user:pwd@192.168.1.10/stream"}'<br>``` | 返回 `CameraResponse` JSON，包含 `camera_id`、`proxy_url` 等字段。 |
| 查看摄像头列表 | ```bash<br>curl http://127.0.0.1:8000/camera/list | jq '.'<br>``` | 输出所有摄像头详情，可配合 `jq` 过滤。 |
| 删除摄像头 | ```bash<br>curl -X DELETE http://127.0.0.1:8000/camera/1<br>``` | 成功返回 `{"message":"Camera deleted"}`。 |

### 7.3 ZLMediaKit 管理
| 场景 | 命令 | 说明 / 输出 |
| --- | --- | --- |
| 启动拉流并自动记录 stream_key | ```bash<br>curl -X POST http://127.0.0.1:8000/camera/1/start<br>``` | 返回 `{"message":"Stream started","camera":{...}}`，其中包含 `stream_key`、`proxy_url`。 |
| 停止拉流 | ```bash<br>curl -X POST http://127.0.0.1:8000/camera/1/stop<br>``` | 关闭 ZLM 代理，返回状态。 |
| 获取截图 | ```bash<br>curl http://127.0.0.1:8000/camera/1/snapshot<br>``` | 返回存储路径：`{"file_path":"/mnt/.../20250101_120000.jpg"}`。 |
| 查询流状态 | ```bash<br>curl http://127.0.0.1:8000/camera/1/status<br>``` | 输出 `{"online":true,"stream":"camera_1"}`；若未启动则提示错误。 |
| 同步 ZLM → 数据库 | ```bash<br>curl -X POST http://127.0.0.1:8000/camera/sync<br>``` | 拉取 ZLM `media/list` 与本地摄像头比对，返回新建/删除数量。 |

> 若需直接调试 ZLM 原生 API，可运行 `uvicorn api_manage:app --reload`，并使用 `curl http://127.0.0.1:8001/add_stream_proxy?...` 之类命令验证。

---

## 8. 开发提示

- 日志由 `app/utils/logger.py` 统一初始化，`LOG_LEVEL=DEBUG` 可输出 Watchdog/行为识别的细节日志。
- `BehaviorService` 只负责动作拼接，事件完成后交由 `EventPostProcessor` 下载、拼接、上传、写库；`BEHAVIOR_EVENT_NOTIFY_URL` 默认执行 HTTP POST 并带重试，结果也写入 Redis 历史。
- 事件拼接的最终视频可通过 PostgreSQL `behavior_events.video_object/video_url` 查询，或读取通知 payload/Redis 历史记录快速定位。
- Redis 历史按状态拆分在 `behavior:event_failures:<status>`（complete/cleared/notify_failed/postprocess_failed），方便定向排查；可按需新增消费者对这些 key 做离线同步。
- Watchdog `Observer` 会在 FastAPI 关闭事件中停止，如需独立运行可手动调用 `start_watching`。

---

## 9. 后续计划

- 提供 `requirements.txt` / Poetry / docker-compose，降低部署成本。
- 完善 `scripts/cron_snapshot.py`、`api_manage.py` 等辅助脚本说明。
- 增补自动化测试与集成测试。

欢迎根据实际业务需求扩展识别逻辑、回传接口或拆分模块。***
