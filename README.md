# Gas Station Video Platform

FastAPI 服务，用于管理加油站摄像头、与 ZLMediaKit/MinIO 交互，并串联本地行为识别（提枪/挂枪）流程。系统在本地目录监听新的 MP4 切片，上传至 MinIO，使用 Redis 队列驱动后续消费，同时提供 REST API 管理摄像头及调试。

## 功能亮点

- **摄像头管理**：通过 `/camera` API 完成 CRUD、ZLM 拉流/停流、快照与流状态同步。
- **快照上传**： `/snapshot/{camera_id}` 直接拉取 ZLM 截图，上传 MinIO，返回预签名 URL。
- **自动入库**：`app/video_watcher.py` 使用 Watchdog 监听 `settings.LOCAL_VIDEO_PATH`，上传新切片到 MinIO 并将对象信息写入 Redis，行为识别统一在消费者端执行。
- **行为识别管线**：`BehaviorService` 按摄像头缓存切片（含 ffmpeg 拼接）、调用算法服务、在提枪→挂枪完整后输出最终视频。
- **任务队列**：`RedisQueueService` 使用 List + `BLPOP` 阻塞消费，并提供 `scripts/consumer_worker.py` 作为常驻消费者。
- **外部集成**：封装 MinIO 生命周期设置（按 `expire_days` 标签过期）、ZLMediaKit `addStreamProxy/getSnap/delStreamProxy`、PostgreSQL（Async SQLAlchemy）。

## 体系结构

```
Camera RTSP ──> ZLM 拉流/切片 ──> 本地 MP4
                                 │
                                 ▼
                           Watchdog 监听
                                 │
                   MinIO 上传 + 预签名
                                 │
                                 ▼
                         Redis video_tasks
                                 │
                     scripts/consumer_worker.py
                                 │
                                 ▼
                 BehaviorService (缓存/拼接/识别)
```

## 目录速览

| 目录/文件 | 说明 |
| --- | --- |
| `app/main.py` | FastAPI 入口，初始化所有服务并注册路由。 |
| `app/routers/*.py` | 摄像头、快照、行为识别 API。 |
| `app/service/` | MinIO、ZLM、摄像头、行为识别、Redis 队列等业务封装。 |
| `app/video_watcher.py` | Watchdog + MinIO 上传 + Redis 入队、消费者实现。 |
| `scripts/consumer_worker.py` | 独立运行的 Redis 队列消费者。 |
| `scripts/init_db.sql` | 创建 `cameras` 表的示例 SQL。 |
| `api_manage.py` | 调试用 ZLMediaKit 代理 API。 |

## 运行前准备

1. **Python**：建议 3.11+，安装虚拟环境。
2. **依赖服务**  
   - MinIO：默认 `127.0.0.1:19000`，桶 `camera-video`。  
   - ZLMediaKit：默认 `http://127.0.0.1:8080/index/api`。  
   - PostgreSQL：默认 `postgresql+asyncpg://video_user:123456@127.0.0.1:15432/video_db`。  
   - Redis：默认 `redis://127.0.0.1:6378/0`。  
   - 行为识别服务：`settings.BEHAVIOR_SERVICE_URL`。  
   - **外部工具**：`ffmpeg`（用于 concat）。
3. **Python 依赖**：在虚拟环境中安装：
   ```bash
   pip install fastapi uvicorn[standard] sqlalchemy[asyncio] asyncpg \
              httpx redis watchdog minio pydantic-settings python-dotenv
   ```
4. **配置**：复制 `.env.example`（如有）或创建 `.env`，覆盖 `app/config.py` 中的默认值，如：
   ```
   ZLM_API_BASE=http://127.0.0.1:8080/index/api
   MINIO_ENDPOINT=127.0.0.1:19000
   ...
   ```
5. **数据库初始化**：执行 `psql` 或 `psql -f scripts/init_db.sql` 创建 `cameras` 表。
6. **本地目录**：确保 `settings.LOCAL_VIDEO_PATH` 指定的目录存在并由 ZLMediaKit 切片输出。
7. **生命周期规则**：首次部署需运行 `python scripts/minio_lifecycleconfig.py`，创建“仅清理带 `expire_days` 标签对象”的生命周期规则，确保 MinIO 与 Redis 中的元数据按同一过期时间清理。

## 启动服务

### FastAPI 主服务

```bash
uvicorn app.main:app_ --host 0.0.0.0 --port 8000 --reload
```

- 启动时会自动初始化 MinIO/ZLM/Redis/行为识别服务实例，并在 `settings.LOCAL_VIDEO_PATH` 监听 MP4。
- API 访问示例：
  - `GET /` → 健康检查
  - `POST /camera/add` → 新增摄像头
  - `POST /camera/{camera_id}/start` → 调用 ZLM 拉流
  - `GET /snapshot/{camera_id}` → 截图上传 MinIO
  - `POST /record/behavior/process` → 手动触发行为识别

### Redis 消费者（必需）

FastAPI 主进程只负责 Watchdog+上传，行为识别和 Redis 清理依赖独立消费者。启动方式：

```bash
python scripts/consumer_worker.py
```

消费者会 `BLPOP video_tasks`，下载对象到临时文件并调用 `BehaviorService.process_clip`。使用 `Ctrl+C` 可优雅退出，脚本会关闭 Redis 连接。生产环境需确保该脚本以 systemd/supervisor 守护运行，否则 Redis 任务会在 `EXPIRE_DAY` 后整队过期。

## 常用操作

- **查看队列积压**：
  ```bash
  redis-cli -h 127.0.0.1 -p 6378 LRANGE video_tasks 0 -1
  ```
- **调试 ZLM API**：启动 `api_manage.py`（`uvicorn api_manage:app --reload`），通过 `/add_stream_proxy` 等端点验证。
- **检查 MinIO 生命周期配置**：参考 `scripts/minio_lifecycleconfig.py` 或 `test.py`，确保仅带 `expire_days` 标签的对象按配置天数过期。
- **定位 Redis TTL 问题**：`EXPIRE_DAY` 到期后队列会被自动删除，可通过日志或 `LLEN video_tasks` 监控积压，确认 `scripts/consumer_worker.py` 长期运行。

## 开发提示

- 日志初始化在 `app/utils/logger.py`。若需要更详细调试，调整 `.env` 中 `LOG_LEVEL=DEBUG`。
- Watchdog 监听的 `Observer` 在 FastAPI 关闭事件中停止，如需单独运行可直接调用 `start_watching`。
- `BehaviorService` 依赖 `ffmpeg` 的 `concat`，请确保 PATH 中可执行。
- 若需要扩展行为识别判定逻辑，可修改 `_is_complete` 并在 API 返回中查看 `actions` 和 `raw`。

## 后续计划

- 提供正式的 `requirements.txt`/`poetry` 文件与 docker-compose。
- 完善 `scripts/cron_snapshot.py`、`api_manage.py` 等辅助工具文档。
- 增加自动化测试（当前只有一些手动脚本 `scripts/test_*.py`）。

欢迎根据业务需求调整配置、完善 API 或将监控与消费者分离成独立服务。
