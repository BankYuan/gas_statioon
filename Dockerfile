# syntax=docker/dockerfile:1

# 运行时镜像，拷贝 PyArmor 加密产物并安装系统依赖（ffmpeg）
# 注意：PyArmor runtime 与 Python 版本需一致，当前保护文件由 Python 3.10 生成，故使用 3.10 基础镜像
FROM python:3.10-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 安装系统依赖（ffmpeg 用于视频拼接）
# Debian slim 新版用 debian.sources，直接覆盖为国内镜像以防 404/网络慢
RUN printf "Types: deb\nURIs: http://mirrors.aliyun.com/debian\nSuites: bookworm bookworm-updates bookworm-backports\nComponents: main contrib non-free non-free-firmware\nSigned-By: /usr/share/keyrings/debian-archive-keyring.gpg\n\nTypes: deb\nURIs: http://mirrors.aliyun.com/debian-security\nSuites: bookworm-security\nComponents: main contrib non-free non-free-firmware\nSigned-By: /usr/share/keyrings/debian-archive-keyring.gpg\n" > /etc/apt/sources.list.d/debian.sources \
 && apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
 && rm -rf /var/lib/apt/lists/*


# 拷贝加密后的代码（需先在宿主机执行 pyarmor gen -r -O build/protected ...）
COPY ./build/protected/ /app/

# 安装 Python 依赖（使用清华镜像加速）
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r /tmp/requirements.txt

# PyArmor runtime 路径
ENV PYTHONPATH=/app/pyarmor_runtime_000000

# 默认启动 FastAPI，可通过 docker-compose 或命令行覆盖
CMD ["uvicorn", "app.main:app_", "--host", "0.0.0.0", "--port", "8000"]
