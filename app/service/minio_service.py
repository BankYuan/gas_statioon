import io
import os
import asyncio
from typing import Optional

from minio import Minio
from minio.commonconfig import Filter, Tag, Tags
from minio.error import S3Error
from minio.lifecycleconfig import Expiration, LifecycleConfig, Rule

from app.config import settings



class MinioService:
    """
    MinIO 业务封装：上传、下载、删除、预签名 URL

    NOTE: 不在构造函数中执行网络 I/O，提供 `init()` 由外部（如 app startup）调用。
    """

    def __init__(self, endpoint=None, access_key=None, secret_key=None, secure: Optional[bool] = None):
        self.endpoint = endpoint or settings.MINIO_ENDPOINT
        self.access_key = access_key or settings.MINIO_ACCESS_KEY
        self.secret_key = secret_key or settings.MINIO_SECRET_KEY
        self.secure = secure if secure is not None else getattr(settings, "MINIO_SECURE", False)
        self.BUCKET_NAME = settings.MINIO_BUCKET
        self.LOCAL_VIDEO_PATH = settings.LOCAL_VIDEO_PATH

        # 仅创建客户端实例，不做网络请求
        self.client = Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
            region="us-east-1",
        )

        self._initialized = False

    def init(self):
        """
        同步初始化：创建 bucket 并设置生命周期。该方法可能会做网络 I/O，
        应在 FastAPI startup 中通过 `run_in_executor` 调用以避免阻塞事件循环。
        """
        if self._initialized:
            return

        try:
            if not self.client.bucket_exists(self.BUCKET_NAME):
                self.client.make_bucket(self.BUCKET_NAME)
            self._ensure_lifecycle()
        except S3Error as exc:
            raise RuntimeError(f"MinIO init failed: {exc}") from exc

        self._initialized = True

    def _ensure_lifecycle(self):
        """幂等设置生命周期规则：按 expire_days 标签过期。"""
        rule_id = f"video-expire-{settings.EXPIRE_DAY}d"
        desired = Rule(
            rule_id=rule_id,
            status="Enabled",
            rule_filter=Filter(tag=Tag("expire_days", str(settings.EXPIRE_DAY))),
            expiration=Expiration(days=settings.EXPIRE_DAY),
        )
        try:
            current = self.client.get_bucket_lifecycle(self.BUCKET_NAME)
            if current and any(r.rule_id == rule_id for r in current.rules):
                return
        except S3Error:
            # 若获取失败，则尝试设置
            pass
        self.client.set_bucket_lifecycle(self.BUCKET_NAME, LifecycleConfig([desired]))

    def make_bucket(self, bucket: str):
        """创建存储桶（若不存在）"""
        try:
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
        except S3Error as exc:
            raise RuntimeError(f"create bucket failed: {exc}") from exc

    def upload_bytes(self, bucket: str, object_name: str, data: bytes, content_type: str = "application/octet-stream"):
        """上传字节流文件"""
        self.make_bucket(bucket)
        try:
            self.client.put_object(
                bucket,
                object_name,
                io.BytesIO(data),
                length=len(data),
                content_type=content_type,
            )
        except S3Error as exc:
            raise RuntimeError(f"upload bytes failed: {exc}") from exc
        return f"/{bucket}/{object_name}"

    def upload_file(self, bucket: str, object_name: str, file_path: str, content_type: Optional[str] = None, expire_days: Optional[int] = None):
        """上传本地文件，并设置过期标签"""
        self.make_bucket(bucket)
        try:
            self.client.fput_object(bucket, object_name, file_path, content_type=content_type)
            # 设置对象标签，用于生命周期管理
            tags = Tags()
            expire_value = expire_days if expire_days is not None else settings.EXPIRE_DAY
            tags["expire_days"] = str(expire_value)
            self.client.set_object_tags(bucket, object_name, tags)
        except S3Error as exc:
            raise RuntimeError(f"upload file failed: {exc}") from exc
        return f"/{bucket}/{object_name}"

    def download_bytes(self, bucket: str, object_name: str) -> bytes:
        """下载对象为字节流"""
        try:
            data = self.client.get_object(bucket, object_name)
            try:
                return data.read()
            finally:
                data.close()
                data.release_conn()
        except S3Error as exc:
            raise RuntimeError(f"download bytes failed: {exc}") from exc

    def delete_object(self, bucket: str, object_name: str):
        """删除对象"""
        try:
            self.client.remove_object(bucket, object_name)
        except S3Error as exc:
            raise RuntimeError(f"delete object failed: {exc}") from exc

    def get_presigned_url(self, bucket: str, object_name: str, expires: int = 259200) -> str:
        """获取 MinIO 文件的预签名 URL"""
        try:
            return self.client.presigned_get_object(bucket, object_name, expires=expires)
        except S3Error as exc:
            raise RuntimeError(f"presign failed: {exc}") from exc

    def get_file_url(self, object_name: str):
        """生成可访问 URL"""
        url = f"http://{settings.MINIO_ENDPOINT}/{settings.MINIO_BUCKET}/{object_name}"
        return url

    # ================================
    # Async-safe 包装：在 to_thread 中调用同步 SDK
    # ================================
    async def upload_file_async(self, bucket: str, object_name: str, file_path: str, content_type: Optional[str] = None, expire_days: Optional[int] = None):
        return await asyncio.to_thread(self.upload_file, bucket, object_name, file_path, content_type, expire_days)

    async def upload_bytes_async(self, bucket: str, object_name: str, data: bytes, content_type: str = "application/octet-stream"):
        return await asyncio.to_thread(self.upload_bytes, bucket, object_name, data, content_type)

    async def download_bytes_async(self, bucket: str, object_name: str) -> bytes:
        return await asyncio.to_thread(self.download_bytes, bucket, object_name)

    async def delete_object_async(self, bucket: str, object_name: str):
        return await asyncio.to_thread(self.delete_object, bucket, object_name)

    async def get_presigned_url_async(self, bucket: str, object_name: str, expires: int = 259200) -> str:
        return await asyncio.to_thread(self.get_presigned_url, bucket, object_name, expires)


# 单例创建函数
# _minio_service: Optional[MinioService] = None
#
# def get_minio_service() -> MinioService:
#     global _minio_service
#
#     if _minio_service is None:
#
#         _minio_service = MinioService(
#             endpoint=settings.MINIO_ENDPOINT,
#             access_key=settings.MINIO_ACCESS_KEY,
#             secret_key=settings.MINIO_SECRET_KEY,
#             secure=settings.MINIO_SECURE,
#         )
#     return _minio_service

