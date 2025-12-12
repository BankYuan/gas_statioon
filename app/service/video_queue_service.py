# import asyncio
# import os
# from app.service.minio_service import MinioService
#
# VIDEO_BUCKET = "camera-video"
# minio_service = MinioService()
#
# queue = asyncio.Queue()
#
# async def producer(file_path: str):
#     """新增文件加入队列"""
#     await queue.put(file_path)
#     print(f"Queued {file_path}")
#
# async def consumer():
#     """从队列中取文件上传到 MinIO"""
#     while True:
#         file_path = await queue.get()
#         try:
#             # 生成对象路径，保留目录结构
#             relative_path = os.path.relpath(file_path, minio_service.LOCAL_VIDEO_PATH)
#             object_name = relative_path.replace("\\", "/")
#             minio_service.upload_file(
#                 bucket=VIDEO_BUCKET,
#                 object_name=object_name,
#                 file_path=file_path,
#                 expire_days=3
#             )
#             print(f"Uploaded {object_name} to MinIO")
#         except Exception as e:
#             print(f"Failed to upload {file_path}: {e}")
#         finally:
#             queue.task_done()
