from minio import Minio

# client = Minio(
#     "127.0.0.1:19000",
#     access_key="minioadmin",
#     secret_key="minioadmin",
#     secure=False
# )

# # 列出桶
# for bucket in client.list_buckets():
#     print(bucket.name)
#
# # 列出桶下的对象
# for obj in client.list_objects("camera-video", recursive=True):
#     print(obj.object_name)

# bucket_name = "camera-video"
#
# # 获取生命周期策略
# try:
#     lifecycle = client.get_bucket_lifecycle(bucket_name)
#     print(lifecycle)
# except Exception as e:
#     print("没有设置生命周期策略:", e)



import secrets

def generate_64bit_key():
    random_bytes = secrets.token_bytes(16)  # 8字节 = 64位
    return random_bytes.hex()

print(generate_64bit_key())