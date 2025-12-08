from pathlib import Path
import sys

from minio import Minio
from minio.lifecycleconfig import LifecycleConfig, Rule, Expiration
from minio.commonconfig import Tag, Filter

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import settings

client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ACCESS_KEY,
    secret_key=settings.MINIO_SECRET_KEY,
    secure=False,
)
bucket = settings.MINIO_BUCKET
expire_days = settings.EXPIRE_DAY

rule = Rule(
    rule_id=f"video-expire-{expire_days}d",
    status="Enabled",
    rule_filter=Filter(tag=Tag("expire_days", str(expire_days))),
    expiration=Expiration(days=expire_days),
)

lc = LifecycleConfig([rule])
client.set_bucket_lifecycle(bucket, lc)
print(f"Lifecycle rule applied: expire_days={expire_days}")
