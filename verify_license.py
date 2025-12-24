import json
import hashlib
import datetime
import os
import subprocess
from typing import Tuple

from app.config import settings

def get_current_image_id() -> str:
    """
    获取当前运行镜像标识：优先取环境变量 IMAGE_ID 或配置 LICENSE_CURRENT_IMAGE_ID。
    """
    env_image = os.environ.get("IMAGE_ID") or settings.LICENSE_CURRENT_IMAGE_ID
    if env_image:
        return env_image
    return ""

def get_mainboard_serial():
    try:
        serial = subprocess.check_output(
            "cat /sys/class/dmi/id/product_uuid",
            shell=True
        )
        return serial.decode().strip()
    except:
        return "unknown_board"

def verify_license(license_file="license.json") -> Tuple[bool, str]:
    try:
        with open(license_file, "r") as f:
            data = json.load(f)
    except Exception as e:
        return False, f"License 文件读取失败: {e}"

    # 镜像校验：期望值来自 license，当前值来自配置或环境
    expected_image_id = data.get("container_id") or ""
    current_image_id = get_current_image_id()
    print(expected_image_id, current_image_id)
    print("dddddddddddddd")
    if expected_image_id:
        if not current_image_id:
            return False, "当前镜像标识缺失，无法验证"
        if expected_image_id != current_image_id:
            return False, f"镜像标识不匹配: expected={expected_image_id} actual={current_image_id}"

    # 检查主板信息（宿主机/VM 场景）
    if data.get("mainboard") and data.get("mainboard") != get_mainboard_serial():
        return False, "主板信息不匹配"

    # 检查过期时间
    expire_date = datetime.datetime.strptime(data.get("auth_time"), "%Y-%m-%d").date()
    if datetime.date.today() > expire_date:
        return False, "License 已过期"

    # 校验 license 哈希值
    image_component = data.get("image_id") or data.get("container_id") or ""
    expected_hash = hashlib.sha256(
        (image_component + data.get("mainboard") + data.get("password") + data.get("auth_time")).encode()
    ).hexdigest()

    if expected_hash != data.get("license"):
        return False, "License 校验失败"

    return True, "License 验证通过"

if __name__ == "__main__":
    valid, message = verify_license()
    if valid:
        print("✅", message)
    else:
        print("❌", message)
