import hashlib
import base64
import subprocess
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
import json


def get_container_id():
    try:
        cid = subprocess.check_output(
            "cat /proc/self/cgroup | grep 'docker' | sed 's/^.*\\///'",
            shell=True
        )
        return cid.decode().strip()[:12]
    except:
        return "unknown_container"


def get_mainboard_serial():
    try:
        serial = subprocess.check_output(
            "cat /sys/class/dmi/id/product_uuid",
            shell=True
        )
        return serial.decode().strip()
    except:
        return "unknown_board"


def generate_key(container_id, board, password):
    raw = (container_id + board + password).encode()
    return hashlib.sha256(raw).digest()  # 32 字节 AES-256 key


def aes_encrypt(key, text):
    iv = get_random_bytes(16)
    cipher = AES.new(key, AES.MODE_CBC, iv)

    # PKCS7 padding
    pad_len = 16 - len(text.encode()) % 16
    text_padded = text + chr(pad_len) * pad_len
    ct = cipher.encrypt(text_padded.encode())

    return base64.b64encode(iv + ct).decode()


if __name__ == "__main__":
    password = "my_password"   # 自设密码
    info = {
        "container_id": get_container_id(),
        "mainboard": get_mainboard_serial()
    }

    print("原始信息:", info)

    # 生成 key
    key = generate_key(info["container_id"], info["mainboard"], password)

    # 明文内容：发给授权服务器
    plaintext = json.dumps(info)

    cipher = aes_encrypt(key, plaintext)

    # 客户端将 cipher 发给你生成 license
    with open("request.txt", "w") as f:
        f.write(cipher)

    print("生成 request.txt 完成")
