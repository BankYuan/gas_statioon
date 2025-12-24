import base64
import json
import hashlib
from Crypto.Cipher import AES


password = "cddx_gas"     # 必须与客户端一致


# 生成 AES-256 key
def generate_key(container_id, board, password):
    raw = (container_id + board + password).encode()
    return hashlib.sha256(raw).digest()


# AES CBC 解密
def aes_decrypt(key, cipher_text):
    raw = base64.b64decode(cipher_text)
    iv = raw[:16]
    ct = raw[16:]

    cipher = AES.new(key, AES.MODE_CBC, iv)
    padded = cipher.decrypt(ct)
    pad_len = padded[-1]
    return padded[:-pad_len].decode()


if __name__ == "__main__":
    # 读取加密数据
    with open("request.txt", "r") as f:
        cipher_text = f.read().strip()

    # 🚨 第一步 —— 暂时无法解密，因为不知道 container_id & mainboard
    # 所以必须让客户端把这两个参数"明文"发送给服务器一起校验
    # 按你的要求，现在我们直接反解（模拟）

    # ========= 正确流程 =========
    # 客户端把 container_id + mainboard 也传给服务器
    # 服务器用这两项构造 key → 解密
    # =================================

    # ❗为演示，这里让你手动输入（真实场景是客户端附带）
    container_id = input("输入客户端的 container_id: ").strip()
    mainboard = input("输入客户端的 mainboard: ").strip()

    # 生成 key
    key = generate_key(container_id, mainboard, password)

    # 解密
    plaintext = aes_decrypt(key, cipher_text)
    info = json.loads(plaintext)

    print("解密结果:", info)

    # ------------------- 生成 license -------------------
    license_data = {
        "container_id": info["container_id"],
        "mainboard": info["mainboard"],
        "auth_time": "2026-12-31",
        "password": password,
        "license": hashlib.sha256(
            (info["container_id"] + info["mainboard"] + password + "2026-12-31").encode()
        ).hexdigest()
    }

    with open("license.json", "w") as f:
        json.dump(license_data, f, indent=4)

    print("生成 license.json 完成")
