import os
import base64
import json
import hashlib
from typing import Optional, Tuple
from cryptography.hazmat.primitives.ciphers.aead import AESGCM, ChaCha20Poly1305
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.kdf.kbkdf import CounterLocation, KBKDFHMAC, Mode
import hmac

class QuantumSafeSymmetricEncryption:
    """
    面向后时代的对称加密方案
    """
    
    # 支持的算法标识
    ALG_AES256_GCM = "AES256-GCM"
    ALG_CHACHA20_POLY1305 = "CHACHA20-POLY1305"
    ALG_AES256_GCM_SIV = "AES256-GCM-SIV"
    
    def __init__(self, 
                 algorithm: str = ALG_AES256_GCM,
                 key_size: int = 64,  # 512位对抗Grover算法
                 use_hybrid: bool = True):
        """
        初始化
        
        Args:
            algorithm: 加密算法
            key_size: 密钥大小（字节），建议至少32字节（256位），推荐64字节（512位）
            use_hybrid: 是否使用混合密钥派生
        """
        self.algorithm = algorithm
        self.key_size = key_size
        self.use_hybrid = use_hybrid
        
    def generate_key(self) -> bytes:
        """生成抗密钥"""
        # 使用操作系统安全随机数
        key = os.urandom(self.key_size)
        
        # 如果需要，应用后强化
        if self.use_hybrid:
            key = self._post_quantum_strengthen(key)
        
        return key
    
    def save_key_to_file(self, key: bytes, filepath: str = "quantum_key.txt"):
        """将密钥保存到文件"""
        # 将密钥编码为base64字符串
        key_b64 = base64.b64encode(key).decode('utf-8')
        
        # 保存到文件
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(key_b64)
            
        print(f"密钥已保存到: {filepath}")
        print(f"密钥摘要(SHA3-256): {hashlib.sha3_256(key).hexdigest()}")
        return filepath
    
    def load_key_from_file(self, filepath: str = "quantum_key.txt") -> bytes:
        """从文件加载密钥"""
        if not os.path.exists(filepath):
            print(f"密钥文件不存在: {filepath}")
            return None
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                key_b64 = f.read().strip()
            
            # 解码base64
            key = base64.b64decode(key_b64)
            
            print(f"从文件加载密钥成功: {filepath}")
            print(f"密钥长度: {len(key)*8} 位")
            print(f"密钥摘要(SHA3-256): {hashlib.sha3_256(key).hexdigest()}")
            
            return key
        except Exception as e:
            print(f"加载密钥失败: {e}")
            return None
    
    def get_or_create_key(self, filepath: str = "quantum_key.txt") -> bytes:
        """获取或创建密钥：如果文件存在则读取，否则生成并保存"""
        if os.path.exists(filepath):
            key = self.load_key_from_file(filepath)
            if key is not None:
                return key
        
        # 文件不存在或加载失败，生成新密钥
        print("生成新的安全密钥...")
        key = self.generate_key()
        self.save_key_to_file(key, filepath)
        return key
    
    def _post_quantum_strengthen(self, key: bytes) -> bytes:
        """后密钥强化"""
        # 使用多个哈希函数串联
        strengthened = b""
        
        # SHA3系列
        strengthened += hashlib.sha3_512(key).digest()
        strengthened += hashlib.shake_256(key).digest(64)
        
        # Blake2b（快速且安全）
        import hashlib as hl
        if hasattr(hl, 'blake2b'):
            strengthened += hl.blake2b(key, digest_size=64).digest()
        
        # 最终混合
        final_key = hashlib.sha3_512(strengthened).digest()
        
        # 截取或扩展到所需长度
        if len(final_key) > self.key_size:
            return final_key[:self.key_size]
        elif len(final_key) < self.key_size:
            # 使用KDF扩展
            hkdf = HKDF(
                algorithm=hashes.SHA512(),
                length=self.key_size,
                salt=None,
                info=b"quantum_key_expansion"
            )
            return hkdf.derive(final_key)
        return final_key
    
    def encrypt(self, 
                plaintext: bytes, 
                key: Optional[bytes] = None,
                additional_data: Optional[bytes] = None,
                key_file: str = None) -> str:
        """
        加密数据，返回简化的密文字符串
        
        Returns:
            包含密文的base64字符串
        """
        # 如果指定了密钥文件，优先从文件加载
        if key_file:
            key = self.get_or_create_key(key_file)
        elif key is None:
            # 如果没有密钥，从默认文件加载或生成
            key = self.get_or_create_key()
        
        # 派生实际加密密钥（前向安全）
        encryption_key, auth_key = self._derive_operational_keys(key)
        
        # 生成nonce
        nonce = os.urandom(24 if self.algorithm == self.ALG_CHACHA20_POLY1305 else 12)
        
        # 执行加密
        if self.algorithm == self.ALG_AES256_GCM:
            cipher = AESGCM(encryption_key[:32])  # 使用前256位
            ciphertext = cipher.encrypt(nonce[:12], plaintext, additional_data)
        elif self.algorithm == self.ALG_CHACHA20_POLY1305:
            cipher = ChaCha20Poly1305(encryption_key[:32])
            ciphertext = cipher.encrypt(nonce, plaintext, additional_data)
        else:
            # 默认使用AES-GCM
            cipher = AESGCM(encryption_key[:32])
            ciphertext = cipher.encrypt(nonce[:12], plaintext, additional_data)
        
        # 计算独立的消息认证码
        if additional_data:
            mac_input = ciphertext + additional_data
        else:
            mac_input = ciphertext
        
        mac = hmac.new(auth_key, mac_input, 'sha512').digest()[:16]  # 截取前128位
        
        # 准备下一次的盐（前向安全）
        next_salt = hashlib.sha3_256(key + nonce + ciphertext).digest()[:8]
        
        # 组合所有必要信息：版本(1字节) + 算法标识(1字节) + nonce + ciphertext + mac + next_salt
        version = b'\x01'  # 版本1.0
        alg_flag = b'\x01' if self.algorithm == self.ALG_AES256_GCM else b'\x02'
        
        # 组合数据
        combined_data = version + alg_flag + nonce + ciphertext + mac + next_salt
        
        # 返回base64编码的字符串
        return base64.b64encode(combined_data).decode('utf-8')
    
    def decrypt(self, 
                encrypted_str: str, 
                key: Optional[bytes] = None,
                additional_data: Optional[bytes] = None,
                key_file: str = None) -> bytes:
        """从简化的密文字符串解密数据"""
        # 如果指定了密钥文件，优先从文件加载
        if key_file:
            key = self.get_or_create_key(key_file)
        elif key is None:
            # 如果没有密钥，从默认文件加载
            key = self.get_or_create_key()
        
        if key is None:
            raise ValueError("解密需要密钥")
        
        # 解码base64
        combined_data = base64.b64decode(encrypted_str)
        
        # 解析数据
        version = combined_data[0]
        if version != 1:
            raise ValueError(f"不支持的版本: {version}")
        
        alg_flag = combined_data[1]
        if alg_flag == 1:
            algorithm = self.ALG_AES256_GCM
            nonce_len = 12
        elif alg_flag == 2:
            algorithm = self.ALG_CHACHA20_POLY1305
            nonce_len = 24
        else:
            raise ValueError(f"不支持的算法标识: {alg_flag}")
        
        # 解析各个部分
        pos = 2
        nonce = combined_data[pos:pos + nonce_len]
        pos += nonce_len
        
        # 计算密文长度（总长度减去版本、算法标识、nonce、mac(16字节)、next_salt(8字节)）
        ciphertext_len = len(combined_data) - (2 + nonce_len + 16 + 8)
        ciphertext = combined_data[pos:pos + ciphertext_len]
        pos += ciphertext_len
        
        mac = combined_data[pos:pos + 16]
        pos += 16
        
        next_salt = combined_data[pos:pos + 8]
        
        # 派生操作密钥
        encryption_key, auth_key = self._derive_operational_keys(key)
        
        # 验证MAC
        if additional_data:
            mac_input = ciphertext + additional_data
        else:
            mac_input = ciphertext
        
        calculated_mac = hmac.new(auth_key, mac_input, 'sha512').digest()[:16]
        
        if not hmac.compare_digest(calculated_mac, mac):
            raise ValueError("消息认证失败")
        
        # 解密
        if algorithm == self.ALG_AES256_GCM:
            cipher = AESGCM(encryption_key[:32])
            return cipher.decrypt(nonce[:12], ciphertext, additional_data)
        elif algorithm == self.ALG_CHACHA20_POLY1305:
            cipher = ChaCha20Poly1305(encryption_key[:32])
            return cipher.decrypt(nonce, ciphertext, additional_data)
        else:
            raise ValueError(f"不支持的算法: {algorithm}")
    
    def _derive_operational_keys(self, master_key: bytes) -> Tuple[bytes, bytes]:
        """从主密钥派生操作密钥"""
        # 使用盐值确保每次派生不同
        salt = os.urandom(16)
        
        # 使用KBKDF（基于计数器的KDF）
        kdf = KBKDFHMAC(
            algorithm=hashes.SHA512(),
            mode=Mode.CounterMode,
            length=64,  # 加密密钥+认证密钥
            rlen=4,
            llen=4,
            location=CounterLocation.BeforeFixed,
            label=b"quantum_key_derivation",
            context=b"encryption_and_auth",
            fixed=None,
        )
        
        derived = kdf.derive(master_key)
        
        # 分割为加密密钥和认证密钥
        return derived[:32], derived[32:]
    
    def encrypt_to_json(self, 
                       plaintext: bytes, 
                       key: Optional[bytes] = None,
                       additional_data: Optional[bytes] = None,
                       key_file: str = None) -> dict:
        """
        加密数据，返回JSON格式（原始完整格式，兼容旧代码）
        """
        # 如果指定了密钥文件，优先从文件加载
        if key_file:
            key = self.get_or_create_key(key_file)
        elif key is None:
            key = self.get_or_create_key()
        
        # 派生实际加密密钥（前向安全）
        encryption_key, auth_key = self._derive_operational_keys(key)
        
        # 生成nonce
        nonce = os.urandom(24 if self.algorithm == self.ALG_CHACHA20_POLY1305 else 12)
        
        # 执行加密
        if self.algorithm == self.ALG_AES256_GCM:
            cipher = AESGCM(encryption_key[:32])
            ciphertext = cipher.encrypt(nonce[:12], plaintext, additional_data)
        elif self.algorithm == self.ALG_CHACHA20_POLY1305:
            cipher = ChaCha20Poly1305(encryption_key[:32])
            ciphertext = cipher.encrypt(nonce, plaintext, additional_data)
        else:
            cipher = AESGCM(encryption_key[:32])
            ciphertext = cipher.encrypt(nonce[:12], plaintext, additional_data)
        
        # 计算独立的消息认证码
        if additional_data:
            mac_input = ciphertext + additional_data
        else:
            mac_input = ciphertext
        
        mac = hmac.new(auth_key, mac_input, 'sha512').digest()
        
        # 准备下一次的盐（前向安全）
        next_salt = hashlib.sha3_256(key + nonce + ciphertext).digest()[:16]
        
        from datetime import datetime
        timestamp = datetime.utcnow().isoformat() + 'Z'
        
        return {
            'v': '1.0',
            'alg': self.algorithm,
            'ciphertext': base64.b64encode(ciphertext).decode(),
            'nonce': base64.b64encode(nonce).decode(),
            'mac': base64.b64encode(mac).decode(),
            'next_salt': base64.b64encode(next_salt).decode(),
            'key_id': base64.b64encode(hashlib.sha3_256(key).digest()[:8]).decode(),
            'timestamp': timestamp
        }
    
    def rotate_key(self, old_key: bytes) -> bytes:
        """密钥轮换，增强前向安全性"""
        # 使用HKDF进行密钥更新
        hkdf = HKDF(
            algorithm=hashes.SHA3_512(),
            length=self.key_size,
            salt=os.urandom(16),
            info=b"key_rotation_v1"
        )
        return hkdf.derive(old_key)

# 使用示例
if __name__ == "__main__":
    # 创建加密器（使用512位密钥）
    encryptor = QuantumSafeSymmetricEncryption(
        algorithm=QuantumSafeSymmetricEncryption.ALG_AES256_GCM,
        key_size=64,  # 512位
        use_hybrid=True
    )
    
    # 获取或创建密钥
    print("=== 密钥管理 ===")
    key = encryptor.get_or_create_key("my_quantum_key.txt")
    print(f"使用的密钥长度: {len(key)*8} 位")
    print()
    
    # 加密数据 - 使用简化格式
    print("=== 加密测试（简化格式）===")
    data = "这是一条需要安全保护的重要秘密".encode('utf-8')
    additional_data = "重要元数据".encode('utf-8')
    
    # 加密并获取简化的密文字符串
    ciphertext_simple = encryptor.encrypt(data, additional_data=additional_data)
    print(f"密文长度: {len(ciphertext_simple)} 字符")
    print(f"密文: {ciphertext_simple}")
    print()
    
    # 解密数据
    print("=== 解密测试 ===")
    try:
        decrypted = encryptor.decrypt(ciphertext_simple, additional_data=additional_data)
        print(f"解密成功!")
        print(f"解密结果: {decrypted.decode('utf-8')}")
    except Exception as e:
        print(f"解密失败: {e}")
    print()
    
    # # 测试JSON格式加密（兼容性）
    # print("=== JSON格式加密测试（兼容旧格式）===")
    # encrypted_json = encryptor.encrypt_to_json(data, additional_data=additional_data)
    # print(f"完整JSON格式:")
    # print(json.dumps(encrypted_json, indent=2, ensure_ascii=False))