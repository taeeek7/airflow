from cryptography.fernet import Fernet
import json

# 암호화/복호화 클래스
class Encryption:
    def __init__(self, key=None):
        # 키가 없으면 새로 생성
        self.key = key if key else Fernet.generate_key()
        self.fernet = Fernet(self.key)
    
    def encrypt(self, data):
        # JSON으로 변환
        json_str = json.dumps(data)
        # AES 암호화 후 URL-safe base64로 인코딩
        return self.fernet.encrypt(json_str.encode()).decode()
    
    def decrypt(self, token):
        # AES 복호화 후 JSON 파싱
        json_str = self.fernet.decrypt(token.encode()).decode()
        return json.loads(json_str)