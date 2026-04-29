from cryptography.fernet import Fernet
import base64
import os
from config.settings import settings

class SecurityVault:
    def __init__(self, key: str = None):
        raw_key = key or settings.encryption_key
        
        if not raw_key:
            raise ValueError("CRITICAL SECURITY ERROR: ENCRYPTION_KEY is missing in .env!")
            
        self.fernet = Fernet(raw_key.encode('utf-8'))

    def encrypt(self, plain_text: str) -> str:
        """Шифрует строку."""
        if not plain_text:
            return ""
        encrypted_bytes = self.fernet.encrypt(plain_text.encode('utf-8'))
        return encrypted_bytes.decode('utf-8')

    def decrypt(self, encrypted_text: str) -> str:
        """Дешифрует строку."""
        if not encrypted_text:
            return ""
        decrypted_bytes = self.fernet.decrypt(encrypted_text.encode('utf-8'))
        return decrypted_bytes.decode('utf-8')

# Singleton для использования во всем приложении
vault = SecurityVault()
