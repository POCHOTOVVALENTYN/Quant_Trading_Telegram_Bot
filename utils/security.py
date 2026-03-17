from cryptography.fernet import Fernet
import base64
import os
from config.settings import settings

class SecurityVault:
    """
    Система безопасности (Этап 18).
    Отвечает за шифрование и дешифрование API ключей с использованием AES256 (Fernet в python'е реализует AES128 CBC или 256).
    """
    def __init__(self, key: str = None):
        """
        Инициализация шифратора. Требует 32-байтного url-safe base64 ключа.
        Если ключ не передан в настройках, он будет сгенерирован (разово).
        """
        raw_key = key or settings.encryption_key
        
        # Если ключа нет (пустая строка), генерируем временный для запуска.
        # В продакшене ключ должен быть жестко прописан в ENV!
        if not raw_key:
            raw_key = Fernet.generate_key().decode('utf-8')
            # Выводим предупреждение в логи, что ключ сгенерирован налету
            from utils.logger import app_logger
            app_logger.warning(
                "ВНИМАНИЕ! Ключ шифрования не найден в .env. "
                "Сгенерирован временный ключ. "
                "Перезапуск бота без сохранения этого ключа сделает базу БД с ключами нечитаемой."
            )
            settings.encryption_key = raw_key
            
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
