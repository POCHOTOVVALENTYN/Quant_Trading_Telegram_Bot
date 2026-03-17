from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr
from typing import Optional

class Settings(BaseSettings):
    telegram_bot_token: SecretStr
    admin_user_ids: str = ""
    
    # Binance API
    api_key_binance: Optional[str] = None
    secret_api_key_binance: Optional[str] = None
    
    # Database
    database_url: str = "postgresql+asyncpg://quant_user:quant_password@localhost:5440/quant_db"
    
    # Redis
    redis_url: str = "redis://localhost:6379"
    
    # Security
    encryption_key: str = "" # AES256 key base64 URL safe
    
    # Trading Defaults
    default_exchange: str = "binance"
    testnet: bool = True
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
