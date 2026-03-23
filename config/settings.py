from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr
from typing import Optional

class Settings(BaseSettings):
    telegram_bot_token: SecretStr
    admin_user_ids: str = ""
    
    # Binance REAL API
    api_key_binance: Optional[str] = None
    secret_api_key_binance: Optional[str] = None
    
    # Binance TEST API (НОВОЕ)
    test_api_key_binance: Optional[str] = None
    test_secret_api_key_binance: Optional[str] = None
    
    # Database
    database_url: str = "postgresql+asyncpg://user:password@localhost:5432/dbname"
    
    # Redis
    redis_url: str = "redis://localhost:6379"
    
    # Security
    encryption_key: str = "" # AES256 key base64 URL safe
    
    # Trading Defaults
    default_exchange: str = "binance"
    testnet: bool = True
    is_trading_enabled: bool = True # Глобальный вкл/выкл
    api_timeout_seconds: float = 5.0 # Стандарт из статьи (контроль зависших запросов)
    
    # Advanced Filters (НОВОЕ)
    max_open_positions: int = 5
    signal_expiry_seconds: int = 120  # Защита от старых сигналов (120с)
    min_listing_days: int = 100       # Фильтр новых монет (100 дней)
    max_funding_rate: float = 0.01    # 1% в час (очень высокий)
    max_open_trades: int = 3          # Новый лимит: не более 3 позиций
    per_trade_margin_pct: float = 0.05 # 5% маржи на каждую новую позицию
    apply_new_entry_rules_after_flat: bool = True  # Включать новые правила только после полного закрытия текущих позиций
    
    # Референсные настройки (из скриншотов)
    leverage: int = 10
    sl_long_pct: float = 0.003        # 0.3%
    sl_short_pct: float = 0.003       # 0.3%
    tp_pct: float = 0.01              # 1% по умолчанию
    sl_correction_enabled: bool = True
    
    # Система усреднений
    averaging_enabled: bool = False
    averaging_step_pct: float = 0.02   # 2%
    averaging_multiplier: float = 1.0  # 1x предыдущего объема
    averaging_max_steps: int = 0       # 0 - выкл
    pyramiding_enabled: bool = False   # Доливка отключена до отдельного включения
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
