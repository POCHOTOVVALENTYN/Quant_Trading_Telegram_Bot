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
    position_size_usdt: float = 0.0   # Фиксированный объём позиции в USDT (0 = авто через % маржи)
    apply_new_entry_rules_after_flat: bool = False  # Включать новые правила только после полного закрытия текущих позиций
    allowed_position_side: str = "BOTH"  # LONG | SHORT | BOTH
    use_daily_timeframe_filter: bool = True   # Включать 1D-фильтр старшего тренда
    daily_filter_ema_period: int = 200        # Базовый EMA для 1D-фильтра
    # Роутер режима рынка (ADX): тренд vs флэт — разные наборы стратегий
    strategy_regime_routing_enabled: bool = True
    regime_adx_trend_min: float = 22.0        # ADX >= порога → режим тренда
    regime_adx_range_max: float = 18.0      # ADX <= порога → режим флэта
    # Extended regime detection thresholds
    regime_atr_high_percentile: float = 75.0  # ATR percentile above this → HIGH volatility
    regime_atr_low_percentile: float = 25.0   # ATR percentile below this → LOW volatility
    regime_atr_lookback: int = 30             # Bars for ATR percentile calculation
    regime_funding_extreme: float = 0.001     # |funding| > 0.1% → EXTREME
    watchdog_soft_floor_seconds: int = 120
    watchdog_hard_floor_seconds: int = 300
    # Session risk multipliers (used in Phase 1C)
    session_asia_risk_mult: float = 0.5
    session_volatility_high_risk_mult: float = 0.7
    session_funding_extreme_risk_mult: float = 0.8

    # Strategy-regime matrix (JSON string: strategy -> list of allowed regime combos)
    # Each entry: {"strategy": "name", "trend": ["TREND","NEUTRAL"], "volatility": ["*"], "funding": ["*"]}
    # "*" means any value is allowed; omitted key means any value is allowed.
    strategy_regime_matrix: str = ""  # empty = use built-in defaults
    
    # Micro-account mode (auto-enables conservative rules when balance < threshold)
    micro_account_mode: bool = True
    micro_account_threshold: float = 20.0  # USD — below this, force max_open_trades=1
    min_r_multiple_after_fees: float = 1.5  # skip entry if expected R after commissions < this
    maker_fee_pct: float = 0.0002  # 0.02% Binance Futures maker
    taker_fee_pct: float = 0.0005  # 0.05% Binance Futures taker

    # Торговые настройки
    leverage: int = 10
    sl_long_pct: float = 0.015        # 1.5% fallback (ATR-based stop is primary)
    sl_short_pct: float = 0.015       # 1.5% fallback
    tp_pct: float = 0.03              # 3% fallback TP
    sl_correction_enabled: bool = True
    
    # Система усреднений
    averaging_enabled: bool = False
    averaging_step_pct: float = 0.02   # 2%
    averaging_multiplier: float = 1.0  # 1x предыдущего объема
    averaging_max_steps: int = 0       # 0 - выкл
    pyramiding_enabled: bool = False   # Доливка отключена до отдельного включения

    # External AI Integration — per-provider keys
    # Cascade order: comma-separated list of backends to try in order
    ai_cascade_order: str = ""

    # Groq (free tier: 30 RPM, Llama 3.3 70B ultra-fast)
    groq_api_key: str = ""
    groq_model: str = "llama-3.3-70b-versatile"

    # xAI / Grok ($25/mo credit)
    grok_api_key: str = ""
    grok_api_url: str = "https://api.x.ai"
    grok_model: str = "grok-3-mini"

    # Google Gemini (free: 15 RPM / 1M TPM)
    gemini_api_key: str = ""
    gemini_model: str = "gemini-2.0-flash"

    # OpenRouter (aggregator, some free models)
    openrouter_api_key: str = ""
    openrouter_model: str = "meta-llama/llama-3.1-8b-instruct"

    # Legacy single-provider (backward compat, overridden by cascade if set)
    # For local Ollama: EXTERNAL_AI_BACKEND=ollama, EXTERNAL_AI_URL=http://ollama:11434
    external_ai_backend: str = ""
    external_ai_url: str = "http://ollama:11434"
    external_ai_api_key: str = ""
    external_ai_model: str = "llama3.1:8b"

    # ML Signal Validator
    ml_validator_enabled: bool = False       # Master switch for ML filtering
    ml_validator_shadow_mode: bool = True    # True = log only, False = actually filter
    ml_validator_threshold: float = 0.60     # Minimum confidence to pass (when not in shadow mode)
    signal_score_threshold: float = 0.50
    ai_win_prob_threshold: float = 0.50

    # Self-learning scorer
    scoring_learner_enabled: bool = True
    scoring_weights_file: str = "data/learned_weights.json"
    scoring_learn_interval_hours: int = 6

    # ─── Trade Management (loss minimization) ───
    trade_mgmt_enabled: bool = True
    # Time stop: max bars with no progress before forced exit
    time_stop_max_bars_breakout: int = 24
    time_stop_max_bars_trend: int = 36
    time_stop_max_bars_mean_reversion: int = 18
    time_stop_hard_limit_bars: int = 120
    time_stop_min_profit_pct: float = 0.5  # 0.5% min profit to stay beyond soft limit
    # Break-even
    be_trigger_r: float = 1.0       # move SL to BE after 1R
    be_buffer_pct: float = 0.0004   # 0.04% buffer above entry for commissions
    be_require_confirmation: bool = True  # require ADX or breakout confirmation
    # Partial reduction
    partial_enabled: bool = True
    partial_trigger_r: float = 1.5  # reduce at 1.5R
    partial_fraction: float = 0.33  # close 33% of position
    # Setup invalidation exit
    invalidation_exit_enabled: bool = True
    # Confirmation-based trailing (advances trailing only on confirmed bars)
    confirmed_trailing_enabled: bool = True
    confirmed_trailing_min_r: float = 1.0  # start confirmed trailing only after 1R

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
