import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Enum, BigInteger, Index, UniqueConstraint
from sqlalchemy.orm import relationship
import enum

from database.session import Base

class SignalType(str, enum.Enum):
    LONG = "LONG"
    SHORT = "SHORT"

class OrderStatus(str, enum.Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"

class PositionStatus(str, enum.Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(BigInteger, unique=True, index=True, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    
    api_keys = relationship("ApiKey", back_populates="user", cascade="all, delete-orphan")

class ApiKey(Base):
    __tablename__ = "api_keys"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    exchange = Column(String, default="binance")
    api_key_encrypted = Column(String, nullable=False)
    secret_key_encrypted = Column(String, nullable=False)
    
    user = relationship("User", back_populates="api_keys")

class Position(Base):
    __tablename__ = "positions"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    signal_id = Column(Integer, ForeignKey("signals.id"), nullable=True)
    symbol = Column(String, index=True, nullable=False)
    side = Column(Enum(SignalType), nullable=True)  # LONG/SHORT; nullable for legacy rows
    entry_price = Column(Float, nullable=False)
    size = Column(Float, nullable=False)
    stop_loss = Column(Float)
    take_profit = Column(Float)
    status = Column(Enum(PositionStatus), default=PositionStatus.OPEN)
    opened_at = Column(DateTime, default=datetime.datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)
    realized_pnl = Column(Float, default=0.0)

class PnLRecord(Base):
    __tablename__ = "pnl_records"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    symbol = Column(String, nullable=False)
    pnl_usd = Column(Float, nullable=False)
    pnl_pct = Column(Float, nullable=False)
    leverage = Column(Integer, default=1)
    closed_at = Column(DateTime, default=datetime.datetime.utcnow)
    reason = Column(String) # STOP, TAKE, MANUAL, TIME

class SettingsPreset(Base):
    __tablename__ = "settings_presets"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False) # "Conservative", "Aggressive"
    
    # Торговые настройки
    leverage = Column(Integer, default=10)
    sl_long_pct = Column(Float, default=0.003)
    sl_short_pct = Column(Float, default=0.003)
    tp_pct = Column(Float, default=0.01) # 1% дефолт
    max_open_positions = Column(Integer, default=5)
    signal_expiry_seconds = Column(Integer, default=120)
    
    # Система усреднений (Этап 7 - CID Ref)
    averaging_enabled = Column(Boolean, default=False)
    averaging_step_pct = Column(Float, default=0.02) # Шаг 2%
    averaging_multiplier = Column(Float, default=1.0) # 1x добавка
    averaging_max_steps = Column(Integer, default=0) # 0 - выкл
    
    is_active = Column(Boolean, default=False)

class Order(Base):
    __tablename__ = "orders"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    exchange_order_id = Column(String, nullable=True)
    client_order_id = Column(String, nullable=True, index=True)  # биржевой clientOrderId
    position_id = Column(Integer, ForeignKey("positions.id"), nullable=True, index=True)
    symbol = Column(String, nullable=False)
    order_type = Column(String, nullable=False) # market, limit, stop, trailing
    side = Column(Enum(SignalType), nullable=False) # LONG/SHORT
    price = Column(Float)
    size = Column(Float, nullable=False)
    status = Column(Enum(OrderStatus), default=OrderStatus.PENDING)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    __table_args__ = (
        Index("ix_orders_exchange_order_id", "exchange_order_id"),
        UniqueConstraint("exchange_order_id", name="uq_orders_exchange_order_id"),
        UniqueConstraint("client_order_id", name="uq_orders_client_order_id"),
    )

class Signal(Base):
    __tablename__ = "signals"
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True, nullable=False)
    signal_type = Column(Enum(SignalType), nullable=False)
    strategy = Column(String) # e.g. "WRD"
    confidence = Column(Float, nullable=True) # Это Score (0-1)
    win_prob = Column(Float, nullable=True)   # AI Win Probability (0-1)
    expected_return = Column(Float, nullable=True) # Expected Return %
    risk = Column(Float, nullable=True)       # Risk Level %
    status = Column(String, default="PENDING") # PENDING, EXECUTED, FAILED
    entry_price = Column(Float, nullable=True)
    stop_loss = Column(Float, nullable=True)
    take_profit = Column(Float, nullable=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)


class AIDecisionLog(Base):
    """Stores every External AI decision for analytics and learning."""
    __tablename__ = "ai_decision_logs"

    id = Column(Integer, primary_key=True, index=True)
    signal_id = Column(Integer, ForeignKey("signals.id"), nullable=True, index=True)
    symbol = Column(String, index=True, nullable=False)
    strategy = Column(String, nullable=True)
    provider = Column(String, nullable=False)
    recommendation = Column(String, nullable=False)  # ENTER / SKIP / PASS
    confidence = Column(Float, nullable=True)
    reasoning = Column(String, nullable=True)
    score = Column(Float, nullable=True)
    win_prob = Column(Float, nullable=True)
    latency_ms = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
