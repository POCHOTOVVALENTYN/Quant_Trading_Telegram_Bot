import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Enum, BigInteger
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
    symbol = Column(String, index=True, nullable=False)
    entry_price = Column(Float, nullable=False)
    size = Column(Float, nullable=False)
    stop_loss = Column(Float)
    take_profit = Column(Float)
    status = Column(Enum(PositionStatus), default=PositionStatus.OPEN)
    opened_at = Column(DateTime, default=datetime.datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)

class Order(Base):
    __tablename__ = "orders"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    exchange_order_id = Column(String, nullable=True)
    symbol = Column(String, nullable=False)
    order_type = Column(String, nullable=False) # market, limit, stop, trailing
    side = Column(Enum(SignalType), nullable=False) # LONG/SHORT
    price = Column(Float)
    size = Column(Float, nullable=False)
    status = Column(Enum(OrderStatus), default=OrderStatus.PENDING)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

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
    entry_price = Column(Float, nullable=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
