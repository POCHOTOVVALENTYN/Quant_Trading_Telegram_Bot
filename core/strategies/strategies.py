import pandas as pd
import numpy as np
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod

class BaseStrategy(ABC):
    @abstractmethod
    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        pass

class StrategyWRD(BaseStrategy):
    def __init__(self, atr_multiplier: float = 1.6):
        self.atr_multiplier = atr_multiplier

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if df.empty or 'atr' not in df.columns:
            return None

        last_row = df.iloc[-1]
        atr = last_row['atr']
        if pd.isna(atr): return None

        daily_range = last_row['high'] - last_row['low']
        vr = daily_range / atr

        if vr > self.atr_multiplier:
            close = last_row['close']
            high = last_row['high']
            low = last_row['low']
            
            top_20_level = high - (daily_range * 0.2)
            bottom_20_level = low + (daily_range * 0.2)
            
            if close > top_20_level:
                return {"strategy": "WRD", "signal": "LONG", "entry_price": close, "confidence": vr}
            elif close < bottom_20_level:
                return {"strategy": "WRD", "signal": "SHORT", "entry_price": close, "confidence": vr}
        return None

class StrategyATRBreakout(BaseStrategy):
    def __init__(self, period: int = 20, multiplier: float = 0.5):
        self.period = period
        self.multiplier = multiplier

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < self.period + 1 or 'atr' not in df.columns:
            return None

        last_row = df.iloc[-1]
        close = last_row['close']
        atr = last_row['atr']
        
        # Берем данные ДО текущей свечи (индексы -21 до -1)
        prev_df = df.iloc[-(self.period+1):-1]
        highest = prev_df['high'].max()
        lowest = prev_df['low'].min()

        if close > (highest + self.multiplier * atr):
            return {"strategy": "ATR Breakout", "signal": "LONG", "entry_price": close, "confidence": 0.7}
        elif close < (lowest - self.multiplier * atr):
            return {"strategy": "ATR Breakout", "signal": "SHORT", "entry_price": close, "confidence": 0.7}
        return None

class StrategyMATrend(BaseStrategy):
    def __init__(self, fast_ma: int = 20, slow_ma: int = 50):
        self.fast_ma = fast_ma
        self.slow_ma = slow_ma

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < self.slow_ma:
            return None
        
        # Рассчитываем MA внутри если их нет
        df = df.copy()
        df['fast_ma'] = df['close'].rolling(window=self.fast_ma).mean()
        df['slow_ma'] = df['close'].rolling(window=self.slow_ma).mean()
        
        last_row = df.iloc[-1]
        if pd.isna(last_row['fast_ma']) or pd.isna(last_row['slow_ma']):
            return None

        if last_row['fast_ma'] > last_row['slow_ma'] and last_row['close'] > last_row['fast_ma']:
            return {"strategy": "MA Trend", "signal": "LONG", "entry_price": last_row['close'], "confidence": 0.6}
        elif last_row['fast_ma'] < last_row['slow_ma'] and last_row['close'] < last_row['fast_ma']:
            return {"strategy": "MA Trend", "signal": "SHORT", "entry_price": last_row['close'], "confidence": 0.6}
        return None

class StrategyDonchian(BaseStrategy):
    def __init__(self, period: int = 20):
        self.period = period

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < self.period + 1:
            return None
        
        close = df.iloc[-1]['close']
        prev_df = df.iloc[-(self.period+1):-1]
        
        if close > prev_df['high'].max():
            return {"strategy": "Donchian", "signal": "LONG", "entry_price": close, "confidence": 0.8}
        elif close < prev_df['low'].min():
            return {"strategy": "Donchian", "signal": "SHORT", "entry_price": close, "confidence": 0.8}
        return None

class StrategyMomentum(BaseStrategy):
    def __init__(self, period: int = 10, threshold: float = 3.0):
        self.period = period
        self.threshold = threshold

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < self.period:
            return None
        
        current_close = df.iloc[-1]['close']
        past_close = df.iloc[-(self.period+1)]['close']
        
        # Rate of Change
        roc = ((current_close - past_close) / past_close) * 100
        
        if roc > self.threshold:
            return {"strategy": "Momentum", "signal": "LONG", "entry_price": current_close, "confidence": abs(roc)/10}
        elif roc < -self.threshold:
            return {"strategy": "Momentum", "signal": "SHORT", "entry_price": current_close, "confidence": abs(roc)/10}
        return None

class StrategyPullback(BaseStrategy):
    def __init__(self, ma_period: int = 20):
        self.ma_period = ma_period

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < 50: # Нужно для тренда
            return None
            
        df = df.copy()
        df['sma'] = df['close'].rolling(window=self.ma_period).mean()
        
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]
        
        if pd.isna(last_row['sma']): return None
        
        # Условие: Тренд выше SMA и цена коснулась MA (или рядом) и развернулась
        if last_row['close'] > last_row['sma'] and prev_row['low'] <= prev_row['sma'] and last_row['close'] > prev_row['close']:
            return {"strategy": "Pullback", "signal": "LONG", "entry_price": last_row['close'], "confidence": 0.65}
        elif last_row['close'] < last_row['sma'] and prev_row['high'] >= prev_row['sma'] and last_row['close'] < prev_row['close']:
            return {"strategy": "Pullback", "signal": "SHORT", "entry_price": last_row['close'], "confidence": 0.65}
        return None

class StrategyVolContraction(BaseStrategy):
    """3. Volatility Contraction Breakout"""
    def __init__(self, period_fast: int = 5, period_slow: int = 20, threshold: float = 0.6):
        self.period_fast = period_fast
        self.period_slow = period_slow
        self.threshold = threshold

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < self.period_slow + 10: return None
        
        atr_fast = df['atr'].iloc[-1]
        atr_slow = df['atr'].iloc[-20:-1].mean()
        
        # Сжатие волатильности
        if atr_fast < (atr_slow * self.threshold):
            # Пробой максимума за 10 дней
            highest_10 = df['high'].iloc[-11:-1].max()
            lowest_10 = df['low'].iloc[-11:-1].min()
            
            if df.iloc[-1]['close'] > highest_10:
                return {"strategy": "Vol Contraction", "signal": "LONG", "entry_price": df.iloc[-1]['close'], "confidence": 0.85}
            elif df.iloc[-1]['close'] < lowest_10:
                return {"strategy": "Vol Contraction", "signal": "SHORT", "entry_price": df.iloc[-1]['close'], "confidence": 0.85}
        return None

class StrategyRangeExpansion(BaseStrategy):
    """8. Range Expansion Breakout"""
    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < 21: return None
        
        current_range = df.iloc[-1]['high'] - df.iloc[-1]['low']
        avg_range = (df['high'] - df['low']).iloc[-21:-1].mean()
        
        if current_range > (1.5 * avg_range):
            close = df.iloc[-1]['close']
            if close > df.iloc[-1]['open']:
                return {"strategy": "Range Expansion", "signal": "LONG", "entry_price": close, "confidence": 0.75}
            else:
                return {"strategy": "Range Expansion", "signal": "SHORT", "entry_price": close, "confidence": 0.75}
        return None

class StrategyOpeningRange(BaseStrategy):
    """9. Opening Range Breakout (имитация для крипто 24/7 - пробой диапазона за последние 3 часа)"""
    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < 180: return None # Допустим работаем на 1m
        
        # Берем диапазон последних 30 свечей как "открытие" текущей сессии
        opening_high = df['high'].iloc[-60:-30].max()
        opening_low = df['low'].iloc[-60:-30].min()
        current_close = df.iloc[-1]['close']
        
        if current_close > opening_high:
             return {"strategy": "Opening Range", "signal": "LONG", "entry_price": current_close, "confidence": 0.7}
        elif current_close < opening_low:
             return {"strategy": "Opening Range", "signal": "SHORT", "entry_price": current_close, "confidence": 0.7}
        return None

class StrategyWideRangeReversal(BaseStrategy):
    """12. Wide Range Reversal"""
    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < 5: return None
        
        prev_candle = df.iloc[-2]
        curr_candle = df.iloc[-1]
        
        # Предыдущая свеча была WRD вниз (Close near low)
        prev_range = prev_candle['high'] - prev_candle['low']
        if prev_range > (1.5 * df['atr'].iloc[-2]):
            if prev_candle['close'] < (prev_candle['low'] + prev_range * 0.2):
                # ТЕКУЩАЯ свеча - бычий разворот
                if curr_candle['close'] > prev_candle['high']:
                    return {"strategy": "WRD Reversal", "signal": "LONG", "entry_price": curr_candle['close'], "confidence": 0.9}
        
        # Медвежий разворот
        if prev_range > (1.5 * df['atr'].iloc[-2]):
            if prev_candle['close'] > (prev_candle['high'] - prev_range * 0.2):
                if curr_candle['close'] < prev_candle['low']:
                    return {"strategy": "WRD Reversal", "signal": "SHORT", "entry_price": curr_candle['close'], "confidence": 0.9}
        return None

class StrategyRuleOf7:
    @staticmethod
    def calculate_targets(high: float, low: float) -> Dict[str, float]:
        L = high - low
        targets = {
            "Цель 1 (7/4)": low + L * (7.0 / 4.0),
            "Цель 2 (7/3)": low + L * (7.0 / 3.0),
            "Цель 3 (7/2)": low + L * (7.0 / 2.0),
        }
        return targets
