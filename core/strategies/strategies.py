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
        # Нам нужно 1440 свечей для 24ч диапазона (на 1m TF)
        if len(df) < 1440 or 'atr' not in df.columns:
            return None

        last_row = df.iloc[-1]
        atr = last_row['atr']
        if pd.isna(atr): return None

        # Ищем максимум и минимум за последние 24 часа (1440 минут)
        daily_high = df['high'].tail(1440).max()
        daily_low = df['low'].tail(1440).min()
        daily_range = daily_high - daily_low
        
        vr = daily_range / atr

        if vr > self.atr_multiplier:
            close = last_row['close']
            
            top_20_level = daily_high - (daily_range * 0.2)
            bottom_20_level = daily_low + (daily_range * 0.2)
            
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
        # Обновлено на EMA
        fast_col = f'ema{self.fast_ma}'
        slow_col = f'ema{self.slow_ma}'
        
        if fast_col not in df.columns or slow_col not in df.columns or len(df) < 2:
            return None
            
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]
        
        fast_curr, slow_curr = last_row[fast_col], last_row[slow_col]
        fast_prev, slow_prev = prev_row[fast_col], prev_row[slow_col]
        
        if pd.isna(fast_curr) or pd.isna(slow_curr):
            return None

        # Логика Event-based (пересечение)
        is_cross_up = fast_prev <= slow_prev and fast_curr > slow_curr
        is_cross_down = fast_prev >= slow_prev and fast_curr < slow_curr

        if is_cross_up and last_row['close'] > fast_curr:
            return {"strategy": "MA Trend", "signal": "LONG", "entry_price": last_row['close'], "confidence": 0.8}
        elif is_cross_down and last_row['close'] < fast_curr:
            return {"strategy": "MA Trend", "signal": "SHORT", "entry_price": last_row['close'], "confidence": 0.8}
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
    """
    Momentum на базе волатильности (ATR).
    Порог срабатывания динамически подстраивается под актив.
    Дистанция за N свечей должна быть > 1.5 ATR.
    """
    def __init__(self, period: int = 10, threshold_multiplier: float = 1.5):
        self.period = period
        self.threshold_multiplier = threshold_multiplier

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        # Используем готовые atr и ma из оркестратора
        if len(df) < self.period or 'atr' not in df.columns:
            return None
        
        last_row = df.iloc[-1]
        current_close = last_row['close']
        past_close = df.iloc[-(self.period+1)]['close']
        atr = last_row['atr']
        
        # Дистанция, пройденная ценой
        distance = current_close - past_close
        
        # Динамический порог входа в USDT пунктах
        threshold = atr * self.threshold_multiplier
        
        if distance > threshold:
            return {"strategy": "Momentum ATR", "signal": "LONG", "entry_price": current_close, "confidence": 0.75}
        elif distance < -threshold:
            return {"strategy": "Momentum ATR", "signal": "SHORT", "entry_price": current_close, "confidence": 0.75}
        return None

class StrategyPullback(BaseStrategy):
    def __init__(self, ma_period: int = 20, global_period: int = 200):
        self.ma_period = ma_period
        self.global_period = global_period

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        ma_col = f'ema{self.ma_period}'
        global_col = f'ema{self.global_period}'
        
        if ma_col not in df.columns or global_col not in df.columns or len(df) < 50:
            return None
            
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]
        ma_v = last_row[ma_col]
        global_v = last_row[global_col]
        
        if pd.isna(ma_v) or pd.isna(global_v): 
            return None
        
        # Двойная валидация: проверка глобального тренда
        global_trend_up = last_row['close'] > global_v
        global_trend_down = last_row['close'] < global_v
        
        # Условие: Глобальный тренд совпадает, цена коснулась локальной MA и отскочила
        if global_trend_up and last_row['close'] > ma_v and prev_row['low'] <= prev_row[ma_col] and last_row['close'] > prev_row['close']:
            return {"strategy": "Pullback", "signal": "LONG", "entry_price": last_row['close'], "confidence": 0.75}
        elif global_trend_down and last_row['close'] < ma_v and prev_row['high'] >= prev_row[ma_col] and last_row['close'] < prev_row['close']:
            return {"strategy": "Pullback", "signal": "SHORT", "entry_price": last_row['close'], "confidence": 0.75}
        return None

class StrategyVolContraction(BaseStrategy):
    """3. Volatility Contraction Breakout (5-hour window on 1m TF)"""
    def __init__(self, period_fast: int = 5, period_slow: int = 20, threshold: float = 0.6):
        self.period_fast = period_fast
        self.period_slow = period_slow
        self.threshold = threshold

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        # Нам нужно значимое окно (например, 300 свечей = 5 часов)
        if len(df) < 300 or 'atr' not in df.columns: return None
        
        atr_fast = df['atr'].iloc[-1]
        atr_slow = df['atr'].iloc[-20:-1].mean()
        
        # Сжатие волатильности
        if atr_fast < (atr_slow * self.threshold):
            # Пробой максимума за последние 5 часов (300 минут)
            highest_5h = df['high'].iloc[-301:-1].max()
            lowest_5h = df['low'].iloc[-301:-1].min()
            
            curr_close = df.iloc[-1]['close']
            if curr_close > highest_5h:
                return {"strategy": "Vol Contraction", "signal": "LONG", "entry_price": curr_close, "confidence": 0.85}
            elif curr_close < lowest_5h:
                return {"strategy": "Vol Contraction", "signal": "SHORT", "entry_price": curr_close, "confidence": 0.85}
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

class StrategyBollingerClusters(BaseStrategy):
    """
    Стратегия из статьи №12: Bollinger Bands + RSI + CSI Clusters.
    Использует отклонение от полос в сочетании с кластерным подтверждением силы.
    """
    def __init__(self, bb_period: int = 40, rsi_limit: float = 60, min_cluster: int = 3):
        self.bb_period = bb_period
        self.rsi_limit = rsi_limit
        self.min_cluster = min_cluster

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        # Нужно как минимум 450 свечей для надежного индикатора CSI/RSI Slow
        if len(df) < 450: 
            return None
        
        # Предполагаем, что индикаторы уже рассчитаны в df оркестратором (или рассчитываем здесь)
        # Если их нет, стратегия не сработает
        required = ['upper', 'lower', 'RSI', 'CSI']
        if not all(col in df.columns for col in required):
            return None
            
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]
        
        # 1. Проверка Кластера (Упрощенно: последние N свечей имеют одинаковый знак CSI)
        recent_csi = df['CSI'].tail(self.min_cluster)
        is_bull_cluster = (recent_csi > 0).all()
        is_bear_cluster = (recent_csi < 0).all()

        # 2. Условия LONG
        long_cond = (
            last_row['close'] < last_row['lower'] and
            last_row['CSI'] > 0 and 
            last_row['CSI'] > prev_row['CSI'] and
            is_bull_cluster and 
            last_row['RSI'] < self.rsi_limit
        )

        # 3. Условия SHORT
        short_cond = (
            last_row['close'] > last_row['upper'] and
            last_row['CSI'] < 0 and 
            last_row['CSI'] < prev_row['CSI'] and
            is_bear_cluster and 
            last_row['RSI'] > (100 - self.rsi_limit)
        )

        if long_cond:
            return {"strategy": "Bollinger Clusters", "signal": "LONG", "entry_price": last_row['close'], "confidence": 0.95}
        elif short_cond:
            return {"strategy": "Bollinger Clusters", "signal": "SHORT", "entry_price": last_row['close'], "confidence": 0.95}
            
        return None

class StrategyTripleSMA(BaseStrategy):
    # Название класса оставлено прежним для обратной совместимости логов
    def __init__(self, fast=9, medium=30, slow=60):
        self.fast = fast
        self.medium = medium
        self.slow = slow

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < self.slow + 1: return None
        
        # Индикаторы ema9, ema30, ema60
        required = [f'ema{self.fast}', f'ema{self.medium}', f'ema{self.slow}']
        if not all(col in df.columns for col in required): return None
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        fast_c, med_c, slow_c = curr[required[0]], curr[required[1]], curr[required[2]]
        fast_p, med_p = prev[required[0]], prev[required[1]]
        
        # Сигнал 1: Пересечение Fast/Medium (Event-driven)
        is_cross_up = fast_p <= med_p and fast_c > med_c
        is_cross_down = fast_p >= med_p and fast_c < med_c
        
        # Сигнал 2: Фильтр Medium/Slow (учет наклона)
        trend_long = slow_c < med_c
        trend_short = slow_c > med_c
        
        if is_cross_up and trend_long:
            return {"strategy": "Triple SMA Filter", "signal": "LONG", "entry_price": curr['close'], "confidence": 0.8}
        elif is_cross_down and trend_short:
            return {"strategy": "Triple SMA Filter", "signal": "SHORT", "entry_price": curr['close'], "confidence": 0.8}
        
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

def get_timeframe_seconds(tf: str) -> int:
    unit = tf[-1]
    val = int(tf[:-1])
    if unit == 'm': return val * 60
    if unit == 'h': return val * 3600
    if unit == 'd': return val * 86400
    return val # fallback to seconds if no unit
