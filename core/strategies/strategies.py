import pandas as pd
import numpy as np
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod

class BaseStrategy(ABC):
    @abstractmethod
    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        pass


class StrategyDonchian(BaseStrategy):
    """
    Donchian Channel Breakout (Turtle Trading — Schwager, Market Wizards).
    Пробой максимума/минимума за N периодов.
    Единственная пробойная стратегия в системе — заменяет ATRBreakout, Momentum, OpeningRange.
    """
    def __init__(self, period: int = 20):
        self.period = period

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < self.period + 2 or 'atr' not in df.columns:
            return None

        close = df.iloc[-1]['close']
        prev_df = df.iloc[-(self.period + 1):-1]

        highest = prev_df['high'].max()
        lowest = prev_df['low'].min()
        atr = df.iloc[-1]['atr']
        if pd.isna(atr) or atr <= 0:
            return None

        # Подтверждение: close выше/ниже уровня + закрытие свечи за пределами канала
        if close > highest:
            strength = (close - highest) / atr
            conf = min(0.9, 0.6 + strength * 0.1)
            return {"strategy": "Donchian", "signal": "LONG", "entry_price": close, "confidence": conf}
        elif close < lowest:
            strength = (lowest - close) / atr
            conf = min(0.9, 0.6 + strength * 0.1)
            return {"strategy": "Donchian", "signal": "SHORT", "entry_price": close, "confidence": conf}
        return None


class StrategyWRD(BaseStrategy):
    """
    Wide Range Day (Schwager, Technical Analysis).
    Детектирует свечи с аномально большим диапазоном.
    Сигнал: если текущая свеча — WRD и закрылась в направлении движения,
    это подтверждает импульс (momentum confirmation).
    """
    def __init__(self, atr_multiplier: float = 1.6):
        self.atr_multiplier = atr_multiplier

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < 20 or 'atr' not in df.columns:
            return None

        last = df.iloc[-1]
        atr = last['atr']
        if pd.isna(atr) or atr <= 0:
            return None

        bar_range = last['high'] - last['low']
        if bar_range <= 0:
            return None

        # WRD: текущий диапазон бара > atr_multiplier * ATR
        if bar_range < (self.atr_multiplier * atr):
            return None

        # Направление определяется по позиции закрытия внутри бара
        body = last['close'] - last['open']
        body_ratio = abs(body) / bar_range

        # Фильтр: тело >= 50% диапазона (убираем доджи и пин-бары)
        if body_ratio < 0.5:
            return None

        conf = min(0.85, 0.6 + (bar_range / atr - self.atr_multiplier) * 0.1)

        if body > 0:
            return {"strategy": "WRD", "signal": "LONG", "entry_price": last['close'], "confidence": conf}
        else:
            return {"strategy": "WRD", "signal": "SHORT", "entry_price": last['close'], "confidence": conf}


class StrategyMATrend(BaseStrategy):
    """
    EMA Crossover с подтверждением (Schwager — тренд-следующая стратегия).
    Используем EMA 20/50 с фильтром глобального тренда EMA 200.
    """
    def __init__(self, fast_ma: int = 20, slow_ma: int = 50, global_ma: int = 200):
        self.fast_ma = fast_ma
        self.slow_ma = slow_ma
        self.global_ma = global_ma

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        fast_col = f'ema{self.fast_ma}'
        slow_col = f'ema{self.slow_ma}'
        global_col = f'ema{self.global_ma}'

        if fast_col not in df.columns or slow_col not in df.columns or len(df) < 2:
            return None

        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]

        fast_curr, slow_curr = last_row[fast_col], last_row[slow_col]
        fast_prev, slow_prev = prev_row[fast_col], prev_row[slow_col]

        if any(pd.isna([fast_curr, slow_curr, fast_prev, slow_prev])):
            return None

        is_cross_up = fast_prev <= slow_prev and fast_curr > slow_curr
        is_cross_down = fast_prev >= slow_prev and fast_curr < slow_curr

        # Фильтр глобального тренда: если EMA200 есть, используем для подтверждения
        has_global = global_col in df.columns and not pd.isna(last_row.get(global_col))
        if has_global:
            global_v = last_row[global_col]
            if is_cross_up and last_row['close'] > fast_curr and last_row['close'] > global_v:
                return {"strategy": "MA Trend", "signal": "LONG", "entry_price": last_row['close'], "confidence": 0.75}
            elif is_cross_down and last_row['close'] < fast_curr and last_row['close'] < global_v:
                return {"strategy": "MA Trend", "signal": "SHORT", "entry_price": last_row['close'], "confidence": 0.75}
        else:
            if is_cross_up and last_row['close'] > fast_curr:
                return {"strategy": "MA Trend", "signal": "LONG", "entry_price": last_row['close'], "confidence": 0.70}
            elif is_cross_down and last_row['close'] < fast_curr:
                return {"strategy": "MA Trend", "signal": "SHORT", "entry_price": last_row['close'], "confidence": 0.70}
        return None


class StrategyPullback(BaseStrategy):
    """
    Pullback Entry (Schwager — вход на откате к MA в направлении тренда).
    Условие: глобальный тренд подтверждён EMA200, цена коснулась EMA20 и отскочила.
    """
    def __init__(self, ma_period: int = 20, global_period: int = 200):
        self.ma_period = ma_period
        self.global_period = global_period

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        ma_col = f'ema{self.ma_period}'
        global_col = f'ema{self.global_period}'

        if ma_col not in df.columns or global_col not in df.columns or len(df) < self.global_period:
            return None

        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]
        ma_v = last_row[ma_col]
        global_v = last_row[global_col]

        if pd.isna(ma_v) or pd.isna(global_v):
            return None

        global_trend_up = last_row['close'] > global_v
        global_trend_down = last_row['close'] < global_v

        if global_trend_up and last_row['close'] > ma_v and prev_row['low'] <= prev_row[ma_col] and last_row['close'] > prev_row['close']:
            return {"strategy": "Pullback", "signal": "LONG", "entry_price": last_row['close'], "confidence": 0.75}
        elif global_trend_down and last_row['close'] < ma_v and prev_row['high'] >= prev_row[ma_col] and last_row['close'] < prev_row['close']:
            return {"strategy": "Pullback", "signal": "SHORT", "entry_price": last_row['close'], "confidence": 0.75}
        return None


class StrategyVolContraction(BaseStrategy):
    """
    Volatility Contraction Breakout (Schwager — сжатие → расширение).
    Когда ATR сжимается ниже 60% от среднего, а затем цена пробивает
    экстремум периода сжатия, это сигнал начала нового тренда.
    """
    def __init__(self, lookback: int = 300, contraction_ratio: float = 0.6):
        self.lookback = lookback
        self.contraction_ratio = contraction_ratio

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < self.lookback or 'atr' not in df.columns:
            return None

        # Среднее ATR за последние 20 свечей vs среднее ATR за lookback
        atr_recent = df['atr'].iloc[-20:].mean()
        atr_baseline = df['atr'].iloc[-self.lookback:-20].mean()

        if pd.isna(atr_recent) or pd.isna(atr_baseline) or atr_baseline <= 0:
            return None

        if atr_recent >= (atr_baseline * self.contraction_ratio):
            return None

        # Сжатие обнаружено — ищем пробой
        highest = df['high'].iloc[-self.lookback:-1].max()
        lowest = df['low'].iloc[-self.lookback:-1].min()
        curr_close = df.iloc[-1]['close']

        if curr_close > highest:
            return {"strategy": "Vol Contraction", "signal": "LONG", "entry_price": curr_close, "confidence": 0.80}
        elif curr_close < lowest:
            return {"strategy": "Vol Contraction", "signal": "SHORT", "entry_price": curr_close, "confidence": 0.80}
        return None


class StrategyWideRangeReversal(BaseStrategy):
    """
    Wide Range Reversal (Schwager — разворотный паттерн).
    После WRD-бара в одном направлении, следующая свеча
    полностью перекрывает его в обратном — сильный разворотный сигнал.
    """
    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < 5 or 'atr' not in df.columns:
            return None

        prev = df.iloc[-2]
        curr = df.iloc[-1]

        prev_range = prev['high'] - prev['low']
        prev_atr = df['atr'].iloc[-2]
        if pd.isna(prev_atr) or prev_atr <= 0 or prev_range <= 0:
            return None

        # Предыдущая свеча должна быть WRD (> 1.5 ATR)
        if prev_range < (1.5 * prev_atr):
            return None

        body_ratio = abs(prev['close'] - prev['open']) / prev_range

        # Бычий разворот: prev закрылся у low, curr закрылся выше prev high
        if prev['close'] < (prev['low'] + prev_range * 0.25) and body_ratio > 0.4:
            if curr['close'] > prev['high']:
                return {"strategy": "WRD Reversal", "signal": "LONG", "entry_price": curr['close'], "confidence": 0.80}

        # Медвежий разворот: prev закрылся у high, curr закрылся ниже prev low
        if prev['close'] > (prev['high'] - prev_range * 0.25) and body_ratio > 0.4:
            if curr['close'] < prev['low']:
                return {"strategy": "WRD Reversal", "signal": "SHORT", "entry_price": curr['close'], "confidence": 0.80}
        return None


class StrategyWilliamsR(BaseStrategy):
    """
    Williams %R Mean-Reversion.
    Вход при выходе из зоны перепроданности/перекупленности
    с подтверждением RSI. Контртрендовая стратегия.
    """
    def __init__(self, overbought: float = -20, oversold: float = -80):
        self.overbought = overbought
        self.oversold = oversold

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < 20 or 'williams_r' not in df.columns or 'RSI_fast' not in df.columns:
            return None

        curr = df.iloc[-1]
        prev = df.iloc[-2]

        wr_curr = curr['williams_r']
        wr_prev = prev['williams_r']
        rsi = curr['RSI_fast']

        if pd.isna(wr_curr) or pd.isna(wr_prev) or pd.isna(rsi):
            return None

        # LONG: выход из перепроданности + RSI подтверждает
        if wr_prev <= self.oversold and wr_curr > self.oversold and rsi < 45:
            return {"strategy": "Williams R", "signal": "LONG", "entry_price": curr['close'], "confidence": 0.75}

        # SHORT: выход из перекупленности + RSI подтверждает
        if wr_prev >= self.overbought and wr_curr < self.overbought and rsi > 55:
            return {"strategy": "Williams R", "signal": "SHORT", "entry_price": curr['close'], "confidence": 0.75}
        return None


class StrategyFundingSqueeze(BaseStrategy):
    """
    Крипто-специфическая стратегия: сквиз на экстремальном фандинге.
    Когда толпа перегружена в одну сторону (экстремальный funding rate),
    маркетмейкеры часто провоцируют движение в противоположную сторону.
    """
    def __init__(self, funding_threshold: float = 0.015):
        self.funding_threshold = funding_threshold

    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        if len(df) < 15 or 'funding_rate' not in df.columns or 'RSI_fast' not in df.columns:
            return None

        last = df.iloc[-1]
        prev = df.iloc[-2]

        fr = last['funding_rate']
        rsi = last['RSI_fast']
        prev_rsi = prev['RSI_fast']

        if pd.isna(fr) or pd.isna(rsi) or pd.isna(prev_rsi):
            return None

        # Толпа в лонгах (высокий positive funding) → ищем шорт
        if fr > self.funding_threshold:
            if prev_rsi >= 70 and rsi < 70:
                return {"strategy": "Funding Squeeze", "signal": "SHORT", "entry_price": last['close'], "confidence": 0.80}

        # Толпа в шортах (высокий negative funding) → ищем лонг
        elif fr < -self.funding_threshold:
            if prev_rsi <= 30 and rsi > 30:
                return {"strategy": "Funding Squeeze", "signal": "LONG", "entry_price": last['close'], "confidence": 0.80}
        return None


class StrategyRuleOf7:
    """
    Rule of 7 (Schwager): расчёт целей на основе диапазона паттерна.
    """
    @staticmethod
    def calculate_targets(high: float, low: float, direction: str = "LONG") -> Dict[str, float]:
        rng = high - low
        if rng <= 0:
            return {}

        if direction == "LONG":
            t1 = high + (rng * 0.5)
            t2 = high + (rng * 1.0)
            t3 = high + (rng * 1.5)
        else:
            t1 = low - (rng * 0.5)
            t2 = low - (rng * 1.0)
            t3 = low - (rng * 1.5)

        return {
            "Target 1 (0.5x)": round(t1, 4),
            "Target 2 (1.0x)": round(t2, 4),
            "Target 3 (1.5x)": round(t3, 4)
        }


def get_timeframe_seconds(tf: str) -> int:
    unit = tf[-1]
    val = int(tf[:-1])
    if unit == 'm': return val * 60
    if unit == 'h': return val * 3600
    if unit == 'd': return val * 86400
    return val
