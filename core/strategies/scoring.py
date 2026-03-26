import pandas as pd
from typing import Dict, Any, FrozenSet

# Контртрендовые/разворотные стратегии: скоринг «тренд vs ema50» для них не релевантен.
MEAN_REVERSION_STRATEGIES: FrozenSet[str] = frozenset({
    "Williams R",
    "WRD Reversal",
    "Funding Squeeze",
})

class SignalScorer:
    """
    Система оценки качества сигнала (0..1).
    Учитывает 4 фактора: тренд, волатильность, объём, моментум.
    """
    def __init__(self, weights: Dict[str, float] = None):
        self.weights = weights or {
            "trend": 0.3,
            "volatility": 0.2,
            "volume": 0.2,
            "momentum": 0.3
        }

    def calculate_score(self, df: pd.DataFrame, signal: Dict[str, Any]) -> float:
        if df.empty or len(df) < 50:
            return 0.0

        last_row = df.iloc[-1]
        direction = signal.get("signal")
        strategy_name = signal.get("strategy") or ""

        # 1. Trend alignment
        trend_score = 0.0
        if strategy_name in MEAN_REVERSION_STRATEGIES:
            trend_score = 0.5
        else:
            trend_col = 'ema50' if 'ema50' in df.columns else None
            if trend_col and not pd.isna(last_row.get(trend_col)):
                if direction == "LONG" and last_row['close'] > last_row[trend_col]:
                    trend_score = 1.0
                elif direction == "SHORT" and last_row['close'] < last_row[trend_col]:
                    trend_score = 1.0
            else:
                sma_50_val = df['close'].tail(50).mean()
                if direction == "LONG" and last_row['close'] > sma_50_val:
                    trend_score = 1.0
                elif direction == "SHORT" and last_row['close'] < sma_50_val:
                    trend_score = 1.0

        # 2. Volatility (ATR выше среднего за 10 свечей)
        vol_score = 0.0
        if 'atr' in df.columns and not pd.isna(last_row.get('atr')):
            atr_ma = df['atr'].tail(10).mean()
            if atr_ma > 0 and last_row['atr'] > atr_ma:
                vol_score = min(1.0, last_row['atr'] / atr_ma - 0.5)

        # 3. Volume spike
        volu_score = 0.0
        vol_avg = df['volume'].tail(20).mean()
        if vol_avg > 0:
            ratio = last_row['volume'] / vol_avg
            if ratio > 1.5:
                volu_score = 1.0
            elif ratio > 1.0:
                volu_score = 0.5

        # 4. Momentum (ROC)
        mom_score = 0.0
        if len(df) > 10:
            close_10 = df.iloc[-10]['close']
            if close_10 > 0:
                roc = ((last_row['close'] - close_10) / close_10) * 100
                if direction == "LONG" and roc > 0:
                    mom_score = min(1.0, roc / 5.0)
                elif direction == "SHORT" and roc < 0:
                    mom_score = min(1.0, abs(roc) / 5.0)

        total_score = (
            trend_score * self.weights["trend"] +
            vol_score * self.weights["volatility"] +
            volu_score * self.weights["volume"] +
            mom_score * self.weights["momentum"]
        )

        return round(total_score, 4)
