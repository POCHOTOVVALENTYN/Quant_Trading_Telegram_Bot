import pandas as pd
from typing import Dict, Any, FrozenSet

# Скоринг «тренд vs ema50» для них искажает смысл сетапа (контртренд / разворот).
MEAN_REVERSION_STRATEGIES: FrozenSet[str] = frozenset({
    "Williams R",
    "Bollinger Clusters",
    "WRD Reversal",
    "WRD",
    "Funding Squeeze",
})

class SignalScorer:
    """
    Система оценки сигналов (Scoring Engine).
    Каждый сигнал получает score от 0 до 1.
    """
    def __init__(self, weights: Dict[str, float] = None):
        self.weights = weights or {
            "trend": 0.3,
            "volatility": 0.2,
            "volume": 0.2,
            "momentum": 0.3
        }

    def calculate_score(self, df: pd.DataFrame, signal: Dict[str, Any]) -> float:
        """
        Score = trend_weight + volatility_weight + volume_weight + momentum_weight
        """
        if df.empty or len(df) < 50:
            return 0.0

        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]
        direction = signal.get("signal")
        strategy_name = signal.get("strategy") or ""
        
        # 1. Trend alignment (M3: используем готовый ema50 вместо дублирующего sma_50)
        trend_score = 0.0
        if strategy_name in MEAN_REVERSION_STRATEGIES:
            # Нейтральный вклад: mean-reversion не должен штрафоваться за «нет тренда по ema50».
            trend_score = 0.5
        else:
            trend_col = 'ema50' if 'ema50' in df.columns else None
            if trend_col:
                if direction == "LONG" and last_row['close'] > last_row[trend_col]:
                    trend_score = 1.0
                elif direction == "SHORT" and last_row['close'] < last_row[trend_col]:
                    trend_score = 1.0
            else:
                # Fallback без копирования df
                sma_50_val = df['close'].tail(50).mean()
                if direction == "LONG" and last_row['close'] > sma_50_val:
                    trend_score = 1.0
                elif direction == "SHORT" and last_row['close'] < sma_50_val:
                    trend_score = 1.0
            
        # 2. Volatility (Используем ATR рост)
        vol_score = 0.0
        if 'atr' in last_row:
            # Если текущий ATR больше среднего ATR за 10 свечей
            atr_ma = df['atr'].tail(10).mean()
            if last_row['atr'] > atr_ma:
                vol_score = 1.0
        
        # 3. Volume spike (Объем выше среднего)
        volu_score = 0.0
        vol_avg = df['volume'].tail(20).mean()
        if last_row['volume'] > vol_avg * 1.5:
            volu_score = 1.0
        elif last_row['volume'] > vol_avg:
            volu_score = 0.5
            
        # 4. Momentum (ROC - Rate of Change)
        mom_score = 0.0
        roc = ((last_row['close'] - df.iloc[-10]['close']) / df.iloc[-10]['close']) * 100
        if direction == "LONG" and roc > 0:
            mom_score = min(1.0, roc / 5.0) # 1.0 при 5% росте
        elif direction == "SHORT" and roc < 0:
            mom_score = min(1.0, abs(roc) / 5.0)

        total_score = (
            trend_score * self.weights["trend"] +
            vol_score * self.weights["volatility"] +
            volu_score * self.weights["volume"] +
            mom_score * self.weights["momentum"]
        )
        
        return total_score
