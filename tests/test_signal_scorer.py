import pytest
import pandas as pd
import numpy as np
from core.strategies.scoring import SignalScorer

def test_signal_scorer_init():
    ss = SignalScorer(weights={"trend": 0.5, "volatility": 0.5, "volume": 0.0, "momentum": 0.0})
    assert ss.weights["trend"] == 0.5
    assert ss.weights["volatility"] == 0.5

def test_signal_scorer_empty_df():
    ss = SignalScorer()
    df = pd.DataFrame()
    score = ss.calculate_score(df, {"signal": "LONG"})
    assert score == 0.0

def test_signal_scorer_trend():
    # Signal says LONG. Last close = 110. SMA_50 = 100.
    # trend score = 1.0 (if close > sma_50)
    # trend weight = 0.3
    # Result should be at least 0.3
    
    # Create mock history for 60 periods to calculate SMA_50
    data = []
    for i in range(60):
        data.append({
            "timestamp": i,
            "open": 90,
            "high": 120,
            "low": 80,
            "close": 100,
            "volume": 1,
            "atr": 1
        })
    # Last row close 110
    data[-1]["close"] = 110
    # prev ROC -10 close 100 -> roc 10%
    
    df = pd.DataFrame(data)
    ss = SignalScorer()
    score = ss.calculate_score(df, {"signal": "LONG"})
    assert score >= 0.3 # trend contribution
    assert score <= 1.0

def test_signal_scorer_mean_reversion_neutral_trend():
    """Mean-reversion не штрафуется за отсутствие выравнивания с ema50."""
    data = []
    for i in range(60):
        data.append({
            "timestamp": i,
            "open": 100,
            "high": 120,
            "low": 80,
            "close": 100,
            "volume": 1,
            "atr": 1,
            "ema50": 110.0,
        })
    data[-1]["close"] = 105  # LONG ниже ema50 — для трендового сетапа trend_score был бы 0
    df = pd.DataFrame(data)
    ss = SignalScorer()
    trend_only = ss.calculate_score(df, {"signal": "LONG", "strategy": "Williams R"})
    without_name = ss.calculate_score(df, {"signal": "LONG"})
    assert trend_only >= without_name

def test_signal_scorer_short_trend():
    # Signal SHORT, last close 90, SMA_50 100
    data = []
    for i in range(60):
        data.append({
            "timestamp": i,
            "open": 100,
            "high": 120,
            "low": 80,
            "close": 110,
            "volume": 1,
            "atr": 1
        })
    data[-1]["close"] = 90
    
    df = pd.DataFrame(data)
    ss = SignalScorer()
    score = ss.calculate_score(df, {"signal": "SHORT"})
    assert score >= 0.3 # trend contribution
