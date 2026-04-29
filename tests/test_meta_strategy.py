import pandas as pd

from core.strategies.meta_strategy import MetaStrategy
from core.strategies.strategies import (
    StrategyDonchian,
    StrategyFundingSqueeze,
    StrategyMATrend,
    StrategyPullback,
    StrategyVolContraction,
    StrategyWRD,
    StrategyWideRangeReversal,
    StrategyWilliamsR,
)


def _df(adx: float, close: float = 100.0, atr: float = 1.0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "close": close,
                "atr": atr,
                "adx": adx,
                "ema50": close - 1.0,
                "ema200": close - 2.0,
            }
        ]
    )


def test_meta_strategy_detects_trend_regime():
    meta = MetaStrategy(adx_trend_min=22.0, adx_flat_max=18.0)

    regime = meta.detect_market_regime(_df(adx=28.0, close=105.0, atr=1.2))

    assert regime == "TREND"


def test_meta_strategy_detects_range_regime():
    meta = MetaStrategy(adx_trend_min=22.0, adx_flat_max=18.0)

    regime = meta.detect_market_regime(_df(adx=12.0, close=100.0, atr=0.2))

    assert regime == "RANGE"


def test_meta_strategy_selects_trend_bucket():
    meta = MetaStrategy()
    strategies = [
        StrategyDonchian(),
        StrategyMATrend(),
        StrategyPullback(),
        StrategyVolContraction(lookback=25),
        StrategyWRD(),
        StrategyWilliamsR(),
        StrategyWideRangeReversal(),
        StrategyFundingSqueeze(),
    ]

    selection = meta.select_strategies(_df(adx=30.0, close=110.0, atr=1.5), strategies)
    names = {type(s).__name__ for s in selection.strategies}

    assert selection.regime == "TREND"
    assert "StrategyDonchian" in names
    assert "StrategyMATrend" in names
    assert "StrategyWRD" in names
    assert "StrategyWilliamsR" not in names


def test_meta_strategy_selects_range_bucket():
    meta = MetaStrategy()
    strategies = [
        StrategyDonchian(),
        StrategyMATrend(),
        StrategyPullback(),
        StrategyVolContraction(lookback=25),
        StrategyWRD(),
        StrategyWilliamsR(),
        StrategyWideRangeReversal(),
        StrategyFundingSqueeze(),
    ]

    selection = meta.select_strategies(_df(adx=10.0, close=100.0, atr=0.2), strategies)
    names = {type(s).__name__ for s in selection.strategies}

    assert selection.regime == "RANGE"
    assert "StrategyWilliamsR" in names
    assert "StrategyWideRangeReversal" in names
    assert "StrategyFundingSqueeze" in names
    assert "StrategyDonchian" not in names
