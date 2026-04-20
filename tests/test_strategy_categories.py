import pandas as pd

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


def _base_df(rows: int = 40) -> pd.DataFrame:
    data = []
    for i in range(rows):
        close = 100.0 + i * 0.4
        data.append(
            {
                "open": close - 0.3,
                "high": close + 0.8,
                "low": close - 0.8,
                "close": close,
                "volume": 1000 + i * 10,
                "vol_ma20": 1000.0,
                "atr": 1.0,
                "ema20": close - 0.5,
                "ema50": close - 1.0,
                "ema200": close - 2.0,
                "adx": 25.0,
                "RSI_fast": 55.0,
                "williams_r": -50.0,
                "funding_rate": 0.0,
            }
        )
    return pd.DataFrame(data)


def test_schwager_strategy_modules_expose_entry_and_exit_logic():
    df = _base_df()
    strategies = [
        StrategyDonchian(),
        StrategyWRD(),
        StrategyMATrend(),
        StrategyPullback(),
        StrategyVolContraction(lookback=25),
        StrategyWideRangeReversal(),
        StrategyWilliamsR(),
        StrategyFundingSqueeze(),
    ]

    for strategy in strategies:
        assert callable(strategy.evaluate)
        assert callable(strategy.exit_signal)
        assert strategy.exit_signal(df, "LONG") is None or isinstance(strategy.exit_signal(df, "LONG"), dict)


def test_donchian_uses_previous_channel_without_lookahead_bias():
    df = _base_df(25)
    # Предыдущий максимум канала = 109.6. Последний бар закрывается выше него.
    df.loc[:, "high"] = df["close"] + 0.2
    df.loc[:, "low"] = df["close"] - 0.2
    df.loc[:, "vol_ma20"] = 500.0
    df.loc[:, "volume"] = 1000.0
    df.loc[:, "atr"] = 1.0
    df.loc[:, "adx"] = 30.0
    df.loc[:, "ema50"] = df["close"] - 1.0
    df.loc[:, "ema200"] = df["close"] - 2.0
    df.iloc[-1, df.columns.get_loc("close")] = df.iloc[-2]["high"] + 0.5
    df.iloc[-1, df.columns.get_loc("high")] = df.iloc[-1]["close"] + 0.1
    df.iloc[-1, df.columns.get_loc("low")] = df.iloc[-1]["close"] - 0.3

    signal = StrategyDonchian(period=20).evaluate(df)

    assert signal is not None
    assert signal["strategy"] == "Donchian"
    assert signal["signal"] == "LONG"


def test_ma_trend_exit_logic_triggers_on_closed_bar_crossback():
    df = _base_df(5)
    df.loc[3, "ema20"] = 105.0
    df.loc[3, "ema50"] = 104.0
    df.loc[4, "ema20"] = 103.0
    df.loc[4, "ema50"] = 104.0
    df.loc[4, "close"] = 103.5

    exit_signal = StrategyMATrend().exit_signal(df, "LONG")

    assert exit_signal is not None
    assert exit_signal["exit"] is True


def test_williams_r_generates_long_mean_reversion_signal():
    df = _base_df(25)
    df.loc[23, "williams_r"] = -85.0
    df.loc[24, "williams_r"] = -75.0
    df.loc[24, "RSI_fast"] = 40.0
    df.loc[24, "adx"] = 15.0
    df.loc[24, "ema50"] = df.loc[24, "close"] + 0.5
    df.loc[24, "ema200"] = df.loc[24, "close"] + 1.0

    signal = StrategyWilliamsR().evaluate(df)

    assert signal is not None
    assert signal["strategy"] == "Williams R"
    assert signal["signal"] == "LONG"


def test_funding_squeeze_generates_short_signal_on_extreme_long_crowding():
    df = _base_df(20)
    df.loc[18, "RSI_fast"] = 75.0
    df.loc[19, "RSI_fast"] = 65.0
    df.loc[19, "funding_rate"] = 0.02
    df.loc[19, "adx"] = 12.0
    df.loc[19, "ema50"] = df.loc[19, "close"] - 0.2
    df.loc[19, "ema200"] = df.loc[19, "close"] - 0.4

    signal = StrategyFundingSqueeze().evaluate(df)

    assert signal is not None
    assert signal["strategy"] == "Funding Squeeze"
    assert signal["signal"] == "SHORT"


def test_vol_contraction_exit_logic_triggers_on_reversal_through_ema():
    df = _base_df(25)
    df.loc[24, "close"] = 100.0
    df.loc[24, "ema20"] = 101.5
    df.loc[24, "atr"] = 1.0

    exit_signal = StrategyVolContraction(lookback=25).exit_signal(df, "LONG")

    assert exit_signal is not None
    assert exit_signal["exit"] is True
