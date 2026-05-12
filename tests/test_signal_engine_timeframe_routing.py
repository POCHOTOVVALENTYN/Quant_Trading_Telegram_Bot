"""Routing: signals only on 15m/1h/4h; per-strategy allowed timeframes."""
import asyncio
import unittest
from unittest.mock import MagicMock, patch, AsyncMock

import pandas as pd

from core.strategies.strategies import StrategyDonchian, StrategyPullback
from services.signal_engine.engine import TradingOrchestrator


def _ohlcv_df(n: int, step_ms: int) -> pd.DataFrame:
    base = 1_700_000_000_000
    rows = []
    for i in range(n):
        o = 100.0 + i * 0.01
        rows.append([base + i * step_ms, o, o + 1.0, o - 1.0, o + 0.5, 1000.0])
    return pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])


class TestSignalTimeframeRouting(unittest.TestCase):
    def setUp(self):
        self.market_data = MagicMock()
        self.execution_engine = MagicMock()
        self.orchestrator = TradingOrchestrator(self.market_data, self.execution_engine)
        nf = MagicMock()
        nf.is_enabled = False
        nf.should_block_entry.return_value = False
        self.orchestrator.news_filter = nf
        self.symbol = "BTC/USDT"

    def _run(self, coro):
        asyncio.run(coro)

    async def _one_candle(
        self,
        timeframe: str,
        df: pd.DataFrame,
        new_row: list,
        strategies,
    ):
        self.orchestrator.strategies = strategies
        self.orchestrator.market_history[self.symbol] = {timeframe: df}
        self.orchestrator._last_eval_candle_ts.clear()
        with patch("services.signal_engine.engine.asyncio.sleep", new=AsyncMock()):
            await self.orchestrator._process_ohlcv(self.symbol, timeframe, new_row)

    def test_5m_no_strategy_evaluate(self):
        """5m: indicators updated, no signal path."""
        df = _ohlcv_df(65, 300_000)
        last_ts = int(df.iloc[-1]["timestamp"])
        new = [last_ts + 300_000, 101.0, 102.0, 100.0, 101.5, 1000.0]
        d = StrategyDonchian()
        with patch.object(d, "evaluate") as ev:
            self._run(self._one_candle("5m", df.iloc[:-1].reset_index(drop=True), new, [d]))
        ev.assert_not_called()

    def test_1m_no_strategy_evaluate(self):
        """1m: no entries; strategies not evaluated."""
        df = _ohlcv_df(65, 60_000)
        last_ts = int(df.iloc[-1]["timestamp"])
        new = [last_ts + 60_000, 101.0, 102.0, 100.0, 101.5, 1000.0]
        d = StrategyDonchian()
        self.execution_engine.schedule_update_positions = MagicMock()
        with patch.object(d, "evaluate") as ev:
            self._run(self._one_candle("1m", df.iloc[:-1].reset_index(drop=True), new, [d]))
        ev.assert_not_called()

    def test_15m_donchian_skipped(self):
        df = _ohlcv_df(65, 900_000)
        last_ts = int(df.iloc[-1]["timestamp"])
        new = [last_ts + 900_000, 101.0, 102.0, 100.0, 101.5, 1000.0]
        d = StrategyDonchian()
        with patch.object(d, "evaluate") as ev:
            self._run(self._one_candle("15m", df.iloc[:-1].reset_index(drop=True), new, [d]))
        ev.assert_not_called()

    def test_1h_donchian_called(self):
        df = _ohlcv_df(65, 3_600_000)
        last_ts = int(df.iloc[-1]["timestamp"])
        new = [last_ts + 3_600_000, 101.0, 102.0, 100.0, 101.5, 1000.0]
        d = StrategyDonchian()
        with patch.object(d, "evaluate", return_value=None) as ev:
            self._run(self._one_candle("1h", df.iloc[:-1].reset_index(drop=True), new, [d]))
        ev.assert_called()

    def test_15m_pullback_called(self):
        df = _ohlcv_df(65, 900_000)
        last_ts = int(df.iloc[-1]["timestamp"])
        new = [last_ts + 900_000, 101.0, 102.0, 100.0, 101.5, 1000.0]
        p = StrategyPullback()
        with patch.object(p, "evaluate", return_value=None) as ev:
            self._run(self._one_candle("15m", df.iloc[:-1].reset_index(drop=True), new, [p]))
        ev.assert_called()

    def test_4h_donchian_called(self):
        df = _ohlcv_df(65, 14_400_000)
        last_ts = int(df.iloc[-1]["timestamp"])
        new = [last_ts + 14_400_000, 101.0, 102.0, 100.0, 101.5, 1000.0]
        d = StrategyDonchian()
        with patch.object(d, "evaluate", return_value=None) as ev:
            self._run(self._one_candle("4h", df.iloc[:-1].reset_index(drop=True), new, [d]))
        ev.assert_called()


if __name__ == "__main__":
    unittest.main()
