"""
Cumulative Volume Delta (CVD) tracker.
Aggregates trade-level buy/sell volume into a rolling CVD per symbol.
Used as a feature for ML and as a supplementary indicator.
"""

import time
from collections import defaultdict, deque
from typing import Dict, Optional


class CVDTracker:
    """
    Maintains rolling CVD (Cumulative Volume Delta) per symbol.
    Feed with individual trades from watch_trades / aggTrade stream.
    """

    def __init__(self, window_seconds: int = 300):
        self.window_seconds = window_seconds
        self._trades: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))

    def on_trade(self, symbol: str, price: float, amount: float, side: str, timestamp_ms: int = 0):
        """
        Record a single trade.
        side: 'buy' or 'sell' (taker side)
        """
        ts = timestamp_ms / 1000.0 if timestamp_ms > 0 else time.time()
        cost = price * abs(amount)
        delta = cost if side == "buy" else -cost
        self._trades[symbol].append((ts, delta))

    def get_cvd(self, symbol: str) -> float:
        """
        Return CVD for the rolling window.
        Positive = buyers dominant, negative = sellers dominant.
        """
        now = time.time()
        cutoff = now - self.window_seconds
        trades = self._trades.get(symbol)
        if not trades:
            return 0.0
        total = sum(delta for ts, delta in trades if ts >= cutoff)
        return total

    def get_cvd_normalized(self, symbol: str) -> float:
        """
        CVD normalized by total volume in window.
        Returns value in [-1, 1] range.
        """
        now = time.time()
        cutoff = now - self.window_seconds
        trades = self._trades.get(symbol)
        if not trades:
            return 0.0
        recent = [(ts, delta) for ts, delta in trades if ts >= cutoff]
        if not recent:
            return 0.0
        total_delta = sum(d for _, d in recent)
        total_volume = sum(abs(d) for _, d in recent)
        if total_volume <= 0:
            return 0.0
        return total_delta / total_volume

    def get_all_symbols(self) -> Dict[str, float]:
        return {sym: self.get_cvd(sym) for sym in self._trades}

    def cleanup(self):
        """Remove stale data outside the window."""
        now = time.time()
        cutoff = now - self.window_seconds * 2
        for symbol in list(self._trades):
            trades = self._trades[symbol]
            while trades and trades[0][0] < cutoff:
                trades.popleft()
