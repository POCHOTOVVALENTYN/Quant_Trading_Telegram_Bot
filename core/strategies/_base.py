from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple

import pandas as pd


class BaseStrategy(ABC):
    @abstractmethod
    def evaluate(self, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        pass

    def exit_signal(self, df: pd.DataFrame, side: str) -> Optional[Dict[str, Any]]:
        return None

    @staticmethod
    def _last_two_rows(df: pd.DataFrame) -> Optional[Tuple[pd.Series, pd.Series]]:
        if len(df) < 2:
            return None
        return df.iloc[-2], df.iloc[-1]

    @staticmethod
    def _has_columns(df: pd.DataFrame, *columns: str) -> bool:
        return all(col in df.columns for col in columns)

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            val = float(value)
            return default if pd.isna(val) else val
        except (TypeError, ValueError):
            return default

    def _volatility_filter(
        self,
        row: pd.Series,
        *,
        min_atr_ratio: float = 0.001,
        max_atr_ratio: float = 0.08,
    ) -> bool:
        close = self._safe_float(row.get("close"))
        atr = self._safe_float(row.get("atr"))
        if close <= 0 or atr <= 0:
            return False
        atr_ratio = atr / close
        return min_atr_ratio <= atr_ratio <= max_atr_ratio

    def _trend_filter(
        self,
        row: pd.Series,
        direction: str,
        *,
        contrarian: bool = False,
        adx_min: float = 18.0,
        adx_max_contrarian: float = 35.0,
    ) -> bool:
        direction = str(direction or "").upper()
        close = self._safe_float(row.get("close"))
        ema50 = self._safe_float(row.get("ema50"), close)
        ema200 = self._safe_float(row.get("ema200"), ema50)
        adx = self._safe_float(row.get("adx"), adx_min if not contrarian else 20.0)

        if close <= 0:
            return False

        if contrarian:
            if adx > adx_max_contrarian:
                return False
            if direction == "LONG":
                return close <= (ema50 * 1.01)
            if direction == "SHORT":
                return close >= (ema50 * 0.99)
            return False

        if adx < adx_min:
            return False
        if direction == "LONG":
            return close >= ema50 and close >= ema200
        if direction == "SHORT":
            return close <= ema50 and close <= ema200
        return False

    @staticmethod
    def _signal(
        strategy: str,
        signal: str,
        entry_price: float,
        confidence: float,
    ) -> Dict[str, Any]:
        return {
            "strategy": strategy,
            "signal": signal,
            "entry_price": float(entry_price),
            "confidence": float(confidence),
        }

    @staticmethod
    def _exit(reason: str, exit_price: float) -> Dict[str, Any]:
        return {
            "exit": True,
            "reason": reason,
            "exit_price": float(exit_price),
        }
