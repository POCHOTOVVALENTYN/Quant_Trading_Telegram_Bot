"""Парсинг universe рынка из строки настроек (MARKET_SYMBOLS / MARKET_TIMEFRAMES).

Единый формат для market-data worker, REST и других сервисов.
"""
from __future__ import annotations

from typing import List, Optional

from utils.symbol_normalizer import SymbolNormalizer


def parse_market_symbols(csv: str, *, max_n: Optional[int] = None) -> List[str]:
    """Разбор CSV символов в формате ccxt, нормализация, опциональная обрезка."""
    raw = [SymbolNormalizer.normalize(s.strip()) for s in csv.split(",") if s.strip()]
    out = [s for s in raw if s]
    if max_n is not None and max_n > 0:
        out = out[:max_n]
    return out


def parse_market_timeframes(csv: str) -> List[str]:
    """Разбор CSV таймфреймов (например ``1m,5m,15m``)."""
    return [t.strip() for t in csv.split(",") if t.strip()]
