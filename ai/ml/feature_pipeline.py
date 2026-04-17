"""
Feature extraction pipeline for ML signal validator.
Joins signal_decision_logs (features at signal time) with pnl_records (outcome)
to produce labeled training data.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import select, and_

_log = logging.getLogger("ml_pipeline")

# Features extracted from signal_decision_logs
FEATURE_COLS = [
    "adx", "atr", "rsi", "volume_ratio", "funding_rate", "score", "win_prob",
    "ai_confidence",
    # Encoded categoricals
    "regime_TREND", "regime_RANGE", "regime_NEUTRAL",
    "volatility_HIGH", "volatility_LOW", "volatility_NORMAL",
    "funding_EXTREME_LONG", "funding_EXTREME_SHORT", "funding_NORMAL",
    "session_ASIA", "session_EU", "session_US",
    "direction_LONG", "direction_SHORT",
]


async def extract_training_data(
    min_date: Optional[datetime] = None,
    max_date: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Extract labeled dataset from DB:
    - X: features from signal_decision_logs (only ACCEPTED signals)
    - y: binary label from pnl_records (1 = positive PnL, 0 = negative)

    Returns DataFrame with FEATURE_COLS + 'target' column.
    """
    from database.session import async_session
    from database.models.all_models import SignalDecisionLog, PnLRecord

    if min_date is None:
        min_date = datetime.now(timezone.utc) - timedelta(days=90)
    if max_date is None:
        max_date = datetime.now(timezone.utc)

    async with async_session() as session:
        # Accepted signals with features
        q_signals = (
            select(SignalDecisionLog)
            .where(
                and_(
                    SignalDecisionLog.outcome == "ACCEPTED",
                    SignalDecisionLog.created_at >= min_date,
                    SignalDecisionLog.created_at <= max_date,
                )
            )
            .order_by(SignalDecisionLog.created_at)
        )
        result = await session.execute(q_signals)
        signals = result.scalars().all()

        # PnL records for outcome labeling
        q_pnl = (
            select(PnLRecord)
            .where(
                and_(
                    PnLRecord.closed_at >= min_date,
                    PnLRecord.closed_at <= max_date,
                )
            )
            .order_by(PnLRecord.closed_at)
        )
        result_pnl = await session.execute(q_pnl)
        pnl_records = result_pnl.scalars().all()

    if not signals:
        _log.warning("No accepted signals found for training data extraction")
        return pd.DataFrame()

    # Build PnL lookup: (symbol, close_time) → pnl_usd
    # Match by symbol and closest time (within 24h window)
    pnl_df = pd.DataFrame([
        {"symbol": p.symbol, "pnl_usd": p.pnl_usd, "closed_at": p.closed_at}
        for p in pnl_records
    ])

    rows = []
    for sig in signals:
        row = {
            "adx": sig.adx, "atr": sig.atr, "rsi": sig.rsi,
            "volume_ratio": sig.volume_ratio, "funding_rate": sig.funding_rate,
            "score": sig.score, "win_prob": sig.win_prob,
            "ai_confidence": sig.ai_confidence or 0.0,
            # One-hot: regime
            "regime_TREND": 1.0 if sig.regime == "TREND" else 0.0,
            "regime_RANGE": 1.0 if sig.regime == "RANGE" else 0.0,
            "regime_NEUTRAL": 1.0 if sig.regime == "NEUTRAL" else 0.0,
            # One-hot: volatility
            "volatility_HIGH": 1.0 if sig.volatility_regime == "HIGH" else 0.0,
            "volatility_LOW": 1.0 if sig.volatility_regime == "LOW" else 0.0,
            "volatility_NORMAL": 1.0 if sig.volatility_regime == "NORMAL" else 0.0,
            # One-hot: funding
            "funding_EXTREME_LONG": 1.0 if sig.funding_regime == "EXTREME_LONG" else 0.0,
            "funding_EXTREME_SHORT": 1.0 if sig.funding_regime == "EXTREME_SHORT" else 0.0,
            "funding_NORMAL": 1.0 if sig.funding_regime == "NORMAL" else 0.0,
            # One-hot: session
            "session_ASIA": 1.0 if sig.session == "ASIA" else 0.0,
            "session_EU": 1.0 if sig.session == "EU" else 0.0,
            "session_US": 1.0 if sig.session == "US" else 0.0,
            # Direction
            "direction_LONG": 1.0 if sig.direction == "LONG" else 0.0,
            "direction_SHORT": 1.0 if sig.direction == "SHORT" else 0.0,
        }

        # Match PnL outcome
        target = None
        if not pnl_df.empty:
            sym_pnl = pnl_df[pnl_df["symbol"] == sig.symbol]
            if not sym_pnl.empty and sig.created_at is not None:
                time_diffs = (sym_pnl["closed_at"] - sig.created_at).abs()
                min_idx = time_diffs.idxmin()
                if time_diffs.loc[min_idx] < timedelta(days=1):
                    target = 1.0 if sym_pnl.loc[min_idx, "pnl_usd"] > 0 else 0.0

        if target is not None:
            row["target"] = target
            rows.append(row)

    df = pd.DataFrame(rows)
    _log.info(f"Extracted {len(df)} labeled samples ({df['target'].sum():.0f} wins)")
    return df


def prepare_features(df: pd.DataFrame):
    """
    Split DataFrame into X (features) and y (target), handling NaN.
    Returns (X: np.ndarray, y: np.ndarray, feature_names: list).
    """
    available = [c for c in FEATURE_COLS if c in df.columns]
    X = df[available].fillna(0).values.astype(np.float32)
    y = df["target"].values.astype(np.float32)
    return X, y, available
