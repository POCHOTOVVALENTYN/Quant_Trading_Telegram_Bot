"""
ScoringLearner — Self-improving statistical scorer.

Pipeline:
  1. Collect completed signals from DB (EXECUTED → Position CLOSED with PnL)
  2. For each signal: join features at entry time with outcome (win/loss)
  3. Run logistic regression / gradient-based weight optimization
  4. Update AIModel weights if new weights improve accuracy
  5. Persist to JSON file for restart survival

This runs periodically (default every 6 hours) as a background task.
"""
import asyncio
import json
import os
import time
import logging
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

from sqlalchemy import select, and_
from database.session import async_session
from database.models.all_models import Signal, Position, PositionStatus, PnLRecord

logger = logging.getLogger(__name__)

# Features used by AIModel (must match ai/model.py weights keys)
FEATURE_KEYS = [
    'atr_ratio', 'range_relative', 'rsi', 'roc_10',
    'price_to_sma20', 'sma20_to_sma50', 'volume_ratio',
    'orderbook_imbalance', 'funding_rate',
]

DEFAULT_WEIGHTS = {
    'atr_ratio': 0.10,
    'range_relative': 0.05,
    'rsi': 0.10,
    'roc_10': 0.15,
    'price_to_sma20': 0.05,
    'sma20_to_sma50': 0.10,
    'volume_ratio': 0.15,
    'orderbook_imbalance': 0.15,
    'funding_rate': 0.15,
}


class ScoringLearner:
    """Learns optimal feature weights from historical signal outcomes."""

    def __init__(
        self,
        weights_file: str = "data/learned_weights.json",
        min_samples: int = 20,
        learning_rate: float = 0.01,
        regularization: float = 0.001,
    ):
        self.weights_file = Path(weights_file)
        self.min_samples = min_samples
        self.lr = learning_rate
        self.reg = regularization
        self.current_weights = dict(DEFAULT_WEIGHTS)
        self._last_learn_ts: float = 0.0
        self._load_weights()

    def _load_weights(self):
        """Load previously learned weights from disk."""
        try:
            if self.weights_file.exists():
                with open(self.weights_file, 'r') as f:
                    data = json.load(f)
                    saved = data.get("weights", {})
                    for k in FEATURE_KEYS:
                        if k in saved:
                            self.current_weights[k] = float(saved[k])
                    logger.info(f"Loaded learned weights from {self.weights_file}")
                    stats = data.get("stats", {})
                    if stats:
                        logger.info(
                            f"  Last trained: {stats.get('last_trained')}, "
                            f"samples: {stats.get('num_samples')}, "
                            f"accuracy: {stats.get('accuracy', 'N/A')}"
                        )
        except Exception as e:
            logger.warning(f"Could not load weights: {e}")

    def _save_weights(self, stats: dict):
        """Persist weights + training stats to disk."""
        try:
            self.weights_file.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "weights": self.current_weights,
                "stats": stats,
            }
            with open(self.weights_file, 'w') as f:
                json.dump(payload, f, indent=2)
            logger.info(f"Saved learned weights to {self.weights_file}")
        except Exception as e:
            logger.error(f"Could not save weights: {e}")

    async def collect_training_data(self) -> List[Dict]:
        """
        Gather signal→outcome pairs from DB.
        A sample = { features (from Signal record), outcome: 1 (win) or 0 (loss) }
        """
        samples = []
        try:
            async with async_session() as session:
                # Signals that were executed
                stmt = (
                    select(Signal, Position, PnLRecord)
                    .outerjoin(Position, and_(
                        Position.signal_id == Signal.id,
                        Position.status == PositionStatus.CLOSED
                    ))
                    .outerjoin(PnLRecord, and_(
                        PnLRecord.symbol == Signal.symbol,
                        PnLRecord.user_id == 1,
                    ))
                    .where(Signal.status == "EXECUTED")
                    .order_by(Signal.timestamp.desc())
                    .limit(500)
                )
                result = await session.execute(stmt)
                rows = result.all()

                for sig, pos, pnl in rows:
                    if pos is None:
                        continue

                    # Determine outcome
                    if pnl and pnl.pnl_usd is not None:
                        is_win = pnl.pnl_usd > 0
                    elif pos.realized_pnl is not None and pos.realized_pnl != 0:
                        is_win = pos.realized_pnl > 0
                    else:
                        continue

                    # Reconstruct partial features from signal record
                    features = self._signal_to_features(sig)
                    if features is None:
                        continue

                    samples.append({
                        "features": features,
                        "signal_type": str(sig.signal_type.value) if sig.signal_type else "LONG",
                        "outcome": 1.0 if is_win else 0.0,
                        "strategy": sig.strategy,
                        "pnl_pct": pnl.pnl_pct if pnl else 0.0,
                    })

        except Exception as e:
            logger.error(f"collect_training_data error: {e}")

        logger.info(f"Collected {len(samples)} training samples from DB")
        return samples

    @staticmethod
    def _signal_to_features(sig: Signal) -> Optional[Dict[str, float]]:
        """
        Reconstruct feature vector from Signal record.
        Since we don't store full features at signal time, we approximate
        from what IS stored (confidence, win_prob, risk, expected_return).
        """
        if sig.confidence is None:
            return None

        # These are approximations — ideally we'd store features at signal creation time.
        # confidence ≈ scorer output (0-1), win_prob ≈ AI output,
        # risk and expected_return give partial info.
        return {
            'atr_ratio': (sig.expected_return or 1.5) / 1.5,
            'range_relative': (sig.risk or 1.0),
            'rsi': 50.0,  # Not stored; use neutral
            'roc_10': ((sig.confidence or 0.5) - 0.5) * 10.0,
            'price_to_sma20': 1.0,
            'sma20_to_sma50': 1.0 + ((sig.confidence or 0.5) - 0.5) * 0.02,
            'volume_ratio': 1.0 + (sig.confidence or 0.5),
            'orderbook_imbalance': 0.0,
            'funding_rate': 0.0,
        }

    def _compute_score(self, features: Dict[str, float], signal_type: str, weights: Dict[str, float]) -> float:
        """Compute raw score from features and weights (same logic as AIModel)."""
        score = 0.0

        vol_ratio = features.get('volume_ratio', 1.0)
        score += max(-1.0, min(1.0, (vol_ratio - 1.0) / 2.0)) * weights['volume_ratio']

        roc = features.get('roc_10', 0.0)
        rsi = features.get('rsi', 50.0)

        if signal_type == "LONG":
            score += (1.0 if roc > 0 else -0.5) * weights['roc_10']
            if 50 < rsi < 75:
                score += weights['rsi']
        else:
            score += (1.0 if roc < 0 else -0.5) * weights['roc_10']
            if 25 < rsi < 50:
                score += weights['rsi']

        trend = features.get('sma20_to_sma50', 1.0)
        if signal_type == "LONG":
            score += (1.0 if trend > 1.0 else -1.0) * weights['sma20_to_sma50']
        else:
            score += (1.0 if trend < 1.0 else -1.0) * weights['sma20_to_sma50']

        imb = features.get('orderbook_imbalance', 0.0)
        score += (imb if signal_type == "LONG" else -imb) * weights['orderbook_imbalance']

        fr = features.get('funding_rate', 0.0)
        if signal_type == "LONG" and fr > 0.01:
            score -= 0.5 * weights['funding_rate']
        elif signal_type == "SHORT" and fr < -0.01:
            score -= 0.5 * weights['funding_rate']

        prob = 0.45 + score * 0.35
        return max(0.01, min(0.99, prob))

    def _evaluate_accuracy(self, samples: List[Dict], weights: Dict[str, float]) -> Tuple[float, float]:
        """
        Compute accuracy and log-loss for given weights on samples.
        Returns: (accuracy, log_loss)
        """
        correct = 0
        total_loss = 0.0

        for s in samples:
            prob = self._compute_score(s["features"], s["signal_type"], weights)
            predicted_win = prob >= 0.5
            actual_win = s["outcome"] > 0.5
            if predicted_win == actual_win:
                correct += 1

            # Binary cross-entropy
            eps = 1e-7
            p = max(eps, min(1 - eps, prob))
            y = s["outcome"]
            total_loss += -(y * np.log(p) + (1 - y) * np.log(1 - p))

        n = len(samples)
        accuracy = correct / n if n > 0 else 0.0
        avg_loss = total_loss / n if n > 0 else float('inf')
        return accuracy, avg_loss

    def optimize_weights(self, samples: List[Dict], epochs: int = 200) -> Dict[str, float]:
        """
        Gradient descent on binary cross-entropy to find better weights.
        Uses numerical gradient for simplicity (no PyTorch/sklearn dependency).
        """
        # Start from current weights
        w = {k: self.current_weights[k] for k in FEATURE_KEYS}
        best_loss = float('inf')
        best_w = dict(w)
        delta = 0.001

        for epoch in range(epochs):
            _, current_loss = self._evaluate_accuracy(samples, w)

            if current_loss < best_loss:
                best_loss = current_loss
                best_w = dict(w)

            # Numerical gradient for each weight
            grad = {}
            for key in FEATURE_KEYS:
                w_plus = dict(w)
                w_plus[key] += delta
                _, loss_plus = self._evaluate_accuracy(samples, w_plus)
                grad[key] = (loss_plus - current_loss) / delta

            # Update
            for key in FEATURE_KEYS:
                w[key] -= self.lr * grad[key]
                w[key] -= self.reg * w[key]  # L2 regularization
                w[key] = max(0.0, min(1.0, w[key]))  # Clamp

            # Normalize to sum=1
            total = sum(w.values())
            if total > 0:
                for key in w:
                    w[key] /= total

        return best_w

    async def learn(self) -> Optional[Dict]:
        """
        Main learning pipeline:
          1. Collect data
          2. Optimize weights
          3. Compare accuracy
          4. Update if improved
        Returns training stats or None if skipped.
        """
        samples = await self.collect_training_data()
        if len(samples) < self.min_samples:
            logger.info(f"Not enough samples for learning: {len(samples)} < {self.min_samples}")
            return None

        # Baseline accuracy with current weights
        old_acc, old_loss = self._evaluate_accuracy(samples, self.current_weights)

        # Optimize
        new_weights = self.optimize_weights(samples)
        new_acc, new_loss = self._evaluate_accuracy(samples, new_weights)

        stats = {
            "last_trained": datetime.now(timezone.utc).isoformat(),
            "num_samples": len(samples),
            "old_accuracy": round(old_acc, 4),
            "old_loss": round(old_loss, 4),
            "new_accuracy": round(new_acc, 4),
            "new_loss": round(new_loss, 4),
            "accuracy": round(new_acc, 4),
            "improved": new_loss < old_loss,
            "win_rate": round(sum(s["outcome"] for s in samples) / len(samples), 4),
            "strategies": {},
        }

        # Per-strategy breakdown
        strat_groups: Dict[str, List] = {}
        for s in samples:
            strat_groups.setdefault(s["strategy"], []).append(s)
        for name, group in strat_groups.items():
            wins = sum(1 for s in group if s["outcome"] > 0.5)
            stats["strategies"][name] = {
                "total": len(group),
                "wins": wins,
                "win_rate": round(wins / len(group), 4) if group else 0,
                "avg_pnl_pct": round(np.mean([s.get("pnl_pct", 0) for s in group]), 4),
            }

        if new_loss < old_loss:
            self.current_weights = new_weights
            logger.info(
                f"Weights IMPROVED: loss {old_loss:.4f} -> {new_loss:.4f}, "
                f"acc {old_acc:.2%} -> {new_acc:.2%}"
            )
        else:
            logger.info(
                f"Weights NOT improved: loss {old_loss:.4f} vs {new_loss:.4f}. Keeping current."
            )

        self._save_weights(stats)
        self._last_learn_ts = time.time()
        return stats

    def get_weights(self) -> Dict[str, float]:
        return dict(self.current_weights)


async def learning_loop(learner: ScoringLearner, ai_model, interval_hours: int = 6):
    """Background task: retrain weights periodically."""
    while True:
        await asyncio.sleep(interval_hours * 3600)
        try:
            stats = await learner.learn()
            if stats and stats.get("improved"):
                ai_model.update_weights(learner.get_weights())
                logger.info("AIModel weights updated from learner")
            if stats:
                from utils.notifier import send_telegram_msg
                _improved = "✅ Да" if stats['improved'] else "❌ Нет"
                msg = (
                    f"🧠 **ОБУЧЕНИЕ СКОРИНГА**\n\n"
                    f"📊 Выборка: `{stats['num_samples']}`\n"
                    f"🎯 Точность: `{stats['old_accuracy']:.1%}` → `{stats['new_accuracy']:.1%}`\n"
                    f"📈 Процент побед: `{stats['win_rate']:.1%}`\n"
                    f"🔄 Улучшено: {_improved}\n\n"
                    f"📋 **По стратегиям:**\n"
                )
                for name, s in stats.get("strategies", {}).items():
                    _wr = f"{s['win_rate']:.0%}"
                    msg += f"  • {name}: {s['wins']}/{s['total']} ({_wr}), PnL `{s['avg_pnl_pct']:.2f}%`\n"
                await send_telegram_msg(msg)
        except Exception as e:
            logger.error(f"Learning loop error: {e}")
