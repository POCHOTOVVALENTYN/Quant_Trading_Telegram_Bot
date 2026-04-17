"""
Lightweight GBM signal classifier with walk-forward validation.
Trained offline, exported as pickle, loaded at runtime for inference.
"""

import logging
import pickle
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple

import numpy as np

_log = logging.getLogger("ml_classifier")

MODEL_PATH = Path("data/ml_signal_model.pkl")
MIN_SAMPLES_TRAIN = 30  # need at least this many labeled samples


class SignalClassifier:
    """GradientBoostingClassifier wrapper with walk-forward training."""

    def __init__(self, model_path: Path = MODEL_PATH):
        self.model_path = model_path
        self.model = None
        self.feature_names: list = []
        self.trained_at: Optional[datetime] = None
        self.train_accuracy: float = 0.0
        self.val_accuracy: float = 0.0
        self._load()

    def _load(self):
        if self.model_path.exists():
            try:
                with open(self.model_path, "rb") as f:
                    state = pickle.load(f)
                self.model = state["model"]
                self.feature_names = state.get("feature_names", [])
                self.trained_at = state.get("trained_at")
                self.train_accuracy = state.get("train_accuracy", 0)
                self.val_accuracy = state.get("val_accuracy", 0)
                _log.info(
                    f"ML model loaded: val_acc={self.val_accuracy:.3f}, "
                    f"features={len(self.feature_names)}, trained={self.trained_at}"
                )
            except Exception as e:
                _log.warning(f"Failed to load ML model: {e}")
                self.model = None

    def is_ready(self) -> bool:
        return self.model is not None

    def predict_proba(self, features: dict) -> float:
        """
        Predict win probability for a single signal.
        Returns 0.5 (neutral) if model not ready.
        """
        if not self.is_ready():
            return 0.5
        try:
            x = np.array([[features.get(f, 0.0) for f in self.feature_names]], dtype=np.float32)
            proba = self.model.predict_proba(x)[0][1]
            return float(proba)
        except Exception as e:
            _log.warning(f"ML predict error: {e}")
            return 0.5

    def get_status(self) -> dict:
        return {
            "ready": self.is_ready(),
            "trained_at": self.trained_at.isoformat() if self.trained_at else None,
            "train_accuracy": self.train_accuracy,
            "val_accuracy": self.val_accuracy,
            "features": len(self.feature_names),
        }


async def train_walk_forward(
    val_days: int = 7,
    model_path: Path = MODEL_PATH,
) -> Tuple[Optional[SignalClassifier], dict]:
    """
    Walk-forward training:
    - Train on data older than val_days
    - Validate on last val_days
    Returns (classifier, stats_dict).
    """
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.metrics import accuracy_score, roc_auc_score
    from ai.ml.feature_pipeline import extract_training_data, prepare_features

    now = datetime.now(timezone.utc)
    val_cutoff = now - timedelta(days=val_days)

    df = await extract_training_data()
    if df.empty or len(df) < MIN_SAMPLES_TRAIN:
        _log.warning(f"Not enough samples for training: {len(df)} < {MIN_SAMPLES_TRAIN}")
        return None, {"error": "insufficient_data", "samples": len(df)}

    X, y, feature_names = prepare_features(df)

    # Temporal split: train on older data, validate on recent
    # Use created_at from the pipeline (implicitly ordered)
    split_idx = max(1, int(len(df) * (1 - val_days / 90)))  # approximate
    X_train, y_train = X[:split_idx], y[:split_idx]
    X_val, y_val = X[split_idx:], y[split_idx:]

    if len(X_val) < 5 or len(np.unique(y_train)) < 2:
        _log.warning("Insufficient validation samples or only one class in training")
        return None, {"error": "degenerate_split", "train": len(X_train), "val": len(X_val)}

    clf = GradientBoostingClassifier(
        max_depth=3,
        n_estimators=80,
        learning_rate=0.05,
        subsample=0.8,
        min_samples_leaf=5,
        random_state=42,
    )
    clf.fit(X_train, y_train)

    train_acc = accuracy_score(y_train, clf.predict(X_train))
    val_acc = accuracy_score(y_val, clf.predict(X_val))
    try:
        val_auc = roc_auc_score(y_val, clf.predict_proba(X_val)[:, 1])
    except Exception:
        val_auc = 0.0

    stats = {
        "train_samples": len(X_train),
        "val_samples": len(X_val),
        "train_accuracy": round(train_acc, 4),
        "val_accuracy": round(val_acc, 4),
        "val_auc": round(val_auc, 4),
        "features": feature_names,
        "trained_at": now.isoformat(),
    }
    _log.info(
        f"ML training complete: train_acc={train_acc:.3f}, val_acc={val_acc:.3f}, "
        f"val_auc={val_auc:.3f}, samples={len(X_train)}+{len(X_val)}"
    )

    # Save model
    model_path.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "model": clf,
        "feature_names": feature_names,
        "trained_at": now,
        "train_accuracy": train_acc,
        "val_accuracy": val_acc,
        "stats": stats,
    }
    with open(model_path, "wb") as f:
        pickle.dump(state, f)

    classifier = SignalClassifier(model_path)
    return classifier, stats
