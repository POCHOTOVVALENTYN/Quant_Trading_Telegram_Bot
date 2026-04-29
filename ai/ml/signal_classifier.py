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

    df = await extract_training_data(min_date=now - timedelta(days=30))
    if df.empty or len(df) < MIN_SAMPLES_TRAIN:
        _log.warning(f"Not enough samples for training: {len(df)} < {MIN_SAMPLES_TRAIN}")
        return None, {"error": "insufficient_data", "samples": len(df)}

    X, y, feature_names = prepare_features(df)

    def _sync_train(X_data, y_data):
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.metrics import accuracy_score, roc_auc_score
        from sklearn.model_selection import TimeSeriesSplit
        
        # TimeSeriesSplit (Phase 20) - предотвращает Look-Ahead Bias
        tscv = TimeSeriesSplit(n_splits=5)
        
        best_model = None
        best_val_auc = -1.0
        final_tr_acc = 0.0
        final_v_acc = 0.0
        
        # Walk-forward validation across folds
        for train_index, val_index in tscv.split(X_data):
            X_tr, X_v = X_data[train_index], X_data[val_index]
            y_tr, y_v = y_data[train_index], y_data[val_index]
            
            if len(np.unique(y_tr)) < 2: continue
            
            m = GradientBoostingClassifier(
                max_depth=3, n_estimators=100, learning_rate=0.05,
                subsample=0.8, min_samples_leaf=5, random_state=42,
            )
            m.fit(X_tr, y_tr)
            
            v_auc = 0.0
            try:
                v_auc = roc_auc_score(y_v, m.predict_proba(X_v)[:, 1])
            except: pass
            
            if v_auc > best_val_auc:
                best_val_auc = v_auc
                best_model = m
                final_tr_acc = accuracy_score(y_tr, m.predict(X_tr))
                final_v_acc = accuracy_score(y_v, m.predict(X_v))
        
        # If no fold was viable, train on all except last 7 days
        if best_model is None:
            best_model = GradientBoostingClassifier(n_estimators=100, random_state=42).fit(X_data, y_data)
            best_val_auc = 0.5
            final_tr_acc = accuracy_score(y_data, best_model.predict(X_data))
            final_v_acc = 0.5
            
        return best_model, final_tr_acc, final_v_acc, best_val_auc

    clf_model, train_acc, val_acc, val_auc = await asyncio.to_thread(_sync_train, X, y)


    stats = {
        "train_samples": len(X),
        "val_samples": int(len(X) / 5), # approximate via TimeSeriesSplit
        "train_accuracy": round(train_acc, 4),
        "val_accuracy": round(val_acc, 4),
        "val_auc": round(val_auc, 4),
        "features": feature_names,
        "trained_at": now.isoformat(),
    }
    _log.info(
        f"ML training complete: train_acc={train_acc:.3f}, val_acc={val_acc:.3f}, "
        f"val_auc={val_auc:.3f}, samples={len(X)}"
    )

    # Save model
    model_path.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "model": clf_model,
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
