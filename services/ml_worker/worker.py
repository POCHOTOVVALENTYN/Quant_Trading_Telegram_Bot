"""
ML Worker service: subscribes to Redis pub/sub for market data snapshots,
runs regime classification and ML inference, publishes enriched context back.

Decouples heavy ML computation from the hot trading path.
"""

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime, timezone

import redis.asyncio as aioredis

from config.settings import settings
from ai.ml.signal_classifier import SignalClassifier

from utils.logger import get_ml_logger

_log = get_ml_logger()

CHANNEL_REQUEST = "ml:inference:request"
CHANNEL_RESPONSE = "ml:inference:response"
CHANNEL_RETRAIN = "ml:retrain"

RETRAIN_INTERVAL = 6 * 3600  # 6 hours


class MLWorker:
    def __init__(self):
        self.classifier = SignalClassifier()
        self.redis: aioredis.Redis = None
        self._running = True
        self._last_retrain = 0.0
        self._signals_processed = 0

    async def start(self):
        _log.info("ML Worker starting...")
        self.redis = aioredis.from_url(settings.redis_url, decode_responses=True)

        loop = asyncio.get_event_loop()
        for sig_name in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig_name, self._shutdown)

        # Run listener and periodic retrain in parallel
        await asyncio.gather(
            self._listen_requests(),
            self._periodic_retrain(),
        )

    def _shutdown(self):
        _log.info("Shutdown requested")
        self._running = False

    async def _listen_requests(self):
        """Subscribe to inference requests, compute, and publish results."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(CHANNEL_REQUEST, CHANNEL_RETRAIN)
        _log.info(f"Subscribed to {CHANNEL_REQUEST}, {CHANNEL_RETRAIN}")

        async for message in pubsub.listen():
            if not self._running:
                break
            if message["type"] != "message":
                continue

            channel = message["channel"]
            try:
                if channel == CHANNEL_RETRAIN:
                    await self._do_retrain()
                elif channel == CHANNEL_REQUEST:
                    data = json.loads(message["data"])
                    # Offload CPU-bound ML inference to a technical thread pool to keep event loop responsive
                    result = await asyncio.to_thread(self._predict, data)
                    await self.redis.publish(CHANNEL_RESPONSE, json.dumps(result))
            except Exception as e:
                _log.error(f"Error processing message on {channel}: {e}")

        await pubsub.unsubscribe()

    def _predict(self, data: dict) -> dict:
        """Run ML inference on a feature dict."""
        request_id = data.get("request_id", "")
        features = data.get("features", {})

        self._signals_processed += 1
        
        # Graceful restart after N signals to prevent memory leaks (Python/ML bounds)
        MAX_SIGNALS_BEFORE_RESTART = 1000
        if self._signals_processed >= MAX_SIGNALS_BEFORE_RESTART:
            _log.info(f"Processed {self._signals_processed} signals. Committing graceful restart to clear memory.")
            self._running = False
            # We don't await here directly as it's sync, but setting _running = False stops the listener
            loop = asyncio.get_event_loop()
            loop.call_soon(self._shutdown)

        if not self.classifier.is_ready():
            return {"request_id": request_id, "ml_prob": 0.5, "status": "model_not_ready"}

        ml_prob = self.classifier.predict_proba(features)
        return {
            "request_id": request_id,
            "ml_prob": round(ml_prob, 4),
            "status": "ok",
            "model_status": self.classifier.get_status(),
        }

    async def _periodic_retrain(self):
        """Retrain the model periodically."""
        while self._running:
            await asyncio.sleep(60)
            import time
            if time.time() - self._last_retrain > RETRAIN_INTERVAL:
                await self._do_retrain()

    async def _do_retrain(self):
        """Execute walk-forward retraining."""
        import time
        _log.info("Starting ML model retraining...")
        try:
            from ai.ml.signal_classifier import train_walk_forward
            classifier, stats = await train_walk_forward()
            if classifier:
                self.classifier = classifier
                _log.info(f"Retrain complete: {stats}")
            else:
                _log.warning(f"Retrain skipped: {stats}")
        except Exception as e:
            _log.error(f"Retrain failed: {e}")
        self._last_retrain = time.time()


async def main():
    worker = MLWorker()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
