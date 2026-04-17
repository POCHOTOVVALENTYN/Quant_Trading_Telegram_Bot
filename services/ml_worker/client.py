import asyncio
import json
import uuid
import logging
from typing import Optional
import redis.asyncio as aioredis
from config.settings import settings

_log = logging.getLogger("ml_client")

class MLWorkerClient:
    def __init__(self):
        self.redis = None
        self.pubsub = None
        self._pending_requests = {}
        self._listen_task = None
        self._running = False
        
    async def start(self):
        self.redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        self.pubsub = self.redis.pubsub()
        await self.pubsub.subscribe("ml:inference:response")
        self._running = True
        self._listen_task = asyncio.create_task(self._listen())
        
    async def stop(self):
        self._running = False
        if self._listen_task:
            self._listen_task.cancel()
        if self.pubsub:
            await self.pubsub.unsubscribe()
        if self.redis:
            await self.redis.close()

    async def _listen(self):
        try:
            async for message in self.pubsub.listen():
                if not self._running:
                    break
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        req_id = data.get("request_id")
                        if req_id in self._pending_requests:
                            self._pending_requests[req_id].set_result(data)
                    except Exception:
                        pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            _log.error(f"MLWorkerClient listen error: {e}")

    async def predict_proba(self, features: dict, timeout: float = 2.0) -> float:
        if not self.redis:
            return 0.5
        
        req_id = str(uuid.uuid4())
        future = asyncio.get_event_loop().create_future()
        self._pending_requests[req_id] = future
        
        req_data = {
            "request_id": req_id,
            "features": features
        }
        
        try:
            await self.redis.publish("ml:inference:request", json.dumps(req_data))
            result = await asyncio.wait_for(future, timeout)
            if result and result.get("status") == "ok":
                return float(result.get("ml_prob", 0.5))
            return 0.5
        except asyncio.TimeoutError:
            _log.warning("ML worker prediction timeout")
            return 0.5
        except Exception as e:
            _log.error(f"ML worker prediction error: {e}")
            return 0.5
        finally:
            self._pending_requests.pop(req_id, None)
