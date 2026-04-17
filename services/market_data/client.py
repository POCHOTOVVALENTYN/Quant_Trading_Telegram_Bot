import asyncio
import json
import logging
from typing import List, Callable, Any, Dict
import redis.asyncio as aioredis
from config.settings import settings

_log = logging.getLogger("market_data_client")

class MarketDataClient:
    """
    Lightweight client for receiving market data via Redis Pub/Sub.
    Compatible with MarketDataService interface for callbacks.
    """
    def __init__(self, symbols: List[str], timeframes: List[str], exchange=None):
        self.symbols = symbols
        self.timeframes = timeframes
        self.callbacks: List[Callable] = []
        self.redis: aioredis.Redis = None
        self.exchange = exchange
        self._running = False
        self._listen_task: asyncio.Task = None
        self._status_map: Dict[str, bool] = {} # "symbol:tf" -> recovering_bool

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 100):
        if self.exchange:
            return await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        return []

    async def fetch_instrument_info(self, symbol: str):
        if self.exchange:
            markets = await self.exchange.load_markets()
            return markets.get(symbol)
        return None

    def is_stream_recovering(self, symbol: str, timeframe: str) -> bool:
        return self._status_map.get(f"{symbol}:{timeframe}", False)

    def register_callback(self, cb: Callable):
        self.callbacks.append(cb)

    async def start(self):
        """Start listening to Redis market data channel."""
        if self._running:
            return
            
        _log.info("MarketDataClient starting (Redis mode)...")
        self.redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        self._running = True
        self._listen_task = asyncio.create_task(self._listen())

    async def stop(self):
        self._running = False
        if self._listen_task:
            self._listen_task.cancel()
        if self.redis:
            await self.redis.close()

    async def _listen(self):
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("market:data")
        _log.info("Subscribed to market:data channel")
        
        try:
            async for message in pubsub.listen():
                if not self._running:
                    break
                if message["type"] != "message":
                    continue
                
                try:
                    payload = json.loads(message["data"])
                    data_type = payload["type"]
                    symbol = payload["symbol"]
                    timeframe = payload["timeframe"]
                    data = payload["data"]
                    
                    if data_type == "status":
                        self._status_map[f"{symbol}:{timeframe}"] = data.get("recovering", False)
                    
                    for cb in self.callbacks:
                        # Call callbacks just like the local MarketDataService would
                        await cb(data_type, symbol, timeframe, data)
                except Exception as e:
                    _log.error(f"Error processing market message: {e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            _log.error(f"MarketDataClient listener error: {e}")
        finally:
            await pubsub.unsubscribe()
