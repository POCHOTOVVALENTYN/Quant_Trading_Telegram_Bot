import asyncio
import json
import logging
import signal
from typing import Any
import redis.asyncio as aioredis
from services.market_data.market_streamer import MarketDataService
from config.settings import settings

from utils.logger import get_market_data_logger

_log = get_market_data_logger()

# Redis Channels
CH_MARKET_DATA = "market:data"

class MarketDataWorker:
    def __init__(self):
        self.redis: aioredis.Redis = None
        self.market_service: MarketDataService = None
        self._running = True

    async def start(self):
        _log.info("Starting Market Data Microservice...")
        self.redis = aioredis.from_url(settings.redis_url, decode_responses=True)
        
        # Setup symbols and timeframes (usually from settings)
        # For a microservice, we might want to fetch these from a central config or DB
        # For now, we reuse the same logic as in main.py but simplified
        
        # Use symbols and timeframes from settings
        symbols = [s.strip() for s in settings.market_symbols.split(",") if s.strip()]
        tfs = [t.strip() for t in settings.market_timeframes.split(",") if t.strip()]
            
        self.market_service = MarketDataService(symbols=symbols, timeframes=tfs)
        self.market_service.register_callback(self._publish_to_redis)
        
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop)
            
        await self.market_service.start()
        _log.info("Market Data Service is running.")
        
        while self._running:
            await asyncio.sleep(1)

    def stop(self):
        _log.info("Shutdown requested...")
        self._running = False
        if self.market_service:
            self.market_service.running = False

    async def _publish_to_redis(self, data_type: str, symbol: str, timeframe: str, data: Any):
        """Callback to push market events to Redis."""
        payload = {
            "type": data_type,
            "symbol": symbol,
            "timeframe": timeframe,
            "data": data,
            "ts": asyncio.get_event_loop().time()
        }
        try:
            # We can use specific channels for higher performance if needed:
            # e.g. market:data:{data_type}:{symbol}
            await self.redis.publish(CH_MARKET_DATA, json.dumps(payload))
        except Exception as e:
            _log.error(f"Failed to publish to Redis: {e}")

async def main():
    worker = MarketDataWorker()
    await worker.start()

if __name__ == "__main__":
    asyncio.run(main())
