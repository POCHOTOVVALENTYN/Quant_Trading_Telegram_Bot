import asyncio
import sys
import os
import redis.asyncio as aioredis
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.market_data.client import MarketDataClient
from config.settings import settings

async def test_redis_and_client():
    print("\n📦 Тестирование MarketDataClient + Redis...")
    
    # 1. Проверка Redis напрямую
    try:
        redis = aioredis.from_url(settings.redis_url)
        ping = await redis.ping()
        if ping:
            print(f"✅ Redis доступен по адресу: {settings.redis_url}")
        await redis.close()
    except Exception as e:
        print(f"❌ Ошибка подключения к Redis: {e}")
        return

    # 2. Проверка MarketDataClient
    client = MarketDataClient(symbols=["BTC/USDT"], timeframes=["1h"])
    try:
        await client.start()
        print("✅ MarketDataClient запущен и подписан на Redis")
        await asyncio.sleep(1) # Даем время на инициализацию
        await client.stop()
        print("✅ MarketDataClient успешно остановлен")
    except Exception as e:
        print(f"❌ Ошибка в работе MarketDataClient: {e}")

if __name__ == "__main__":
    asyncio.run(test_redis_and_client())
