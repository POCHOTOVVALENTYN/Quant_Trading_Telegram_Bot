import asyncio
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.market_data.client import MarketDataClient

async def test_market_connectivity():
    print("\n🌐 Тестирование подключения к маркет-дате...")
    client = MarketDataClient(symbols=["BTC/USDT", "ETH/USDT"], timeframes=["1h"])
    
    try:
        # Проверка 1: Получение цен
        ticker = await client.fetch_ticker("BTC/USDT")
        print(f"✅ Ticker BTC/USDT: {ticker['last']}")
        
        # Проверка 2: Получение свечей
        candles = await client.fetch_candles("ETH/USDT", "1h", limit=5)
        print(f"✅ ETH/USDT Candles: получено {len(candles)} шт.")
        
        if len(candles) > 0:
            print("🚀 Связь с Binance API установлена успешно!")
        else:
            print("❌ Свечи не получены.")
            
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(test_market_connectivity())
