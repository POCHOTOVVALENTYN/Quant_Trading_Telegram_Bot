import asyncio
import ccxt.pro as ccxtpro
from config.settings import settings
import os
from dotenv import load_dotenv

async def test_binance_connection():
    load_dotenv()
    
    print("--- Проверка подключения к Binance Futures ---")
    
    # Используем ключи из настроек (если они там есть) или напрямую из ENV
    api_key = settings.binance_api_key or os.getenv("API_KEY_BINANCE")
    api_secret = settings.binance_api_secret or os.getenv("SECRET_API_KEY_BINANCE")
    
    if not api_key or not api_secret:
        print("❌ ОШИБКА: API ключи не найдены в .env или настройках!")
        return

    exchange = ccxtpro.binance({
        'apiKey': api_key,
        'secret': api_secret,
        'options': {
            'defaultType': 'future'
        }
    })
    
    # Переключение на Testnet если нужно
    exchange.set_sandbox_mode(settings.testnet)
    print(f"Режим: {'SANDBOX (Testnet)' if settings.testnet else 'REAL MARKET'}")

    try:
        balance = await exchange.fetch_balance()
        print("✅ Успешное подключение!")
        
        usdt_balance = balance.get('USDT', {}).get('free', 0)
        print(f"💰 Доступный баланс USDT на фьючерсах: {usdt_balance}")
        
    except Exception as e:
        print(f"❌ Произошла ошибка при подключении: {e}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(test_binance_connection())
