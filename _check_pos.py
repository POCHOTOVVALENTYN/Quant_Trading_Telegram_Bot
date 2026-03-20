import asyncio
import os
import ccxt.pro as ccxt
from dotenv import load_dotenv

# По умолчанию в докере .env рядом
load_dotenv("/app/.env")

async def check():
    api_key = os.getenv('TEST_API_KEY_BINANCE')
    secret = os.getenv('TEST_SECRET_API_KEY_BINANCE')
    
    # Имитируем настройки бота
    ex = ccxt.binance({
        'apiKey': api_key,
        'secret': secret,
        'options': {'defaultType': 'future'}
    })
    ex.set_sandbox_mode(True)
    
    try:
        print("🔍 Запрашиваю позиции с Binance Testnet...")
        pos_data = await ex.fetch_positions()
        active = [p for p in pos_data if abs(float(p.get('contracts', 0))) > 1e-8]
        
        print(f"✅ Найдено активных позиций: {len(active)}")
        for p in active:
            print(f"📊 {p['symbol']} | Side: {p['side']} | Amt: {p['contracts']} | EntryPrice: {p['entryPrice']}")
            
        print("\n🔍 Запрашиваю Algo-ордера (стопы)...")
        algo_raw = await ex.request('openAlgoOrders', 'fapiPrivate', 'GET', {})
        # Приводим к списку
        ao_source = algo_raw.get("algoOrders", []) if isinstance(algo_raw, dict) else (algo_raw if isinstance(algo_raw, list) else [])
        print(f"✅ Найдено Algo-ордеров: {len(ao_source)}")
        for o in ao_source:
            print(f"🛡 {o['symbol']} | Type: {o['type']} | Price: {o['triggerPrice']} | Status: {o['algoStatus']}")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await ex.close()

if __name__ == "__main__":
    asyncio.run(check())
