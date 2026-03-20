import asyncio
import os
import ccxt.pro as ccxt
from dotenv import load_dotenv

load_dotenv("/app/.env")

async def check():
    api_key = os.getenv('TEST_API_KEY_BINANCE')
    secret = os.getenv('TEST_SECRET_API_KEY_BINANCE')
    
    ex = ccxt.binance({
        'apiKey': api_key,
        'secret': secret,
        'options': {'defaultType': 'future'}
    })
    ex.set_sandbox_mode(True)
    
    try:
        print("🔍 Запрашиваю историю сделок (последние 10)...")
        trades = await ex.fetch_my_trades(limit=10)
        
        if not trades:
            print("🌑 История сделок пуста.")
            return

        for t in trades:
            # t['info'] содержит сырые данные Binance
            pnl = t.get('info', {}).get('realizedPnl', '0.0')
            print(f"💰 {t['datetime']} | {t['symbol']} | {t['side'].upper()} | Qty: {t['amount']} | Price: {t['price']} | PnL: {pnl}")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        await ex.close()

if __name__ == "__main__":
    asyncio.run(check())
