import ccxt.pro as ccxtpro
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

async def main():
    api_key = os.getenv("TEST_API_KEY_BINANCE", "").strip()
    secret = os.getenv("TEST_SECRET_API_KEY_BINANCE", "").strip()
    
    exchange = ccxtpro.binance({
        'apiKey': api_key,
        'secret': secret,
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })
    
    exchange.urls['api'] = exchange.urls['demo']
    # Not calling set_sandbox_mode(True) because it throws NotSupported
    
    try:
        await exchange.load_markets()
        print("Markets loaded.")
        bal = await exchange.fetch_balance()
        print(f"USDT Balance: {bal.get('total', {}).get('USDT')}")
    except Exception as e:
        print(f"Error fetching balance: {type(e).__name__} - {e}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(main())
