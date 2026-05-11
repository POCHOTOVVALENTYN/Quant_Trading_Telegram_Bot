import asyncio
import ccxt.pro as ccxt
import os

async def debug():
    print("DEBUG: Testing Binance Demo Connectivity")
    ex = ccxt.binance({
        'apiKey': 'lsMNYQkUizF76W0U17ySAnK0uI53T8fUwaeYvE6yVq7S6z0z0z0z0z0z0z0z0z0z', # Dummy or Real
        'secret': 'dummy',
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })
    
    # Прямая подмена на Demo FAPI
    ex.urls['api']['fapiPublic'] = 'https://demo-fapi.binance.com/fapi/v1'
    ex.urls['api']['fapiPrivate'] = 'https://demo-fapi.binance.com/fapi/v1'
    ex.urls['api']['public'] = 'https://demo-fapi.binance.com/fapi/v1'
    ex.urls['api']['private'] = 'https://demo-fapi.binance.com/fapi/v1'
    
    try:
        markets = await ex.load_markets()
        print(f"✅ Markets loaded: {len(markets)} symbols found.")
        print(f"Sample: {list(markets.keys())[:5]}")
        
        # Проверка BTC/USDT
        if 'BTC/USDT:USDT' in markets:
            print("✅ BTC/USDT:USDT is present")
        elif 'BTC/USDT' in markets:
            print("✅ BTC/USDT is present")
        else:
            print(f"❌ BTC/USDT not found. Available keys: {list(markets.keys())[:10]}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await ex.close()

if __name__ == "__main__":
    asyncio.run(debug())
