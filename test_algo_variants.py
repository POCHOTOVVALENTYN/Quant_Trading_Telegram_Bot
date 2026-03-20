import ccxt.async_support as ccxt
import os
import asyncio

async def test_algo():
    api_key = os.getenv('TEST_API_KEY_BINANCE')
    api_secret = os.getenv('TEST_SECRET_API_KEY_BINANCE')
    
    exchange = ccxt.binance({
        'apiKey': api_key,
        'secret': api_secret,
        'options': {'defaultType': 'future'}
    })
    exchange.urls['api']['fapiPublic'] = 'https://testnet.binancefuture.com/fapi/v1'
    exchange.urls['api']['fapiPrivate'] = 'https://testnet.binancefuture.com/fapi/v1'
    
    symbol = 'TRXUSDT'
    # Пробуем разные варианты
    tests = [
        # 1. С closePosition=true но БЕЗ positionSide (текущий вариант)
        {'algoType': 'CONDITIONAL', 'symbol': symbol, 'side': 'SELL', 'type': 'STOP_MARKET', 'triggerPrice': '0.30465', 'closePosition': 'true', 'workingType': 'MARK_PRICE'},
        # 2. С closePosition=true и positionSide='BOTH'
        {'algoType': 'CONDITIONAL', 'symbol': symbol, 'side': 'SELL', 'type': 'STOP_MARKET', 'triggerPrice': '0.30465', 'closePosition': 'true', 'positionSide': 'BOTH', 'workingType': 'MARK_PRICE'},
        # 3. БЕЗ closePosition но с quantity и reduceOnly
        {'algoType': 'CONDITIONAL', 'symbol': symbol, 'side': 'SELL', 'type': 'STOP_MARKET', 'triggerPrice': '0.30465', 'quantity': '10', 'reduceOnly': 'true', 'workingType': 'MARK_PRICE'},
    ]
    
    for i, params in enumerate(tests, 1):
        print(f"\n--- TEST {i}: {params} ---")
        try:
            res = await exchange.request('algoOrder', 'fapiPrivate', 'POST', params)
            print(f"SUCCESS: {res}")
        except Exception as e:
            print(f"FAILED: {e}")
            
    await exchange.close()

if __name__ == "__main__":
    asyncio.run(test_algo())
