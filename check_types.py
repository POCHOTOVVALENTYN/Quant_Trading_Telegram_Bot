
import asyncio
import ccxt.pro as ccxt
import os
from dotenv import load_dotenv

async def check_account_mode():
    load_dotenv()
    exchange = ccxt.binance({
        'apiKey': os.getenv('TEST_API_KEY_BINANCE'),
        'secret': os.getenv('TEST_SECRET_API_KEY_BINANCE'),
        'options': {'defaultType': 'future'},
    })
    exchange.set_sandbox_mode(True)
    
    try:
        # Check Position Mode (Hedge vs One-Way)
        try:
            res = await exchange.fapiPrivateGetPositionSideDual()
            print(f"Hedge Mode (dualSidePosition): {res.get('dualSidePosition')}")
        except Exception as e:
            print(f"Error checking Hedge Mode: {e}")
            
        # Check Multi-Asset Mode
        try:
            res = await exchange.fapiPrivateGetMultiAssetsMargin()
            print(f"Multi-Asset Mode: {res.get('multiAssetsMargin')}")
        except Exception as e:
            print(f"Error checking Multi-Asset Mode: {e}")
            
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(check_account_mode())
