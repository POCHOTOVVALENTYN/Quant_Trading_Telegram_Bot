import asyncio
import pandas as pd
import ccxt.async_support as ccxt
from config.settings import settings
import core.indicators.indicators as ind

async def check_assets(symbols):
    exchange = ccxt.binance({
        'apiKey': settings.api_key_binance,
        'secret': settings.secret_api_key_binance,
        'options': {'defaultType': 'future'}
    })
    
    print(f"--- Мульти-диагностика активов (LIVE) ---")
    
    try:
        for symbol in symbols:
            print(f"\nАнализ {symbol}...")
            candles = await exchange.fetch_ohlcv(symbol, '15m', limit=100)
            df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            df['rsi'] = ind.calculate_rsi(df['close'])
            adx_df = ind.calculate_adx(df)
            df['adx'] = adx_df['adx']
            df['ema20'] = ind.calculate_ema(df['close'], 20)
            df['ema50'] = ind.calculate_ema(df['close'], 50)
            
            last = df.iloc[-1]
            trend = "📈 UP" if last['ema20'] > last['ema50'] else "📉 DOWN"
            
            print(f"  Цена: ${last['close']:.2f}")
            print(f"  RSI: {last['rsi']:.2f} | ADX: {last['adx']:.2f}")
            print(f"  Тренд: {trend}")
            
            if last['adx'] > 25:
                print("  🔥 Сильный тренд! Шанс сигнала высокий.")
            elif last['rsi'] < 35 or last['rsi'] > 65:
                print("  ⚠️ Близко к зонам перекупленности/перепроданности.")
            else:
                print("  💤 Спокойный рынок.")
                
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(check_assets(['BTC/USDT', 'ETH/USDT', 'SOL/USDT']))
