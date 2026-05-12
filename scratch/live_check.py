import asyncio
import pandas as pd
import ccxt.async_support as ccxt
from config.settings import settings
import core.indicators.indicators as ind

async def get_live_diagnostics():
    exchange = ccxt.binance({
        'apiKey': settings.api_key_binance,
        'secret': settings.secret_api_key_binance,
        'options': {'defaultType': 'future'}
    })
    
    symbol = 'BTC/USDT'
    print(f"--- Диагностика {symbol} (LIVE) ---")
    
    try:
        # Получаем последние 100 свечей 15м
        candles = await exchange.fetch_ohlcv(symbol, '15m', limit=100)
        df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # Рассчитываем индикаторы вручную по функциям модуля
        df['rsi'] = ind.calculate_rsi(df['close'])
        adx_df = ind.calculate_adx(df)
        df['adx'] = adx_df['adx']
        df['atr'] = ind.calculate_atr(df)
        df['ema20'] = ind.calculate_ema(df['close'], 20)
        df['ema50'] = ind.calculate_ema(df['close'], 50)
        
        last = df.iloc[-1]
        
        print(f"Текущая цена: ${last['close']:.2f}")
        print(f"RSI (14): {last['rsi']:.2f}")
        print(f"ADX: {last['adx']:.2f}")
        print(f"ATR: {last['atr']:.2f}")
        
        ema20 = last['ema20']
        ema50 = last['ema50']
        trend = "📈 UP" if ema20 > ema50 else "📉 DOWN"
        print(f"Тренд (EMA 20/50): {trend}")
        
        # Анализ состояния
        if last['adx'] < 20:
            print("Статус: Низкая волатильность (ADX < 20). Трендовые стратегии (Rule of 7, Donchian) в режиме ожидания.")
        else:
            print("Статус: Рынок активен. ИИ анализирует условия для входа.")
            
        if last['rsi'] > 70:
            print("Предупреждение: Перекупленность (RSI > 70).")
        elif last['rsi'] < 30:
            print("Предупреждение: Перепроданность (RSI < 30).")
            
    except Exception as e:
        print(f"Ошибка диагностики: {e}")
    finally:
        await exchange.close()

if __name__ == "__main__":
    asyncio.run(get_live_diagnostics())
