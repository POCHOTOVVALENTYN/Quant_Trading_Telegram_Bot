
import asyncio
import pandas as pd
import sys
import os

# Add root to sys path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ai.backtest import BacktestEngine, fetch_candles
from config.settings import settings

async def run_optimization():
    symbol = "SOL/USDT"
    timeframe = "15m"
    days = 30
    
    print(f"--- ADX Optimization for {symbol} {timeframe} ({days} days) ---")
    print("Fetching candles...")
    df = await fetch_candles(symbol, timeframe, days)
    print(f"Downloaded {len(df)} candles.\n")

    thresholds = [15, 18, 20, 22, 25]
    results = []

    for thr in thresholds:
        print(f"Testing ADX Trend Threshold: {thr}...")
        # Hack settings for this run
        settings.regime_adx_trend_min = float(thr)
        
        engine = BacktestEngine(initial_balance=1000)
        res = engine.run(df)
        
        results.append({
            "ADX_THR": thr,
            "Trades": res.get("total_trades", 0),
            "WinRate": f"{res.get('win_rate', 0):.1%}",
            "PnL": f"${res.get('total_pnl', 0):.2f}",
            "Drawdown": f"{res.get('max_drawdown_pct', 0):.2f}%",
            "PF": res.get("profit_factor", 0)
        })

    # Print summary table
    print("\n" + "="*70)
    print(f"{'ADX THR':<10} | {'Trades':<8} | {'WinRate':<8} | {'PnL':<10} | {'DD':<10} | {'PF':<5}")
    print("-" * 70)
    for r in results:
        print(f"{r['ADX_THR']:<10} | {r['Trades']:<8} | {r['WinRate']:<8} | {r['PnL']:<10} | {r['Drawdown']:<10} | {r['PF']:<5.2f}")
    print("="*70)

if __name__ == "__main__":
    asyncio.run(run_optimization())
