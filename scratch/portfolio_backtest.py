
import asyncio
import pandas as pd
import sys
import os

# Add root to sys path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ai.backtest import BacktestEngine, fetch_candles
from config.settings import settings

async def run_portfolio_backtest():
    symbols = settings.market_symbols.split(",")
    # Filter out known bad symbols or update them
    symbols = [s.strip() for s in symbols if s.strip() not in ["MATIC/USDT"]]
    
    timeframe = "15m"
    days = 30
    
    # Use our optimized settings
    settings.regime_adx_trend_min = 18.0
    settings.regime_adx_range_max = 15.0
    
    print(f"--- Portfolio Backtest (ADX Trend={settings.regime_adx_trend_min}) ---")
    print(f"Period: {days} days | Timeframe: {timeframe}")
    print(f"Symbols: {', '.join(symbols)}\n")

    summary = []

    for symbol in symbols:
        try:
            print(f"Testing {symbol}...")
            df = await fetch_candles(symbol, timeframe, days)
            if len(df) < 200:
                print(f"  Skipping {symbol}: insufficient data ({len(df)} candles)")
                continue
                
            engine = BacktestEngine(initial_balance=1000)
            res = engine.run(df)
            
            summary.append({
                "Symbol": symbol,
                "Trades": res.get("total_trades", 0),
                "WinRate": f"{res.get('win_rate', 0):.1%}",
                "PnL": res.get("total_pnl", 0),
                "Return": f"{res.get('return_pct', 0):.2f}%",
                "Sharp": res.get("sharpe", 0)
            })
        except Exception as e:
            print(f"  Error testing {symbol}: {e}")

    # Sort by PnL desc
    summary.sort(key=lambda x: x["PnL"], reverse=True)

    print("\n" + "="*80)
    print(f"{'Symbol':<12} | {'Trades':<8} | {'WinRate':<8} | {'PnL ($)':<12} | {'Return':<10} | {'Sharpe':<8}")
    print("-" * 80)
    total_pnl = 0
    for r in summary:
        total_pnl += r["PnL"]
        print(f"{r['Symbol']:<12} | {r['Trades']:<8} | {r['WinRate']:<8} | {r['PnL']:<12.2f} | {r['Return']:<10} | {r['Sharp']:<8.3f}")
    print("-" * 80)
    print(f"{'TOTAL PORTFOLIO PnL:':<34} ${total_pnl:.2f}")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(run_portfolio_backtest())
