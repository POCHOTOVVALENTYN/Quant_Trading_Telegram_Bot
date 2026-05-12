
import asyncio
import pandas as pd
import numpy as np
import sys
import os

# Add root to sys path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ai.backtest import BacktestEngine, fetch_candles, calculate_indicators
from config.settings import settings

class CleanSwingEngine(BacktestEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ai_threshold = 0.58 # Slightly lower to allow more swing setups

    def run_swing(self, df_1w, df_1d, df_4h):
        df_1w = calculate_indicators(df_1w)
        df_1d = calculate_indicators(df_1d)
        df_4h = calculate_indicators(df_4h)

        open_trade = None
        warmup = 100
        
        for i in range(warmup, len(df_4h)):
            bar_4h = df_4h.iloc[i]
            ts = bar_4h['timestamp']
            self.equity_curve.append(self.balance)

            if open_trade:
                is_long = open_trade["direction"] == "LONG"
                sl, tp = open_trade["sl"], open_trade["tp"]
                if (is_long and bar_4h['low'] <= sl) or (not is_long and bar_4h['high'] >= sl):
                    self.balance += self._close_trade(open_trade, sl, "SL")["pnl"]
                    self.trades.append(self._close_trade(open_trade, sl, "SL"))
                    open_trade = None
                elif (is_long and bar_4h['high'] >= tp) or (not is_long and bar_4h['low'] <= tp):
                    self.balance += self._close_trade(open_trade, tp, "TP")["pnl"]
                    self.trades.append(self._close_trade(open_trade, tp, "TP"))
                    open_trade = None
            
            if open_trade: continue

            # 1. Global Filter
            bar_1w = df_1w[df_1w['timestamp'] <= ts].iloc[-1]
            bar_1d = df_1d[df_1d['timestamp'] <= ts].iloc[-1]
            
            # Simple Trend Rule: Price above EMA200 (W1) and EMA50 (D1)
            bias = 0
            if bar_1w['close'] > bar_1w['ema200'] and bar_1d['close'] > bar_1d['ema50']: bias = 1
            elif bar_1w['close'] < bar_1w['ema200'] and bar_1d['close'] < bar_1d['ema50']: bias = -1

            if bias == 0: continue

            # 2. Strategy Signal (4H)
            eval_df = df_4h.iloc[max(0, i-200):i].copy()
            candidates = []
            for strategy in self.strategies:
                signal = strategy.evaluate(eval_df)
                if not signal or (bias == 1 and signal['signal'] == "SHORT") or (bias == -1 and signal['signal'] == "LONG"):
                    continue
                
                score = self.scorer.calculate_score(eval_df, signal)
                if score < self.score_threshold: continue
                candidates.append({"signal": signal, "score": score})

            if candidates:
                top = sorted(candidates, key=lambda x: x['score'], reverse=True)[0]['signal']
                entry = bar_4h['open']
                atr = bar_4h['atr']
                sl = entry - (2.5 * atr) if top['signal'] == "LONG" else entry + (2.5 * atr)
                tp = entry + (4.0 * atr) if top['signal'] == "LONG" else entry - (4.0 * atr)
                
                open_trade = {
                    "entry": entry, "sl": sl, "tp": tp, "direction": top['signal'], 
                    "strategy": top['strategy'], "size": (self.balance * 0.05 * self.leverage) / entry
                }

        return self._compile_results()

async def run_suite():
    symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"]
    days = 180
    print(f"--- CLEAN SWING H4 SUITE (6 MONTHS) ---")
    
    total_pnl = 0
    for symbol in symbols:
        print(f"Testing {symbol}...")
        dfs = await asyncio.gather(
            fetch_candles(symbol, "1w", days+30),
            fetch_candles(symbol, "1d", days+10),
            fetch_candles(symbol, "4h", days)
        )
        engine = CleanSwingEngine(initial_balance=1000)
        res = engine.run_swing(*dfs)
        pnl = res.get('total_pnl', 0)
        total_pnl += pnl
        print(f"  {symbol}: Trades={res.get('total_trades')}, WinRate={res.get('win_rate'):.1%}, PnL=${pnl:.2f}")

    print("\n" + "="*40)
    print(f"TOTAL PORTFOLIO PnL: ${total_pnl:.2f}")

if __name__ == "__main__":
    asyncio.run(run_suite())
