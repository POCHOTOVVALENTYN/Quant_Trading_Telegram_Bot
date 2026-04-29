
import asyncio
import pandas as pd
import numpy as np
import sys
import os
import random

# Add root to sys path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ai.backtest import BacktestEngine, fetch_candles, calculate_indicators
from config.settings import settings

class ScientificEngine(BacktestEngine):
    def run_swing(self, df_1w, df_1d, df_4h):
        df_1w = calculate_indicators(df_1w)
        df_1d = calculate_indicators(df_1d)
        df_4h = calculate_indicators(df_4h)
        open_trade = None
        warmup = 100
        for i in range(warmup, len(df_4h)):
            bar = df_4h.iloc[i]
            ts = bar['timestamp']
            if open_trade:
                is_long = open_trade["direction"] == "LONG"
                sl, tp = open_trade["sl"], open_trade["tp"]
                if (is_long and bar['low'] <= sl) or (not is_long and bar['high'] >= sl):
                    self.balance += self._close_trade(open_trade, sl, "SL")["pnl"]
                    self.trades.append(self._close_trade(open_trade, sl, "SL"))
                    open_trade = None
                elif (is_long and bar['high'] >= tp) or (not is_long and bar['low'] <= tp):
                    self.balance += self._close_trade(open_trade, tp, "TP")["pnl"]
                    self.trades.append(self._close_trade(open_trade, tp, "TP"))
                    open_trade = None
            if open_trade: continue
            b_1w = df_1w[df_1w['timestamp'] <= ts].iloc[-1]
            b_1d = df_1d[df_1d['timestamp'] <= ts].iloc[-1]
            bias = 1 if (b_1w['close'] > b_1w['ema200'] and b_1d['close'] > b_1d['ema50']) else -1 if (b_1w['close'] < b_1w['ema200'] and b_1d['close'] < b_1d['ema50']) else 0
            if bias == 0: continue
            eval_df = df_4h.iloc[max(0, i-200):i].copy()
            for strategy in self.strategies:
                signal = strategy.evaluate(eval_df)
                if not signal or (bias == 1 and signal['signal'] == "SHORT") or (bias == -1 and signal['signal'] == "LONG"): continue
                if self.scorer.calculate_score(eval_df, signal) < 0.55: continue
                entry = bar['open']
                sl = entry - (2.5 * bar['atr']) if signal['signal'] == "LONG" else entry + (2.5 * bar['atr'])
                tp = entry + (4.0 * bar['atr']) if signal['signal'] == "LONG" else entry - (4.0 * bar['atr'])
                open_trade = {"entry": entry, "sl": sl, "tp": tp, "direction": signal['signal'], "strategy": signal['strategy'], "size": (self.balance * 0.05 * 10) / entry}
                break
        return self._compile_results()

async def validate():
    symbols = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]
    days = 180
    print(f"--- GLOBAL SCIENTIFIC VALIDATION (H4 Swing) ---")
    
    for symbol in symbols:
        print(f"\n>>> Analyzing {symbol}...")
        df_1w, df_1d, df_4h = await asyncio.gather(fetch_candles(symbol, "1w", days+30), fetch_candles(symbol, "1d", days+10), fetch_candles(symbol, "4h", days))
        engine = ScientificEngine(initial_balance=1000)
        res = engine.run_swing(df_1w, df_1d, df_4h)
        
        if res.get('total_trades', 0) == 0:
            print(f"    NO TRADES executed in 6 months.")
            continue
            
        mc = engine.run_monte_carlo(500)
        print(f"    PnL: ${res['total_pnl']:.2f} | WinRate: {res['win_rate']:.1%}")
        print(f"    Monte Carlo Prob of Loss: {mc['prob_of_negative_return']:.1f}%")
        print(f"    95% Max Drawdown: {mc['max_drawdown_95_percentile']:.2f}%")

if __name__ == "__main__":
    asyncio.run(validate())
