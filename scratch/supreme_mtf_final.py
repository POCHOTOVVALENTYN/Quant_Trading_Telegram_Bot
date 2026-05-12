
import asyncio
import pandas as pd
import numpy as np
import sys
import os

# Add root to sys path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ai.backtest import BacktestEngine, fetch_candles, calculate_indicators
from config.settings import settings

class SupremeMTFEngine(BacktestEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ai_threshold = 0.58

    def run_supreme(self, df_1w, df_1d, df_4h, df_1h, df_15m):
        dfs = [calculate_indicators(d) for d in [df_1w, df_1d, df_4h, df_1h, df_15m]]
        df_1w, df_1d, df_4h, df_1h, df_15m = dfs

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

            # MTF Bias
            b_1w = df_1w[df_1w['timestamp'] <= ts].iloc[-1]
            b_1d = df_1d[df_1d['timestamp'] <= ts].iloc[-1]
            bias = 1 if (b_1w['close'] > b_1w['ema200'] and b_1d['close'] > b_1d['ema50']) else -1 if (b_1w['close'] < b_1w['ema200'] and b_1d['close'] < b_1d['ema50']) else 0
            if bias == 0: continue

            eval_df = df_4h.iloc[max(0, i-200):i].copy()
            for strategy in self.strategies:
                signal = strategy.evaluate(eval_df)
                if not signal or (bias == 1 and signal['signal'] == "SHORT") or (bias == -1 and signal['signal'] == "LONG"): continue
                
                # Precision Check (1H & 15M)
                b_1h = df_1h[df_1h['timestamp'] <= ts].iloc[-1]
                b_15m = df_15m[df_15m['timestamp'] <= ts].iloc[-1]
                if (signal['signal'] == "LONG" and (b_15m['RSI'] > 75 or b_1h['RSI'] > 75)) or (signal['signal'] == "SHORT" and (b_15m['RSI'] < 25 or b_1h['RSI'] < 25)):
                    continue

                score = self.scorer.calculate_score(eval_df, signal)
                if score < self.score_threshold: continue
                
                entry = bar_4h['open']
                sl = entry - (2.5 * bar_4h['atr']) if signal['signal'] == "LONG" else entry + (2.5 * bar_4h['atr'])
                tp = entry + (4.0 * bar_4h['atr']) if signal['signal'] == "LONG" else entry - (4.0 * bar_4h['atr'])
                open_trade = {"entry": entry, "sl": sl, "tp": tp, "direction": signal['signal'], "strategy": signal['strategy'], "size": (self.balance * 0.05 * self.leverage) / entry}
                break
        return self._compile_results()

async def run_supreme_suite():
    symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT"]
    days = 180
    print(f"--- SUPREME MTF H4 SUITE (6 MONTHS) ---")
    for s in symbols:
        dfs = await asyncio.gather(*[fetch_candles(s, tf, days+30) for tf in ["1w", "1d", "4h", "1h", "15m"]])
        res = SupremeMTFEngine(initial_balance=1000).run_supreme(*dfs)
        print(f"  {s}: Trades={res.get('total_trades')}, WinRate={res.get('win_rate'):.1%}, PnL=${res.get('total_pnl'):.2f}")

if __name__ == "__main__":
    asyncio.run(run_supreme_suite())
