
import asyncio
import pandas as pd
import numpy as np
import sys
import os

# Add root to sys path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ai.backtest import BacktestEngine, fetch_candles, calculate_indicators
from config.settings import settings

class MTFBacktestEngine(BacktestEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ai_threshold = 0.60

    def run_mtf(self, df_1w, df_1d, df_4h, df_1h, df_15m):
        """
        Advanced MTF Execution:
        - 1W/1D: Market Regime / Global Bias
        - 4H: Signal Generation
        - 1H/15M: Entry Precision (momentum alignment)
        """
        # Indicators
        df_1w = calculate_indicators(df_1w)
        df_1d = calculate_indicators(df_1d)
        df_4h = calculate_indicators(df_4h)
        df_1h = calculate_indicators(df_1h)
        df_15m = calculate_indicators(df_15m)

        open_trade = None
        warmup = 100 # 4h bars
        
        # We iterate over 4H bars (Execution Timeframe)
        for i in range(warmup, len(df_4h)):
            bar_4h = df_4h.iloc[i]
            ts = bar_4h['timestamp']
            self.equity_curve.append(self.balance)

            # 1. Manage Exits (H4 Resolution)
            if open_trade:
                is_long = open_trade["direction"] == "LONG"
                sl = open_trade["sl"]
                tp = open_trade["tp"]
                
                if (is_long and bar_4h['low'] <= sl) or (not is_long and bar_4h['high'] >= sl):
                    closed = self._close_trade(open_trade, sl, "SL")
                    self.balance += closed["pnl"]
                    self.trades.append(closed)
                    open_trade = None
                    continue
                elif (is_long and bar_4h['high'] >= tp) or (not is_long and bar_4h['low'] <= tp):
                    closed = self._close_trade(open_trade, tp, "TP")
                    self.balance += closed["pnl"]
                    self.trades.append(closed)
                    open_trade = None
                    continue
            
            if open_trade: continue

            # 2. GLOBAL BIAS (1W & 1D)
            # Find closest previous bars on higher TFs
            bar_1w = df_1w[df_1w['timestamp'] <= ts].iloc[-1]
            bar_1d = df_1d[df_1d['timestamp'] <= ts].iloc[-1]
            
            bias = 0 # 1 for LONG only, -1 for SHORT only, 0 for NEUTRAL
            if bar_1w['close'] > bar_1w['ema200'] and bar_1d['close'] > bar_1d['ema50']:
                bias = 1
            elif bar_1w['close'] < bar_1w['ema200'] and bar_1d['close'] < bar_1d['ema50']:
                bias = -1

            # 3. SIGNAL GENERATION (4H)
            eval_df_4h = df_4h.iloc[max(0, i-200):i].copy()
            candidates = []
            for strategy in self.strategies:
                signal = strategy.evaluate(eval_df_4h)
                if not signal: continue
                
                direction = signal['signal']
                
                # Check Global Bias Alignment
                if bias == 1 and direction == "SHORT": continue
                if bias == -1 and direction == "LONG": continue
                
                # 4. PRECISION ENTRY (1H & 15M)
                # Check if momentum on low TFs is not EXTREME against us
                bar_1h = df_1h[df_1h['timestamp'] <= ts].iloc[-1]
                bar_15m = df_15m[df_15m['timestamp'] <= ts].iloc[-1]
                
                # Rule: Don't buy if 15m RSI > 75 (Overbought on micro-scale)
                if direction == "LONG" and (bar_15m['RSI'] > 75 or bar_1h['RSI'] > 75): continue
                # Rule: Don't short if 15m RSI < 25 (Oversold on micro-scale)
                if direction == "SHORT" and (bar_15m['RSI'] < 25 or bar_1h['RSI'] < 25): continue

                score = self.scorer.calculate_score(eval_df_4h, signal)
                if score < self.score_threshold: continue

                candidates.append({"signal": signal, "score": score})

            if candidates:
                candidates.sort(key=lambda x: x['score'], reverse=True)
                top = candidates[0]['signal']
                
                # Entry on NEXT bar OPEN (Standard)
                entry_price = bar_4h['open']
                sl_price = self.risk.calculate_atr_stop(entry_price, bar_4h['atr'], top['signal'], 2.0)
                tp_price = entry_price + (entry_price - sl_price) * 3.0 if top['signal'] == "LONG" else entry_price - (sl_price - entry_price) * 3.0
                
                open_trade = {
                    "entry": entry_price, "sl": sl_price, "tp": tp_price,
                    "direction": top['signal'], "strategy": top['strategy'],
                    "size": (self.balance * 0.05 * self.leverage) / entry_price,
                    "score": candidates[0]['score'], "bar_idx": i
                }

        return self._compile_results()

async def main():
    symbol = "SOL/USDT"
    days = 180 # 6 months for H4
    print(f"--- ADVANCED MTF BACKTEST ({symbol} | 6 Months) ---")
    
    tasks = [
        fetch_candles(symbol, "1w", days + 30),
        fetch_candles(symbol, "1d", days + 10),
        fetch_candles(symbol, "4h", days),
        fetch_candles(symbol, "1h", days),
        fetch_candles(symbol, "15m", days)
    ]
    dfs = await asyncio.gather(*tasks)
    
    engine = MTFBacktestEngine(initial_balance=1000)
    results = engine.run_mtf(*dfs)
    
    print("\n" + "="*60)
    print("MTF BACKTEST RESULTS (4H Entry + W1/D1 Filter)")
    print("="*60)
    print(f"Total trades: {results.get('total_trades', 0)}")
    print(f"Win rate:     {results.get('win_rate', 0):.1%}")
    print(f"Profit Factor: {results.get('profit_factor', 0):.2f}")
    print(f"Total PnL:    ${results.get('total_pnl', 0):.2f}")
    print(f"Return:       {results.get('return_pct', 0):.2f}%")
    print(f"Max Drawdown: {results.get('max_drawdown_pct', 0):.2f}%")

if __name__ == "__main__":
    asyncio.run(main())
