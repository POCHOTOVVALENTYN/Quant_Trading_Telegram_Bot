import asyncio
import pandas as pd
from typing import Dict, Any, List

from utils.logger import get_signal_logger, app_logger
from services.market_data.market_streamer import MarketDataService
from core.indicators.indicators import calculate_atr
from core.strategies.strategies import (
    StrategyWRD, StrategyATRBreakout, StrategyMATrend, 
    StrategyDonchian, StrategyMomentum, StrategyPullback, 
    StrategyVolContraction, StrategyRangeExpansion, 
    StrategyOpeningRange, StrategyWideRangeReversal,
    StrategyRuleOf7
)
from core.strategies.scoring import SignalScorer
from ai.feature_generator import FeatureGenerator
from ai.model import AIModel
from core.risk.risk_manager import RiskManager
from core.execution.engine import ExecutionEngine
from database.session import async_session
from database.models.all_models import Signal

logger = get_signal_logger()

class TradingOrchestrator:
    def __init__(self, market_data: MarketDataService, execution_engine: ExecutionEngine):
        self.market_data = market_data
        self.execution = execution_engine
        
        # Инициализация полного ансамбля из 10 стратегий (Группы 1-4)
        self.strategies = [
            StrategyWRD(atr_multiplier=1.6),
            StrategyATRBreakout(period=20, multiplier=0.5),
            StrategyMATrend(fast_ma=20, slow_ma=50),
            StrategyDonchian(period=20),
            StrategyMomentum(period=10, threshold=3.0),
            StrategyPullback(ma_period=20),
            StrategyVolContraction(threshold=0.6),
            StrategyRangeExpansion(),
            StrategyOpeningRange(),
            StrategyWideRangeReversal()
        ]
        
        self.scorer = SignalScorer()
        self.ai_model = AIModel()
        self.market_history: Dict[str, Dict[str, pd.DataFrame]] = {}
        
        # Кэш внешних данных для AI
        self.funding_rates: Dict[str, float] = {}
        self.orderbooks: Dict[str, Any] = {}
        
        self.market_data.register_callback(self.on_market_data)

    async def start(self):
        logger.info("Запуск Торгового Движка v2 (10 стратегий + AI Layer)...")
        await self.market_data.start()

    async def stop(self):
        logger.info("Остановка Торгового Движка...")
        await self.market_data.stop()

    async def on_market_data(self, data_type: str, symbol: str, timeframe: str, data: Any):
        if data_type == "ohlcv":
            await self._process_ohlcv(symbol, timeframe, data)
        elif data_type == "orderbook":
            self.orderbooks[symbol] = data
        elif data_type == "funding_rate":
            # data здесь - словарь всех ставок
            if isinstance(data, dict):
                for sym, rate_info in data.items():
                    if isinstance(rate_info, dict):
                        self.funding_rates[sym] = rate_info.get('fundingRate', 0.0)

    async def _process_ohlcv(self, symbol: str, timeframe: str, new_candle: list):
        if symbol not in self.market_history:
            self.market_history[symbol] = {}
        if timeframe not in self.market_history[symbol]:
            self.market_history[symbol][timeframe] = pd.DataFrame(
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )

        df = self.market_history[symbol][timeframe]
        new_row = {
            'timestamp': new_candle[0],
            'open': float(new_candle[1]),
            'high': float(new_candle[2]),
            'low': float(new_candle[3]),
            'close': float(new_candle[4]),
            'volume': float(new_candle[5])
        }
        
        if not df.empty and df.iloc[-1]['timestamp'] == new_candle[0]:
            df.iloc[-1] = new_row
        else:
            df.loc[len(df)] = new_row
            
        if len(df) > 200: 
            df = df.tail(200).reset_index(drop=True)
        self.market_history[symbol][timeframe] = df

        # --- НОВОЕ: Обновление Трейлинг-стопов и Пирамидинга (Этап 3 Плана) ---
        if timeframe == "1m": # Для мгновенной реакции на цену
            current_price = df.iloc[-1]['close']
            atr = df.iloc[-1].get('atr', 100.0) # Если ATR еще не рассчитан, берем дефолт
            asyncio.create_task(self.execution.update_positions(symbol, current_price, atr))

        if len(df) < 60: 
            return

        # 1. Расчет базовых индикаторов (ATR для стратегий)
        try:
            df['atr'] = calculate_atr(df, period=14)
        except Exception as e:
            return

        # 2. Проверка ансамбля стратегий
        for strategy in self.strategies:
            signal = strategy.evaluate(df)
            if signal:
                # 3. Скоринг (СТРОГИЙ ФИЛЬТР: Score > 0.65)
                score = self.scorer.calculate_score(df, signal)
                if score < 0.65:
                    continue

                # 4. AI FEATURE GENERATION (с учетом внешних данных)
                fr = self.funding_rates.get(symbol, 0.0)
                ob = self.orderbooks.get(symbol)
                features = FeatureGenerator.generate_features(df, funding_rate=fr, orderbook=ob)
                
                # 5. AI PREDICTION (Win Probability / Risk / Expected Return)
                ai_prediction = self.ai_model.predict_win_probability(features, signal['signal'])
                
                logger.info(f"СИГНАЛ: {signal['strategy']} | Score: {score:.2f} | AI Win Prob: {ai_prediction['win_prob']:.2f}")

                # AI FILTER (СТРОГИЙ: Probability > 60%)
                if ai_prediction['win_prob'] < 0.60:
                    logger.info(f"AI отклонил сигнал {signal['strategy']} (Prob {ai_prediction['win_prob']:.2f} < 0.60)")
                    continue

                # 6. Дополнение данными
                targets = StrategyRuleOf7.calculate_targets(df.iloc[-1]['high'], df.iloc[-1]['low'])
                enrich_signal = {
                    "symbol": symbol,
                    "signal": signal['signal'],
                    "entry_price": signal['entry_price'],
                    "strategy": signal['strategy'],
                    "targets": targets,
                    "score": score,
                    "ai_data": ai_prediction
                }
                
                # 7. Сохранение в БД расширенных данных
                async with async_session() as session:
                    new_sig_model = Signal(
                        symbol=symbol,
                        signal_type=signal['signal'],
                        strategy=signal['strategy'],
                        confidence=score,
                        win_prob=ai_prediction['win_prob'],
                        expected_return=ai_prediction['expected_return'],
                        risk=ai_prediction['risk'],
                        entry_price=signal['entry_price']
                    )
                    session.add(new_sig_model)
                    await session.commit()

                # 8. Исполнение
                balance, drawdown, open_trades = await self.execution.get_account_metrics()
                asyncio.create_task(
                    self.execution.execute_signal(enrich_signal, balance, drawdown, open_trades)
                )
