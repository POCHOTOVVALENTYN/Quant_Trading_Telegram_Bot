import asyncio
import pandas as pd
from typing import Dict, Any, List, Optional
import time
from datetime import datetime
from collections import deque

from config.settings import settings
from utils.logger import get_signal_logger, app_logger
from utils.notifier import send_telegram_msg
from services.market_data.market_streamer import MarketDataService
from core.indicators.indicators import calculate_atr
from core.strategies.strategies import (
    StrategyWRD, StrategyATRBreakout, StrategyMATrend, 
    StrategyDonchian, StrategyMomentum, StrategyPullback, 
    StrategyVolContraction, StrategyRangeExpansion, 
    StrategyOpeningRange, StrategyWideRangeReversal,
    StrategyRuleOf7, get_timeframe_seconds
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
        self.risk_manager = execution_engine.risk_manager
        
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
        
        # Статистика для Heartbeat (Этап 6)
        self.processed_candles = 0
        self.start_time = time.time()
        self.errors_count = 0
        
        self.market_data.register_callback(self.on_market_data)

        # Circuit breaker: если идет лавина ошибок - отключаем торговлю
        self._recent_error_ts = deque(maxlen=200)

    async def start(self):
        logger.info("Запуск Торгового Движка v2 (10 стратегий + AI Layer)...")
        await self._prefetch_history()
        # Запуск Heartbeat задачи (Этап 6)
        asyncio.create_task(self._heartbeat_loop())
        await self.market_data.start()
    
    async def _prefetch_history(self):
        """Загрузка истории параллельно для всех символов и таймфреймов"""
        logger.info("💾 Загружаем историю свечей (Parallel Prefetching)...")
        tasks = []
        for symbol in self.market_data.symbols:
            if symbol not in self.market_history:
                self.market_history[symbol] = {}
            for tf in self.market_data.timeframes:
                tasks.append(self._fetch_and_store_history(symbol, tf))
        
        results = await asyncio.gather(*tasks)
        loaded_count = sum(1 for r in results if r)
        logger.info(f"✅ Загружена история для {loaded_count} инструментов.")

    async def _fetch_and_store_history(self, symbol: str, tf: str):
        try:
            history = await self.market_data.fetch_ohlcv(symbol, tf, limit=100)
            if history:
                df = pd.DataFrame(history, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                self.market_history[symbol][tf] = df
                return True
        except Exception as e:
            logger.error(f"Ошибка предзагрузки {symbol} {tf}: {e}")
        return False

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
            self.execution.schedule_update_positions(symbol, current_price, atr)

        if len(df) < 60: 
            return

        # 1. Расчет базовых индикаторов (ATR для стратегий)
        try:
            self.processed_candles += 1
            df['atr'] = calculate_atr(df, period=14)
            # 2. Проверка ансамбля стратегий
            for strategy in self.strategies:
                signal = strategy.evaluate(df)
                if signal:
                    # DEDUPE: если за последние N секунд уже был свежий сигнал по символу — пропускаем
                    try:
                        from sqlalchemy import select
                        from datetime import timedelta
                        cutoff = datetime.utcnow() - timedelta(seconds=max(30, settings.signal_expiry_seconds))
                        async with async_session() as session:
                            q = (
                                select(Signal)
                                .where(Signal.symbol == symbol)
                                .where(Signal.timestamp >= cutoff)
                                .where(Signal.status.in_(["PENDING", "EXECUTING", "EXECUTED"]))
                                .order_by(Signal.timestamp.desc())
                                .limit(1)
                            )
                            r = await session.execute(q)
                            recent = r.scalar_one_or_none()
                            if recent:
                                continue
                    except Exception:
                        # дедуп — best-effort, не должен ломать торговлю
                        pass
                    # 3. Фильтрация (НОВОЕ по плану)
                    # 3.1 Защита от старых сигналов (Signal Expiry)
                    candle_time = df.iloc[-1]['timestamp']
                    now_ts = time.time()
                    candle_ts = candle_time if not isinstance(candle_time, datetime) else candle_time.timestamp()
                    if candle_ts > 10**11: candle_ts /= 1000 # ms to s
                    
                    tf_secs = get_timeframe_seconds(timeframe)
                    # Сигнал актуален, если мы внутри свечи ИЛИ прошло не более N секунд после её закрытия
                    if (now_ts - (candle_ts + tf_secs)) > settings.signal_expiry_seconds:
                        logger.warning(f"⚠️ Сигнал {symbol} {signal['strategy']} ПРОПУЩЕН: устарел на {int(now_ts - (candle_ts+tf_secs))}с")
                        continue

                    # 3.2 Проверка даты листинга (Защита от новых монет)
                    info = await self.market_data.fetch_instrument_info(symbol)
                    if info and 'info' in info and 'onboardDate' in info['info']:
                        # onboardDate в Binance обычно в ms
                        onboard_ts = int(info['info']['onboardDate']) / 1000
                        onboard_date_str = datetime.fromtimestamp(onboard_ts).isoformat()
                        if not self.risk_manager.check_listing_days(onboard_date_str, settings.min_listing_days):
                            logger.warning(f"Монета {symbol} слишком новая. Листинг {onboard_date_str}")
                            continue

                    # 3.3 Проверка лимита позиций (из настроек)
                    balance, drawdown, open_trades = await self.execution.get_account_metrics()
                    if open_trades >= settings.max_open_positions:
                        logger.warning(f"Лимит позиций достигнут ({open_trades} >= {settings.max_open_positions})")
                        # Уведомление в TG (раз в 15 минут, чтобы не спамить)
                        now = time.time()
                        if not hasattr(self, '_last_limit_notify') or (now - self._last_limit_notify) > 900:
                            self._last_limit_notify = now
                            await send_telegram_msg(
                                f"⚠️ **ВХОД ПРОПУЩЕН**\n\n"
                                f"Символ: {symbol}\n"
                                f"Причина: Достигнут лимит позиций ({open_trades}/{settings.max_open_positions})\n"
                                f"Закройте одну из открытых позиций, чтобы открыть новую."
                            )
                        return # Прекращаем обработку символа

                    # 3.4 Проверка Funding Rate (Защита от списаний)
                    fr = self.funding_rates.get(symbol, 0.0)
                    if abs(fr) > settings.max_funding_rate:
                        logger.warning(f"Слишком высокий Funding Rate для {symbol}: {fr*100:.4f}% > {settings.max_funding_rate*100:.2f}%")
                        continue

                    # 3.5 Regime Detection (Фильтр волатильности)
                    if not self.risk_manager.is_volatility_sufficient(df):
                        logger.info(f"💤 [{symbol}] Regime Detection: Низкая волатильность (рынок спит)")
                        continue

                    # 4. Скоринг (СТРОГИЙ ФИЛЬТР: Score > 0.65)
                    score = self.scorer.calculate_score(df, signal)
                    if score < 0.65:
                        logger.info(f"🔍 [{symbol}] Scorer отклонил {signal['strategy']}: {score:.2f} < 0.65")
                        continue

                    # 5. AI FEATURE GENERATION (с учетом внешних данных)
                    fr = self.funding_rates.get(symbol, 0.0)
                    ob = self.orderbooks.get(symbol)
                    features = FeatureGenerator.generate_features(df, funding_rate=fr, orderbook=ob)
                    
                    # 5. AI PREDICTION (Win Probability / Risk / Expected Return)
                    ai_prediction = self.ai_model.predict_win_probability(features, signal['signal'])
                    
                    logger.info(f"СИГНАЛ: {signal['strategy']} | Score: {score:.2f} | AI Win Prob: {ai_prediction['win_prob']:.2f}")

                    # AI FILTER (СТРОГИЙ: Probability > 60%)
                    if ai_prediction['win_prob'] < 0.60:
                        logger.info(f"🤖 [{symbol}] AI отклонил {signal['strategy']}: Prob {ai_prediction['win_prob']:.2f} < 0.60")
                        continue

                    # 6. Дополнение данными
                    targets = StrategyRuleOf7.calculate_targets(df.iloc[-1]['high'], df.iloc[-1]['low'])
                    sl = self.risk_manager.calculate_atr_stop(signal['entry_price'], df['atr'].iloc[-1], signal['signal'])
                    # Берем первую цель из Rule of 7 как основной TP для индикации
                    tp = next(iter(targets.values())) if targets else signal['entry_price'] * (1.05 if signal['signal'] == "LONG" else 0.95)

                    enrich_signal = {
                        "symbol": symbol,
                        "signal": signal['signal'],
                        "entry_price": signal['entry_price'],
                        "strategy": signal['strategy'],
                        "targets": targets,
                        "stop_loss": sl,
                        "take_profit": tp,
                        "score": score,
                        "atr": df['atr'].iloc[-1],
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
                            entry_price=signal['entry_price'],
                            stop_loss=sl,
                            take_profit=tp,
                            status="PENDING"
                        )
                        session.add(new_sig_model)
                        await session.commit()
                        enrich_signal["id"] = new_sig_model.id

                    # 7.5 Уведомление о сигнале (Этап 4)
                    status_msg = (
                        f"🚀 **СИГНАЛ: {signal['strategy']}**\n\n"
                        f"🔸 Символ: {symbol}\n"
                        f"🔸 Направление: {'🟢 LONG' if signal['signal'] == 'LONG' else '🔴 SHORT'}\n\n"
                        f"💰 Вход: {signal['entry_price']:.2f}\n"
                        f"🛡 Stop Loss: {sl:.2f}\n"
                        f"🎯 Take Profit: {tp:.2f}\n"
                        f"🕒 Время: {datetime.now().strftime('%H:%M:%S')}\n\n"
                        f"🤖 **AI ВЕРДИКТ:**\n"
                        f"📈 Вероятность: {ai_prediction['win_prob']*100:.0f}%\n"
                        f"💰 Доходность: {ai_prediction['expected_return']:.2f}%\n"
                        f"⚠️ Риск: {ai_prediction['risk']:.2f}\n"
                        f"📊 Score: {score:.2f}\n\n"
                        f"ℹ️ Статус: ⌛️ В ОЖИДАНИИ"
                    )
                    await send_telegram_msg(status_msg)

                    # 8. Исполнение
                    if settings.is_trading_enabled:
                        balance, drawdown, open_trades = await self.execution.get_account_metrics()
                        asyncio.create_task(
                            self.execution.execute_signal(enrich_signal, balance, drawdown, open_trades)
                        )
                    else:
                        logger.info(f"Трейдинг отключен (is_trading_enabled=False). Сигнал {symbol} сохранен, но не исполнен.")
        except Exception as e:
            self.errors_count += 1
            app_logger.error(f"❌ Ошибка в Resilient Loop для {symbol}: {e}")

            # Circuit breaker: если ошибок много за короткое время — отключаем торговлю
            now = time.time()
            self._recent_error_ts.append(now)
            window_sec = 600  # 10 минут
            errors_in_window = sum(1 for ts in self._recent_error_ts if (now - ts) <= window_sec)
            if errors_in_window >= 20 and settings.is_trading_enabled:
                settings.is_trading_enabled = False
                await send_telegram_msg(
                    f"🛑 **CIRCUIT BREAKER**\n\n"
                    f"За последние 10 минут слишком много ошибок: {errors_in_window}.\n"
                    f"Торговля автоматически отключена (is_trading_enabled=False)."
                )

            # Уведомляем админа о критическом сбое (раз в 30 минут)
            if not hasattr(self, '_last_error_notify') or (now - self._last_error_notify) > 1800:
                self._last_error_notify = now
                await send_telegram_msg(f"⚠️ **КРИТИЧЕСКИЙ СБОЙ: Resilient Loop**\n\nСимвол: {symbol}\nОшибка: {str(e)[:200]}")

    async def _heartbeat_loop(self):
        """Задача 'Пульс' (Этап 6) - отчет раз в час"""
        while True:
            await asyncio.sleep(3600)
            uptime_hours = (time.time() - self.start_time) / 3600
            msg = (
                f"💓 **HEARTBEAT: БОТ АКТИВЕН**\n\n"
                f"⏱ Аптайм: {uptime_hours:.1f} ч\n"
                f"📊 Обработано свечей: {self.processed_candles}\n"
                f"⚠️ Ошибок за сессию: {self.errors_count}\n"
                f"🛡 Монет в мониторинге: {len(self.market_history)}\n"
            )
            await send_telegram_msg(msg)
