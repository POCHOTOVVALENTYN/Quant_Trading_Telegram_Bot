import asyncio
import pandas as pd
import traceback
from typing import Dict, Any, List, Optional
import time
from datetime import datetime, timezone
from collections import defaultdict, deque

from config.settings import settings
from utils.logger import get_signal_logger, app_logger
from utils.notifier import send_telegram_msg
from services.market_data.market_streamer import MarketDataService
from core.strategies.strategies import (
    StrategyWRD, StrategyDonchian, StrategyMATrend,
    StrategyPullback, StrategyVolContraction,
    StrategyWideRangeReversal, StrategyWilliamsR,
    StrategyFundingSqueeze, StrategyRuleOf7,
    get_timeframe_seconds
)
from core.indicators.indicators import (
    calculate_atr, calculate_rsi, calculate_bollinger_bands, calculate_csi, calculate_sma,
    calculate_ema, calculate_adx, calculate_williams_r, calculate_vwap
)
from core.strategies.scoring import SignalScorer
from ai.feature_generator import FeatureGenerator
from ai.model import AIModel, ExternalAIAdapter, ProviderConfig
from ai.scoring_learner import ScoringLearner, learning_loop
from core.risk.risk_manager import RiskManager
from core.execution.engine import ExecutionEngine
from database.session import async_session
from database.models.all_models import Signal, AIDecisionLog
from utils.exporter import exporter

logger = get_signal_logger()

class TradingOrchestrator:
    def __init__(self, market_data: MarketDataService, execution_engine: ExecutionEngine):
        self.market_data = market_data
        self.execution = execution_engine
        self.risk_manager = execution_engine.risk_manager

        # Консолидированный ансамбль: 8 стратегий в 4 группах (по Швагеру)
        # Breakout (3): Donchian, WRD, VolContraction
        # Trend (2): MATrend, Pullback
        # Mean Reversion (2): WilliamsR, WideRangeReversal
        # Crypto-specific (1): FundingSqueeze
        self.strategies = [
            StrategyDonchian(period=20),
            StrategyWRD(atr_multiplier=1.6),
            StrategyVolContraction(lookback=300, contraction_ratio=0.6),
            StrategyMATrend(fast_ma=20, slow_ma=50, global_ma=200),
            StrategyPullback(ma_period=20, global_period=200),
            StrategyWilliamsR(),
            StrategyWideRangeReversal(),
            StrategyFundingSqueeze(),
        ]

        self.scorer = SignalScorer()
        self.scoring_learner = ScoringLearner(weights_file=settings.scoring_weights_file)
        self.ai_model = AIModel(weights=self.scoring_learner.get_weights())
        self.external_ai = self._init_external_ai()
        self.market_history: Dict[str, Dict[str, pd.DataFrame]] = {}

        self.funding_rates: Dict[str, float] = {}
        self.orderbooks: Dict[str, Any] = {}
        self.processed_candles = 0
        self.start_time = time.time()
        self.errors_count = 0

        self._last_signals_cache: Dict[tuple, float] = {}
        self._stale_signals_count: Dict[str, int] = defaultdict(int)
        self._stale_signals_last_log_ts: float = 0.0
        self._last_eval_candle_ts: Dict[tuple, float] = {}

        self._recent_error_ts = deque(maxlen=200)
        self._instrument_info_cache: Dict[str, Any] = {}

        self.market_data.register_callback(self.on_market_data)

    @staticmethod
    def _init_external_ai() -> ExternalAIAdapter:
        providers: Dict[str, ProviderConfig] = {}

        if settings.groq_api_key:
            providers["groq"] = ProviderConfig(
                name="groq", api_key=settings.groq_api_key,
                model=settings.groq_model,
            )
        if settings.grok_api_key:
            providers["grok"] = ProviderConfig(
                name="grok", api_key=settings.grok_api_key,
                model=settings.grok_model, base_url=settings.grok_api_url,
            )
        if settings.gemini_api_key:
            providers["gemini"] = ProviderConfig(
                name="gemini", api_key=settings.gemini_api_key,
                model=settings.gemini_model,
            )
        if settings.openrouter_api_key:
            providers["openrouter"] = ProviderConfig(
                name="openrouter", api_key=settings.openrouter_api_key,
                model=settings.openrouter_model,
            )

        cascade = [s.strip() for s in settings.ai_cascade_order.split(",") if s.strip()] if settings.ai_cascade_order else []

        # Legacy fallback: single-provider env vars
        if not cascade and settings.external_ai_backend:
            cascade = [settings.external_ai_backend]
            if settings.external_ai_backend not in providers:
                providers[settings.external_ai_backend] = ProviderConfig(
                    name=settings.external_ai_backend,
                    api_key=settings.external_ai_api_key,
                    model=settings.external_ai_model,
                    base_url=settings.external_ai_url,
                )

        adapter = ExternalAIAdapter(providers=providers, cascade_order=cascade)

        if cascade and providers:
            adapter.enable()
        return adapter

    async def start(self):
        logger.info(f"Запуск Торгового Движка (8 стратегий, Schwager-based ensemble)...")
        await self._prefetch_history()
        asyncio.create_task(self._heartbeat_loop())
        if settings.scoring_learner_enabled:
            asyncio.create_task(learning_loop(
                self.scoring_learner, self.ai_model,
                interval_hours=settings.scoring_learn_interval_hours
            ))
            logger.info(f"ScoringLearner: background learning every {settings.scoring_learn_interval_hours}h")
        await self.market_data.start()

    async def _prefetch_history(self):
        logger.info("Загружаем историю свечей...")
        symbols = self.market_data.symbols
        timeframes = self.market_data.timeframes
        total_tasks = len(symbols) * len(timeframes)
        processed = 0
        semaphore = asyncio.Semaphore(5)

        async def sem_fetch(s, t):
            nonlocal processed
            async with semaphore:
                res = await self._fetch_and_store_history(s, t)
                processed += 1
                if processed % 5 == 0 or processed == total_tasks:
                    logger.info(f"Прогресс загрузки истории: {processed}/{total_tasks}")
                await asyncio.sleep(0.3)
                return res

        tasks = []
        for symbol in symbols:
            if symbol not in self.market_history:
                self.market_history[symbol] = {}
            for tf in timeframes:
                tasks.append(sem_fetch(symbol, tf))

        results = await asyncio.gather(*tasks)
        loaded_count = sum(1 for r in results if r)
        logger.info(f"Загружена история для {loaded_count} инструментов.")

    async def _fetch_and_store_history(self, symbol: str, tf: str):
        try:
            history = await self.market_data.fetch_ohlcv(symbol, tf, limit=600)
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
            if isinstance(data, dict):
                for sym, rate_info in data.items():
                    if isinstance(rate_info, dict):
                        self.funding_rates[sym] = rate_info.get('fundingRate', 0.0)
        elif data_type == "avg_price":
            pass

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

        if len(df) > 1000:
            df = df.tail(1000).reset_index(drop=True)
        self.market_history[symbol][timeframe] = df

        # Обновление трейлинг-стопов по 1-минутным свечам.
        # CRITICAL: используем ATR из ПРЕДЫДУЩЕЙ (уже рассчитанной) свечи,
        # т.к. индикаторы для текущей свечи ещё не рассчитаны.
        if timeframe == "1m" and len(df) >= 3:
            current_price = df.iloc[-1]['close']
            atr = float(df.iloc[-2].get('atr', 0.0) or 0.0)
            if atr <= 0:
                atr = float(df.iloc[-3].get('atr', 0.0) or 0.0) if len(df) >= 4 else 0.0
            if atr <= 0:
                pass  # Skip trailing if no valid ATR
            else:
                adx = None
                df_15m = self.market_history.get(symbol, {}).get("15m")
                if df_15m is not None and not df_15m.empty and len(df_15m) > 1 and "adx" in df_15m.columns:
                    adx = df_15m.iloc[-2].get("adx", None)
                elif "adx" in df.columns:
                    adx = df.iloc[-2].get("adx", None)
                try:
                    adx = float(adx) if adx is not None else None
                except Exception:
                    adx = None
                asyncio.create_task(self.execution.schedule_update_positions(symbol, current_price, atr, adx))

        if len(df) < 60:
            return

        try:
            await asyncio.sleep(0.05)
            self.processed_candles += 1
            df = self._calculate_indicators(df)
            df['funding_rate'] = self.funding_rates.get(symbol, 0.0)
            self.market_history[symbol][timeframe] = df

            # Сигналы только по закрытым свечам
            if len(df) < 2:
                return
            df_eval = df.iloc[:-1].copy()
            if len(df_eval) < 60:
                return
            df_eval = self._calculate_indicators(df_eval)
            df_eval['funding_rate'] = self.funding_rates.get(symbol, 0.0)

            eval_candle_ts = df_eval.iloc[-1]['timestamp']
            try:
                eval_candle_ts = float(eval_candle_ts.timestamp()) if isinstance(eval_candle_ts, datetime) else float(eval_candle_ts)
            except Exception:
                eval_candle_ts = 0.0
            if eval_candle_ts > 10**11:
                eval_candle_ts /= 1000.0
            eval_key = (symbol, timeframe)
            if self._last_eval_candle_ts.get(eval_key) == eval_candle_ts:
                return
            self._last_eval_candle_ts[eval_key] = eval_candle_ts

            # ADX-фильтр для трендовых стратегий
            current_adx = df_eval.iloc[-1].get('adx', 0)
            if timeframe in ["1m", "5m"] and "15m" in self.market_history.get(symbol, {}):
                df_15m = self.market_history[symbol]["15m"]
                if not df_15m.empty and len(df_15m) > 2:
                    current_adx = df_15m.iloc[-2].get('adx', current_adx)

            for strategy in self.strategies:
                signal = strategy.evaluate(df_eval)
                if signal:
                    # ADX-фильтр: трендовые стратегии требуют ADX >= 20
                    trend_strategies = ["MA Trend", "Donchian", "Pullback"]
                    if signal['strategy'] in trend_strategies and current_adx < 20:
                        logger.info(f"[{symbol}] ADX={current_adx:.1f}<20, skip {signal['strategy']}")
                        continue

                    # DEDUPE
                    now = time.time()
                    cooldown = max(30, settings.signal_expiry_seconds)
                    cache_key = (symbol, signal['strategy'], signal['signal'])
                    last_ts = self._last_signals_cache.get(cache_key, 0)
                    if (now - last_ts) < cooldown:
                        continue
                    self._last_signals_cache[cache_key] = now

                    # Daily drawdown halt
                    if self.risk_manager.is_daily_halted():
                        logger.warning(f"[{symbol}] Daily drawdown limit reached, all entries halted")
                        return

                    # Duplicate-position guard: skip if position already open for this symbol
                    if symbol in self.execution.active_trades and not settings.pyramiding_enabled:
                        continue

                    # Фильтр направления
                    allowed_side = str(getattr(settings, "allowed_position_side", "BOTH") or "BOTH").upper()
                    signal_side = str(signal.get("signal", "")).upper()
                    if allowed_side in {"LONG", "SHORT"} and signal_side != allowed_side:
                        continue

                    # Фильтр Signal Expiry
                    candle_time = df_eval.iloc[-1]['timestamp']
                    now_ts = time.time()
                    candle_ts = candle_time if not isinstance(candle_time, datetime) else candle_time.timestamp()
                    if candle_ts > 10**11:
                        candle_ts /= 1000
                    tf_secs = get_timeframe_seconds(timeframe)
                    candle_age = now_ts - candle_ts
                    if candle_age > (tf_secs + settings.signal_expiry_seconds):
                        key = f"{symbol}:{signal['strategy']}"
                        self._stale_signals_count[key] += 1
                        if (now_ts - self._stale_signals_last_log_ts) >= 60:
                            top = sorted(self._stale_signals_count.items(), key=lambda kv: kv[1], reverse=True)[:5]
                            summary = ", ".join([f"{k}={v}" for k, v in top]) if top else "no-data"
                            logger.warning(f"Stale signals (60s summary): {summary}")
                            self._stale_signals_last_log_ts = now_ts
                            self._stale_signals_count.clear()
                        continue

                    # Листинг-фильтр (cached — один запрос на символ)
                    if symbol not in self._instrument_info_cache:
                        self._instrument_info_cache[symbol] = await self.market_data.fetch_instrument_info(symbol)
                    info = self._instrument_info_cache[symbol]
                    if info and 'info' in info and 'onboardDate' in info['info']:
                        onboard_ts = int(info['info']['onboardDate']) / 1000
                        onboard_date_str = datetime.fromtimestamp(onboard_ts).isoformat()
                        if not self.risk_manager.check_listing_days(onboard_date_str, settings.min_listing_days):
                            logger.warning(f"Монета {symbol} слишком новая. Листинг {onboard_date_str}")
                            continue

                    # Лимит позиций:
                    # берем максимум из live-метрик биржи и локального кэша,
                    # чтобы избежать ложной блокировки входов при рассинхроне active_trades.
                    cached_balance, cached_drawdown, metrics_open_count = await self.execution.get_account_metrics()
                    live_open_count = max(int(metrics_open_count or 0), len(self.execution.active_trades))
                    if live_open_count >= self.risk_manager.max_open_trades:
                        logger.warning(f"Лимит позиций ({live_open_count} >= {self.risk_manager.max_open_trades})")
                        now = time.time()
                        if not hasattr(self, '_last_limit_notify') or (now - self._last_limit_notify) > 900:
                            self._last_limit_notify = now
                            await send_telegram_msg(
                                f"⚠️ **ВХОД ПРОПУЩЕН**\n\n"
                                f"🔹 Символ: `{symbol}`\n"
                                f"📂 Причина: Лимит позиций ({live_open_count}/{self.risk_manager.max_open_trades})"
                            )
                        return

                    # Корреляционный фильтр
                    if not self.risk_manager.check_correlation_limit(symbol, signal['signal'], self.execution.active_trades):
                        continue

                    # Funding Rate фильтр
                    fr = self.funding_rates.get(symbol, 0.0)
                    if abs(fr) > settings.max_funding_rate:
                        logger.warning(f"Высокий Funding Rate {symbol}: {fr*100:.4f}%")
                        continue

                    # Regime Detection
                    if not self.risk_manager.is_volatility_sufficient(df_eval):
                        continue

                    # Скоринг (порог 0.55 — снижен с 0.65 т.к. стратегий меньше и они качественнее)
                    score = self.scorer.calculate_score(df_eval, signal)
                    if score < 0.55:
                        logger.info(f"[{symbol}] Score {score:.2f} < 0.55, skip {signal['strategy']}")
                        continue

                    # AI/Statistical prediction
                    ob = self.orderbooks.get(symbol)
                    features = FeatureGenerator.generate_features(df_eval, funding_rate=fr, orderbook=ob)
                    ai_prediction = self.ai_model.predict_win_probability(features, signal['signal'])

                    logger.info(f"SIGNAL: {signal['strategy']} | {symbol} {signal['signal']} | Score: {score:.2f} | WinProb: {ai_prediction['win_prob']:.2f}")

                    # AI фильтр (порог 0.55 — снижен: модель статистическая, не настоящий ML)
                    if ai_prediction['win_prob'] < 0.55:
                        logger.info(f"[{symbol}] AI prob {ai_prediction['win_prob']:.2f} < 0.55, skip")
                        continue

                    # External AI filter (optional — cascade: Groq → Grok → Gemini → OpenRouter)
                    ext_ai_decision = None
                    if self.external_ai.is_enabled:
                        try:
                            market_ctx = {
                                "atr": float(df_eval['atr'].iloc[-1]) if 'atr' in df_eval.columns else None,
                                "rsi": float(df_eval['RSI_fast'].iloc[-1]) if 'RSI_fast' in df_eval.columns else None,
                                "adx": float(current_adx) if current_adx else None,
                                "volume_ratio": features.get('volume_ratio'),
                                "funding_rate": fr,
                                "trend": "UP" if df_eval.iloc[-1]['close'] > df_eval.iloc[-1].get('ema50', 0) else "DOWN"
                            }
                            _ai_start = time.time()
                            ext_result = await self.external_ai.analyze_signal(
                                {**signal, "score": score, "stop_loss": None, "take_profit": None},
                                market_ctx
                            )
                            _ai_ms = int((time.time() - _ai_start) * 1000)
                            provider_name = ext_result.get("provider", "?")
                            rec = ext_result.get("recommendation", "PASS")
                            conf = ext_result.get("confidence", 0)
                            reasoning = ext_result.get("reasoning", "")

                            ext_ai_decision = {
                                "provider": provider_name, "recommendation": rec,
                                "confidence": conf, "reasoning": reasoning,
                                "latency_ms": _ai_ms,
                            }
                            logger.info(
                                f"[{symbol}] AI ({provider_name}): {rec} conf={conf:.2f} "
                                f"({_ai_ms}ms) — {reasoning[:80]}"
                            )

                            # Save to DB for analytics
                            try:
                                async with async_session() as s:
                                    s.add(AIDecisionLog(
                                        symbol=symbol, strategy=signal.get("strategy"),
                                        provider=provider_name, recommendation=rec,
                                        confidence=conf, reasoning=reasoning[:500],
                                        score=score, win_prob=ai_prediction["win_prob"],
                                        latency_ms=_ai_ms,
                                    ))
                                    await s.commit()
                            except Exception:
                                pass

                            if rec == "SKIP" and conf > 0.6:
                                continue
                        except Exception as ext_err:
                            logger.warning(f"[{symbol}] External AI error (non-blocking): {ext_err}")

                    # Расчёт SL/TP (guard against NaN ATR)
                    raw_atr = df_eval['atr'].iloc[-1] if 'atr' in df_eval.columns else 0.0
                    if pd.isna(raw_atr) or raw_atr <= 0:
                        raw_atr = df_eval['atr'].dropna().iloc[-1] if 'atr' in df_eval.columns and not df_eval['atr'].dropna().empty else 0.0
                    safe_atr = float(raw_atr) if not pd.isna(raw_atr) and raw_atr > 0 else 0.0

                    if safe_atr <= 0:
                        logger.warning(f"[{symbol}] ATR=0/NaN, skip signal (cannot compute stop)")
                        continue

                    lookback_p = df_eval.tail(20)
                    pattern_high = lookback_p['high'].max()
                    pattern_low = lookback_p['low'].min()
                    targets = StrategyRuleOf7.calculate_targets(pattern_high, pattern_low, signal['signal'])
                    sl = self.risk_manager.calculate_atr_stop(signal['entry_price'], safe_atr, signal['signal'])
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
                        "atr": safe_atr,
                        "ai_data": ai_prediction,
                        "timeframe": timeframe
                    }

                    # БД
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

                        # Link AI decision to signal
                        if ext_ai_decision and new_sig_model.id:
                            try:
                                session.add(AIDecisionLog(
                                    signal_id=new_sig_model.id,
                                    symbol=symbol, strategy=signal.get("strategy"),
                                    provider=ext_ai_decision["provider"],
                                    recommendation=ext_ai_decision["recommendation"],
                                    confidence=ext_ai_decision.get("confidence"),
                                    reasoning=ext_ai_decision.get("reasoning", "")[:500],
                                    score=score, win_prob=ai_prediction["win_prob"],
                                    latency_ms=ext_ai_decision.get("latency_ms"),
                                ))
                                await session.commit()
                            except Exception:
                                pass

                    # Уведомление
                    now_utc = datetime.now(timezone.utc).strftime('%H:%M:%S')
                    _dir_emoji = "🟢 LONG" if enrich_signal['signal'] == "LONG" else "🔴 SHORT"
                    status_msg = (
                        f"🚀 **НОВЫЙ СИГНАЛ**\n\n"
                        f"📊 Стратегия: {enrich_signal['strategy']}\n"
                        f"🔹 Символ: `{symbol}`\n"
                        f"📌 Направление: {_dir_emoji}\n\n"
                        f"💰 Вход: `{enrich_signal['entry_price']:.4f}`\n"
                        f"🛡 Стоп-лосс: `{enrich_signal['stop_loss']:.4f}`\n"
                        f"🎯 Тейк-профит: `{enrich_signal['take_profit']:.4f}`\n\n"
                        f"🤖 Win Prob: `{ai_prediction.get('win_prob', 0)*100:.1f}%`\n"
                        f"📈 Ож. доход: `{ai_prediction.get('expected_return', 0):.2f}%`\n"
                        f"📊 Score: `{score:.2f}`\n\n"
                        f"🕒 Время (UTC): {now_utc}\n"
                        f"⏳ Статус: ОЖИДАНИЕ"
                    )
                    await send_telegram_msg(status_msg)

                    # Исполнение
                    if settings.is_trading_enabled:
                        asyncio.create_task(
                            self.execution.execute_signal(enrich_signal, cached_balance, cached_drawdown, live_open_count)
                        )
                    else:
                        logger.info(f"Trading disabled. Signal {symbol} saved but not executed.")
        except Exception as e:
            self.errors_count += 1
            app_logger.error(f"Error in signal loop for {symbol}: {e}\n{traceback.format_exc()}")

            now = time.time()
            self._recent_error_ts.append(now)
            window_sec = 600
            errors_in_window = sum(1 for ts in self._recent_error_ts if (now - ts) <= window_sec)
            if errors_in_window >= 20 and settings.is_trading_enabled:
                settings.is_trading_enabled = False
                await send_telegram_msg(
                    f"🚨 **АВАРИЙНАЯ ОСТАНОВКА**\n\n"
                    f"⚠️ Слишком много ошибок за 10 мин: {errors_in_window}\n"
                    f"🔴 Торговля автоматически остановлена"
                )

            if not hasattr(self, '_last_error_notify') or (now - self._last_error_notify) > 1800:
                self._last_error_notify = now
                await send_telegram_msg(
                    f"❌ **КРИТИЧЕСКАЯ ОШИБКА**\n\n"
                    f"🔹 Символ: `{symbol}`\n"
                    f"⚠️ Ошибка: {str(e)[:200]}"
                )

    def _calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df['ema9'] = calculate_ema(df['close'], 9)
        df['ema20'] = calculate_ema(df['close'], 20)
        df['ema30'] = calculate_ema(df['close'], 30)
        df['ema50'] = calculate_ema(df['close'], 50)
        df['ema60'] = calculate_ema(df['close'], 60)
        df['ema200'] = calculate_ema(df['close'], 200)

        df['atr'] = calculate_atr(df, period=14)
        df['RSI'] = calculate_rsi(df['close'], period=21)
        df['RSI_fast'] = calculate_rsi(df['close'], period=14)

        upper, ma, lower = calculate_bollinger_bands(df['close'], period=20, std=2.0)
        df['upper'] = upper
        df['lower'] = lower

        adx_df = calculate_adx(df, period=14)
        df['adx'] = adx_df['adx']
        df['+di'] = adx_df['+di']
        df['-di'] = adx_df['-di']

        df['williams_r'] = calculate_williams_r(df, period=14)

        return df

    async def _heartbeat_loop(self):
        while True:
            await asyncio.sleep(3600)
            uptime_hours = (time.time() - self.start_time) / 3600
            msg = (
                f"💓 **ПУЛЬС БОТА**\n\n"
                f"⏱ Аптайм: `{uptime_hours:.1f}ч`\n"
                f"📊 Свечей обработано: `{self.processed_candles}`\n"
                f"❌ Ошибок: `{self.errors_count}`\n"
                f"🔍 Символов: `{len(self.market_history)}`\n"
                f"📈 Стратегий: `{len(self.strategies)}`"
            )
            await send_telegram_msg(msg)
