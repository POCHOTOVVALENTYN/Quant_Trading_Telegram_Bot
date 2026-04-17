import asyncio
import pandas as pd
import numpy as np
import traceback
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
import time
from datetime import datetime, timezone
from collections import defaultdict, deque


@dataclass
class MarketContext:
    """Multi-dimensional market regime snapshot for a symbol."""
    trend: str = "NEUTRAL"          # TREND / RANGE / NEUTRAL (ADX-based)
    volatility: str = "NORMAL"      # HIGH / LOW / NORMAL (ATR percentile)
    funding: str = "NORMAL"         # EXTREME_LONG / EXTREME_SHORT / NORMAL
    session: str = "US"             # ASIA / EU / US (UTC hour-based)
    daily_bias: Optional[str] = None  # LONG / SHORT / None

from config.settings import settings
from utils.logger import get_signal_logger, app_logger
from utils.notifier import send_telegram_msg
from services.market_data.market_streamer import MarketDataService
from core.strategies.strategies import (
    StrategyWRD, StrategyDonchian, StrategyMATrend,
    StrategyPullback, StrategyVolContraction,
    StrategyWideRangeReversal, StrategyWilliamsR,
    StrategyFundingSqueeze, StrategyRuleOf7,
    get_timeframe_seconds,
)
from core.indicators.indicators import (
    calculate_atr, calculate_rsi, calculate_bollinger_bands, calculate_csi, calculate_sma,
    calculate_ema, calculate_adx, calculate_williams_r, calculate_vwap
)
from core.strategies.scoring import SignalScorer, DynamicStrategyScorer
from ai.feature_generator import FeatureGenerator
from ai.model import AIModel, ExternalAIAdapter, ProviderConfig
from ai.scoring_learner import ScoringLearner, learning_loop
from services.ml_worker.client import MLWorkerClient
from core.risk.risk_manager import RiskManager
from core.indicators.cvd import CVDTracker
from ai.news_filter import NewsFilter
from core.execution.engine import ExecutionEngine
from database.session import async_session
from database.models.all_models import Signal, AIDecisionLog, SignalDecisionLog
from utils.exporter import exporter
from utils.metrics import (
    signals_generated, signals_filtered, signals_accepted,
    signal_score_hist, ai_latency_hist, regime_gauge,
    balance_gauge, drawdown_gauge, open_positions_gauge,
    signal_stage_total,
)

# Торговые сигналы только на этих TF (1m — трейлинг; 5m — данные/индикаторы; 1d — bias для фильтра).
TRADING_SIGNAL_TIMEFRAMES = frozenset({"15m", "1h", "4h"})

# Higher-TF сетапы (4h+1h) vs исполнение (15m+1h); Funding Squeeze — на всех трёх.
_STRATEGY_TIMEFRAME_MATRIX = {
    StrategyDonchian: frozenset({"4h", "1h"}),
    StrategyWRD: frozenset({"4h", "1h"}),
    StrategyVolContraction: frozenset({"4h", "1h"}),
    StrategyMATrend: frozenset({"4h", "1h"}),
    StrategyPullback: frozenset({"15m", "1h"}),
    StrategyWilliamsR: frozenset({"15m", "1h"}),
    StrategyWideRangeReversal: frozenset({"15m", "1h"}),
    StrategyFundingSqueeze: frozenset({"15m", "1h", "4h"}),
}

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

        self.dynamic_strategy_scorer = DynamicStrategyScorer()
        self.scorer = SignalScorer(dynamic_scorer=self.dynamic_strategy_scorer)
        self.scoring_learner = ScoringLearner(weights_file=settings.scoring_weights_file)
        self.ai_model = AIModel(weights=self.scoring_learner.get_weights())
        self.external_ai = self._init_external_ai()
        self.ml_classifier = MLWorkerClient() if settings.ml_validator_enabled else None
        self.cvd_tracker = CVDTracker(window_seconds=300)
        self.news_filter = NewsFilter(check_interval=3600)

        self.execution.register_trade_close_callback(self.dynamic_strategy_scorer.record_trade)
        self.market_history: Dict[str, Dict[str, pd.DataFrame]] = {}

        self.funding_rates: Dict[str, float] = {}
        self.orderbooks: Dict[str, Any] = {}
        self.processed_candles = 0
        self.start_time = time.time()
        self.errors_count = 0

        self._last_signals_cache: Dict[tuple, float] = {}
        self._stale_signals_count: Dict[str, int] = defaultdict(int)
        self._stale_signals_last_log_ts: float = 0.0
        self._expiry_debug_last_log_ts: Dict[str, float] = {}
        self._expiry_resync_last_attempt_ts: Dict[str, float] = {}
        self._last_eval_candle_ts: Dict[tuple, float] = {}

        self._recent_error_ts = deque(maxlen=200)
        self._instrument_info_cache: Dict[str, Any] = {}

        self.market_data.register_callback(self.on_market_data)

    # ATR multipliers for take-profit per strategy (R:R design)
    # Trend-following: wider targets (3-4 ATR), Mean-reversion: tighter (2-2.5 ATR)
    _STRATEGY_ATR_TP = {
        "Donchian":        4.0,
        "WRD":             3.5,
        "Vol Contraction": 3.0,
        "MA Trend":        3.5,
        "Pullback":        3.0,
        "Williams R":      2.0,
        "WRD Reversal":    2.5,
        "Funding Squeeze": 2.0,
    }

    _STRATEGY_SETUP_GROUP = {
        "Donchian": "breakout", "WRD": "breakout", "Vol Contraction": "breakout",
        "MA Trend": "trend", "Pullback": "trend",
        "Williams R": "mean_reversion", "WRD Reversal": "mean_reversion",
        "Funding Squeeze": "mean_reversion",
    }

    # Default strategy-regime matrix (used when settings.strategy_regime_matrix is empty)
    _DEFAULT_REGIME_MATRIX = {
        "Donchian":          {"trend": ["TREND", "NEUTRAL"], "volatility": ["*"], "funding": ["*"]},
        "WRD":               {"trend": ["TREND", "NEUTRAL"], "volatility": ["*"], "funding": ["*"]},
        "Vol Contraction":   {"trend": ["TREND", "NEUTRAL"], "volatility": ["LOW", "NORMAL"], "funding": ["*"]},
        "MA Trend":          {"trend": ["TREND", "NEUTRAL"], "volatility": ["*"], "funding": ["*"]},
        "Pullback":          {"trend": ["TREND", "NEUTRAL"], "volatility": ["*"], "funding": ["*"]},
        "Williams R":        {"trend": ["RANGE", "NEUTRAL"], "volatility": ["LOW", "NORMAL"], "funding": ["*"]},
        "WRD Reversal":      {"trend": ["RANGE", "NEUTRAL"], "volatility": ["*"], "funding": ["*"]},
        "Funding Squeeze":   {"trend": ["*"], "volatility": ["HIGH", "NORMAL"], "funding": ["EXTREME_LONG", "EXTREME_SHORT"]},
    }

    @staticmethod
    def _classify_market_regime(adx: float, trend_min: float, range_max: float) -> str:
        """TREND | RANGE | NEUTRAL — в зоне между порогами не режем входы (fail-safe)."""
        try:
            a = float(adx)
        except (TypeError, ValueError):
            return "NEUTRAL"
        if a >= trend_min:
            return "TREND"
        if a <= range_max:
            return "RANGE"
        return "NEUTRAL"

    def _build_market_context(
        self, symbol: str, df_eval: pd.DataFrame, market_regime: str, current_adx: float
    ) -> MarketContext:
        """Build a multi-dimensional MarketContext for the current bar."""
        ctx = MarketContext(trend=market_regime)

        # Volatility regime (ATR percentile vs lookback)
        lookback = int(getattr(settings, "regime_atr_lookback", 30))
        if "atr" in df_eval.columns and len(df_eval) >= lookback:
            atr_window = df_eval["atr"].tail(lookback).dropna()
            if len(atr_window) >= 5:
                current_atr = float(atr_window.iloc[-1])
                pct_rank = float((atr_window < current_atr).sum() / len(atr_window) * 100)
                if pct_rank >= settings.regime_atr_high_percentile:
                    ctx.volatility = "HIGH"
                elif pct_rank <= settings.regime_atr_low_percentile:
                    ctx.volatility = "LOW"

        # Funding regime
        fr = self.funding_rates.get(symbol, 0.0)
        extreme_thr = float(getattr(settings, "regime_funding_extreme", 0.001))
        if fr > extreme_thr:
            ctx.funding = "EXTREME_LONG"
        elif fr < -extreme_thr:
            ctx.funding = "EXTREME_SHORT"

        # Session (UTC-based)
        utc_hour = datetime.now(timezone.utc).hour
        if 0 <= utc_hour < 8:
            ctx.session = "ASIA"
        elif 8 <= utc_hour < 14:
            ctx.session = "EU"
        else:
            ctx.session = "US"

        # Daily bias (reuse existing method)
        ctx.daily_bias = self._get_daily_trend_bias(symbol)
        return ctx

    async def _persist_decision_log(self, sdl: dict, flags: dict, outcome: str) -> None:
        """Persist a signal decision log entry (fire-and-forget, never blocks trading)."""
        # Emit Prometheus metrics
        try:
            signals_generated.labels(
                strategy=sdl.get("strategy", "?"), direction=sdl.get("direction", "?")
            ).inc()
            if outcome.startswith("FILTERED:"):
                filter_name = outcome.split(":", 1)[1]
                signals_filtered.labels(filter_name=filter_name).inc()
            elif outcome == "ACCEPTED":
                signals_accepted.labels(strategy=sdl.get("strategy", "?")).inc()
                if sdl.get("score") is not None:
                    signal_score_hist.observe(sdl["score"])
        except Exception:
            pass
        try:
            async with async_session() as s:
                s.add(SignalDecisionLog(
                    symbol=sdl.get("symbol"), timeframe=sdl.get("timeframe"),
                    strategy=sdl.get("strategy"), direction=sdl.get("direction"),
                    entry_price=sdl.get("entry_price"),
                    adx=sdl.get("adx"), atr=sdl.get("atr"),
                    rsi=sdl.get("rsi"), volume_ratio=sdl.get("volume_ratio"),
                    funding_rate=sdl.get("funding_rate"),
                    regime=sdl.get("regime"), daily_bias=sdl.get("daily_bias"),
                    volatility_regime=sdl.get("volatility_regime"),
                    funding_regime=sdl.get("funding_regime"),
                    session=sdl.get("session"),
                    score=sdl.get("score"), win_prob=sdl.get("win_prob"),
                    ai_recommendation=sdl.get("ai_recommendation"),
                    ai_confidence=sdl.get("ai_confidence"),
                    f_daily_filter=flags.get("f_daily_filter"),
                    f_regime_router=flags.get("f_regime_router"),
                    f_adx_threshold=flags.get("f_adx_threshold"),
                    f_cooldown=flags.get("f_cooldown"),
                    f_daily_halt=flags.get("f_daily_halt"),
                    f_duplicate_pos=flags.get("f_duplicate_pos"),
                    f_side_filter=flags.get("f_side_filter"),
                    f_expiry=flags.get("f_expiry"),
                    f_listing_age=flags.get("f_listing_age"),
                    f_max_positions=flags.get("f_max_positions"),
                    f_correlation=flags.get("f_correlation"),
                    f_funding_rate=flags.get("f_funding_rate"),
                    f_volatility=flags.get("f_volatility"),
                    f_score=flags.get("f_score"),
                    f_ai_prob=flags.get("f_ai_prob"),
                    f_ext_ai=flags.get("f_ext_ai"),
                    f_ml_validator=flags.get("f_ml_validator"),
                    outcome=outcome,
                ))
                await s.commit()
        except Exception as e:
            logger.debug(f"Failed to persist decision log: {e}")

    @staticmethod
    def _mark_signal_stage(stage: str, strategy: str, timeframe: str) -> None:
        try:
            signal_stage_total.labels(
                stage=stage,
                strategy=strategy or "?",
                timeframe=timeframe or "?",
            ).inc()
        except Exception:
            pass

    @staticmethod
    def _compute_expiry_lag_after_close(
        candle_time,
        timeframe: str,
        now_ts: float | None = None,
        stream_recovering: bool = False,
    ) -> tuple[float, float, int, float]:
        """Return (lag_after_close, expiry_window, tf_secs, candle_ts_seconds)."""
        _now = float(time.time() if now_ts is None else now_ts)
        candle_ts = candle_time if not isinstance(candle_time, datetime) else candle_time.timestamp()
        candle_ts = float(candle_ts)
        if candle_ts > 10**11:
            candle_ts /= 1000.0
        tf_secs = int(get_timeframe_seconds(timeframe))
        base_expiry = int(getattr(settings, "signal_expiry_seconds", 120) or 120)
        extra_lag_budget = int(max(0, tf_secs * 0.15))
        expiry_window = max(base_expiry, extra_lag_budget)
        if stream_recovering:
            # During local stream recovery allow up to ~75% of TF lag-after-close
            # to avoid false expiry on freshly resynced bars.
            expiry_window = max(expiry_window, int(tf_secs * 0.75))
        # OHLCV timestamps are candle open time.
        lag_after_close = _now - (candle_ts + tf_secs)
        return float(lag_after_close), float(expiry_window), tf_secs, float(candle_ts)

    def _get_regime_matrix(self) -> dict:
        """Load regime matrix: config override or built-in default."""
        raw = getattr(settings, "strategy_regime_matrix", "")
        if raw:
            try:
                import json
                return json.loads(raw)
            except Exception:
                logger.warning("Invalid strategy_regime_matrix JSON, using defaults")
        return self._DEFAULT_REGIME_MATRIX

    def _strategy_allowed_for_regime(self, strategy_name: str, regime: str) -> bool:
        """Legacy single-dimension check (backward compat)."""
        return self._strategy_allowed_for_context(strategy_name, MarketContext(trend=regime))

    def _strategy_allowed_for_context(self, strategy_name: str, ctx: MarketContext) -> bool:
        """Check if strategy is allowed under the current multi-dimensional MarketContext."""
        matrix = self._get_regime_matrix()
        rules = matrix.get(strategy_name)
        if rules is None:
            return True  # unknown strategy → always allowed (fail-safe)
        for dim, current_val in [("trend", ctx.trend), ("volatility", ctx.volatility), ("funding", ctx.funding)]:
            allowed = rules.get(dim, ["*"])
            if "*" in allowed:
                continue
            if current_val not in allowed:
                return False
        return True

    def _get_daily_trend_bias(self, symbol: str) -> Optional[str]:
        """
        Returns daily trend for a symbol: LONG / SHORT / None.
        None = insufficient data → fail-safe (don't block entries).
        """
        df_1d = self.market_history.get(symbol, {}).get("1d")
        if df_1d is None or df_1d.empty or len(df_1d) < 30:
            return None
        try:
            ema_period = max(20, int(getattr(settings, "daily_filter_ema_period", 200) or 200))
            ema_col = f"ema{ema_period}"
            if ema_col not in df_1d.columns:
                return None
            row = df_1d.iloc[-2] if len(df_1d) >= 2 else df_1d.iloc[-1]
            close = float(row.get("close", 0.0) or 0.0)
            ema_v = float(row.get(ema_col, 0.0) or 0.0)
            if close <= 0 or ema_v <= 0:
                return None
            bias = "LONG" if close >= ema_v else "SHORT"
            logger.debug(
                f"[{symbol}] 1D bias={bias} (close={close:.2f} vs EMA{ema_period}={ema_v:.2f}, "
                f"delta={((close / ema_v) - 1) * 100:+.2f}%)"
            )
            return bias
        except Exception:
            return None

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
        if self.ml_classifier:
            await self.ml_classifier.start()
            
        await self.risk_manager.initialize_redis()

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
        if self.ml_classifier:
            await self.ml_classifier.stop()

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
        elif data_type == "trade":
            if isinstance(data, dict):
                trade_ts = int(data.get("timestamp", 0))
                if trade_ts > 0:
                    try:
                        from utils.metrics import ws_latency_ms
                        latency = (time.time() * 1000) - trade_ts
                        if latency > 0:
                            ws_latency_ms.observe(latency)
                    except Exception:
                        pass
                
                if self.cvd_tracker:
                    self.cvd_tracker.on_trade(
                        symbol=symbol,
                        price=float(data.get("price", 0)),
                        amount=float(data.get("amount", 0)),
                        side=str(data.get("side", "buy")),
                        timestamp_ms=trade_ts,
                    )
        elif data_type == "avg_price":
            pass

    async def _execute_with_retry(self, signal_data: dict, balance: float, drawdown: float, open_count: int, max_retries: int = 2):
        """Execute signal with retry on transient failures (network, timestamp sync)."""
        symbol = signal_data.get("symbol", "?")
        for attempt in range(1, max_retries + 1):
            try:
                await self.execution.execute_signal(signal_data, balance, drawdown, open_count)
                return
            except Exception as e:
                err = str(e)
                is_retryable = any(s in err for s in [
                    "Timestamp", "recvWindow", "timeout", "ConnectionError",
                    "ECONNRESET", "network", "502", "503",
                ])
                if is_retryable and attempt < max_retries:
                    wait = 2 ** attempt
                    logger.warning(
                        f"[{symbol}] execute_signal attempt {attempt}/{max_retries} failed "
                        f"(retryable): {err[:120]}. Retrying in {wait}s..."
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(
                        f"[{symbol}] execute_signal failed after {attempt} attempt(s): {err[:200]}"
                    )
                    return

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
                trade_tf = None
                if symbol in self.execution.active_trades:
                    _ttf = self.execution.active_trades[symbol].get("timeframe", "1h")
                    trade_tf = self.market_history.get(symbol, {}).get(_ttf)
                
                cvd_val = self.cvd_tracker.get_normalized_delta(symbol) if self.cvd_tracker else 0.0
                asyncio.create_task(self.execution.schedule_update_positions(symbol, current_price, atr, adx, df_tf=trade_tf, cvd_val=cvd_val))

        if len(df) < 60:
            return

        try:
            await asyncio.sleep(0.05)
            self.processed_candles += 1
            
            # Оптимизация: Считаем все индикаторы только для сигнальных ТФ
            is_signal_tf = timeframe in TRADING_SIGNAL_TIMEFRAMES
            df = self._calculate_indicators(df, minimal=not is_signal_tf)
            
            df['funding_rate'] = self.funding_rates.get(symbol, 0.0)
            self.market_history[symbol][timeframe] = df

            if not is_signal_tf:
                return

            # Сигналы только по закрытым свечам
            if len(df) < 2:
                return
            df_eval = df.iloc[:-1].copy()
            if len(df_eval) < 60:
                return
            # df_eval уже содержит индикаторы из df выше 
            # (но только если мы не в 1m)
            # Если мы в signal timeframe, df уже полностью посчитан.
            # Если нет - мы уже вышли выше.
            # Так что df_eval уже готов.

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

            current_adx = df_eval.iloc[-1].get('adx', 0)

            market_regime = "NEUTRAL"
            if getattr(settings, "strategy_regime_routing_enabled", True):
                try:
                    adx_val = float(current_adx)
                    if pd.isna(adx_val) or adx_val <= 0:
                        market_regime = "NEUTRAL"
                    else:
                        market_regime = self._classify_market_regime(
                            adx_val,
                            float(getattr(settings, "regime_adx_trend_min", 22.0)),
                            float(getattr(settings, "regime_adx_range_max", 18.0)),
                        )
                except Exception:
                    market_regime = "NEUTRAL"

            # Build multi-dimensional market context
            mkt_ctx = self._build_market_context(symbol, df_eval, market_regime, current_adx)

            fr = self.funding_rates.get(symbol, 0.0)
            _last_row = df_eval.iloc[-1]
            _rsi_val = float(_last_row.get('RSI_fast', 0)) if 'RSI_fast' in df_eval.columns else None
            _atr_val = float(_last_row.get('atr', 0)) if 'atr' in df_eval.columns else None
            _vol_ratio = float(_last_row['volume'] / df_eval['volume'].rolling(20).mean().iloc[-1]) if len(df_eval) >= 20 and df_eval['volume'].rolling(20).mean().iloc[-1] > 0 else None

            if self.news_filter.is_enabled:
                asyncio.ensure_future(self.news_filter.check_sentiment(self.external_ai))
                if self.news_filter.should_block_entry():
                    logger.info(
                        f"[{symbol}] News filter RISK-OFF active "
                        f"(sentiment={self.news_filter.last_sentiment}), skipping all signals"
                    )
                    return

            for strategy in self.strategies:
                allowed_tfs = _STRATEGY_TIMEFRAME_MATRIX.get(type(strategy))
                if allowed_tfs is not None and timeframe not in allowed_tfs:
                    continue
                signal = strategy.evaluate(df_eval)
                if signal:
                    self._mark_signal_stage("raw", signal.get("strategy", ""), timeframe)
                    sdl = {
                        "symbol": symbol, "timeframe": timeframe,
                        "strategy": signal.get("strategy", ""),
                        "direction": signal.get("signal", ""),
                        "entry_price": signal.get("entry_price"),
                        "adx": float(current_adx) if current_adx else None,
                        "atr": _atr_val, "rsi": _rsi_val,
                        "volume_ratio": _vol_ratio,
                        "funding_rate": fr, "regime": market_regime,
                        "daily_bias": mkt_ctx.daily_bias,
                        "volatility_regime": mkt_ctx.volatility,
                        "funding_regime": mkt_ctx.funding,
                        "session": mkt_ctx.session,
                        "score": None, "win_prob": None,
                        "ai_recommendation": None, "ai_confidence": None,
                    }
                    _filter_flags = {}

                    # 1D higher-TF filter
                    if getattr(settings, "use_daily_timeframe_filter", True):
                        daily_bias = self._get_daily_trend_bias(symbol)
                        sdl["daily_bias"] = daily_bias
                        if daily_bias is not None:
                            mtf_guarded = {"Donchian", "WRD", "Vol Contraction", "MA Trend", "Pullback"}
                            if signal.get("strategy") in mtf_guarded:
                                if str(signal.get("signal", "")).upper() != daily_bias:
                                    logger.info(
                                        f"[{symbol}] 1D filter: {signal['strategy']} {signal['signal']} "
                                        f"conflicts with daily {daily_bias}, skip"
                                    )
                                    _filter_flags["f_daily_filter"] = False
                                    self._mark_signal_stage("filtered_daily_filter", signal.get("strategy", ""), timeframe)
                                    await self._persist_decision_log(sdl, _filter_flags, "FILTERED:daily_filter")
                                    continue
                    _filter_flags["f_daily_filter"] = True
                    self._mark_signal_stage("passed_daily_filter", signal.get("strategy", ""), timeframe)

                    # Regime router (multi-dimensional)
                    if getattr(settings, "strategy_regime_routing_enabled", True):
                        strat_name = signal.get("strategy", "")
                        if not self._strategy_allowed_for_context(strat_name, mkt_ctx):
                            logger.info(
                                f"[{symbol}] Context {mkt_ctx.trend}/{mkt_ctx.volatility}/{mkt_ctx.funding}: skip {strat_name}"
                            )
                            _filter_flags["f_regime_router"] = False
                            self._mark_signal_stage("filtered_regime_router", signal.get("strategy", ""), timeframe)
                            await self._persist_decision_log(sdl, _filter_flags, "FILTERED:regime_router")
                            continue
                    _filter_flags["f_regime_router"] = True
                    self._mark_signal_stage("passed_regime_router", signal.get("strategy", ""), timeframe)

                    # ADX threshold for trend strategies
                    trend_strategies = ["MA Trend", "Donchian", "Pullback"]
                    if signal['strategy'] in trend_strategies and current_adx < 20:
                        logger.info(f"[{symbol}] ADX={current_adx:.1f}<20, skip {signal['strategy']}")
                        _filter_flags["f_adx_threshold"] = False
                        self._mark_signal_stage("filtered_adx_threshold", signal.get("strategy", ""), timeframe)
                        await self._persist_decision_log(sdl, _filter_flags, "FILTERED:adx_threshold")
                        continue
                    _filter_flags["f_adx_threshold"] = True

                    # Cooldown dedupe
                    now = time.time()
                    cooldown = max(30, settings.signal_expiry_seconds)
                    cache_key = (symbol, signal['strategy'], signal['signal'])
                    last_ts = self._last_signals_cache.get(cache_key, 0)
                    if (now - last_ts) < cooldown:
                        _filter_flags["f_cooldown"] = False
                        await self._persist_decision_log(sdl, _filter_flags, "FILTERED:cooldown")
                        continue
                    self._last_signals_cache[cache_key] = now
                    _filter_flags["f_cooldown"] = True

                    # Daily drawdown halt
                    if self.risk_manager.is_daily_halted():
                        logger.warning(f"[{symbol}] Daily drawdown limit reached, all entries halted")
                        _filter_flags["f_daily_halt"] = False
                        await self._persist_decision_log(sdl, _filter_flags, "FILTERED:daily_halt")
                        return
                    _filter_flags["f_daily_halt"] = True

                    # Duplicate-position guard
                    if symbol in self.execution.active_trades and not settings.pyramiding_enabled:
                        _filter_flags["f_duplicate_pos"] = False
                        self._mark_signal_stage("filtered_duplicate_pos", signal.get("strategy", ""), timeframe)
                        await self._persist_decision_log(sdl, _filter_flags, "FILTERED:duplicate_pos")
                        continue
                    _filter_flags["f_duplicate_pos"] = True

                    # Direction filter
                    allowed_side = str(getattr(settings, "allowed_position_side", "BOTH") or "BOTH").upper()
                    signal_side = str(signal.get("signal", "")).upper()
                    if allowed_side in {"LONG", "SHORT"} and signal_side != allowed_side:
                        _filter_flags["f_side_filter"] = False
                        self._mark_signal_stage("filtered_side_filter", signal.get("strategy", ""), timeframe)
                        await self._persist_decision_log(sdl, _filter_flags, "FILTERED:side_filter")
                        continue
                    _filter_flags["f_side_filter"] = True

                    # Signal expiry
                    candle_time = df_eval.iloc[-1]['timestamp']
                    now_ts = time.time()
                    stream_recovering = False
                    try:
                        stream_recovering = bool(self.market_data.is_stream_recovering(symbol, timeframe))
                    except Exception:
                        stream_recovering = False
                    bar_lag_after_close, expiry_window, tf_secs, candle_ts = self._compute_expiry_lag_after_close(
                        candle_time, timeframe, now_ts=now_ts, stream_recovering=stream_recovering
                    )
                    if bar_lag_after_close > expiry_window:
                        # Try one cheap REST refresh before rejecting as stale:
                        # if WS stream lagged, REST may already have a fresher bar.
                        resync_key = f"{symbol}:{timeframe}"
                        last_resync = float(self._expiry_resync_last_attempt_ts.get(resync_key, 0.0))
                        if (now_ts - last_resync) >= 30.0:
                            self._expiry_resync_last_attempt_ts[resync_key] = now_ts
                            try:
                                fresh = await self.market_data.fetch_ohlcv(symbol, timeframe, limit=3)
                                if fresh and len(fresh) >= 2:
                                    latest_closed = fresh[-2]
                                    latest_closed_ts = float(latest_closed[0])
                                    if latest_closed_ts > 10**11:
                                        latest_closed_ts /= 1000.0
                                    if latest_closed_ts > (candle_ts + 1e-6):
                                        logger.warning(
                                            f"[{symbol}] Stale {timeframe} eval candle, REST has newer closed bar. "
                                            "Skipping current cycle to await refreshed WS candle."
                                        )
                                        continue
                            except Exception:
                                pass
                        key = f"{symbol}:{signal['strategy']}"
                        self._stale_signals_count[key] += 1
                        dbg_key = f"{symbol}:{timeframe}:{signal.get('strategy', 'unknown')}"
                        last_dbg = float(self._expiry_debug_last_log_ts.get(dbg_key, 0.0))
                        if (now_ts - last_dbg) >= 30.0:
                            logger.info(
                                f"[EXPIRY_DEBUG] {symbol} {timeframe} {signal.get('strategy', 'unknown')}: "
                                f"lag_after_close={bar_lag_after_close:.1f}s > limit={expiry_window:.1f}s "
                                f"(tf={tf_secs}s, base_expiry={int(getattr(settings, 'signal_expiry_seconds', 120) or 120)}s, "
                                f"extra_lag={int(max(0, tf_secs * 0.15))}s, recovering={int(stream_recovering)})"
                            )
                            self._expiry_debug_last_log_ts[dbg_key] = now_ts
                        if (now_ts - self._stale_signals_last_log_ts) >= 60:
                            top = sorted(self._stale_signals_count.items(), key=lambda kv: kv[1], reverse=True)[:5]
                            summary = ", ".join([f"{k}={v}" for k, v in top]) if top else "no-data"
                            logger.warning(f"Stale signals (60s summary): {summary}")
                            self._stale_signals_last_log_ts = now_ts
                            self._stale_signals_count.clear()
                        _filter_flags["f_expiry"] = False
                        self._mark_signal_stage("filtered_expiry", signal.get("strategy", ""), timeframe)
                        await self._persist_decision_log(sdl, _filter_flags, "FILTERED:expiry")
                        continue
                    _filter_flags["f_expiry"] = True

                    # Listing age filter
                    if symbol not in self._instrument_info_cache:
                        self._instrument_info_cache[symbol] = await self.market_data.fetch_instrument_info(symbol)
                    info = self._instrument_info_cache[symbol]
                    if info and 'info' in info and 'onboardDate' in info['info']:
                        onboard_ts = int(info['info']['onboardDate']) / 1000
                        onboard_date_str = datetime.fromtimestamp(onboard_ts).isoformat()
                        if not self.risk_manager.check_listing_days(onboard_date_str, settings.min_listing_days):
                            logger.warning(f"Монета {symbol} слишком новая. Листинг {onboard_date_str}")
                            _filter_flags["f_listing_age"] = False
                            self._mark_signal_stage("filtered_listing_age", signal.get("strategy", ""), timeframe)
                            await self._persist_decision_log(sdl, _filter_flags, "FILTERED:listing_age")
                            continue
                    _filter_flags["f_listing_age"] = True

                    # Position limit
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
                        _filter_flags["f_max_positions"] = False
                        self._mark_signal_stage("filtered_max_positions", signal.get("strategy", ""), timeframe)
                        await self._persist_decision_log(sdl, _filter_flags, "FILTERED:max_positions")
                        return
                    _filter_flags["f_max_positions"] = True

                    # Correlation filter
                    if not self.risk_manager.check_correlation_limit(symbol, signal['signal'], self.execution.active_trades):
                        _filter_flags["f_correlation"] = False
                        self._mark_signal_stage("filtered_correlation", signal.get("strategy", ""), timeframe)
                        await self._persist_decision_log(sdl, _filter_flags, "FILTERED:correlation")
                        continue
                    _filter_flags["f_correlation"] = True

                    # Funding rate filter
                    if abs(fr) > settings.max_funding_rate:
                        logger.warning(f"Высокий Funding Rate {symbol}: {fr*100:.4f}%")
                        _filter_flags["f_funding_rate"] = False
                        self._mark_signal_stage("filtered_funding_rate", signal.get("strategy", ""), timeframe)
                        await self._persist_decision_log(sdl, _filter_flags, "FILTERED:funding_rate")
                        continue
                    _filter_flags["f_funding_rate"] = True

                    # Volatility regime filter
                    if not self.risk_manager.is_volatility_sufficient(df_eval):
                        _filter_flags["f_volatility"] = False
                        self._mark_signal_stage("filtered_volatility", signal.get("strategy", ""), timeframe)
                        await self._persist_decision_log(sdl, _filter_flags, "FILTERED:volatility")
                        continue
                    _filter_flags["f_volatility"] = True

                    # Pre-trade feasibility gate: skip impossible orders before scoring/AI/execution.
                    raw_atr = df_eval['atr'].iloc[-1] if 'atr' in df_eval.columns else 0.0
                    if pd.isna(raw_atr) or raw_atr <= 0:
                        raw_atr = df_eval['atr'].dropna().iloc[-1] if 'atr' in df_eval.columns and not df_eval['atr'].dropna().empty else 0.0
                    safe_atr = float(raw_atr) if not pd.isna(raw_atr) and raw_atr > 0 else 0.0
                    if safe_atr <= 0:
                        logger.warning(f"[{symbol}] ATR=0/NaN, skip signal (cannot compute stop)")
                        self._mark_signal_stage("filtered_invalid_atr", signal.get("strategy", ""), timeframe)
                        continue
                    candidate_sl = self.risk_manager.calculate_atr_stop(signal['entry_price'], safe_atr, signal['signal'])
                    size_check = self.risk_manager.assess_trade_feasibility(
                        account_balance=float(cached_balance or 0.0),
                        entry_price=float(signal['entry_price']),
                        stop_loss_price=float(candidate_sl),
                        market_info=info,
                        market_context={
                            "session": mkt_ctx.session,
                            "volatility": mkt_ctx.volatility,
                            "funding": mkt_ctx.funding,
                            "symbol": symbol,
                        },
                    )
                    if not size_check.get("feasible", False):
                        logger.info(f"[{symbol}] Size infeasible ({size_check.get('reason', 'unknown')}), skip {signal['strategy']}")
                        self._mark_signal_stage("filtered_size_feasibility", signal.get("strategy", ""), timeframe)
                        await self._persist_decision_log(sdl, _filter_flags, f"FILTERED:{size_check.get('reason', 'size_feasibility')}")
                        continue

                    # Scoring
                    score = self.scorer.calculate_score(df_eval, signal)
                    sdl["score"] = score
                    score_threshold = float(getattr(settings, "signal_score_threshold", 0.55) or 0.55)
                    if score < score_threshold:
                        logger.info(f"[{symbol}] Score {score:.2f} < {score_threshold:.2f}, skip {signal['strategy']}")
                        _filter_flags["f_score"] = False
                        self._mark_signal_stage("filtered_score", signal.get("strategy", ""), timeframe)
                        await self._persist_decision_log(sdl, _filter_flags, "FILTERED:score")
                        continue
                    _filter_flags["f_score"] = True
                    self._mark_signal_stage("passed_score", signal.get("strategy", ""), timeframe)

                    # AI/Statistical prediction
                    ob = self.orderbooks.get(symbol)
                    cvd_val = self.cvd_tracker.get_cvd_normalized(symbol) if self.cvd_tracker else 0.0
                    features = FeatureGenerator.generate_features(df_eval, funding_rate=fr, orderbook=ob, cvd_norm=cvd_val)
                    ai_prediction = self.ai_model.predict_win_probability(features, signal['signal'])
                    sdl["win_prob"] = ai_prediction['win_prob']

                    logger.info(f"SIGNAL: {signal['strategy']} | {symbol} {signal['signal']} | Score: {score:.2f} | WinProb: {ai_prediction['win_prob']:.2f}")

                    ai_threshold = float(getattr(settings, "ai_win_prob_threshold", 0.55) or 0.55)
                    if ai_prediction['win_prob'] < ai_threshold:
                        logger.info(f"[{symbol}] AI prob {ai_prediction['win_prob']:.2f} < {ai_threshold:.2f}, skip")
                        _filter_flags["f_ai_prob"] = False
                        self._mark_signal_stage("filtered_ai_prob", signal.get("strategy", ""), timeframe)
                        await self._persist_decision_log(sdl, _filter_flags, "FILTERED:ai_prob")
                        continue
                    _filter_flags["f_ai_prob"] = True
                    self._mark_signal_stage("passed_ai_prob", signal.get("strategy", ""), timeframe)

                    # External AI filter (cascade: Groq -> Grok -> Gemini -> OpenRouter)
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
                            sdl["ai_recommendation"] = rec
                            sdl["ai_confidence"] = conf
                            logger.info(
                                f"[{symbol}] AI ({provider_name}): {rec} conf={conf:.2f} "
                                f"({_ai_ms}ms) — {reasoning[:80]}"
                            )

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
                                _filter_flags["f_ext_ai"] = False
                                await self._persist_decision_log(sdl, _filter_flags, "FILTERED:ext_ai")
                                continue
                        except Exception as ext_err:
                            logger.warning(f"[{symbol}] External AI error (non-blocking): {ext_err}")
                    _filter_flags["f_ext_ai"] = True

                    # ML classifier (shadow or gate mode)
                    if self.ml_classifier:
                        ml_features = {
                            "adx": sdl.get("adx", 0), "atr": sdl.get("atr", 0),
                            "rsi": sdl.get("rsi", 0), "volume_ratio": sdl.get("volume_ratio", 0),
                            "funding_rate": sdl.get("funding_rate", 0),
                            "score": sdl.get("score", 0), "win_prob": sdl.get("win_prob", 0),
                            "ai_confidence": sdl.get("ai_confidence", 0),
                            "cvd_norm": cvd_val,
                            "regime_TREND": 1.0 if market_regime == "TREND" else 0.0,
                            "regime_RANGE": 1.0 if market_regime == "RANGE" else 0.0,
                            "regime_NEUTRAL": 1.0 if market_regime == "NEUTRAL" else 0.0,
                            "volatility_HIGH": 1.0 if mkt_ctx.volatility == "HIGH" else 0.0,
                            "volatility_LOW": 1.0 if mkt_ctx.volatility == "LOW" else 0.0,
                            "volatility_NORMAL": 1.0 if mkt_ctx.volatility == "NORMAL" else 0.0,
                            "funding_EXTREME_LONG": 1.0 if mkt_ctx.funding == "EXTREME_LONG" else 0.0,
                            "funding_EXTREME_SHORT": 1.0 if mkt_ctx.funding == "EXTREME_SHORT" else 0.0,
                            "funding_NORMAL": 1.0 if mkt_ctx.funding == "NORMAL" else 0.0,
                            "session_ASIA": 1.0 if mkt_ctx.session == "ASIA" else 0.0,
                            "session_EU": 1.0 if mkt_ctx.session == "EU" else 0.0,
                            "session_US": 1.0 if mkt_ctx.session == "US" else 0.0,
                            "direction_LONG": 1.0 if signal.get("signal") == "LONG" else 0.0,
                            "direction_SHORT": 1.0 if signal.get("signal") == "SHORT" else 0.0,
                        }
                        ml_prob = await self.ml_classifier.predict_proba(ml_features)
                        logger.info(f"[{symbol}] ML classifier: prob={ml_prob:.3f}")
                        signal["ml_prob"] = ml_prob

                        if not settings.ml_validator_shadow_mode:
                            if ml_prob < settings.ml_validator_threshold:
                                logger.info(f"[{symbol}] ML filtered: {ml_prob:.3f} < {settings.ml_validator_threshold}")
                                _filter_flags["f_ml_validator"] = False
                                await self._persist_decision_log(sdl, _filter_flags, "FILTERED:ml_validator")
                                continue
                        _filter_flags["f_ml_validator"] = True

                    lookback_p = df_eval.tail(20)
                    pattern_high = lookback_p['high'].max()
                    pattern_low = lookback_p['low'].min()
                    targets = StrategyRuleOf7.calculate_targets(pattern_high, pattern_low, signal['signal'])
                    sl = self.risk_manager.calculate_atr_stop(signal['entry_price'], safe_atr, signal['signal'])

                    # Per-strategy ATR-based TP with proper R:R ratios
                    _atr_tp_mult = self._STRATEGY_ATR_TP.get(signal['strategy'], 3.0)
                    atr_tp = (
                        signal['entry_price'] + safe_atr * _atr_tp_mult
                        if signal['signal'] == "LONG"
                        else signal['entry_price'] - safe_atr * _atr_tp_mult
                    )
                    rule7_tp = next(iter(targets.values()), None) if targets else None
                    if rule7_tp is not None:
                        if signal['signal'] == "LONG":
                            tp = max(atr_tp, rule7_tp)
                        else:
                            tp = min(atr_tp, rule7_tp)
                    else:
                        tp = atr_tp

                    _setup_group = self._STRATEGY_SETUP_GROUP.get(signal['strategy'], "trend")

                    _invalidation_level = None
                    _breakout_level = None
                    _ma_at_entry = {}
                    try:
                        if _setup_group == "breakout":
                            _lookback_inv = df_eval.tail(20)
                            if signal['signal'] == "LONG":
                                _breakout_level = float(_lookback_inv['high'].max())
                                _invalidation_level = float(_lookback_inv['low'].min())
                            else:
                                _breakout_level = float(_lookback_inv['low'].min())
                                _invalidation_level = float(_lookback_inv['high'].max())
                        elif _setup_group == "trend":
                            _lr = df_eval.iloc[-1]
                            if signal['signal'] == "LONG":
                                _invalidation_level = float(_lr.get('ema50', 0) or 0) if 'ema50' in df_eval.columns else None
                            else:
                                _invalidation_level = float(_lr.get('ema50', 0) or 0) if 'ema50' in df_eval.columns else None
                        for col in ('ema20', 'ema50', 'ema200'):
                            if col in df_eval.columns:
                                v = df_eval.iloc[-1].get(col)
                                if v is not None and not (isinstance(v, float) and v != v):
                                    _ma_at_entry[col] = float(v)
                    except Exception:
                        pass

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
                        "timeframe": timeframe,
                        "market_context": {
                            "session": mkt_ctx.session,
                            "volatility": mkt_ctx.volatility,
                            "funding": mkt_ctx.funding,
                            "symbol": symbol,
                        },
                        "setup_group": _setup_group,
                        "breakout_level": _breakout_level,
                        "invalidation_level": _invalidation_level,
                        "ma_at_entry": _ma_at_entry,
                    }
                    self._mark_signal_stage("accepted", signal.get("strategy", ""), timeframe)

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

                    # Log accepted signal to decision journal
                    await self._persist_decision_log(sdl, _filter_flags, "ACCEPTED")

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

                    if settings.is_trading_enabled:
                        asyncio.create_task(
                            self._execute_with_retry(enrich_signal, cached_balance, cached_drawdown, live_open_count)
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

    def _calculate_indicators(self, df: pd.DataFrame, minimal: bool = False) -> pd.DataFrame:
        if minimal:
            df['atr'] = calculate_atr(df, period=14)
            adx_df = calculate_adx(df, period=14)
            df['adx'] = adx_df['adx']
            return df
            
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
            if self.cvd_tracker:
                try:
                    self.cvd_tracker.cleanup()
                except Exception:
                    pass

            await send_telegram_msg(msg)
