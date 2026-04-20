import asyncio
import random
import ccxt.pro as ccxtpro
from typing import Dict, Callable, Tuple
from core.strategies.strategies import get_timeframe_seconds
from utils.logger import get_market_data_logger
from utils.binance_api import BinanceCallPolicy, BinanceRateLimiter, call_with_binance_retry

try:
    from utils.metrics import (
        market_data_stream_age_seconds,
        market_data_stream_recovering,
        market_data_stream_restarts,
        market_data_watchdog_events,
    )
except Exception:
    market_data_stream_age_seconds = None
    market_data_stream_recovering = None
    market_data_stream_restarts = None
    market_data_watchdog_events = None

app_logger = get_market_data_logger()

class MarketDataService:
    def __init__(self, symbols: list[str], timeframes: list[str], exchange: ccxtpro.binance = None):
        """
        Инициализация сервиса маркет-даты (Этап 4).
        
        Источники: Binance WebSocket + REST fallback
        Потоки: свечи, ордербук, funding rate.
        """
        self.symbols = symbols
        self.timeframes = timeframes
        self.last_candle_time: Dict[str, float] = {}
        from config.settings import settings
        
        if exchange:
            self.exchange = exchange
            # Принудительно ставим боевой WS для стабильной маркет-даты, 
            # даже если основной клиент в sandbox-режиме
            prod_ws = "wss://fstream.binance.com/ws"
            if hasattr(self.exchange, 'urls'):
                self.exchange.urls['test']['ws']['future'] = prod_ws
                self.exchange.urls['api']['ws']['future'] = prod_ws
                app_logger.info(f"🔮 MarketDataService: используем PROD WS для графиков: {prod_ws}")
        else:
            self.exchange = ccxtpro.binance({
                'enableRateLimit': True,
                'timeout': int(settings.api_timeout_seconds * 1000), 
                'options': {'defaultType': 'future'}
            })
            if settings.testnet:
                self.exchange.set_sandbox_mode(True)
                # Переопределяем WS на боевой для стабильности
                self.exchange.urls['test']['ws']['future'] = "wss://fstream.binance.com/ws"
                self.exchange.urls['api']['ws']['future'] = "wss://fstream.binance.com/ws"
        self.running = False
        self.callbacks = [] # type: list[Callable]
        self.instrument_info = {} # Кэш инфо об инструментах
        self._last_ws_error_log_ts: Dict[str, float] = {}
        self._last_ws_global_error_log_ts: float = 0.0
        self._watchdog_consecutive_failures: Dict[str, int] = {}
        self._stream_recovering: Dict[str, bool] = {}
        self._last_global_ws_reset_ts: float = 0.0
        self._tasks: Dict[str, asyncio.Task] = {}
        self._streaming_active = False
        self._rest_limiter = BinanceRateLimiter(max_concurrent=2)

    async def _rest_call(self, op, *, ctx: str):
        try:
            return await call_with_binance_retry(
                op=op,
                exchange=self.exchange,
                limiter=self._rest_limiter,
                policy=BinanceCallPolicy(max_attempts=4, base_delay=0.5, max_delay=5.0, timeout_seconds=20.0),
            )
        except Exception as e:
            app_logger.warning(f"⚠️ [BINANCE_REST] {ctx}: {e}")
            raise

    @staticmethod
    def _split_stream_key(stream_key: str) -> Tuple[str, str]:
        symbol, timeframe = stream_key.rsplit(":", 1)
        return symbol, timeframe

    def _soft_stale_threshold_seconds(self, timeframe: str) -> float:
        from config.settings import settings
        tf_seconds = max(60, int(get_timeframe_seconds(timeframe)))
        # For OHLCV streams we expect frequent updates of current candle.
        # 2x timeframe is too permissive (especially for 1h/4h) and hides dead streams.
        return float(max(int(getattr(settings, "watchdog_soft_floor_seconds", 120) or 120), tf_seconds * 0.25))

    def _hard_stale_threshold_seconds(self, timeframe: str) -> float:
        from config.settings import settings
        tf_seconds = max(60, int(get_timeframe_seconds(timeframe)))
        return float(max(int(getattr(settings, "watchdog_hard_floor_seconds", 300) or 300), tf_seconds * 0.5))

    def _set_stream_metrics(self, stream_key: str, *, age_seconds: float = None, recovering: bool = None) -> None:
        symbol, timeframe = self._split_stream_key(stream_key)
        try:
            if age_seconds is not None and market_data_stream_age_seconds:
                market_data_stream_age_seconds.labels(symbol=symbol, timeframe=timeframe).set(max(0.0, float(age_seconds)))
            if recovering is not None and market_data_stream_recovering:
                market_data_stream_recovering.labels(symbol=symbol, timeframe=timeframe).set(1 if recovering else 0)
        except Exception:
            pass

    def _mark_stream_alive(self, stream_key: str, now: float) -> None:
        self.last_candle_time[stream_key] = now
        old_recovering = self._stream_recovering.get(stream_key, False)
        self._stream_recovering[stream_key] = False
        self._watchdog_consecutive_failures[stream_key] = 0
        self._set_stream_metrics(stream_key, age_seconds=0.0, recovering=False)
        
        if old_recovering:
            symbol, timeframe = self._split_stream_key(stream_key)
            for cb in self.callbacks:
                asyncio.create_task(cb("status", symbol, timeframe, {"recovering": False}))

    def _mark_stream_recovering(self, stream_key: str, *, reason: str) -> None:
        self._stream_recovering[stream_key] = True
        self._watchdog_consecutive_failures[stream_key] = self._watchdog_consecutive_failures.get(stream_key, 0) + 1
        self._set_stream_metrics(stream_key, recovering=True)
        
        symbol, timeframe = self._split_stream_key(stream_key)
        for cb in self.callbacks:
            asyncio.create_task(cb("status", symbol, timeframe, {"recovering": True, "reason": reason}))
        try:
            if market_data_stream_restarts:
                symbol, timeframe = self._split_stream_key(stream_key)
                market_data_stream_restarts.labels(symbol=symbol, timeframe=timeframe, reason=reason).inc()
        except Exception:
            pass

    def is_stream_recovering(self, symbol: str, timeframe: str) -> bool:
        """Public read access for orchestrator safeguards (expiry/entry guards)."""
        stream_key = f"{symbol}:{timeframe}"
        return bool(self._stream_recovering.get(stream_key, False))

    async def _refresh_stream_via_rest(self, stream_key: str) -> bool:
        """Try to refresh a stale OHLCV stream via REST without global WS reset."""
        symbol, timeframe = self._split_stream_key(stream_key)
        try:
            rest_candles = await self._rest_call(
                lambda: self.exchange.fetch_ohlcv(symbol, timeframe, limit=3),
                ctx=f"refresh_stream_via_rest({symbol},{timeframe})",
            )
            if not rest_candles:
                return False
            self._mark_stream_alive(stream_key, asyncio.get_event_loop().time())
            try:
                if market_data_stream_restarts:
                    market_data_stream_restarts.labels(
                        symbol=symbol, timeframe=timeframe, reason="watchdog_rest_refresh"
                    ).inc()
            except Exception:
                pass
            for cb in self.callbacks:
                await cb("ohlcv", symbol, timeframe, rest_candles[-1])
            return True
        except Exception:
            return False

    async def _restart_stream(self, symbol: str, timeframe: str):
        """Force cancel and restart a specific OHLCV stream task."""
        stream_key = f"{symbol}:{timeframe}"
        if stream_key in self._tasks:
            app_logger.warning(f"🔄 [WATCHDOG] Cancelling dead stream task for {stream_key}")
            self._tasks[stream_key].cancel()
            try:
                await self._tasks[stream_key]
            except asyncio.CancelledError:
                pass
        
        # Give it a moment to clear
        await asyncio.sleep(0.5)
        self._tasks[stream_key] = asyncio.create_task(self.watch_ohlcv(symbol, timeframe))
        app_logger.info(f"🚀 [WATCHDOG] Stream task for {stream_key} restarted")

    async def _close_ws_clients(self, reason: str) -> None:
        try:
            target_urls = []
            if hasattr(self.exchange, "urls"):
                ws_api = self.exchange.urls.get("api", {}).get("ws", {}).get("future")
                ws_test = self.exchange.urls.get("test", {}).get("ws", {}).get("future")
                if ws_api:
                    target_urls.append(ws_api)
                if ws_test and ws_test not in target_urls:
                    target_urls.append(ws_test)
            for u in target_urls:
                if u in getattr(self.exchange, "clients", {}):
                    client = self.exchange.clients[u]
                    await client.close()
                    del self.exchange.clients[u]
            if target_urls and market_data_watchdog_events:
                market_data_watchdog_events.labels(event_type=reason).inc()
        except Exception as close_err:
            app_logger.debug(f"WS cleanup error ({reason}): {close_err}")

    def register_callback(self, cb: Callable):
        self.callbacks.append(cb)

    async def watch_ohlcv(self, symbol: str, timeframe: str):
        app_logger.info(f"Начало отслеживания OHLCV для {symbol} ({timeframe})")
        reconnect_backoff = 1.0
        stream_key = f"{symbol}:{timeframe}"
        self._mark_stream_alive(stream_key, asyncio.get_event_loop().time())
        while self.running:
            try:
                candles = await self.exchange.watch_ohlcv(symbol, timeframe)
                reconnect_backoff = 1.0
                self._mark_stream_alive(stream_key, asyncio.get_event_loop().time())
                if not candles or len(candles) == 0:
                    await asyncio.sleep(1) # Страховка от беск. цикла при пустых данных
                    continue
                # notify systems
                for cb in self.callbacks:
                    await cb("ohlcv", symbol, timeframe, candles[-1])
            except Exception as e:
                now = asyncio.get_event_loop().time()
                err_text = str(e)
                # Антишум:
                # 1) по каждому потоку не чаще 1 раза в 15с;
                # 2) глобально не чаще 1 раза в 2с при массовом reconnect storm.
                should_log = (now - self._last_ws_error_log_ts.get(stream_key, 0.0)) >= 15.0
                should_log_global = (now - self._last_ws_global_error_log_ts) >= 2.0
                if self.running and should_log:
                    if (
                        ("Connection closed by the user" in err_text)
                        or ("Abnormal closure of client" in err_text)
                        or ("ping-pong keepalive missing on time" in err_text)
                        or ("timed out due to a ping-pong keepalive" in err_text)
                    ):
                        if should_log_global:
                            app_logger.warning(f"WS reconnect (OHLCV {symbol}/{timeframe}): {err_text}")
                            self._last_ws_global_error_log_ts = now
                    elif should_log_global:
                        app_logger.warning(f"WS reconnect (OHLCV {symbol}/{timeframe}): {err_text}")
                        self._last_ws_global_error_log_ts = now
                    else:
                        if should_log_global:
                            app_logger.error(f"Ошибка WebSocket (OHLCV {symbol}/{timeframe}): {err_text}")
                            self._last_ws_global_error_log_ts = now
                    self._last_ws_error_log_ts[stream_key] = now
                # Local stream recovery: do not globally reset all WS clients here.
                # Global resets are handled by watchdog only when degradation is widespread.
                self._mark_stream_recovering(stream_key, reason="ws_reconnect")
                jitter = random.uniform(0.1, min(reconnect_backoff * 0.3, 5.0))
                await asyncio.sleep(reconnect_backoff + jitter)
                reconnect_backoff = min(reconnect_backoff * 2.0, 60.0)

                if reconnect_backoff >= 30.0:
                    try:
                        rest_candles = await self._rest_call(
                            lambda: self.exchange.fetch_ohlcv(symbol, timeframe, limit=5),
                            ctx=f"watch_ohlcv.rest_fallback({symbol},{timeframe})",
                        )
                        if rest_candles:
                            self._mark_stream_alive(stream_key, asyncio.get_event_loop().time())
                            try:
                                if market_data_stream_restarts:
                                    market_data_stream_restarts.labels(symbol=symbol, timeframe=timeframe, reason="rest_fallback").inc()
                            except Exception:
                                pass
                            for cb in self.callbacks:
                                await cb("ohlcv", symbol, timeframe, rest_candles[-1])
                            app_logger.info(
                                f"♻️ REST fallback delivered data for {symbol}/{timeframe}"
                            )
                    except Exception as rest_err:
                        app_logger.debug(f"REST fallback failed for {symbol}/{timeframe}: {rest_err}")

    
    async def watch_trades(self, symbol: str):
        """Stream aggregated trades for CVD calculation."""
        reconnect_backoff = 1.0
        while self.running:
            try:
                # Force Binance aggTrades stream to accurately separate taker/maker logic
                trades = await self.exchange.watch_trades(symbol, params={"name": "aggTrade"})
                reconnect_backoff = 1.0
                if trades:
                    for t in trades:
                        for cb in self.callbacks:
                            await cb("trade", symbol, None, t)
            except Exception as e:
                if self.running:
                    app_logger.debug(f"WS trades reconnect ({symbol}): {e}")
                await asyncio.sleep(reconnect_backoff)
                reconnect_backoff = min(reconnect_backoff * 2.0, 30.0)

    async def watch_orderbook(self, symbol: str):
        app_logger.info(f"Начало отслеживания Orderbook для {symbol}")
        # Для Binance Futures CCXT Pro иногда требует формат symbol:USDT
        if ":" not in symbol:
            target_symbol = f"{symbol}:USDT"
        else:
            target_symbol = symbol
            
        while self.running:
            try:
                orderbook = await self.exchange.watch_order_book(target_symbol)
                if not orderbook:
                    await asyncio.sleep(1)
                    continue
                for cb in self.callbacks:
                    await cb("orderbook", symbol, None, orderbook)
            except Exception as e:
                app_logger.error(f"Ошибка WebSocket (Orderbook {symbol}): {str(e)}")
                await asyncio.sleep(5)

    async def fetch_funding_rates(self):
        """
        Funding rate часто запрашивается через REST, так как нет WS стрима у ccxtpro
        (в ccxtpro watch_funding_rate поддерживается не везде). 
        Используем REST fallback периодически.
        """
        app_logger.info("Начало проверки Funding Rates")
        while self.running:
            try:
                rates = await self._rest_call(
                    lambda: self.exchange.fetch_funding_rates(self.symbols),
                    ctx="fetch_funding_rates",
                )
                for cb in self.callbacks:
                    await cb("funding_rate", "ALL", None, rates)
            except Exception as e:
                # Funding rates не критичны для выживания контура исполнения.
                app_logger.warning(f"Ошибка получения Funding Rates: {str(e)}")
            await asyncio.sleep(60 * 5) # Раз в 5 минут

    async def fetch_avg_prices(self):
        """
        Poll 5-min Weighted Average Price (Binance specific) for all symbols.
        Used by Spread Momentum strategy.
        """
        app_logger.info("Начало мониторинга Average Prices (5m)")
        while self.running:
            for symbol in self.symbols:
                try:
                    # Некоторые версии CCXT или биржи могут не поддерживать fetch_avg_price
                    if hasattr(self.exchange, 'fetch_avg_price'):
                        res = await self._rest_call(
                            lambda: self.exchange.fetch_avg_price(symbol),
                            ctx=f"fetch_avg_price({symbol})",
                        )
                        avg_price = float(res['price'])
                    else:
                        # Fallback: берем текущую цену из тикера
                        res = await self._rest_call(
                            lambda: self.exchange.fetch_ticker(symbol),
                            ctx=f"fetch_ticker({symbol})",
                        )
                        avg_price = float(res['last'])
                    
                    for cb in self.callbacks:
                        await cb("avg_price", symbol, None, avg_price)
                except Exception as e:
                    app_logger.debug(f"AvgPrice poll error for {symbol}: {e}")
            await asyncio.sleep(60) # Раз в минуту

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 100):
        """REST-запрос истории для холодного старта"""
        try:
            return await self._rest_call(
                lambda: self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit),
                ctx=f"fetch_ohlcv({symbol},{timeframe})",
            )
        except Exception as e:
            app_logger.error(f"Ошибка fetch_ohlcv для {symbol}: {e}")
            return []

    async def fetch_instrument_info(self, symbol: str):
        """Получение даты листинга и другой инфы из exchange.fetch_markets() или implicit call"""
        try:
            if symbol in self.instrument_info:
                return self.instrument_info[symbol]
                
            markets = await self._rest_call(
                lambda: self.exchange.fetch_markets(),
                ctx=f"fetch_markets({symbol})",
            )
            for m in markets:
                if m['symbol'] == symbol:
                    # ccxt не всегда отдает 'info' с датой листинга напрямую в fetch_markets
                    # Но обычно в Binance это можно вытянуть из info['onboardDate']
                    self.instrument_info[symbol] = m
                    return m
            return None
        except Exception as e:
            app_logger.error(f"Ошибка получения инфо об инструменте {symbol}: {e}")
            return None

    async def start(self):
        self.running = True
        app_logger.info("MarketDataService успешно запущен")
        
        tasks = []
        for sym in self.symbols:
            from config.settings import settings
            if not settings.testnet:
                tasks.append(asyncio.create_task(self.watch_orderbook(sym)))
            # Запускаем aggTrades (нужно для CVD) в любом случае
            tasks.append(asyncio.create_task(self.watch_trades(sym)))
            await asyncio.sleep(0.2) # Staggered (Stage 19)
            
            for tf in self.timeframes:
                stream_key = f"{sym}:{tf}"
                self._tasks[stream_key] = asyncio.create_task(self.watch_ohlcv(sym, tf))
                await asyncio.sleep(0.2) # Staggered (Stage 19)
        
        # Average price poll (for Spread Momentum strategy)
        tasks.append(asyncio.create_task(self.fetch_avg_prices()))
        
        # Funding rate poll
        tasks.append(asyncio.create_task(self.fetch_funding_rates()))

        # ЗАПУСК WATCHDOG
        tasks.append(asyncio.create_task(self._watchdog_loop()))
        
        await asyncio.gather(*tasks)

    async def _watchdog_loop(self):
        """
        Watches for stale data streams. Two escalation levels:
        - soft: per-timeframe stale threshold -> mark recovery + reset WS clients
        - hard: larger per-timeframe threshold -> force-close ALL WS clients
        """
        app_logger.info("🐕 Watchdog запущен")
        while self.running:
            now = asyncio.get_event_loop().time()
            stale_streams = []
            hard_stale_streams = []

            for stream_key, last_time in list(self.last_candle_time.items()):
                symbol, timeframe = self._split_stream_key(stream_key)
                gap = now - last_time
                self._set_stream_metrics(stream_key, age_seconds=gap)
                hard_thr = self._hard_stale_threshold_seconds(timeframe)
                soft_thr = self._soft_stale_threshold_seconds(timeframe)
                if gap > hard_thr:
                    self._mark_stream_recovering(stream_key, reason="watchdog_hard")
                    hard_stale_streams.append((stream_key, gap, soft_thr, hard_thr))
                    # Специфичное требование Этапа 19: если минутный стрим мертв более 3 минут -> Force Restart
                    if timeframe == "1m" and gap > 180:
                         await self._restart_stream(symbol, timeframe)
                elif gap > soft_thr:
                    self._mark_stream_recovering(stream_key, reason="watchdog_soft")
                    stale_streams.append((stream_key, gap, soft_thr, hard_thr))
                else:
                    self._watchdog_consecutive_failures[stream_key] = 0
                    self._stream_recovering[stream_key] = False
                    self._set_stream_metrics(stream_key, recovering=False)

            # First try local REST refresh for stale streams to avoid reconnect storms.
            refreshed = set()
            for stream_key, *_ in (hard_stale_streams + stale_streams):
                ok = await self._refresh_stream_via_rest(stream_key)
                if ok:
                    refreshed.add(stream_key)
            if refreshed:
                hard_stale_streams = [x for x in hard_stale_streams if x[0] not in refreshed]
                stale_streams = [x for x in stale_streams if x[0] not in refreshed]

            hard_ratio = (len(hard_stale_streams) / max(1, len(self.last_candle_time))) if self.last_candle_time else 0.0
            can_global_reset = (now - self._last_global_ws_reset_ts) >= 300.0

            if hard_stale_streams and hard_ratio >= 0.60 and can_global_reset:
                total_streams = len(self.last_candle_time)
                app_logger.warning(
                    f"🚨 [WATCHDOG] {len(hard_stale_streams)}/{total_streams} streams exceeded hard stale threshold "
                    f"— force-closing ALL WS clients for full reconnect"
                )
                await self._close_ws_clients("watchdog_hard")
                self._last_global_ws_reset_ts = now
            elif stale_streams:
                total_streams = len(self.last_candle_time)
                app_logger.warning(
                    f"⚠️ [WATCHDOG] {len(stale_streams)}/{total_streams} streams exceeded soft stale threshold "
                    f"(local recovery only)"
                )

            await asyncio.sleep(30)

    async def stop(self):
        self.running = False
        await self.exchange.close()
        app_logger.info("MarketDataService остановлен")
