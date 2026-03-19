import asyncio
import ccxt.pro as ccxtpro
from typing import Dict, Callable
from utils.logger import app_logger

class MarketDataService:
    def __init__(self, symbols: list[str], timeframes: list[str], exchange: ccxtpro.binance = None):
        """
        Инициализация сервиса маркет-даты (Этап 4).
        
        Источники: Binance WebSocket + REST fallback
        Потоки: свечи, ордербук, funding rate.
        """
        self.symbols = symbols
        self.timeframes = timeframes
        from config.settings import settings
        
        if exchange:
            self.exchange = exchange
        else:
            self.exchange = ccxtpro.binance({
                'enableRateLimit': True,
                'timeout': int(settings.api_timeout_seconds * 1000), # в мс
                'options': {
                    'defaultType': 'future' # Binance Futures
                }
            })
            if settings.testnet:
                self.exchange.set_sandbox_mode(True)
        self.running = False
        self.callbacks = [] # type: list[Callable]
        self.instrument_info = {} # Кэш инфо об инструментах

    def register_callback(self, cb: Callable):
        self.callbacks.append(cb)

    async def watch_ohlcv(self, symbol: str, timeframe: str):
        app_logger.info(f"Начало отслеживания OHLCV для {symbol} ({timeframe})")
        while self.running:
            try:
                candles = await self.exchange.watch_ohlcv(symbol, timeframe)
                if not candles or len(candles) == 0:
                    continue
                # notify systems
                for cb in self.callbacks:
                    await cb("ohlcv", symbol, timeframe, candles[-1])
            except Exception as e:
                app_logger.error(f"Ошибка WebSocket (OHLCV {symbol}): {str(e)}")
                await asyncio.sleep(5)
                # ccxtpro handles fallback under the hood to REST if WS drops, 
                # but we can explicitly call fetch_ohlcv if needed.

    async def watch_orderbook(self, symbol: str):
        app_logger.info(f"Начало отслеживания Orderbook для {symbol}")
        while self.running:
            try:
                orderbook = await self.exchange.watch_order_book(symbol)
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
                rates = await self.exchange.fetch_funding_rates(self.symbols)
                for cb in self.callbacks:
                    await cb("funding_rate", "ALL", None, rates)
            except Exception as e:
                app_logger.error(f"Ошибка получения Funding Rates: {str(e)}")
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
                        res = await self.exchange.fetch_avg_price(symbol)
                        avg_price = float(res['price'])
                    else:
                        # Fallback: берем текущую цену из тикера
                        res = await self.exchange.fetch_ticker(symbol)
                        avg_price = float(res['last'])
                    
                    for cb in self.callbacks:
                        await cb("avg_price", symbol, None, avg_price)
                except Exception as e:
                    app_logger.debug(f"AvgPrice poll error for {symbol}: {e}")
            await asyncio.sleep(60) # Раз в минуту

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 100):
        """REST-запрос истории для холодного старта"""
        try:
            return await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        except Exception as e:
            app_logger.error(f"Ошибка fetch_ohlcv для {symbol}: {e}")
            return []

    async def fetch_instrument_info(self, symbol: str):
        """Получение даты листинга и другой инфы из exchange.fetch_markets() или implicit call"""
        try:
            if symbol in self.instrument_info:
                return self.instrument_info[symbol]
                
            markets = await self.exchange.fetch_markets()
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
            # Запуск orderbook
            tasks.append(asyncio.create_task(self.watch_orderbook(sym)))
            
            # Запуск OHLCV по всем таймфреймам
            for tf in self.timeframes:
                tasks.append(asyncio.create_task(self.watch_ohlcv(sym, tf)))
        
        # Average price poll (for Spread Momentum strategy)
        tasks.append(asyncio.create_task(self.fetch_avg_prices()))
        
        # Funding rate poll
        tasks.append(asyncio.create_task(self.fetch_funding_rates()))
        
        await asyncio.gather(*tasks)

    async def stop(self):
        self.running = False
        await self.exchange.close()
        app_logger.info("MarketDataService остановлен")
