from contextlib import asynccontextmanager
import asyncio
from fastapi import FastAPI
import ccxt.pro as ccxtpro

from config.settings import settings
from database.session import engine, Base
from database.models import all_models  # Загружает модели в Base.metadata
from services.market_data.market_streamer import MarketDataService
from core.risk.risk_manager import RiskManager
from core.execution.engine import ExecutionEngine
from services.signal_engine.engine import TradingOrchestrator
from utils.logger import app_logger

# Глобальная переменная для ссылок на процессы
orchestrator = None
exchange_client = None
reconcile_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global orchestrator, exchange_client, reconcile_task
    
    # 1. Инициализация Базы Данных
    app_logger.info("🚀 [1/5] Инициализация базы данных...")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        app_logger.info("✅ База данных готова.")
        
        # 0. ГАРАНТИРУЕМ ПОЛЬЗОВАТЕЛЯ (Это должно быть ПЕРЕД синхронизацией)
        from database.session import async_session
        from database.models.all_models import User
        from sqlalchemy import select
        async with async_session() as session:
            result = await session.execute(select(User).where(User.id == 1))
            user_exists = result.scalar_one_or_none()
            if not user_exists:
                app_logger.info("👥 [DB] Создание дефолтного пользователя (ID=1)...")
                new_user = User(id=1, telegram_id=0)
                session.add(new_user)
                try:
                    await session.commit()
                    app_logger.info("✅ Пользователь ID=1 создан.")
                except Exception as e:
                    await session.rollback()
                    app_logger.warning(f"⚠️ Не удалось создать пользователя (возможно, уже есть): {e}")
            else:
                app_logger.info("✅ Пользователь ID=1 найден.")
    except Exception as e:
        app_logger.error(f"❌ Ошибка БД: {e}")

    # 2. Инициализация биржевого клиента Binance
    # Выбор ключей в зависимости от режима (Testnet или Real)
    api_key = settings.api_key_binance
    secret = settings.secret_api_key_binance
    
    # Debug: что реально загружено из settings
    app_logger.info(f"DEBUG Settings: testnet={settings.testnet}")
    app_logger.info(f"DEBUG Settings: test_api_key_binance={'Loaded ('+settings.test_api_key_binance[:4]+')' if settings.test_api_key_binance else 'None'}")
    app_logger.info(f"DEBUG Settings: api_key_binance={'Loaded ('+settings.api_key_binance[:4]+')' if settings.api_key_binance else 'None'}")

    if settings.testnet and settings.test_api_key_binance:
        app_logger.info("Используются ТЕСТОВЫЕ API ключи (Binance Testnet)")
        api_key = settings.test_api_key_binance.strip()
        secret = settings.test_secret_api_key_binance.strip()
    else:
        app_logger.info("Используются РЕАЛЬНЫЕ API ключи (или ключи по умолчанию)")
        api_key = settings.api_key_binance.strip() if settings.api_key_binance else ""
        secret = settings.secret_api_key_binance.strip() if settings.secret_api_key_binance else ""
    
    if api_key:
        app_logger.info(f"FINAL API Key Selected: {api_key[:4]}...{api_key[-4:] if len(api_key)>4 else ''}")
    
    app_logger.info(f"Инициализация клиента Binance API (Testnet: {settings.testnet})...")
    exchange_client = ccxtpro.binance({
        'apiKey': api_key,
        'secret': secret,
        'enableRateLimit': True,
        'options': {'defaultType': 'future'},
        'timeout': 30000 # 30 секунд
    })
    exchange_client.set_sandbox_mode(settings.testnet)
    
    # ПРИНУДИТЕЛЬНО МЕНЯЕМ URL КОНЕКТА (т.к. старый fstream.binancefuture.com тормозит/не работает)
    if settings.testnet:
        working_ws_url = "wss://testnet.binancefuture.com/ws-fapi/v1"
        exchange_client.urls['test']['ws']['future'] = working_ws_url
        exchange_client.urls['api']['ws']['future'] = working_ws_url
        app_logger.info(f"🚀 WebSocket URL переопределен на: {working_ws_url}")
    
    app_logger.info(f"API URLs: {exchange_client.urls}")
    try:
        await exchange_client.load_markets()
    except Exception as e:
        app_logger.error(f"Не удалось загрузить markets: {e}")
    
    # 3. Инициализация ключевых микромодулей (Риск, Исполнение ордеров, Данные рынка)
    # Расширяем мониторинг до Топ-20 популярных монет
    all_symbols = [
        "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT",
        "ADA/USDT", "DOGE/USDT", "DOT/USDT", "LINK/USDT",
        "BCH/USDT", "TRX/USDT", "LTC/USDT", "AVAX/USDT", "ATOM/USDT",
        "UNI/USDT", "ETC/USDT", "FIL/USDT", "LDO/USDT", "APT/USDT"
    ]
    
    symbols_to_monitor = []
    if exchange_client.markets:
        for s in all_symbols:
            if s in exchange_client.markets:
                symbols_to_monitor.append(s)
            else:
                app_logger.warning(f"⚠️ Монета {s} отсутствует в 'markets' биржи. Пропускаем.")
    else:
        app_logger.warning("❌ Рынки не загружены (None). Используем Топ-10 монет для мониторинга.")
        symbols_to_monitor = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT", "DOGE/USDT", "DOT/USDT", "LTC/USDT", "AVAX/USDT"]

    if not symbols_to_monitor:
        app_logger.error("❌ Список монет для мониторинга ПУСТ.")
        symbols_to_monitor = ["BTC/USDT"] # Fallback

    market_data = MarketDataService(
        symbols=symbols_to_monitor, 
        timeframes=["1m", "5m", "15m", "1h"],
        exchange=exchange_client
    )
    
    risk_manager = RiskManager(
        max_risk_pct=0.02, 
        max_drawdown_pct=0.20, 
        max_open_trades=5
    )
    
    execution_engine = ExecutionEngine(
        exchange_client=exchange_client, 
        risk_manager=risk_manager
    )

    # 3. Синхронизация: биржа <-> БД (Идет ПОСЛЕ создания пользователя)
    app_logger.info("🚀 [3/5] Синхронизация текущих позиций...")
    try:
        await execution_engine.reconcile_full()
        app_logger.info("✅ Синхронизация завершена.")
    except Exception as e:
        app_logger.error(f"⚠️ Ошибка reconcile: {e}")

    # Periodic reconcile loop (keeps state aligned with exchange)
    async def _reconcile_loop():
        while True:
            await asyncio.sleep(60)  # every 60s
            try:
                await execution_engine.reconcile_full()
                # Вывод текущих метрик для отладки
                bal, dd, count = await execution_engine.get_account_metrics()
                app_logger.info(f"📊 [MONITOR] Balance={bal:.2f} USDT | Drawdown={dd*100:.2f}% | Positions={count}")
            except Exception as e:
                app_logger.error(f"Periodic reconcile error: {e}")
    reconcile_task = asyncio.create_task(_reconcile_loop())
    
    # 4. Инициализация Оркестратора
    app_logger.info("🚀 [4/5] Инициализация Оркестратора...")
    orchestrator = TradingOrchestrator(
        market_data=market_data, 
        execution_engine=execution_engine
    )

    # 5. Запуск Оркестратора
    app_logger.info("🚀 [5/5] Запуск Торгового Движка (Orchestrator)...")
    asyncio.create_task(orchestrator.start())
    
    app_logger.info("🎉 Платформа успешно запущена!")
    
    yield  # --- Здесь работает FastAPI ---
    
    # 6. Корректное завершение работы при выключении (Graceful Shutdown)
    app_logger.info("Выключение торговой платформы...")
    if reconcile_task:
        reconcile_task.cancel()
    if orchestrator:
        await orchestrator.stop()
    if exchange_client:
        await exchange_client.close()
    await engine.dispose()
    app_logger.info("Все ресурсы корректно освобождены.")

# Экземпляр веб-сервера FastAPI, который является Gateway для REST
app = FastAPI(title="Quant Trading System API", lifespan=lifespan)

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "trading-engine"}

@app.get("/api/v1/status")
async def get_system_status():
    if not orchestrator or not exchange_client:
        return {"status": "initializing"}
        
    try:
        balance, drawdown, open_trades = await orchestrator.execution.get_account_metrics()
        return {
            "status": "running",
            "balance": balance,
            "drawdown": f"{drawdown*100:.2f}%",
            "open_trades": open_trades,
            "testnet": settings.testnet,
            "symbols": orchestrator.market_data.symbols
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}
@app.get("/api/v1/presets")
async def get_presets():
    from database.session import async_session
    from database.models.all_models import SettingsPreset
    from sqlalchemy import select
    
    async with async_session() as session:
        # Инициализация дефолтных пресетов при первом запросе, если их нет
        stmt = select(SettingsPreset)
        result = await session.execute(stmt)
        presets = result.scalars().all()
        
        if not presets:
            p1 = SettingsPreset(name="Conservative 🛡", sl_long_pct=0.003, sl_short_pct=0.003, max_open_positions=3)
            p2 = SettingsPreset(name="Aggressive 🔥", sl_long_pct=0.008, sl_short_pct=0.008, max_open_positions=10)
            session.add_all([p1, p2])
            await session.commit()
            presets = [p1, p2]
            
        return [
            {
                "name": p.name, 
                "is_active": p.is_active,
                "leverage": p.leverage,
                "tp_pct": p.tp_pct,
                "max_open_positions": p.max_open_positions,
                "averaging_enabled": p.averaging_enabled
            } for p in presets
        ]

@app.post("/api/v1/presets/apply/{name}")
async def apply_preset(name: str):
    from database.session import async_session
    from database.models.all_models import SettingsPreset
    from sqlalchemy import select, update
    
    async with async_session() as session:
        stmt = select(SettingsPreset).where(SettingsPreset.name == name)
        result = await session.execute(stmt)
        preset = result.scalar_one_or_none()
        
        if not preset:
            return {"status": "error", "message": "Preset not found"}
            
        # Сбрасываем все, активируем один
        await session.execute(update(SettingsPreset).values(is_active=False))
        preset.is_active = True
        
        # Применяем к глобальному объекту settings в памяти
        settings.sl_long_pct = preset.sl_long_pct
        settings.sl_short_pct = preset.sl_short_pct
        settings.tp_pct = preset.tp_pct
        settings.max_open_positions = preset.max_open_positions
        settings.leverage = preset.leverage
        settings.signal_expiry_seconds = preset.signal_expiry_seconds
        
        # Усреднения
        settings.averaging_enabled = preset.averaging_enabled
        settings.averaging_step_pct = preset.averaging_step_pct
        settings.averaging_multiplier = preset.averaging_multiplier
        settings.averaging_max_steps = preset.averaging_max_steps
        
        await session.commit()
        return {"status": "success", "message": f"Preset {name} applied"}

@app.post("/api/v1/toggle")
async def toggle_trading():
    settings.is_trading_enabled = not settings.is_trading_enabled
    return {"status": "success", "is_enabled": settings.is_trading_enabled}

@app.get("/api/v1/exchange/check")
async def check_exchange_connection():
    if not exchange_client:
        return {"status": "error", "message": "Exchange client not initialized"}
    try:
        await exchange_client.fetch_balance()
        return {"status": "success", "message": "Connected to Binance"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/v1/stats")
async def get_stats():
    from database.session import async_session
    from database.models.all_models import PnLRecord as PnLModel
    from sqlalchemy import select, func
    from datetime import datetime, timedelta
    
    async with async_session() as session:
        # Статика за последние 24 часа
        one_day_ago = datetime.utcnow() - timedelta(days=1)
        stmt = select(
            func.sum(PnLModel.pnl_usd),
            func.avg(PnLModel.pnl_pct),
            func.count(PnLModel.id)
        ).where(PnLModel.closed_at >= one_day_ago)
        
        result = await session.execute(stmt)
        total_usd, avg_pct, count = result.fetchone()
        
        return {
            "daily": {
                "pnl_usd": float(total_usd or 0),
                "avg_pct": float(avg_pct or 0),
                "trades_count": count
            }
        }

@app.get("/api/v1/trades")
async def get_active_trades():
    if not orchestrator:
        return {"trades": []}
    return {"trades": orchestrator.execution.active_trades}

@app.post("/api/v1/trades/close/{symbol}")
async def close_trade(symbol: str):
    # CCXT использует BTC/USDT, но в URL удобнее передавать BTCUSDT или кодировать /
    # Попробуем найти символ. Если в URL передали BTC_USDT, заменим на BTC/USDT
    normalized_symbol = symbol.replace("_", "/")
    if not orchestrator:
        return {"status": "error", "message": "Engine not ready"}
    
    success = await orchestrator.execution.manual_close(normalized_symbol)
    if success:
        return {"status": "success", "symbol": normalized_symbol}
    else:
        return {"status": "error", "message": "Trade not found", "symbol": normalized_symbol}
