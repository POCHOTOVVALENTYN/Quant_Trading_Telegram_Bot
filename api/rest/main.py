import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
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
    
    print("!!!!!!!! APP STARTING !!!!!!!!", flush=True) # DEBUG
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
        'options': {
            'defaultType': 'future',
            'testnet': settings.testnet,
            'adjustForTimeDifference': True,
            'recvWindow': 10000, 
        },
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
    
    # 1. Загрузка списка символов для мониторинга
    # Возвращаем ПОЛНЫЙ список после оптимизации CPU
    desired_symbols = [
        "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT",
        "DOGE/USDT", "ADA/USDT", "TRX/USDT", "LINK/USDT", "DOT/USDT",
        "LTC/USDT", "BCH/USDT", "SHIB/USDT", "UNI/USDT", "NEAR/USDT", 
        "MATIC/USDT", "FIL/USDT", "ICP/USDT", "APT/USDT"
    ]
    
    # K2: Фильтрация — оставляем только символы, реально доступные на бирже
    available_markets = set(exchange_client.markets.keys()) if exchange_client.markets else set()
    base_symbols = []
    skipped = []
    for s in desired_symbols:
        # Проверяем через несколько форматов (BTC/USDT и BTC/USDT:USDT)
        if s in available_markets or f"{s}:USDT" in available_markets:
            base_symbols.append(s)
        else:
            skipped.append(s)
    
    # K2+Testnet: Дополнительная проверка — пробуем загрузить 1 свечу для каждого символа
    if settings.testnet and base_symbols:
        verified_symbols = []
        for s in base_symbols:
            try:
                candles = await exchange_client.fetch_ohlcv(s, '1h', limit=1)
                if candles:
                    verified_symbols.append(s)
                else:
                    skipped.append(s)
            except Exception:
                skipped.append(s)
            await asyncio.sleep(0.2)  # Пауза чтобы не триггерить рейт-лимит
        base_symbols = verified_symbols
    
    if skipped:
        app_logger.warning(f"⚠️ [K2] Символы отсутствуют на бирже и пропущены: {list(set(skipped))}")
    app_logger.info(f"✅ Валидных символов: {len(base_symbols)} из {len(desired_symbols)}")
    
    tfs = ["1m", "5m", "15m", "1h", "4h"]
    if settings.use_daily_timeframe_filter and "1d" not in tfs:
        tfs.append("1d")
    
    app_logger.info(f"🚀 [1/6] Запуск мониторинга {len(base_symbols)} монет на {len(tfs)} ТФ...")
    
    # 2. Инициализация сервисов
    market_data = MarketDataService(
        symbols=base_symbols, 
        timeframes=tfs,
        exchange=exchange_client
    )
    
    risk_manager = RiskManager(
        max_risk_pct=0.02, 
        max_drawdown_pct=0.20, 
        max_open_trades=settings.max_open_trades
    )
    
    execution_engine = ExecutionEngine(
        exchange_client=exchange_client, 
        risk_manager=risk_manager
    )

    # 4. Предварительная настройка двигателя и ПЕРВИЧНАЯ СИНХРОНИЗАЦИЯ
    try:
        await execution_engine.start()
        # Сразу опрашиваем биржу, чтобы active_trades заполнились ДО того, как API станет доступно
        await execution_engine.reconcile_full()
        app_logger.info("✅ Синхронизация ExecutionEngine завершена.")
    except Exception as e:
        app_logger.error(f"⚠️ Ошибка инициализации ExecutionEngine: {e}")

    # Periodic reconcile loop (keeps state aligned with exchange)
    async def _reconcile_loop():
        # Testnet algo orders expire fast (~2-5 min), so reconcile more frequently.
        # On production, 300s is fine since algo orders persist until triggered.
        RECONCILE_INTERVAL = 60 if settings.testnet else 120
        while True:
            # Сначала ждем интервал, т.к. первичный reconcile уже сделан выше
            await asyncio.sleep(RECONCILE_INTERVAL)
            try:
                await execution_engine.reconcile_full()
                # Вывод текущих метрик для отладки
                bal, dd, count = await execution_engine.get_account_metrics()
                app_logger.info(f"📊 [MONITOR] Balance={bal:.2f} USDT | Drawdown={dd*100:.2f}% | Positions={count}")
            except Exception as e:
                app_logger.error(f"Periodic reconcile error: {e}")
    reconcile_task = asyncio.create_task(_reconcile_loop())
    
    # 4. Инициализация Оркестратора
    app_logger.info("🚀 [4/6] Инициализация Оркестратора...")
    orchestrator = TradingOrchestrator(
        market_data=market_data, 
        execution_engine=execution_engine
    )

    # 5. Запуск Оркестратора в фоновой задаче (не блокируя FastAPI)
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
    if execution_engine:
        await execution_engine.stop()
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


@app.get("/api/v1/runtime-settings")
async def get_runtime_settings():
    return {
        "is_trading_enabled": settings.is_trading_enabled,
        "pyramiding_enabled": settings.pyramiding_enabled,
        "per_trade_margin_pct": settings.per_trade_margin_pct,
        "position_size_usdt": settings.position_size_usdt,
        "max_open_trades": settings.max_open_trades,
        "leverage": settings.leverage,
        "tp_pct": settings.tp_pct,
        "signal_expiry_seconds": settings.signal_expiry_seconds,
        "allowed_position_side": settings.allowed_position_side,
        "apply_after_flat": settings.apply_new_entry_rules_after_flat,
    }


@app.post("/api/v1/runtime-settings/pyramiding/toggle")
async def toggle_pyramiding_runtime():
    settings.pyramiding_enabled = not settings.pyramiding_enabled
    return {"status": "success", "pyramiding_enabled": settings.pyramiding_enabled}


@app.post("/api/v1/runtime-settings/per-trade-margin")
async def set_per_trade_margin_pct_runtime(value: float):
    # 1%-30% безопасный диапазон для runtime-настроек
    clamped = max(0.01, min(0.30, float(value)))
    settings.per_trade_margin_pct = clamped
    return {"status": "success", "per_trade_margin_pct": settings.per_trade_margin_pct}


@app.post("/api/v1/runtime-settings/position-size-usdt")
async def set_position_size_usdt_runtime(value: float):
    # 0 = выключить фикс и вернуться к расчету по % маржи.
    clamped = max(0.0, min(100000.0, float(value)))
    settings.position_size_usdt = clamped
    return {"status": "success", "position_size_usdt": settings.position_size_usdt}


@app.post("/api/v1/runtime-settings/max-open-trades")
async def set_max_open_trades_runtime(value: int):
    clamped = max(1, min(20, int(value)))
    settings.max_open_trades = clamped
    # Важно синхронизировать RiskManager уже запущенного оркестратора
    if orchestrator and orchestrator.execution and orchestrator.execution.risk_manager:
        orchestrator.execution.risk_manager.max_open_trades = clamped
    return {"status": "success", "max_open_trades": settings.max_open_trades}


@app.post("/api/v1/runtime-settings/tp-pct")
async def set_tp_pct_runtime(value: float):
    # 0.1%..20%
    clamped = max(0.001, min(0.20, float(value)))
    settings.tp_pct = clamped
    return {"status": "success", "tp_pct": settings.tp_pct}


@app.post("/api/v1/runtime-settings/signal-expiry")
async def set_signal_expiry_runtime(value: int):
    # 60..600 сек
    clamped = max(60, min(600, int(value)))
    settings.signal_expiry_seconds = clamped
    return {"status": "success", "signal_expiry_seconds": settings.signal_expiry_seconds}


@app.post("/api/v1/runtime-settings/allowed-side")
async def set_allowed_side_runtime(value: str):
    norm = str(value or "").upper()
    if norm not in {"LONG", "SHORT", "BOTH"}:
        return {"status": "error", "message": "allowed side must be LONG, SHORT or BOTH"}
    settings.allowed_position_side = norm
    return {"status": "success", "allowed_position_side": settings.allowed_position_side}


@app.post("/api/v1/runtime-settings/leverage")
async def set_leverage_runtime(value: int):
    requested = int(value)
    if requested < 1 or requested > 125:
        return {"status": "error", "message": "Leverage must be in range 1..125"}

    if not exchange_client:
        return {"status": "error", "message": "Exchange client not initialized"}
    try:
        # Синхронизируем с серверным временем Binance перед приватным запросом.
        await exchange_client.load_time_difference()
    except Exception:
        pass

    # Проверяем применимость плеча на бирже на 1-2 ликвидных символах до изменения runtime-настроек.
    symbols_to_check = []
    if orchestrator and orchestrator.market_data and orchestrator.market_data.symbols:
        symbols_to_check = list(orchestrator.market_data.symbols[:2])
    elif getattr(exchange_client, "symbols", None):
        symbols_to_check = [s for s in exchange_client.symbols if s.endswith("/USDT")][:2]
    if not symbols_to_check:
        symbols_to_check = ["BTC/USDT"]

    failed = []
    for sym in symbols_to_check:
        try:
            await exchange_client.set_leverage(requested, sym)
        except Exception as e:
            err = str(e)
            # Однократный retry при -1021 (drift/recvWindow)
            if "-1021" in err:
                try:
                    await exchange_client.load_time_difference()
                    await exchange_client.set_leverage(requested, sym)
                    continue
                except Exception as e2:
                    err = str(e2)
            failed.append((sym, err))

    if failed:
        sym, err = failed[0]
        return {
            "status": "error",
            "message": f"Exchange rejected leverage {requested}x for {sym}: {err[:180]}",
        }

    settings.leverage = requested
    return {"status": "success", "leverage": settings.leverage, "exchange_check": "ok"}

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


@app.post("/api/v1/stats/reset")
async def reset_stats(scope: str = "all"):
    from database.session import async_session
    from database.models.all_models import PnLRecord as PnLModel
    from sqlalchemy import delete

    # Сейчас поддерживаем только binance/all.
    if scope.lower() not in {"all", "binance"}:
        return {"status": "error", "message": "Unsupported scope. Use all or binance."}

    async with async_session() as session:
        await session.execute(delete(PnLModel))
        await session.commit()
    return {"status": "success", "scope": scope.lower()}


@app.get("/api/v1/history")
async def get_trade_history(limit: int = 20):
    from database.session import async_session
    from database.models.all_models import PnLRecord as PnLModel
    from sqlalchemy import select

    clamped = max(1, min(200, int(limit)))
    async with async_session() as session:
        stmt = select(PnLModel).order_by(PnLModel.closed_at.desc()).limit(clamped)
        result = await session.execute(stmt)
        rows = result.scalars().all()

    items = []
    for r in rows:
        items.append({
            "id": r.id,
            "symbol": r.symbol,
            "pnl_usd": float(r.pnl_usd or 0.0),
            "pnl_pct": float(r.pnl_pct or 0.0),
            "reason": r.reason or "AUTO",
            "closed_at": r.closed_at.isoformat() if r.closed_at else None,
        })
    return {"items": items, "count": len(items)}

@app.get("/api/v1/trades")
async def get_active_trades():
    if not orchestrator:
        return {"trades": {}}

    # Принудительная синхронизация перед отдачей в UI:
    # убирает "призрачные" позиции в Telegram/дашборде после внешних закрытий.
    try:
        await orchestrator.execution.reconcile_full()
    except Exception as e:
        app_logger.warning(f"⚠️ reconcile_full before /trades failed: {e}")

    trades = orchestrator.execution.active_trades.copy()
    if not trades:
        return {"trades": {}}
    
    try:
        # Пытаемся получить текущие цены
        symbols = list(trades.keys())
        # Используем fetch_ticker если fetch_tickers не вернул данные
        tickers = {}
        try:
            app_logger.info(f"📊 [DEBUG] Запрос цен для: {symbols}")
            # Используем ГЛОБАЛЬНЫЙ exchange_client
            tickers = await exchange_client.fetch_tickers(symbols)
            app_logger.info(f"📊 [DEBUG] Получено тикеров: {list(tickers.keys())}")
        except Exception as te:
            app_logger.warning(f"Ошибка fetch_tickers, пробуем по одному: {te}")
            for s in symbols:
                try:
                    tickers[s] = await exchange_client.fetch_ticker(s)
                except:
                    continue

        for symbol, info in trades.items():
            # Пробуем найти тикер (учитываем, что CCXT может добавить :USDT для фьючерсов)
            ticker = tickers.get(symbol) or tickers.get(symbol + ":USDT")
            
            if ticker and (ticker.get('last') or ticker.get('close')):
                curr_price = ticker.get('last') or ticker.get('close')
                info['current_price'] = curr_price
                
                # Расчет PnL
                entry = info.get('entry', 0)
                size = float(info.get('current_size') or 0)
                is_long = str(info.get('signal_type', '')).upper() == "LONG"
                
                if entry > 0 and size > 0:
                    if is_long:
                        pnl_usd = (curr_price - entry) * size
                        pnl_pct = ((curr_price / entry) - 1) * 100
                    else:
                        pnl_usd = (entry - curr_price) * size
                        pnl_pct = ((entry / curr_price) - 1) * 100
                        
                    info['pnl_usd'] = pnl_usd
                    info['pnl_pct'] = pnl_pct
                else:
                    info['pnl_usd'] = 0.0
                    info['pnl_pct'] = 0.0
            else:
                # Если цену так и не нашли
                info['current_price'] = None
                info['pnl_usd'] = 0.0
                info['pnl_pct'] = 0.0

    except Exception as e:
        app_logger.error(f"Глобальная ошибка при расчете PnL: {e}")
        
    return {"trades": trades}

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


@app.post("/api/v1/trades/reduce/{symbol}")
async def reduce_trade(symbol: str, fraction: float):
    normalized_symbol = symbol.replace("_", "/")
    if not orchestrator:
        return {"status": "error", "message": "Engine not ready", "symbol": normalized_symbol}
    result = await orchestrator.execution.manual_reduce(normalized_symbol, float(fraction))
    if isinstance(result, dict):
        result.setdefault("symbol", normalized_symbol)
        return result
    return {"status": "error", "message": "Unexpected reduce response", "symbol": normalized_symbol}


@app.get("/api/v1/positions")
async def get_positions():
    """Open positions in a flat list for the dashboard."""
    if not orchestrator:
        return []
    trades = orchestrator.execution.active_trades.copy()
    result = []
    for symbol, info in trades.items():
        result.append({
            "symbol": symbol,
            "side": str(info.get("signal_type", "")).upper(),
            "signal_type": str(info.get("signal_type", "")).upper(),
            "entry_price": info.get("entry", 0),
            "stop_loss": info.get("stop_loss", 0),
            "take_profit": info.get("take_profit", 0),
            "unrealized_pnl": info.get("pnl_usd", 0),
            "current_size": info.get("current_size", 0),
        })
    return result


@app.get("/api/v1/signals")
async def get_recent_signals(limit: int = 30):
    """Recent signals from DB."""
    from database.session import async_session as _async_session
    from database.models.all_models import Signal
    from sqlalchemy import select

    async with _async_session() as session:
        stmt = select(Signal).order_by(Signal.id.desc()).limit(min(limit, 100))
        rows = (await session.execute(stmt)).scalars().all()
    return [
        {
            "id": r.id, "symbol": r.symbol,
            "signal_type": r.signal_type.value if hasattr(r.signal_type, 'value') else str(r.signal_type),
            "strategy": r.strategy, "confidence": r.confidence,
            "win_prob": r.win_prob, "expected_return": r.expected_return,
            "risk": r.risk, "status": r.status,
            "entry_price": r.entry_price, "stop_loss": r.stop_loss,
            "take_profit": r.take_profit,
            "timestamp": r.timestamp.isoformat() if r.timestamp else None,
        }
        for r in rows
    ]


@app.get("/api/v1/ai/status")
async def get_ai_status():
    if not orchestrator:
        return {"status": "not_ready"}
    adapter = orchestrator.external_ai
    return {
        "enabled": adapter.is_enabled,
        "providers": adapter.get_status(),
    }


@app.get("/api/v1/ai/decisions")
async def get_ai_decisions(limit: int = 50):
    """Recent AI decisions for analytics."""
    from database.session import async_session as _async_session
    from database.models.all_models import AIDecisionLog
    from sqlalchemy import select

    async with _async_session() as session:
        stmt = select(AIDecisionLog).order_by(AIDecisionLog.id.desc()).limit(min(limit, 200))
        rows = (await session.execute(stmt)).scalars().all()
    items = []
    for r in rows:
        items.append({
            "id": r.id, "signal_id": r.signal_id, "symbol": r.symbol,
            "strategy": r.strategy, "provider": r.provider,
            "recommendation": r.recommendation, "confidence": r.confidence,
            "reasoning": (r.reasoning or "")[:200], "score": r.score,
            "win_prob": r.win_prob, "latency_ms": r.latency_ms,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        })
    return {"count": len(items), "items": items}


@app.get("/api/v1/ai/decisions/summary")
async def get_ai_decisions_summary():
    """Aggregate AI decision statistics."""
    from database.session import async_session as _async_session
    from database.models.all_models import AIDecisionLog
    from sqlalchemy import select, func

    async with _async_session() as session:
        stmt = select(
            AIDecisionLog.provider,
            AIDecisionLog.recommendation,
            func.count().label("cnt"),
            func.avg(AIDecisionLog.confidence).label("avg_conf"),
            func.avg(AIDecisionLog.latency_ms).label("avg_latency"),
        ).group_by(AIDecisionLog.provider, AIDecisionLog.recommendation)
        rows = (await session.execute(stmt)).all()

    summary = {}
    for provider, rec, cnt, avg_conf, avg_lat in rows:
        if provider not in summary:
            summary[provider] = {"total": 0, "decisions": {}}
        summary[provider]["total"] += cnt
        summary[provider]["decisions"][rec] = {
            "count": cnt,
            "avg_confidence": round(float(avg_conf or 0), 3),
            "avg_latency_ms": round(float(avg_lat or 0), 0),
        }
    return summary


@app.get("/api/v1/risk/daily")
async def get_daily_risk():
    """Daily PnL and drawdown status."""
    if not orchestrator:
        return {"status": "not_ready"}
    return orchestrator.risk_manager.get_daily_stats()


@app.get("/api/v1/learner/status")
async def get_learner_status():
    """Scoring learner weights and training stats."""
    if not orchestrator:
        return {"status": "not_ready"}
    learner = orchestrator.scoring_learner
    import json
    from pathlib import Path
    stats = {}
    try:
        wf = Path(learner.weights_file)
        if wf.exists():
            with open(wf) as f:
                stats = json.load(f).get("stats", {})
    except Exception:
        pass
    return {
        "enabled": settings.scoring_learner_enabled,
        "weights": learner.get_weights(),
        "last_training": stats,
    }


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Real-time monitoring dashboard with auto-refresh."""
    return _DASHBOARD_HTML


_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Trading Bot Dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0d1117;color:#c9d1d9;padding:16px}
h1{color:#58a6ff;margin-bottom:12px;font-size:1.4rem}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:14px;margin-bottom:14px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px}
.card h2{font-size:.95rem;color:#8b949e;margin-bottom:10px;text-transform:uppercase;letter-spacing:1px}
.metric{display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #21262d;font-size:.88rem}
.metric:last-child{border:none}
.metric .val{color:#58a6ff;font-weight:600}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:.78rem;font-weight:600}
.badge.long{background:#1a4a2e;color:#3fb950}.badge.short{background:#4a1a1a;color:#f85149}
.badge.enter{background:#1a3a4a;color:#58a6ff}.badge.skip{background:#4a3a1a;color:#d29922}
.badge.ok{background:#1a4a2e;color:#3fb950}.badge.err{background:#4a1a1a;color:#f85149}
table{width:100%;border-collapse:collapse;font-size:.82rem}
th{text-align:left;color:#8b949e;padding:6px 8px;border-bottom:1px solid #30363d}
td{padding:5px 8px;border-bottom:1px solid #21262d}
tr:hover{background:#1c2128}
.timer{color:#484f58;font-size:.75rem;float:right}
.status-dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:6px}
.status-dot.on{background:#3fb950}.status-dot.off{background:#f85149}
.refresh-bar{height:2px;background:#58a6ff;transition:width linear;position:fixed;top:0;left:0}
</style>
</head>
<body>
<div class="refresh-bar" id="rbar"></div>
<h1>Trading Bot <span style="color:#3fb950">Live Dashboard</span> <span class="timer" id="timer">--</span></h1>

<div class="grid">
  <div class="card" id="c-positions"><h2>Open Positions</h2><p style="color:#484f58">Loading...</p></div>
  <div class="card" id="c-daily"><h2>Daily Risk</h2><p style="color:#484f58">Loading...</p></div>
  <div class="card" id="c-ai"><h2>AI Providers</h2><p style="color:#484f58">Loading...</p></div>
  <div class="card" id="c-stats"><h2>AI Decision Stats</h2><p style="color:#484f58">Loading...</p></div>
  <div class="card" id="c-learner"><h2>Scoring Learner</h2><p style="color:#484f58">Loading...</p></div>
</div>

<div class="card" id="c-signals" style="margin-bottom:14px"><h2>Recent Signals</h2><p style="color:#484f58">Loading...</p></div>
<div class="card" id="c-decisions"><h2>Recent AI Decisions</h2><p style="color:#484f58">Loading...</p></div>

<script>
const BASE = location.origin;
const REFRESH = 15000;
let countdown = REFRESH/1000;

async function fetchJSON(path) {
  try { const r = await fetch(BASE + path); return r.ok ? await r.json() : null; }
  catch { return null; }
}

function badge(cls, text) { return `<span class="badge ${cls}">${text}</span>`; }
function dot(on) { return `<span class="status-dot ${on?'on':'off'}"></span>`; }
function pct(v) { return v != null ? (v*100).toFixed(1)+'%' : '-'; }
function num(v,d=2) { return v != null ? Number(v).toFixed(d) : '-'; }

async function refresh() {
  const [positions, dailyRisk, aiStatus, aiSummary, learner, signals, decisions] = await Promise.all([
    fetchJSON('/api/v1/positions'),
    fetchJSON('/api/v1/risk/daily'),
    fetchJSON('/api/v1/ai/status'),
    fetchJSON('/api/v1/ai/decisions/summary'),
    fetchJSON('/api/v1/learner/status'),
    fetchJSON('/api/v1/signals?limit=15'),
    fetchJSON('/api/v1/ai/decisions?limit=20'),
  ]);

  // Positions
  const cp = document.getElementById('c-positions');
  if (positions && Array.isArray(positions)) {
    if (positions.length === 0) {
      cp.innerHTML = '<h2>Open Positions</h2><p style="color:#484f58">No open positions</p>';
    } else {
      let h = '<h2>Open Positions (' + positions.length + ')</h2><table><tr><th>Symbol</th><th>Side</th><th>Entry</th><th>PnL</th><th>SL</th><th>TP</th></tr>';
      for (const p of positions) {
        const side = (p.signal_type||p.side||'').toUpperCase();
        const cls = side==='LONG'?'long':'short';
        const pnl = p.unrealized_pnl||p.pnl||0;
        h += `<tr><td>${p.symbol}</td><td>${badge(cls,side)}</td><td>${num(p.entry_price,4)}</td>`;
        h += `<td style="color:${pnl>=0?'#3fb950':'#f85149'}">${num(pnl,3)} USDT</td>`;
        h += `<td>${num(p.stop_loss,4)}</td><td>${num(p.take_profit,4)}</td></tr>`;
      }
      cp.innerHTML = h + '</table>';
    }
  }

  // Daily Risk
  const cdr = document.getElementById('c-daily');
  if (dailyRisk && dailyRisk.daily_pnl_usd !== undefined) {
    const pnl = dailyRisk.daily_pnl_usd;
    const dd = dailyRisk.daily_drawdown_pct;
    const halted = dailyRisk.halted;
    let h = '<h2>Daily Risk</h2>';
    h += `<div class="metric"><span>Daily PnL</span><span class="val" style="color:${pnl>=0?'#3fb950':'#f85149'}">${pnl>=0?'+':''}${pnl} USDT</span></div>`;
    h += `<div class="metric"><span>Drawdown</span><span class="val">${dd}%</span></div>`;
    h += `<div class="metric"><span>Trading</span><span class="val">${halted?badge('err','HALTED'):badge('ok','ACTIVE')}</span></div>`;
    cdr.innerHTML = h;
  }

  // AI Status
  const ca = document.getElementById('c-ai');
  if (aiStatus) {
    let h = `<h2>AI Providers</h2><div class="metric"><span>Cascade enabled</span><span class="val">${aiStatus.enabled?'Yes':'No'}</span></div>`;
    if (aiStatus.providers && typeof aiStatus.providers === 'object') {
      for (const [name, info] of Object.entries(aiStatus.providers)) {
        const healthy = info.healthy !== false;
        h += `<div class="metric"><span>${dot(healthy)}${name}</span><span class="val">${healthy?badge('ok','OK'):badge('err','DOWN')}</span></div>`;
      }
    }
    ca.innerHTML = h;
  }

  // AI Summary
  const cs = document.getElementById('c-stats');
  if (aiSummary && typeof aiSummary === 'object') {
    let h = '<h2>AI Decision Stats</h2><table><tr><th>Provider</th><th>Action</th><th>Count</th><th>Avg Conf</th><th>Avg Latency</th></tr>';
    for (const [prov, data] of Object.entries(aiSummary)) {
      for (const [rec, vals] of Object.entries(data.decisions||{})) {
        const cls = rec==='ENTER'?'enter':rec==='SKIP'?'skip':'';
        h += `<tr><td>${prov}</td><td>${badge(cls,rec)}</td><td>${vals.count}</td><td>${pct(vals.avg_confidence/100)}</td><td>${vals.avg_latency_ms}ms</td></tr>`;
      }
    }
    cs.innerHTML = h + '</table>';
  }

  // Learner
  const cl = document.getElementById('c-learner');
  if (learner) {
    let h = `<h2>Scoring Learner</h2><div class="metric"><span>Active</span><span class="val">${learner.enabled?'Yes':'No'}</span></div>`;
    if (learner.weights) {
      for (const [k, v] of Object.entries(learner.weights)) {
        h += `<div class="metric"><span>${k}</span><span class="val">${num(v,3)}</span></div>`;
      }
    }
    if (learner.last_training && learner.last_training.samples) {
      h += `<div class="metric"><span>Last samples</span><span class="val">${learner.last_training.samples}</span></div>`;
    }
    cl.innerHTML = h;
  }

  // Signals
  const csi = document.getElementById('c-signals');
  if (signals && Array.isArray(signals)) {
    let h = '<h2>Recent Signals (' + signals.length + ')</h2><table><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Strategy</th><th>Score</th><th>WinProb</th><th>Status</th></tr>';
    for (const s of signals.slice(0,15)) {
      const side = (s.signal_type||'').toUpperCase();
      const cls = side==='LONG'?'long':side==='SHORT'?'short':'';
      h += `<tr><td>${(s.timestamp||'').slice(0,19)}</td><td>${s.symbol}</td><td>${badge(cls,side)}</td>`;
      h += `<td>${s.strategy||'-'}</td><td>${pct(s.confidence)}</td><td>${pct(s.win_prob)}</td>`;
      h += `<td>${s.status||'-'}</td></tr>`;
    }
    csi.innerHTML = h + '</table>';
  }

  // AI Decisions
  const cd = document.getElementById('c-decisions');
  if (decisions && decisions.items) {
    let h = '<h2>Recent AI Decisions (' + decisions.count + ')</h2><table><tr><th>Time</th><th>Symbol</th><th>Provider</th><th>Rec</th><th>Conf</th><th>Latency</th><th>Reasoning</th></tr>';
    for (const d of decisions.items.slice(0,20)) {
      const cls = d.recommendation==='ENTER'?'enter':d.recommendation==='SKIP'?'skip':'';
      h += `<tr><td>${(d.created_at||'').slice(0,19)}</td><td>${d.symbol}</td><td>${d.provider}</td>`;
      h += `<td>${badge(cls,d.recommendation)}</td><td>${pct(d.confidence)}</td><td>${d.latency_ms||'-'}ms</td>`;
      h += `<td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${d.reasoning||'-'}</td></tr>`;
    }
    cd.innerHTML = h + '</table>';
  }

  countdown = REFRESH/1000;
}

function tick() {
  countdown--;
  document.getElementById('timer').textContent = `refresh in ${Math.max(0,countdown)}s`;
  const pct = 100 - (countdown/(REFRESH/1000))*100;
  document.getElementById('rbar').style.width = pct+'%';
}

refresh();
setInterval(refresh, REFRESH);
setInterval(tick, 1000);
</script>
</body>
</html>"""


