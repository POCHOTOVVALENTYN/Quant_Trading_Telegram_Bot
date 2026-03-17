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

@asynccontextmanager
async def lifespan(app: FastAPI):
    global orchestrator, exchange_client
    
    # 1. Инициализация Базы Данных
    app_logger.info("Инициализация базы данных...")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        app_logger.info("Таблицы базы данных успешно синхронизированы.")
    except Exception as e:
        app_logger.error(f"Ошибка БД: {e}")

    # 2. Инициализация биржевого клиента Binance
    app_logger.info(f"Инициализация клиента Binance API (Testnet: {settings.testnet})...")
    exchange_client = ccxtpro.binance({
        'apiKey': settings.api_key_binance,
        'secret': settings.secret_api_key_binance,
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })
    exchange_client.set_sandbox_mode(settings.testnet)
    
    # 3. Инициализация ключевых микромодулей (Риск, Исполнение ордеров, Данные рынка)
    # Важно: ccxt использует формат BTC/USDT, а не BTCUSDT!
    market_data = MarketDataService(
        symbols=["BTC/USDT", "ETH/USDT"], 
        timeframes=["1m", "5m", "15m", "1h"]
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
    
    # 4. Инициализация Оркестратора
    orchestrator = TradingOrchestrator(
        market_data=market_data, 
        execution_engine=execution_engine
    )

    # 5. Запуск Оркестратора в фоновой задаче (не блокируя FastAPI)
    app_logger.info("Запуск Торгового Движка (Orchestrator)...")
    asyncio.create_task(orchestrator.start())
    
    app_logger.info("✅ Платформа успешно запущена!")
    
    yield  # --- Здесь работает FastAPI ---
    
    # 6. Корректное завершение работы при выключении (Graceful Shutdown)
    app_logger.info("Выключение торговой платформы...")
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
