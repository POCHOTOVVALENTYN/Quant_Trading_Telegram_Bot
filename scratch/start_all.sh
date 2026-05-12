#!/bin/zsh
killall -9 Python python3 uvicorn 2>/dev/null
pkill -9 -f "services"
pkill -9 -f "api"
export PYTHONPATH=$(pwd)
export REDIS_URL="redis://127.0.0.1:6379"
export DATABASE_URL="postgresql+asyncpg://quant_user:quant_password@127.0.0.1:5440/quant_db"
echo "Cleaning logs..."
rm logs/*.log 2>/dev/null
echo "Starting Market Data..."
python3 -m services.market_data.worker > logs/market_data_live.log 2>&1 &
sleep 2
echo "Starting ML Worker..."
python3 -m services.ml_worker.worker > logs/ml_worker_live.log 2>&1 &
sleep 2
echo "Starting Engine..."
python3 -m uvicorn api.rest.main:app --host 0.0.0.0 --port 8000 > logs/engine_live.log 2>&1 &
sleep 5
echo "Starting Telegram Bot..."
python3 -m api.telegram.main > logs/telegram_live.log 2>&1 &
sleep 2
ps aux | grep -E "services|api|uvicorn" | grep -v "grep"
