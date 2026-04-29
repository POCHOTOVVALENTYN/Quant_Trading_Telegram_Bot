#!/usr/bin/env bash
# Рестарт QuantTradingBot (API + Telegram)
set -e

echo "🛑 Останавливаем старые процессы..."
pkill -9 -f uvicorn || true
pkill -9 -f "api/telegram/main.py" || true
sleep 2

echo "🚀 Запускаем Trading Engine (REST API)..."
export PYTHONPATH=$PYTHONPATH:.
nohup .venv/bin/uvicorn api.rest.main:app --host 0.0.0.0 --port 8000 > rest_api.log 2>&1 &
sleep 5

echo "🤖 Запускаем Telegram Bot..."
nohup .venv/bin/python api/telegram/main.py > telegram_bot.log 2>&1 &

echo "✅ Перезапуск завершен. Логи в rest_api.log и telegram_bot.log"
