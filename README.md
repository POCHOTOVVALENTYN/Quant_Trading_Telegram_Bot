# Quant Trading Bot 🚀

Это алгоритмическая торговая платформа и Telegram-бот (написанный на FastAPI/AsyncIO), реализующий стратегии из книги *Technical Analysis: The Complete Resource for Financial Market Technicians*.

## Реализованные стратегии (согласно книге):
1. **Wide Range Day (WRD)** — поиск дней с расширенным волатильным диапазоном (VR > 1.6).
2. **Measured Move** — расчет целей движения C + (B - A).
3. **Rule of 7** — расчет 3-х целей (T1, T2, T3) на основе множителей от High-Low.
4. Индикаторы: **ATR, True Range (TR), SMA** (написаны с нуля через `pandas`, без сторонних C-зависимостей).
5. Риск-менеджмент: **Pyramiding (50/30/20)**, **ATR-Stop Loss**, **Trailing Stop**.

---

## 🚀 Как запустить (Docker)

Проект полностью контейнеризирован (содержит 4 контейнера: Trading Engine, Telegram Bot, PostgreSQL, Redis).

### Шаг 1: Настройка окружения
В корневой папке проекта есть файл конфигурации базы: `.env`. Откройте его и впишите **ваш реальный токен** от BotFather:
```shell
TELEGRAM_BOT_TOKEN="123456789:ABCDefgh..."
```

### Шаг 2: Запуск
Включите Docker Desktop (если вы на Mac/Windows) и выполните в корневой папке скрипта команду:
```shell
docker-compose up -d --build
```

### Шаг 3: Миграции БД (Единожды)
Как только контейнеры поднимутся, накатите таблицы в PostgreSQL с помощью Alembic:
```shell
alembic upgrade head
```
*(Либо скрипт `main.py` создаст их самостоятельно при первом запуске)*

### Аудит: БД ↔ Binance Futures

Сверка открытых позиций в PostgreSQL с фактическими позициями на бирже и краткая статистика по сделкам за окно (по умолчанию 15 минут):

```bash
# из корня репозитория, при заполненном .env (DATABASE_URL, ключи Binance)
python3 scripts/audit_db_exchange.py

# другое окно для подсчёта сделок + JSON в stdout
python3 scripts/audit_db_exchange.py --trades-minutes 60 --json
```

Код выхода: `0` — нет расхождений по OPEN позициям, `1` — ошибка подключения или найдены несоответствия.

---

## 📊 Дашборд
Фронтенд-часть для отображения визуализированной Equity Curve (Кривой доходности) доступна как отдельный отчет в `services/dashboard/visualizer.py`. Можно сгенерировать HTML отчет с Plotly-графиком напрямую, если натравить его на DataFrame с вашими сделками!
# Quant_Trading_Telegram_Bot
