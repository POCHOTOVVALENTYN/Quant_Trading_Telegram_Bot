#!/usr/bin/env python3
"""
10-минутный (или произвольный) мониторинг здоровья бота:
- Telegram: getMe + тестовые уведомления админам
- БД: открытые позиции (positions OPEN)
- Binance Futures: активные позиции + сделки за окно
- Сверка БД ↔ биржа по символам/объёмам
- Логи: подсчёт ERROR за последние N минут в error/execution/signal

Запуск из корня проекта:
  python3 scripts/monitor_bot_health.py --duration 600 --interval 60
"""
from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# корень репозитория в PYTHONPATH
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _norm_sym(s: str) -> str:
    if not s:
        return ""
    base = s.split(":")[0].replace("/", "").strip().upper()
    if base.endswith("USDT") and len(base) > 4:
        return f"{base[:-4]}/USDT"
    return s


@dataclass
class DbPosition:
    id: int
    symbol: str
    side: str
    size: float
    entry_price: float


async def telegram_check(token: str, admin_ids: List[int]) -> Tuple[bool, str]:
    import httpx
    base = f"https://api.telegram.org/bot{token}"
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(f"{base}/getMe")
        data = r.json()
        if not data.get("ok"):
            return False, f"getMe failed: {data}"
        bot_user = data.get("result", {}).get("username", "?")
        ok_msg = f"OK @bot={bot_user}"
        if not admin_ids:
            return True, ok_msg + " (admin_user_ids пуст — отправка тестов пропущена)"
        sent = []
        for chat_id in admin_ids:
            payload = {
                "chat_id": chat_id,
                "text": f"🔍 **Мониторинг бота**\nСтарт проверки Telegram API.\nВремя (UTC): {datetime.now(timezone.utc).isoformat()}",
                "parse_mode": "Markdown",
            }
            r2 = await client.post(f"{base}/sendMessage", json=payload)
            j2 = r2.json()
            sent.append(f"chat={chat_id} ok={j2.get('ok')} err={j2.get('description', '')}")
        return True, ok_msg + " | " + "; ".join(sent)


async def telegram_ping(token: str, admin_ids: List[int], iteration: int, summary: str) -> None:
    if not admin_ids:
        return
    import httpx
    base = f"https://api.telegram.org/bot{token}"
    text = (
        f"📊 **Чек #{iteration}**\n"
        f"{summary}\n"
        f"_UTC {datetime.now(timezone.utc).strftime('%H:%M:%S')}_"
    )
    async with httpx.AsyncClient(timeout=15.0) as client:
        for chat_id in admin_ids:
            try:
                await client.post(
                    f"{base}/sendMessage",
                    json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
                )
            except Exception as e:
                print(f"[WARN] Telegram send failed: {e}")


async def load_db_open_positions_asyncpg() -> List[DbPosition]:
    import asyncpg
    from config.settings import settings

    dsn = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            """
            SELECT id, symbol, side::text AS side, size, entry_price
            FROM positions
            WHERE status = 'OPEN'
            """
        )
    finally:
        await conn.close()
    out: List[DbPosition] = []
    for r in rows:
        side = (r["side"] or "").upper() or "?"
        out.append(
            DbPosition(
                id=r["id"],
                symbol=_norm_sym(r["symbol"]),
                side=side,
                size=float(r["size"] or 0),
                entry_price=float(r["entry_price"] or 0),
            )
        )
    return out


async def load_db_open_positions() -> List[DbPosition]:
    try:
        from sqlalchemy import select
        from database.session import async_session
        from database.models.all_models import Position, PositionStatus

        out: List[DbPosition] = []
        async with async_session() as session:
            res = await session.execute(
                select(Position).where(Position.status == PositionStatus.OPEN)
            )
            for p in res.scalars().all():
                side = p.side.value if p.side else "?"
                out.append(
                    DbPosition(
                        id=p.id,
                        symbol=_norm_sym(p.symbol),
                        side=side,
                        size=float(p.size or 0),
                        entry_price=float(p.entry_price or 0),
                    )
                )
        return out
    except Exception as e:
        print(f"[WARN] SQLAlchemy DB load failed ({e}), trying asyncpg...")
        try:
            return await load_db_open_positions_asyncpg()
        except Exception as e2:
            print(f"[ERROR] asyncpg DB load failed: {e2}")
            return []


def _exchange_keys():
    from config.settings import settings
    if settings.testnet and settings.test_api_key_binance and settings.test_secret_api_key_binance:
        return (
            settings.test_api_key_binance.strip(),
            settings.test_secret_api_key_binance.strip(),
        )
    k = (settings.api_key_binance or "").strip()
    s = (settings.secret_api_key_binance or "").strip()
    return k, s


async def exchange_connect():
    try:
        import ccxt.pro as ccxtpro
    except ImportError:
        return None, "Пакет ccxt не установлен (pip install ccxt)"
    from config.settings import settings

    api_key, secret = _exchange_keys()
    if not api_key or not secret:
        return None, "API ключи не заданы (.env: test_* или api_key_binance)"
    ex = ccxtpro.binance(
        {
            "apiKey": api_key,
            "secret": secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": "future",
                "testnet": bool(settings.testnet),
                "adjustForTimeDifference": True,
                "recvWindow": 10000,
            },
            "timeout": 30000,
        }
    )
    # Как в api/rest/main.py (новые версии ccxt ругаются на sandbox+futures — оставляем options.testnet)
    try:
        ex.set_sandbox_mode(bool(settings.testnet))
    except Exception:
        pass
    if settings.testnet:
        working_ws_url = "wss://testnet.binancefuture.com/ws-fapi/v1"
        if hasattr(ex, "urls") and ex.urls:
            ex.urls.setdefault("test", {}).setdefault("ws", {})["future"] = working_ws_url
            ex.urls.setdefault("api", {}).setdefault("ws", {})["future"] = working_ws_url
    try:
        await ex.load_markets()
    except Exception as e:
        await ex.close()
        return None, f"load_markets: {e}"
    return ex, None


async def fetch_live_positions(ex) -> List[Dict[str, Any]]:
    raw = await ex.fetch_positions()
    active = []
    for p in raw or []:
        c = float(p.get("contracts") or p.get("pa") or 0)
        if abs(c) <= 1e-8:
            continue
        sym = _norm_sym(p.get("symbol") or "")
        side = str(p.get("side") or "").upper()
        if side not in ("LONG", "SHORT"):
            side = "LONG" if c > 0 else "SHORT"
        active.append(
            {
                "symbol": sym,
                "side": side,
                "contracts": abs(c),
                "entry": float(p.get("entryPrice") or 0),
                "raw_symbol": p.get("symbol"),
            }
        )
    return active


async def fetch_recent_trades(ex, symbols: List[str], since_ms: int) -> Dict[str, int]:
    """Количество сделок по символам за период since_ms."""
    counts: Dict[str, int] = {}
    for sym in symbols:
        ccxt_sym = sym
        if ":" not in sym:
            ccxt_sym = f"{sym}:USDT"
        try:
            trades = await ex.fetch_my_trades(ccxt_sym, since=since_ms, limit=100)
            counts[sym] = len(trades or [])
        except Exception as e:
            counts[sym] = -1  # ошибка
            print(f"[WARN] fetch_my_trades {sym}: {e}")
    return counts


def compare_db_exchange(db_rows: List[DbPosition], live: List[Dict[str, Any]]) -> List[str]:
    issues: List[str] = []
    db_by = {r.symbol: r for r in db_rows}
    live_by = {x["symbol"]: x for x in live}

    for sym, row in db_by.items():
        if sym not in live_by:
            issues.append(f"DB OPEN без позиции на бирже: {sym} id={row.id}")
    for sym, x in live_by.items():
        if sym not in db_by:
            issues.append(f"Биржа имеет позицию, нет OPEN в БД: {sym} {x['side']} {x['contracts']}")
    for sym in set(db_by) & set(live_by):
        d, l = db_by[sym], live_by[sym]
        if d.side != l["side"]:
            issues.append(f"Side mismatch {sym}: DB={d.side} EX={l['side']}")
        if abs(d.size - l["contracts"]) > max(1e-6, d.size * 0.02):
            issues.append(
                f"Size mismatch {sym}: DB={d.size:.6f} EX={l['contracts']:.6f}"
            )
    return issues


LOG_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)")


def scan_log_errors(log_path: Path, since_utc: datetime, max_lines: int = 8000) -> Tuple[int, List[str]]:
    if not log_path.is_file():
        return 0, [f"нет файла {log_path}"]
    try:
        data = log_path.read_bytes()
    except Exception as e:
        return 0, [f"read error: {e}"]
    text = data.decode("utf-8", errors="replace")
    lines = text.splitlines()
    tail = lines[-max_lines:] if len(lines) > max_lines else lines
    err_count = 0
    samples: List[str] = []
    for line in tail:
        m = LOG_TS_RE.match(line)
        if not m:
            continue
        try:
            ts = datetime.strptime(m.group(1).split(".")[0], "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
        except Exception:
            continue
        if ts < since_utc:
            continue
        if "| ERROR" in line or " ERROR " in line or "❌" in line:
            err_count += 1
            if len(samples) < 5:
                samples.append(line[:220])
    return err_count, samples


async def one_iteration(
    iteration: int,
    ex,
    window_start_ms: int,
    admin_ids: List[int],
    token: str,
) -> str:
    from config.settings import settings

    db_pos = await load_db_open_positions()
    live: List[Dict[str, Any]] = []
    ex_err: Optional[str] = None
    trades_err: Optional[str] = None
    if ex:
        try:
            live = await fetch_live_positions(ex)
        except Exception as e:
            ex_err = str(e)[:240]
            live = []
    if ex and not ex_err:
        issues = compare_db_exchange(db_pos, live)
    else:
        issues = []
        if not ex:
            issues.append("биржа: нет клиента (ключи/ccxt)")
        if ex_err:
            issues.append(f"биржа: {ex_err}")

    symbols = list({p.symbol for p in db_pos} | {x["symbol"] for x in live})
    trades_counts: Dict[str, int] = {}
    if ex and symbols and not ex_err:
        try:
            trades_counts = await fetch_recent_trades(ex, symbols, window_start_ms)
        except Exception as e:
            trades_err = str(e)[:200]

    since_logs = datetime.now(timezone.utc) - timedelta(minutes=6)
    logs_dir = ROOT / "logs"
    err_n, _ = scan_log_errors(logs_dir / "error.log", since_logs)
    ex_n, _ = scan_log_errors(logs_dir / "execution.log", since_logs)
    sig_n, _ = scan_log_errors(logs_dir / "signal.log", since_logs)

    parts = [
        f"testnet={settings.testnet}",
        f"DB_OPEN={len(db_pos)} EX_LIVE={len(live)}",
        f"log_ERR(~6m) error={err_n} execution={ex_n} signal={sig_n}",
    ]
    if issues:
        parts.append("issues: " + "; ".join(issues[:8]))
    else:
        parts.append("DB↔EX: OK")
    if trades_counts:
        tc = ", ".join(f"{k}:{v}" for k, v in list(trades_counts.items())[:6])
        parts.append(f"trades(win): {tc}")
    if trades_err:
        parts.append(f"trades_err: {trades_err}")

    summary = "\n".join(parts)
    await telegram_ping(token, admin_ids, iteration, summary)
    return summary


async def main_async(duration_sec: int, interval_sec: int) -> int:
    os.chdir(ROOT)
    from config.settings import settings

    token = settings.telegram_bot_token.get_secret_value()
    admin_ids = [int(x.strip()) for x in settings.admin_user_ids.split(",") if x.strip()]

    print("=== Telegram ===")
    ok, msg = await telegram_check(token, admin_ids)
    print(msg if ok else f"FAIL: {msg}")

    ex, err = await exchange_connect()
    if err:
        print(f"=== Exchange: {err} ===")
    else:
        print("=== Exchange: connected ===")

    started = datetime.now(timezone.utc)
    window_ms = int((started - timedelta(minutes=12)).timestamp() * 1000)
    iteration = 0
    end_monotonic = time.monotonic() + duration_sec

    report_lines: List[str] = []
    report_lines.append(f"Monitor start UTC {started.isoformat()} duration={duration_sec}s interval={interval_sec}s")

    try:
        while time.monotonic() < end_monotonic:
            iteration += 1
            print(f"\n--- Iteration {iteration} ---")
            try:
                summary = await one_iteration(iteration, ex, window_ms, admin_ids, token)
                print(summary)
                report_lines.append(f"#{iteration} {summary.replace(chr(10), ' | ')}")
            except Exception as e:
                print(f"ITER ERROR: {e}")
                report_lines.append(f"#{iteration} ERROR: {e}")
            wait = min(interval_sec, max(0.0, end_monotonic - time.monotonic()))
            if wait <= 0:
                break
            await asyncio.sleep(wait)
    finally:
        if ex:
            await ex.close()

    out_path = ROOT / "logs" / f"monitor_report_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
    out_path.write_text("\n".join(report_lines), encoding="utf-8")
    print(f"\nReport saved: {out_path}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Мониторинг бота: Telegram, БД, биржа, логи")
    p.add_argument("--duration", type=int, default=600, help="Длительность в секундах (по умолчанию 600 = 10 мин)")
    p.add_argument("--interval", type=int, default=60, help="Интервал между чеками в секундах")
    args = p.parse_args()
    return asyncio.run(main_async(args.duration, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())
