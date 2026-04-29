#!/usr/bin/env python3
"""
Сверка открытых позиций в PostgreSQL с Binance Futures и краткая сводка по сделкам за окно времени.

Запуск из корня репозитория (нужен .env с DATABASE_URL и ключами Binance):

  python3 scripts/audit_db_exchange.py
  python3 scripts/audit_db_exchange.py --trades-minutes 30

Код выхода: 0 — расхождений нет, 1 — есть расхождения или критическая ошибка.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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


def _ccxt_futures_symbol(sym: str) -> str:
    """BTC/USDT -> BTC/USDT:USDT для ccxt futures."""
    s = _norm_sym(sym)
    if ":USDT" in sym:
        return sym.split(":")[0] + ":USDT" if "/" in sym else f"{s}:USDT"
    return f"{s}:USDT"


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


def _make_exchange():
    import ccxt

    from config.settings import settings

    api_key, secret = _exchange_keys()
    if not api_key or not secret:
        raise RuntimeError("В .env не заданы API ключи Binance (test_* или api_key_binance).")

    ex = ccxt.binance(
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
    try:
        ex.set_sandbox_mode(bool(settings.testnet))
    except Exception:
        pass
    if settings.testnet:
        working_ws_url = "wss://testnet.binancefuture.com/ws-fapi/v1"
        if hasattr(ex, "urls") and ex.urls:
            ex.urls.setdefault("test", {}).setdefault("ws", {})["future"] = working_ws_url
            ex.urls.setdefault("api", {}).setdefault("ws", {})["future"] = working_ws_url
    return ex, settings


async def _fetch_db_open() -> List[Dict[str, Any]]:
    import asyncpg

    from config.settings import settings

    dsn = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            """
            SELECT id, symbol, side::text AS side, size, entry_price, status::text AS status,
                   opened_at
            FROM positions
            WHERE status::text = 'OPEN'
            ORDER BY id
            """
        )
    finally:
        await conn.close()
    out = []
    for r in rows:
        out.append(
            {
                "id": r["id"],
                "symbol": _norm_sym(r["symbol"]),
                "side": (r["side"] or "").upper(),
                "size": float(r["size"] or 0),
                "entry_price": float(r["entry_price"] or 0),
                "opened_at": r["opened_at"],
            }
        )
    return out


def _fetch_exchange_positions(ex) -> List[Dict[str, Any]]:
    raw = ex.fetch_positions()
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
            }
        )
    return active


def _fetch_algo_open(ex, clean_symbol: str) -> List[Dict[str, Any]]:
    """openAlgoOrders для одного символа (BTCUSDT)."""
    try:
        raw = ex.request(
            "openAlgoOrders",
            "fapiPrivate",
            "GET",
            {"symbol": clean_symbol.replace("/", "").split(":")[0]},
        )
    except Exception as e:
        return [{"error": str(e)}]
    orders = raw.get("algoOrders", []) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
    slim = []
    for o in orders[:20]:
        slim.append(
            {
                "algoId": o.get("algoId"),
                "type": o.get("type"),
                "triggerPrice": o.get("triggerPrice"),
                "algoStatus": o.get("algoStatus"),
            }
        )
    return slim


def _count_trades(ex, symbol: str, since_ms: int) -> Tuple[int, Optional[str]]:
    try:
        ccxt_sym = _ccxt_futures_symbol(symbol)
        trades = ex.fetch_my_trades(ccxt_sym, since=since_ms, limit=200)
        return len(trades or []), None
    except Exception as e:
        return 0, str(e)


def _compare(
    db_rows: List[Dict[str, Any]], ex_rows: List[Dict[str, Any]]
) -> Tuple[List[str], bool]:
    issues: List[str] = []
    db_by = {r["symbol"]: r for r in db_rows}
    ex_by = {r["symbol"]: r for r in ex_rows}

    for sym, row in db_by.items():
        if sym not in ex_by:
            issues.append(f"❌ В БД OPEN есть {sym} (id={row['id']}), на бирже позиции нет.")
    for sym, x in ex_by.items():
        if sym not in db_by:
            issues.append(
                f"❌ На бирже есть позиция {sym} {x['side']} {x['contracts']}, в БД нет OPEN."
            )
    for sym in set(db_by) & set(ex_by):
        d, l = db_by[sym], ex_by[sym]
        if d["side"] != l["side"]:
            issues.append(f"⚠️ Сторона {sym}: БД={d['side']} биржа={l['side']}")
        tol = max(1e-6, d["size"] * 0.02)
        if abs(d["size"] - l["contracts"]) > tol:
            issues.append(
                f"⚠️ Объём {sym}: БД={d['size']:.6f} биржа={l['contracts']:.6f}"
            )
    return issues, len(issues) == 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Сверка OPEN позиций БД ↔ Binance Futures + сделки за окно."
    )
    parser.add_argument(
        "--trades-minutes",
        type=int,
        default=15,
        help="За сколько минут назад считать сделки по символам (по умолчанию 15).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Вывести машиночитаемый JSON в stdout.",
    )
    args = parser.parse_args()

    os.chdir(ROOT)

    report: Dict[str, Any] = {
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "testnet": None,
        "db_open": [],
        "exchange_positions": [],
        "issues": [],
        "trades": {},
        "algo_preview": {},
        "errors": [],
    }

    try:
        report["db_open"] = asyncio.run(_fetch_db_open())
    except Exception as e:
        report["errors"].append(f"БД: {e}")
        print(f"❌ Ошибка подключения к БД: {e}", file=sys.stderr)
        if args.json:
            print(json.dumps(report, indent=2, default=str))
        return 1

    try:
        ex, settings = _make_exchange()
        report["testnet"] = settings.testnet
        ex.load_markets()
        report["exchange_positions"] = _fetch_exchange_positions(ex)
    except Exception as e:
        report["errors"].append(f"Биржа: {e}")
        print(f"❌ Ошибка биржи: {e}", file=sys.stderr)
        if args.json:
            print(json.dumps(report, indent=2, default=str))
        return 1

    issues, ok = _compare(report["db_open"], report["exchange_positions"])
    report["issues"] = issues

    since_ms = int((time.time() - args.trades_minutes * 60) * 1000)
    symbols = sorted(
        set(r["symbol"] for r in report["db_open"])
        | set(r["symbol"] for r in report["exchange_positions"])
    )
    for sym in symbols:
        n, err = _count_trades(ex, sym, since_ms)
        report["trades"][sym] = {"count": n, "error": err}
        clean = sym.replace("/", "").split(":")[0]
        report["algo_preview"][sym] = _fetch_algo_open(ex, clean)
        time.sleep(0.2)  # Rate limit protection: 5 requests per second max

    try:
        ex.close()
    except Exception:
        pass

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print("=== Сверка БД ↔ Binance Futures ===")
        print(f"UTC: {report['time_utc']}  testnet={report['testnet']}")
        print(f"\nOPEN в БД: {len(report['db_open'])}")
        for r in report["db_open"]:
            print(
                f"  id={r['id']} {r['symbol']} {r['side']} size={r['size']:.6f} entry={r['entry_price']:.6f}"
            )
        print(f"\nПозиции на бирже: {len(report['exchange_positions'])}")
        for r in report["exchange_positions"]:
            print(
                f"  {r['symbol']} {r['side']} contracts={r['contracts']:.6f} entry={r['entry']:.6f}"
            )
        print(f"\nСделки за последние {args.trades_minutes} мин (по символам):")
        for sym, t in report["trades"].items():
            err = f" ({t['error']})" if t.get("error") else ""
            print(f"  {sym}: {t['count']} шт.{err}")
        print("\nAlgo-ордера (превью, до 20 на символ):")
        for sym, aos in report["algo_preview"].items():
            print(f"  {sym}: {len(aos)} записей — {aos[:3]}...")

        if issues:
            print("\n⚠️ Замечания:")
            for i in issues:
                print(" ", i)
        else:
            print("\n✅ Расхождений по OPEN позициям не обнаружено.")

    return 0 if ok and not report["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
