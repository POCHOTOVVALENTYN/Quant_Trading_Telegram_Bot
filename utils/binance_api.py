import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional, TypeVar

try:
    from utils.metrics import api_429_errors
except Exception:
    api_429_errors = None


T = TypeVar("T")


@dataclass(frozen=True)
class BinanceCallPolicy:
    max_attempts: int = 4
    base_delay: float = 0.5
    max_delay: float = 8.0
    timeout_seconds: Optional[float] = None
    sync_time_on_1021: bool = True
    use_testnet: bool = False


class BinanceRateLimiter:
    """Simple async limiter to reduce concurrent pressure on Binance REST API."""

    def __init__(self, max_concurrent: int = 4):
        self._sem = asyncio.Semaphore(max(1, int(max_concurrent)))

    async def run(self, op: Callable[[], Awaitable[T]]) -> T:
        async with self._sem:
            return await op()


def classify_binance_error(err: Exception) -> str:
    text = str(err or "").lower()
    if "-1021" in text or "timestamp for this request" in text:
        return "time_sync"
    if "429" in text or "ratelimit" in text or "rate limit" in text or "too many requests" in text:
        return "rate_limit"
    if "requesttimeout" in text or "timed out" in text or "timeout" in text:
        return "timeout"
    if "networkerror" in text or "connection reset" in text or "socket" in text or "econnreset" in text:
        return "network"
    if "temporarily unavailable" in text or "service unavailable" in text or "server error" in text or "502" in text or "503" in text or "504" in text:
        return "server"
    if "insufficient balance" in text or "account has insufficient balance" in text:
        return "insufficient_funds"
    return "fatal"


async def call_with_binance_retry(
    *,
    op: Callable[[], Awaitable[T]],
    exchange=None,
    policy: Optional[BinanceCallPolicy] = None,
    limiter: Optional[BinanceRateLimiter] = None,
) -> T:
    policy = policy or BinanceCallPolicy()
    delay = policy.base_delay
    last_err = None

    for attempt in range(1, policy.max_attempts + 1):
        try:
            async def _run():
                if policy.timeout_seconds and policy.timeout_seconds > 0:
                    return await asyncio.wait_for(op(), timeout=policy.timeout_seconds)
                return await op()

            if limiter:
                return await limiter.run(_run)
            return await _run()
        except Exception as err:
            last_err = err
            category = classify_binance_error(err)

            if category == "rate_limit":
                try:
                    if api_429_errors:
                        api_429_errors.inc()
                except Exception:
                    pass

            if category == "time_sync" and exchange is not None and policy.sync_time_on_1021:
                try:
                    await exchange.load_time_difference()
                except Exception:
                    pass
            elif category not in {"rate_limit", "timeout", "network", "server"}:
                raise

            if attempt >= policy.max_attempts:
                raise

            await asyncio.sleep(delay)
            delay = min(delay * 2.0, policy.max_delay)

    raise last_err
