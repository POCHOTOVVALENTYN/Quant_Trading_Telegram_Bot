"""
NLP News Filter (Phase 5C).
Provides a global risk-off flag based on cached news sentiment analysis.
Runs periodically (once per hour) to minimize API cost.
"""

import asyncio
import logging
import time
from typing import Optional

_log = logging.getLogger("news_filter")


class NewsFilter:
    """
    Global news-based risk filter.
    When risk_off is True, all new entries should be blocked.
    """

    def __init__(self, check_interval: int = 3600):
        self.check_interval = check_interval  # seconds between checks
        self.risk_off: bool = False
        self.last_sentiment: Optional[str] = None  # POSITIVE / NEGATIVE / NEUTRAL
        self.last_check_ts: float = 0.0
        self.last_summary: str = ""
        self._enabled = False

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    def enable(self):
        self._enabled = True

    def disable(self):
        self._enabled = False
        self.risk_off = False

    def should_block_entry(self) -> bool:
        """Returns True if news sentiment is risk-off and filter is enabled."""
        if not self._enabled:
            return False
        return self.risk_off

    async def check_sentiment(self, ai_adapter=None) -> dict:
        """
        Analyze macro news sentiment using the external AI adapter.
        Updates risk_off flag. Returns sentiment dict.
        """
        now = time.time()
        if now - self.last_check_ts < self.check_interval:
            return {
                "risk_off": self.risk_off,
                "sentiment": self.last_sentiment,
                "cached": True,
            }

        self.last_check_ts = now

        if ai_adapter is None or not ai_adapter.is_enabled:
            self.last_sentiment = "NEUTRAL"
            self.risk_off = False
            return {"risk_off": False, "sentiment": "NEUTRAL", "cached": False}

        prompt = (
            "You are a crypto market macro analyst. "
            "Assess the current overall crypto market risk environment in one word: "
            "POSITIVE (bullish, safe to trade), NEGATIVE (bearish/risky, avoid new positions), "
            "or NEUTRAL (mixed). "
            "Consider regulatory news, exchange issues, major hack events, "
            "macro events (CPI, FOMC) from the past 24 hours. "
            "Respond with ONLY one word: POSITIVE, NEGATIVE, or NEUTRAL."
        )

        try:
            result = await ai_adapter.analyze_signal(
                {"symbol": "MARKET", "signal": "CHECK", "strategy": "NEWS_FILTER", "score": 0},
                {"prompt_override": prompt},
            )
            reasoning = result.get("reasoning", "").strip().upper()

            if "NEGATIVE" in reasoning:
                self.last_sentiment = "NEGATIVE"
                self.risk_off = True
            elif "POSITIVE" in reasoning:
                self.last_sentiment = "POSITIVE"
                self.risk_off = False
            else:
                self.last_sentiment = "NEUTRAL"
                self.risk_off = False

            self.last_summary = result.get("reasoning", "")[:200]
            _log.info(
                f"News sentiment: {self.last_sentiment}, risk_off={self.risk_off}"
            )
        except Exception as e:
            _log.warning(f"News sentiment check failed: {e}")
            self.last_sentiment = "NEUTRAL"
            self.risk_off = False

        return {
            "risk_off": self.risk_off,
            "sentiment": self.last_sentiment,
            "summary": self.last_summary,
            "cached": False,
        }

    def get_status(self) -> dict:
        return {
            "enabled": self._enabled,
            "risk_off": self.risk_off,
            "sentiment": self.last_sentiment,
            "last_check": self.last_check_ts,
            "summary": self.last_summary[:100] if self.last_summary else None,
        }
