import logging
import json
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class AIModel:
    """
    Statistical Signal Scorer (rule-based heuristic, NOT a trained ML model).
    Weights can be updated by ScoringLearner from historical outcomes.
    """
    def __init__(self, weights: Optional[Dict[str, float]] = None):
        self.weights = weights or {
            'atr_ratio': 0.10,
            'range_relative': 0.05,
            'rsi': 0.10,
            'roc_10': 0.15,
            'price_to_sma20': 0.05,
            'sma20_to_sma50': 0.10,
            'volume_ratio': 0.15,
            'orderbook_imbalance': 0.15,
            'funding_rate': 0.15
        }

    def update_weights(self, new_weights: Dict[str, float]):
        for k, v in new_weights.items():
            if k in self.weights:
                self.weights[k] = v
        logger.info(f"Weights updated: {self.weights}")

    def predict_win_probability(self, features: Dict[str, float], signal_type: str) -> Dict[str, float]:
        if not features:
            return {"win_prob": 0.5, "expected_return": 1.0, "risk": 1.0}
        try:
            score = 0.0

            vol_ratio = features.get('volume_ratio', 1.0)
            score += max(-1.0, min(1.0, (vol_ratio - 1.0) / 2.0)) * self.weights['volume_ratio']

            roc = features.get('roc_10', 0.0)
            rsi = features.get('rsi', 50.0)

            if signal_type == "LONG":
                score += (1.0 if roc > 0 else -0.5) * self.weights['roc_10']
                if 50 < rsi < 75:
                    score += self.weights['rsi']
                elif rsi > 80:
                    score -= 0.5 * self.weights['rsi']
            else:
                score += (1.0 if roc < 0 else -0.5) * self.weights['roc_10']
                if 25 < rsi < 50:
                    score += self.weights['rsi']
                elif rsi < 20:
                    score -= 0.5 * self.weights['rsi']

            trend_align = features.get('sma20_to_sma50', 1.0)
            if signal_type == "LONG":
                score += (1.0 if trend_align > 1.0 else -1.0) * self.weights['sma20_to_sma50']
            else:
                score += (1.0 if trend_align < 1.0 else -1.0) * self.weights['sma20_to_sma50']

            imb = features.get('orderbook_imbalance', 0.0)
            score += (imb if signal_type == "LONG" else -imb) * self.weights['orderbook_imbalance']

            fr = features.get('funding_rate', 0.0)
            if signal_type == "LONG" and fr > 0.01:
                score -= 0.5 * self.weights['funding_rate']
            elif signal_type == "SHORT" and fr < -0.01:
                score -= 0.5 * self.weights['funding_rate']

            win_prob = 0.45 + (score * 0.35)
            win_prob = max(0.15, min(0.85, win_prob))

            atr_ratio = features.get('atr_ratio', 1.0)
            expected_return_pct = atr_ratio * 1.5
            risk = 1.0 + (features.get('range_relative', 1.0) * 0.5)

            return {
                "win_prob": round(win_prob, 2),
                "expected_return": round(expected_return_pct, 2),
                "risk": round(risk, 2)
            }
        except Exception as e:
            logger.error(f"Statistical scorer error: {e}")
            return {"win_prob": 0.5, "expected_return": 1.5, "risk": 1.0}


# ---------------------------------------------------------------------------
# EXTERNAL AI ADAPTER — cascade multi-provider
# ---------------------------------------------------------------------------

import time
from dataclasses import dataclass, field


async def _call_openai_compatible(
    url: str,
    api_key: str,
    model: str,
    prompt: str,
    timeout: int = 30,
    extra_headers: Optional[Dict] = None,
) -> str:
    import aiohttp

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    if extra_headers:
        headers.update(extra_headers)

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 300,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            url, json=payload, headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"HTTP {resp.status}: {text[:300]}")
            data = await resp.json()
            return data.get("choices", [{}])[0].get("message", {}).get("content", "")


@dataclass
class ProviderConfig:
    name: str
    api_key: str
    model: str
    base_url: str = ""


@dataclass
class _ProviderHealth:
    fail_count: int = 0
    backoff_until: float = 0.0
    total_calls: int = 0
    total_ok: int = 0


class ExternalAIAdapter:
    """
    Cascade multi-provider AI adapter for signal analysis.

    Tries providers in cascade_order. If the primary fails or is in backoff,
    falls through to the next. Tracks per-provider health automatically.

    Supported backends: groq, grok, gemini, openrouter, ollama, openai, custom.
    """
    def __init__(
        self,
        providers: Optional[Dict[str, ProviderConfig]] = None,
        cascade_order: Optional[list] = None,
        # Legacy single-provider fallback
        backend: str = "",
        base_url: str = "",
        api_key: str = "",
        model_name: str = "",
    ):
        self._providers: Dict[str, ProviderConfig] = providers or {}
        self._cascade: list = cascade_order or []
        self._health: Dict[str, _ProviderHealth] = {}
        self._enabled = False

        # Legacy: if no cascade but single backend provided, wrap it
        if not self._cascade and backend:
            self._providers[backend] = ProviderConfig(
                name=backend, api_key=api_key, model=model_name, base_url=base_url
            )
            self._cascade = [backend]

        for name in self._cascade:
            self._health[name] = _ProviderHealth()

    def enable(self):
        active = [n for n in self._cascade if self._providers.get(n, ProviderConfig("","","")).api_key]
        if not active:
            logger.warning("ExternalAI: no providers with API keys configured, staying disabled")
            return
        self._enabled = True
        logger.info(f"ExternalAI enabled — cascade: {' → '.join(active)}")

    def disable(self):
        self._enabled = False

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    def get_status(self) -> Dict[str, Any]:
        result = {}
        for name in self._cascade:
            cfg = self._providers.get(name)
            h = self._health.get(name, _ProviderHealth())
            has_key = bool(cfg and cfg.api_key)
            result[name] = {
                "configured": has_key,
                "model": cfg.model if cfg else "",
                "calls": h.total_calls,
                "ok": h.total_ok,
                "fails": h.fail_count,
                "in_backoff": time.time() < h.backoff_until,
            }
        return result

    async def analyze_signal(
        self,
        signal_data: Dict[str, Any],
        market_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not self._enabled:
            return {"recommendation": "PASS", "confidence": 0.0, "reasoning": "disabled"}

        prompt = self._build_prompt(signal_data, market_context)
        now = time.time()
        errors = []

        for name in self._cascade:
            cfg = self._providers.get(name)
            if not cfg or not cfg.api_key:
                continue

            h = self._health[name]
            if now < h.backoff_until:
                continue

            h.total_calls += 1
            try:
                raw = await self._call_provider(name, cfg, prompt)
                h.total_ok += 1
                h.fail_count = 0
                h.backoff_until = 0.0
                result = self._parse_response(raw)
                result["provider"] = name
                return result
            except Exception as e:
                h.fail_count += 1
                backoff_sec = min(300.0, 15.0 * (2 ** min(h.fail_count - 1, 5)))
                h.backoff_until = now + backoff_sec
                errors.append(f"{name}: {str(e)[:100]}")
                logger.warning(f"ExternalAI [{name}] failed (backoff {backoff_sec:.0f}s): {e}")

        err_summary = "; ".join(errors) if errors else "all providers in backoff or unconfigured"
        logger.error(f"ExternalAI cascade exhausted: {err_summary}")
        return {"recommendation": "PASS", "confidence": 0.0, "reasoning": f"cascade exhausted: {err_summary[:200]}"}

    # ---- provider dispatch ----

    async def _call_provider(self, name: str, cfg: ProviderConfig, prompt: str) -> str:
        if name == "groq":
            return await _call_openai_compatible(
                "https://api.groq.com/openai/v1/chat/completions",
                cfg.api_key, cfg.model or "llama-3.3-70b-versatile", prompt
            )
        elif name == "grok":
            url = cfg.base_url or "https://api.x.ai"
            if "/chat/completions" not in url:
                url = url.rstrip("/") + "/v1/chat/completions"
            return await _call_openai_compatible(
                url, cfg.api_key, cfg.model or "grok-3-mini", prompt
            )
        elif name == "gemini":
            return await self._call_gemini(cfg, prompt)
        elif name == "openrouter":
            return await _call_openai_compatible(
                "https://openrouter.ai/api/v1/chat/completions",
                cfg.api_key, cfg.model or "meta-llama/llama-3.1-8b-instruct:free", prompt,
                extra_headers={"HTTP-Referer": "https://github.com/QuantTradingBot"},
            )
        elif name == "ollama":
            return await self._call_ollama(cfg, prompt)
        elif name == "openai":
            return await _call_openai_compatible(
                "https://api.openai.com/v1/chat/completions",
                cfg.api_key, cfg.model or "gpt-4o-mini", prompt
            )
        elif name == "custom":
            url = (cfg.base_url or "http://localhost:8080").rstrip("/") + "/v1/chat/completions"
            return await _call_openai_compatible(url, cfg.api_key, cfg.model, prompt)
        raise ValueError(f"Unknown provider: {name}")

    @staticmethod
    async def _call_gemini(cfg: ProviderConfig, prompt: str) -> str:
        import aiohttp
        model = cfg.model or "gemini-2.0-flash"
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={cfg.api_key}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 300},
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"Gemini HTTP {resp.status}: {text[:300]}")
                data = await resp.json()
                candidates = data.get("candidates", [])
                if candidates:
                    parts = candidates[0].get("content", {}).get("parts", [])
                    return parts[0].get("text", "") if parts else ""
                return ""

    @staticmethod
    async def _call_ollama(cfg: ProviderConfig, prompt: str) -> str:
        import aiohttp
        url = f"{(cfg.base_url or 'http://localhost:11434').rstrip('/')}/api/generate"
        payload = {
            "model": cfg.model or "llama3.1:8b",
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.1, "num_predict": 300},
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"Ollama HTTP {resp.status}")
                data = await resp.json()
                return data.get("response", "")

    # ---- prompt & parsing ----

    @staticmethod
    def _build_prompt(signal: Dict, context: Dict) -> str:
        return (
            "You are a cryptocurrency trading risk analyst. "
            "Evaluate this signal and respond ONLY with valid JSON.\n\n"
            f"SIGNAL:\n"
            f"  Symbol: {signal.get('symbol')}\n"
            f"  Direction: {signal.get('signal')}\n"
            f"  Strategy: {signal.get('strategy')}\n"
            f"  Entry: {signal.get('entry_price')}\n"
            f"  SL: {signal.get('stop_loss')}\n"
            f"  TP: {signal.get('take_profit')}\n"
            f"  Score: {signal.get('score')}\n\n"
            f"MARKET:\n"
            f"  ATR: {context.get('atr')}\n"
            f"  RSI: {context.get('rsi')}\n"
            f"  ADX: {context.get('adx')}\n"
            f"  Volume Ratio: {context.get('volume_ratio')}\n"
            f"  Funding Rate: {context.get('funding_rate')}\n"
            f"  Trend: {context.get('trend')}\n\n"
            'Respond ONLY: {"recommendation":"ENTER" or "SKIP","confidence":0.0-1.0,"reasoning":"brief"}'
        )

    @staticmethod
    def _parse_response(raw: str) -> Dict[str, Any]:
        try:
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start >= 0 and end > start:
                parsed = json.loads(raw[start:end])
                return {
                    "recommendation": str(parsed.get("recommendation", "PASS")).upper(),
                    "confidence": float(parsed.get("confidence", 0.0)),
                    "reasoning": str(parsed.get("reasoning", "")),
                }
        except (json.JSONDecodeError, ValueError, TypeError):
            pass
        return {"recommendation": "PASS", "confidence": 0.0, "reasoning": f"Parse error: {raw[:200]}"}
