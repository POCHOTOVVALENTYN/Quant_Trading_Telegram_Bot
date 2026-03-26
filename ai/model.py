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
# EXTERNAL AI ADAPTER — all providers
# ---------------------------------------------------------------------------

# OpenAI-compatible chat completions format, reused by many providers
async def _call_openai_compatible(
    url: str,
    api_key: str,
    model: str,
    prompt: str,
    timeout: int = 30,
    extra_headers: Optional[Dict] = None,
) -> str:
    """Generic caller for any OpenAI-compatible /chat/completions endpoint."""
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


class ExternalAIAdapter:
    """
    Multi-provider AI adapter for signal analysis.

    Supported backends (all use free/minimal tiers):
      "ollama"      — Local Ollama (100% free, Llama 3.1 / Mistral / Gemma)
      "gemini"      — Google Gemini API (free: 15 RPM / 1M TPM)
      "grok"        — xAI Grok API ($25/mo free credit)
      "groq"        — Groq Cloud (free tier, Llama 3 / Mixtral, ultra fast)
      "openrouter"  — OpenRouter aggregator (some models free)
      "openai"      — OpenAI (paid, GPT-4o-mini ~$0.15/1M tokens)
      "custom"      — Any OpenAI-compatible endpoint
    """
    def __init__(
        self,
        backend: str = "ollama",
        base_url: str = "http://localhost:11434",
        api_key: str = "",
        model_name: str = "llama3.1:8b",
    ):
        self.backend = backend.lower().strip()
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model_name = model_name
        self._enabled = False

    def enable(self):
        self._enabled = True
        logger.info(f"ExternalAI enabled: {self.backend} / {self.model_name}")

    def disable(self):
        self._enabled = False

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    async def analyze_signal(
        self,
        signal_data: Dict[str, Any],
        market_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not self._enabled:
            return {"recommendation": "PASS", "confidence": 0.0, "reasoning": "disabled"}

        prompt = self._build_prompt(signal_data, market_context)

        try:
            raw = await self._dispatch(prompt)
            return self._parse_response(raw)
        except Exception as e:
            logger.error(f"ExternalAI [{self.backend}] error: {e}")
            return {"recommendation": "PASS", "confidence": 0.0, "reasoning": str(e)[:200]}

    # ---- routing ----

    async def _dispatch(self, prompt: str) -> str:
        if self.backend == "ollama":
            return await self._call_ollama(prompt)
        elif self.backend == "gemini":
            return await self._call_gemini(prompt)
        elif self.backend == "grok":
            return await self._call_grok(prompt)
        elif self.backend == "groq":
            return await self._call_groq(prompt)
        elif self.backend == "openrouter":
            return await self._call_openrouter(prompt)
        elif self.backend == "openai":
            return await self._call_openai(prompt)
        elif self.backend == "custom":
            return await self._call_custom(prompt)
        raise ValueError(f"Unknown backend: {self.backend}")

    # ---- individual providers ----

    async def _call_ollama(self, prompt: str) -> str:
        import aiohttp
        url = f"{self.base_url}/api/generate"
        payload = {
            "model": self.model_name,
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

    async def _call_gemini(self, prompt: str) -> str:
        """Google Gemini via REST (free tier: gemini-2.0-flash)."""
        import aiohttp
        model = self.model_name or "gemini-2.0-flash"
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={self.api_key}"
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

    async def _call_grok(self, prompt: str) -> str:
        """xAI Grok via OpenAI-compatible API ($25/mo free)."""
        url = self.base_url or "https://api.x.ai/v1/chat/completions"
        if "/chat/completions" not in url:
            url = url.rstrip("/") + "/v1/chat/completions"
        model = self.model_name or "grok-3-mini"
        return await _call_openai_compatible(url, self.api_key, model, prompt)

    async def _call_groq(self, prompt: str) -> str:
        """Groq Cloud free tier (Llama 3 70B / Mixtral at 500+ tokens/sec)."""
        url = "https://api.groq.com/openai/v1/chat/completions"
        model = self.model_name or "llama-3.3-70b-versatile"
        return await _call_openai_compatible(url, self.api_key, model, prompt)

    async def _call_openrouter(self, prompt: str) -> str:
        """OpenRouter — aggregator with free models."""
        url = "https://openrouter.ai/api/v1/chat/completions"
        model = self.model_name or "meta-llama/llama-3.1-8b-instruct:free"
        return await _call_openai_compatible(
            url, self.api_key, model, prompt,
            extra_headers={"HTTP-Referer": "https://github.com/QuantTradingBot"},
        )

    async def _call_openai(self, prompt: str) -> str:
        url = "https://api.openai.com/v1/chat/completions"
        model = self.model_name or "gpt-4o-mini"
        return await _call_openai_compatible(url, self.api_key, model, prompt)

    async def _call_custom(self, prompt: str) -> str:
        url = self.base_url.rstrip("/") + "/v1/chat/completions"
        return await _call_openai_compatible(url, self.api_key, self.model_name, prompt)

    # ---- prompt & parsing ----

    def _build_prompt(self, signal: Dict, context: Dict) -> str:
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
