import json
import logging
import os
import threading
import time
import asyncio
import hashlib

from dotenv import load_dotenv
from openai import AsyncOpenAI, OpenAI

# Load environment variables
load_dotenv()

# Configuration
# Configuration
load_dotenv()
LLM_MOCK_MODE = str(os.getenv("LLM_MOCK_MODE", "false")).strip().lower() in {"1", "true", "yes", "y", "on"}

# --- 1. Smart Model Config (Default/Primary) ---
SMART_API_KEY = os.getenv("SMART_API_KEY", os.getenv("DEEPSEEK_API_KEY"))
SMART_BASE_URL = os.getenv("SMART_BASE_URL", os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"))
MODEL_SMART = os.getenv("MODEL_SMART", os.getenv("DEEPSEEK_MODEL", "deepseek-chat"))

# --- 2. Fast Model Config (Secondary) ---
# If FAST keys are not set, fallback to SMART keys (Aggregation Gateway scenario)
FAST_API_KEY = os.getenv("FAST_API_KEY", SMART_API_KEY)
FAST_BASE_URL = os.getenv("FAST_BASE_URL", SMART_BASE_URL)
MODEL_FAST = os.getenv("MODEL_FAST", MODEL_SMART)

# Setup Logger
logger = logging.getLogger(__name__)

if not SMART_API_KEY:
    logger.warning("SMART_API_KEY (or DEEPSEEK_API_KEY) not found. Main LLM calls will fail.")

# Initialize Clients (Lazy Loading)
client_smart = None
aclient_smart = None
client_fast = None
aclient_fast = None

# Runtime controls (loaded lazily from env)
_runtime_loaded = False
_runtime_lock = threading.Lock()
_sync_locks = {"smart": threading.Lock(), "fast": threading.Lock()}
_async_state = {
    "loop": None,
    "rate_locks": {},
    "semaphores": {},
    "cache_lock": None,
}
_next_allowed_sync = {"smart": 0.0, "fast": 0.0}
_next_allowed_async = {"smart": 0.0, "fast": 0.0}
_runtime = {
    "timeout_seconds": 45.0,
    "max_retries": 1,
    "backoff_base_seconds": 0.4,
    "qps_smart": 0.0,
    "qps_fast": 0.0,
    "concurrency_smart": 8,
    "concurrency_fast": 16,
    "breaker_fail_threshold": 8,
    "breaker_cooldown_seconds": 20.0,
    "enable_cache": True,
    "cache_max_size": 2000,
}
_cache_usage_stats = {
    "smart": {"calls": 0, "hit_tokens": 0, "miss_tokens": 0},
    "fast": {"calls": 0, "hit_tokens": 0, "miss_tokens": 0},
}
_breaker_state = {
    "smart": {"fails": 0, "open_until": 0.0},
    "fast": {"fails": 0, "open_until": 0.0},
}
_sync_cache_lock = threading.Lock()
_response_cache = {}


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return int(default)


def _load_runtime_controls(force: bool = False):
    global _runtime_loaded
    with _runtime_lock:
        if _runtime_loaded and not force:
            return
        _runtime["timeout_seconds"] = max(5.0, _float_env("LLM_TIMEOUT_SECONDS", 45.0))
        _runtime["max_retries"] = max(0, _int_env("LLM_MAX_RETRIES", 1))
        _runtime["backoff_base_seconds"] = max(0.05, _float_env("LLM_BACKOFF_BASE_SECONDS", 0.4))
        _runtime["qps_smart"] = max(0.0, _float_env("LLM_QPS_SMART", 0.0))
        _runtime["qps_fast"] = max(0.0, _float_env("LLM_QPS_FAST", 0.0))
        _runtime["concurrency_smart"] = max(1, _int_env("LLM_MAX_CONCURRENCY_SMART", 8))
        _runtime["concurrency_fast"] = max(1, _int_env("LLM_MAX_CONCURRENCY_FAST", 16))
        _runtime["breaker_fail_threshold"] = max(1, _int_env("LLM_BREAKER_FAIL_THRESHOLD", 8))
        _runtime["breaker_cooldown_seconds"] = max(1.0, _float_env("LLM_BREAKER_COOLDOWN_SECONDS", 20.0))
        _runtime["enable_cache"] = str(os.getenv("LLM_ENABLE_CACHE", "true")).strip().lower() in {"1", "true", "yes", "y", "on"}
        _runtime["cache_max_size"] = max(100, _int_env("LLM_CACHE_MAX_SIZE", 2000))
        # Async primitives (locks/semaphores) are loop-bound. Mark dirty so
        # they rebuild lazily on the next async call under the current loop.
        _async_state["loop"] = None
        _async_state["rate_locks"] = {}
        _async_state["semaphores"] = {}
        _async_state["cache_lock"] = None
        _runtime_loaded = True


def _ensure_async_primitives():
    loop = asyncio.get_running_loop()
    if (
        _async_state["loop"] is loop
        and _async_state["semaphores"].get("smart") is not None
        and _async_state["semaphores"].get("fast") is not None
        and _async_state["rate_locks"].get("smart") is not None
        and _async_state["rate_locks"].get("fast") is not None
        and _async_state["cache_lock"] is not None
    ):
        return

    _async_state["loop"] = loop
    _async_state["rate_locks"] = {
        "smart": asyncio.Lock(),
        "fast": asyncio.Lock(),
    }
    _async_state["semaphores"] = {
        "smart": asyncio.Semaphore(int(_runtime["concurrency_smart"])),
        "fast": asyncio.Semaphore(int(_runtime["concurrency_fast"])),
    }
    _async_state["cache_lock"] = asyncio.Lock()


def _async_rate_lock_for(model_key: str):
    _ensure_async_primitives()
    return _async_state["rate_locks"][model_key]


def _async_semaphore_for(model_key: str):
    _ensure_async_primitives()
    return _async_state["semaphores"][model_key]


def _async_cache_lock():
    _ensure_async_primitives()
    return _async_state["cache_lock"]


def _breaker_is_open(model_key: str) -> bool:
    return time.monotonic() < float(_breaker_state[model_key]["open_until"])


def _breaker_mark_success(model_key: str):
    _breaker_state[model_key]["fails"] = 0
    _breaker_state[model_key]["open_until"] = 0.0


def _breaker_mark_failure(model_key: str):
    fails = int(_breaker_state[model_key]["fails"]) + 1
    _breaker_state[model_key]["fails"] = fails
    threshold = int(_runtime["breaker_fail_threshold"])
    if fails >= threshold:
        cooldown = float(_runtime["breaker_cooldown_seconds"])
        _breaker_state[model_key]["open_until"] = time.monotonic() + cooldown
        _breaker_state[model_key]["fails"] = 0
        logger.warning(
            f"LLM breaker opened for {model_key}: cooldown={cooldown:.1f}s threshold={threshold}"
        )


def _cache_key(prompt: str, system_prompt: str, json_mode: bool, model_type: str) -> str:
    payload = f"{model_type}|{int(bool(json_mode))}|{system_prompt}|{prompt}"
    return hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()


def _to_usage_dict(usage_obj):
    if usage_obj is None:
        return {}
    if isinstance(usage_obj, dict):
        return usage_obj
    data = {}
    for k in ("prompt_cache_hit_tokens", "prompt_cache_miss_tokens", "prompt_tokens", "completion_tokens", "total_tokens"):
        try:
            v = getattr(usage_obj, k, None)
            if v is not None:
                data[k] = v
        except Exception:
            pass
    # Some providers may nest in prompt_tokens_details.
    try:
        ptd = getattr(usage_obj, "prompt_tokens_details", None)
        if ptd is not None:
            if isinstance(ptd, dict):
                data.update({
                    "prompt_cache_hit_tokens": data.get("prompt_cache_hit_tokens", ptd.get("prompt_cache_hit_tokens")),
                    "prompt_cache_miss_tokens": data.get("prompt_cache_miss_tokens", ptd.get("prompt_cache_miss_tokens")),
                })
            else:
                data.update({
                    "prompt_cache_hit_tokens": data.get("prompt_cache_hit_tokens", getattr(ptd, "prompt_cache_hit_tokens", None)),
                    "prompt_cache_miss_tokens": data.get("prompt_cache_miss_tokens", getattr(ptd, "prompt_cache_miss_tokens", None)),
                })
    except Exception:
        pass
    return data


def _record_cache_usage(model_key: str, usage_obj):
    ud = _to_usage_dict(usage_obj)
    try:
        hit = int(ud.get("prompt_cache_hit_tokens") or 0)
    except Exception:
        hit = 0
    try:
        miss = int(ud.get("prompt_cache_miss_tokens") or 0)
    except Exception:
        miss = 0
    if hit <= 0 and miss <= 0:
        return
    bucket = _cache_usage_stats.get(model_key)
    if not bucket:
        return
    bucket["calls"] = int(bucket.get("calls", 0)) + 1
    bucket["hit_tokens"] = int(bucket.get("hit_tokens", 0)) + hit
    bucket["miss_tokens"] = int(bucket.get("miss_tokens", 0)) + miss
    total = bucket["hit_tokens"] + bucket["miss_tokens"]
    hit_ratio = (bucket["hit_tokens"] / total) if total > 0 else 0.0
    # Keep logs light: first 3 calls, then every 20 calls.
    if bucket["calls"] <= 3 or bucket["calls"] % 20 == 0:
        logger.info(
            "DeepSeekContextCache model=%s calls=%s hit_tokens=%s miss_tokens=%s hit_ratio=%.2f%%",
            model_key,
            bucket["calls"],
            bucket["hit_tokens"],
            bucket["miss_tokens"],
            hit_ratio * 100.0,
        )


def get_cache_usage_stats() -> dict:
    return {
        "smart": dict(_cache_usage_stats.get("smart", {})),
        "fast": dict(_cache_usage_stats.get("fast", {})),
    }


def _cache_get_sync(key: str):
    if not _runtime.get("enable_cache", True):
        return None
    with _sync_cache_lock:
        return _response_cache.get(key)


def _cache_set_sync(key: str, value: str):
    if not _runtime.get("enable_cache", True):
        return
    with _sync_cache_lock:
        _response_cache[key] = value
        max_size = int(_runtime.get("cache_max_size", 2000))
        if len(_response_cache) > max_size:
            # Drop oldest inserted key (dict preserves insertion order in py3.7+)
            first_key = next(iter(_response_cache))
            _response_cache.pop(first_key, None)


async def _cache_get_async(key: str):
    if not _runtime.get("enable_cache", True):
        return None
    async with _async_cache_lock():
        return _response_cache.get(key)


async def _cache_set_async(key: str, value: str):
    if not _runtime.get("enable_cache", True):
        return
    async with _async_cache_lock():
        _response_cache[key] = value
        max_size = int(_runtime.get("cache_max_size", 2000))
        if len(_response_cache) > max_size:
            first_key = next(iter(_response_cache))
            _response_cache.pop(first_key, None)


def _model_key(model_type: str) -> str:
    return "fast" if str(model_type).lower() == "fast" else "smart"


def reset_llm_runtime_state(reset_breaker: bool = True) -> None:
    """
    Reset in-memory LLM runtime state to avoid cross-run contamination.
    Safe for research batch loops where multiple runs share one Python process.
    """
    global _next_allowed_sync, _next_allowed_async

    with _sync_cache_lock:
        _response_cache.clear()
    for mk in ("smart", "fast"):
        if mk in _cache_usage_stats:
            _cache_usage_stats[mk]["calls"] = 0
            _cache_usage_stats[mk]["hit_tokens"] = 0
            _cache_usage_stats[mk]["miss_tokens"] = 0

    _next_allowed_sync = {"smart": 0.0, "fast": 0.0}
    _next_allowed_async = {"smart": 0.0, "fast": 0.0}

    if reset_breaker:
        _breaker_state["smart"]["fails"] = 0
        _breaker_state["smart"]["open_until"] = 0.0
        _breaker_state["fast"]["fails"] = 0
        _breaker_state["fast"]["open_until"] = 0.0


def _qps_for(model_key: str) -> float:
    return float(_runtime["qps_fast"] if model_key == "fast" else _runtime["qps_smart"])


def _sync_rate_limit(model_key: str):
    qps = _qps_for(model_key)
    if qps <= 0:
        return
    interval = 1.0 / qps
    with _sync_locks[model_key]:
        now = time.monotonic()
        wait = _next_allowed_sync[model_key] - now
        if wait > 0:
            time.sleep(wait)
            now = time.monotonic()
        _next_allowed_sync[model_key] = now + interval


async def _async_rate_limit(model_key: str):
    qps = _qps_for(model_key)
    if qps <= 0:
        return
    interval = 1.0 / qps
    async with _async_rate_lock_for(model_key):
        now = time.monotonic()
        wait = _next_allowed_async[model_key] - now
        if wait > 0:
            await asyncio.sleep(wait)
            now = time.monotonic()
        _next_allowed_async[model_key] = now + interval


def _ensure_clients_initialized():
    global client_smart, aclient_smart, client_fast, aclient_fast
    if client_smart is not None:
        return
    _load_runtime_controls()

    # Smart Clients
    client_smart = OpenAI(api_key=SMART_API_KEY, base_url=SMART_BASE_URL, timeout=_runtime["timeout_seconds"])
    aclient_smart = AsyncOpenAI(api_key=SMART_API_KEY, base_url=SMART_BASE_URL, timeout=_runtime["timeout_seconds"])

    # Fast Clients
    if FAST_API_KEY == SMART_API_KEY and FAST_BASE_URL == SMART_BASE_URL:
        client_fast = client_smart
        aclient_fast = aclient_smart
    else:
        client_fast = OpenAI(api_key=FAST_API_KEY, base_url=FAST_BASE_URL, timeout=_runtime["timeout_seconds"])
        aclient_fast = AsyncOpenAI(api_key=FAST_API_KEY, base_url=FAST_BASE_URL, timeout=_runtime["timeout_seconds"])


def get_client(model_type: str, is_async: bool = False):
    """Select appropriate client based on model type."""
    _ensure_clients_initialized()
    if model_type.lower() == "fast":
        return aclient_fast if is_async else client_fast
    return aclient_smart if is_async else client_smart


def get_model_id(model_type: str) -> str:
    """Select model ID based on type."""
    if model_type.lower() == "fast":
        return MODEL_FAST
    return MODEL_SMART


def call_llm(prompt: str, system_prompt: str = "You are a helpful assistant in a real estate simulation.", json_mode: bool = False, model_type: str = "smart") -> str:
    """
    Call LLM via OpenAI SDK (Supports Dual Providers).
    model_type: 'smart' (default) or 'fast'
    """
    _load_runtime_controls()
    current_client = get_client(model_type, is_async=False)
    key = _model_key(model_type)
    kwargs = {
        "model": get_model_id(model_type),
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "temperature": 0.7
    }
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}
    cache_key = _cache_key(prompt, system_prompt, json_mode, model_type)
    cached = _cache_get_sync(cache_key)
    if cached is not None:
        return cached

    retries = int(_runtime["max_retries"])
    for attempt in range(retries + 1):
        if _breaker_is_open(key):
            logger.warning(f"LLM breaker open ({model_type}), return default error.")
            return "Error: circuit_open"
        try:
            _sync_rate_limit(key)
            response = current_client.chat.completions.create(**kwargs)
            _breaker_mark_success(key)
            _record_cache_usage(key, getattr(response, "usage", None))
            content = response.choices[0].message.content.strip()
            _cache_set_sync(cache_key, content)
            return content
        except Exception as e:
            _breaker_mark_failure(key)
            if attempt >= retries:
                logger.error(f"LLM Call Failed ({model_type}): {e}")
                return f"Error: {str(e)}"
            delay = float(_runtime["backoff_base_seconds"]) * (2 ** attempt)
            logger.info(f"Retrying sync LLM ({model_type}) in {delay:.3f}s")
            time.sleep(delay)


def safe_call_llm(prompt: str, default_return: dict, system_prompt: str = "", model_type: str = "smart") -> dict:
    """
    Call LLM and parse JSON response. Returns default if failure.
    """
    if LLM_MOCK_MODE:
        logger.info(f"LLM mock mode enabled for sync call ({model_type}).")
        return default_return

    json_prompt = prompt + "\n\n请只输出JSON格式，不要包含Markdown代码块或其他文本。"

    response_text = call_llm(json_prompt, system_prompt, json_mode=True, model_type=model_type)

    clean_text = response_text.replace("```json", "").replace("```", "").strip()

    try:
        return json.loads(clean_text)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse JSON. Response: {response_text}")
        try:
            start = clean_text.find('{')
            end = clean_text.rfind('}')
            if start != -1 and end != -1:
                return json.loads(clean_text[start:end + 1])
        except BaseException:
            pass
        return default_return


async def call_llm_async(prompt: str, system_prompt: str = "You are a helpful assistant in a real estate simulation.", json_mode: bool = False, model_type: str = "smart") -> str:
    """
    Async Call LLM via OpenAI SDK (Supports Dual Providers).
    """
    _load_runtime_controls()
    current_client = get_client(model_type, is_async=True)
    key = _model_key(model_type)
    kwargs = {
        "model": get_model_id(model_type),
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "temperature": 0.7
    }
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}
    cache_key = _cache_key(prompt, system_prompt, json_mode, model_type)
    cached = await _cache_get_async(cache_key)
    if cached is not None:
        return cached

    sem = _async_semaphore_for(key)
    retries = int(_runtime["max_retries"])
    for attempt in range(retries + 1):
        if _breaker_is_open(key):
            logger.warning(f"Async LLM breaker open ({model_type}), return default error.")
            return "Error: circuit_open"
        try:
            async with sem:
                await _async_rate_limit(key)
                response = await current_client.chat.completions.create(**kwargs)
            _breaker_mark_success(key)
            _record_cache_usage(key, getattr(response, "usage", None))
            content = response.choices[0].message.content.strip()
            await _cache_set_async(cache_key, content)
            return content
        except Exception as e:
            _breaker_mark_failure(key)
            if attempt >= retries:
                logger.error(f"Async LLM Call Failed ({model_type}): {e}")
                return f"Error: {str(e)}"
            delay = float(_runtime["backoff_base_seconds"]) * (2 ** attempt)
            logger.info(f"Retrying async LLM ({model_type}) in {delay:.3f}s")
            await asyncio.sleep(delay)


async def safe_call_llm_async(prompt: str, default_return: dict, system_prompt: str = "", model_type: str = "smart") -> dict:
    """
    Async wrapper for safe JSON LLM calls.
    """
    if LLM_MOCK_MODE:
        logger.info(f"LLM mock mode enabled for async call ({model_type}).")
        return default_return

    json_prompt = prompt + "\n\n请只输出JSON格式，不要包含Markdown代码块或其他文本。"

    response_text = await call_llm_async(json_prompt, system_prompt, json_mode=True, model_type=model_type)

    clean_text = response_text.replace("```json", "").replace("```", "").strip()

    try:
        return json.loads(clean_text)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse JSON (Async). Response: {response_text}")
        try:
            start = clean_text.find('{')
            end = clean_text.rfind('}')
            if start != -1 and end != -1:
                return json.loads(clean_text[start:end + 1])
        except BaseException:
            pass
        return default_return
