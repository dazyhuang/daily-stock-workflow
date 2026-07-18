"""
统一 LLM Provider 抽象层
==========================
替代 debate_engine.py / llm_scorer.py / rescore.py 里各自独立的 call_llm 实现。

支持两种 API 格式：
- anthropic-messages (MiniMax M3)
- openai-completions / openai-chat (volcengine ark-code)
- openai-codex-responses (GPT-5.6 Sol)

用法：
    from providers import call_llm, call_llm_structured

    # 简单调用
    text = call_llm("prompt", model="openai/gpt-5.6-sol")

    # Structured output（返回 Pydantic 模型）
    result = call_llm_structured("prompt", model="openai/gpt-5.6-sol",
                                 schema=MySchema)
"""

import os
import json
import time
import logging
import threading
import re
from pathlib import Path

# Volcengine 熔断：主模型重试耗尽后当日全局熔断，后续所有请求直接切 MiniMax
# （不能用 threading.local() — ThreadPoolExecutor 每线程独立，状态不共享）
_volcan_circuit_broken: bool = False
_volcan_circuit_day: str = ""
_volcan_circuit_lock = __import__("threading").Lock()
_volcan_request_lock = threading.Lock()
_volcan_last_request_at = 0.0
from typing import Optional, Type, TypeVar, Any
import urllib.request
import urllib.error
import socket
from pydantic import BaseModel, Field, field_validator
from model_router import resolve_model_route

logger = logging.getLogger("daily_stock_workflow.providers")

_ENV_LOADED = False
_CONFIG_LOAD_LOCK = threading.RLock()


def _load_project_env() -> None:
    """Load local .env once so standalone debate/provider runs see model keys."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    with _CONFIG_LOAD_LOCK:
        if _ENV_LOADED:
            return
        env_files = [
            Path.home() / ".openclaw" / ".env",
            Path(__file__).resolve().parents[1] / ".env",
        ]
        for env_file in env_files:
            if not env_file.exists():
                continue
            try:
                for line in env_file.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key, value.strip().strip("\"'"))
            except Exception as exc:
                logger.warning(f"[providers] .env 读取失败 {env_file}: {exc}")
        os.environ.setdefault("MINIMAX_ALLOW_MX_DIRECT_KEY", "1")
        _ENV_LOADED = True


def _today_key() -> str:
    return time.strftime("%Y-%m-%d")


def _is_volcan_circuit_open() -> bool:
    """当日火山重试耗尽后直接跳过主模型；跨自然日自动恢复一次探测机会。"""
    global _volcan_circuit_broken, _volcan_circuit_day
    with _volcan_circuit_lock:
        if not _volcan_circuit_broken:
            return False
        if _volcan_circuit_day != _today_key():
            _volcan_circuit_broken = False
            _volcan_circuit_day = ""
            return False
        return True


def _trip_volcan_circuit() -> None:
    global _volcan_circuit_broken, _volcan_circuit_day
    with _volcan_circuit_lock:
        _volcan_circuit_broken = True
        _volcan_circuit_day = _today_key()


def _wait_for_volcan_request_slot() -> None:
    """Space request starts without reducing the three-stock debate parallelism."""
    global _volcan_last_request_at
    try:
        interval = max(0.0, float(os.getenv("VOLCAN_REQUEST_INTERVAL_SEC", "1.0")))
    except (TypeError, ValueError):
        interval = 1.0
    if interval <= 0:
        return
    with _volcan_request_lock:
        now = time.monotonic()
        wait = interval - (now - _volcan_last_request_at)
        if wait > 0:
            time.sleep(wait)
        _volcan_last_request_at = time.monotonic()

# ── 全局配置（延迟加载）────────────────────────────────────
_PROVIDER_MAP: dict = {}
_MODEL_PROVIDER_MAP: dict = {
    # volcengine
    "ark-code-latest": "volcengine-plan",
    "ark-code": "volcengine-plan",
    # minimax
    "MiniMax-M3": "minimax-portal",
    "MiniMax-M2": "minimax-portal",
    # Codex/OpenAI-compatible bundled models
    "gpt-5.6-sol": "openai-codex",
    "gpt-5.4-mini": "openai-codex",
    "gpt-5.2": "openai-codex",
}

T = TypeVar("T", bound=object)


LLM_ERROR_AUTH = "AUTH_ERROR"
LLM_ERROR_RATE_LIMIT = "RATE_LIMIT"
LLM_ERROR_TRANSIENT_NETWORK = "TRANSIENT_NETWORK"
LLM_ERROR_EMPTY_OUTPUT = "EMPTY_OUTPUT"
LLM_ERROR_PARSE = "PARSE_ERROR"
LLM_ERROR_UNKNOWN = "UNKNOWN"


LLM_RETRY_POLICY_VERSION = "2026-07-09.node-retry-v1"


def effective_llm_retries(node_name: str = "default", default: int = 3) -> int:
    """Node-level retry budget, inspired by TradingAgents llm_max_retries."""
    aliases = [
        f"LLM_MAX_RETRIES_{str(node_name or 'default').upper().replace('-', '_')}",
        "LLM_MAX_RETRIES",
    ]
    for key in aliases:
        raw = os.getenv(key)
        if raw not in (None, ""):
            try:
                return max(1, int(raw))
            except Exception:
                pass
    return max(1, int(default or 1))


def classify_llm_error(exc: Exception | None) -> str:
    """Classify model failures so callers can distinguish global vs per-stock fallback."""
    if exc is None:
        return LLM_ERROR_UNKNOWN
    if isinstance(exc, urllib.error.HTTPError):
        if exc.code in (401, 403):
            return LLM_ERROR_AUTH
        if exc.code == 429:
            return LLM_ERROR_RATE_LIMIT
        if 500 <= exc.code < 600:
            return LLM_ERROR_TRANSIENT_NETWORK
    if isinstance(exc, (urllib.error.URLError, TimeoutError, socket.timeout)):
        return LLM_ERROR_TRANSIENT_NETWORK
    text = str(exc).lower()
    if any(token in text for token in (
        "401", "unauthorized", "invalid api key", "api key", "apikey",
        "未配置", "认证", "权限",
    )):
        return LLM_ERROR_AUTH
    if any(token in text for token in ("429", "rate limit", "quota", "额度", "限流", "too many requests")):
        return LLM_ERROR_RATE_LIMIT
    if any(token in text for token in (
        "ssl", "unexpected_eof", "eof occurred", "timed out", "timeout",
        "connection reset", "connection aborted", "remote end closed",
        "temporarily unavailable", "read operation timed out",
    )):
        return LLM_ERROR_TRANSIENT_NETWORK
    if any(token in text for token in ("empty", "空结果", "no output", "未找到 output_text")):
        return LLM_ERROR_EMPTY_OUTPUT
    if any(token in text for token in ("json", "parse", "schema", "解析")):
        return LLM_ERROR_PARSE
    return LLM_ERROR_UNKNOWN


def is_global_model_failure(exc: Exception | None) -> bool:
    """Only hard auth/quota/rate-limit failures should trip a workflow-wide circuit."""
    return classify_llm_error(exc) in {LLM_ERROR_AUTH, LLM_ERROR_RATE_LIMIT}


def _iter_json_values(text: str):
    """Yield complete JSON values found in text, preferring fenced blocks first."""
    if not text:
        return

    sources = []
    for match in re.finditer(r"```(?:json)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL):
        block = match.group(1).strip()
        if block:
            sources.append(block)
    sources.append(text.strip())

    decoder = json.JSONDecoder()
    seen = set()
    for source in sources:
        for idx, ch in enumerate(source):
            if ch not in "{[":
                continue
            key = (id(source), idx)
            if key in seen:
                continue
            seen.add(key)
            try:
                value, _ = decoder.raw_decode(source[idx:])
            except json.JSONDecodeError:
                continue
            yield value


def _schema_field_names(schema: Type[BaseModel]) -> set[str]:
    fields = getattr(schema, "model_fields", None) or getattr(schema, "__fields__", {})
    return set(fields.keys())


def _split_model_ref(model: str) -> tuple[str, str]:
    """Return (provider_name, model_name) for provider/model or alias refs."""
    if "/" in model:
        provider_name, model_name = model.split("/", 1)
    else:
        provider_name = _MODEL_PROVIDER_MAP.get(model, "")
        model_name = model
    if provider_name in {"openai", "codex"}:
        provider_name = "openai-codex"
    return provider_name, model_name


def _reasoning_effort_for_model(model: str, requested: str = "high") -> str:
    """GPT-5.6 Sol always runs at the user-selected maximum reasoning level."""
    _provider_name, model_name = _split_model_ref(str(model or ""))
    if model_name.lower() == "gpt-5.6-sol":
        return "max"
    return str(requested or "high")


def _schema_for_json_schema(schema: Type[BaseModel]) -> dict:
    """Build a strict JSON schema for OpenAI-style structured outputs."""
    if hasattr(schema, "model_json_schema"):
        json_schema = schema.model_json_schema()
    else:
        json_schema = schema.schema()

    def _strict_object(node: Any) -> None:
        if not isinstance(node, dict):
            return
        if node.get("type") == "object" and isinstance(node.get("properties"), dict):
            node["additionalProperties"] = False
            # Responses strict json_schema requires every property to be listed
            # as required; nullable fields stay optional semantically via null.
            node["required"] = list(node["properties"].keys())
        for value in node.values():
            if isinstance(value, dict):
                _strict_object(value)
            elif isinstance(value, list):
                for item in value:
                    _strict_object(item)

    _strict_object(json_schema)
    return json_schema


def _normalize_position_ratio(value: Any, has_percent: bool = False) -> Optional[float]:
    """Normalize LLM position ratios to 0.0-1.0."""
    try:
        ratio = float(value)
    except (TypeError, ValueError):
        return None
    if has_percent or ratio > 1:
        ratio = ratio / 100
    return max(0.0, min(1.0, ratio))


def extract_json_object(
    text: str,
    required_keys: Optional[set[str]] = None,
    wrapper_key: Optional[str] = None,
) -> Optional[dict]:
    """
    Extract the best complete JSON object from noisy LLM text.

    This avoids the common rfind("{") bug where an inner object fragment is
    treated as the whole response.
    """
    required_keys = required_keys or set()
    fallback = None

    for value in _iter_json_values(text):
        if not isinstance(value, dict):
            continue
        fallback = value

        if wrapper_key and isinstance(value.get(wrapper_key), dict):
            inner = value[wrapper_key]
            if not required_keys or required_keys.issubset(inner.keys()):
                return inner

        if not required_keys or required_keys.issubset(value.keys()):
            return value

    # Fallback: 尝试从文本中提取关键字段（处理 LLM 输出碎片化的情况）
    if required_keys:
        result = {}
        # 找 signal 字段（支持 field_name: value、field_name=value、带/不带引号）
        m_sig = re.search(r'(?:["\'])?signal(?:["\'])?\s*[:=：]\s*["\']?([A-Z]+)["\']?', text, re.IGNORECASE)
        if m_sig:
            result["signal"] = m_sig.group(1).upper()
        # 找 confidence 字段
        m_conf = re.search(r'(?:["\'])?confidence(?:["\'])?\s*[:=：]\s*["\']?(\d+)', text, re.IGNORECASE)
        if m_conf:
            result["confidence"] = int(m_conf.group(1))
        # 找 position_ratio 字段；25% / 25 都归一化为 0.25
        m_ratio = re.search(r'(?:["\'])?position_ratio(?:["\'])?\s*[:=：]\s*["\']?([0-9.]+)\s*(%)?', text, re.IGNORECASE)
        if m_ratio:
            ratio = _normalize_position_ratio(m_ratio.group(1), has_percent=bool(m_ratio.group(2)))
            if ratio is not None:
                result["position_ratio"] = ratio
        # 找 reason 字段（支持引号内内容，也支持 reason=... 到行尾）
        m_reason = re.search(
            r'(?:["\'])?reason(?:["\'])?\s*[:=：]\s*(?:"([^"]+)"|\'([^\']+)\'|([^\n\r]+))',
            text,
            re.IGNORECASE,
        )
        if m_reason:
            reason = next((g for g in m_reason.groups() if g), "").strip()
            reason = re.sub(r'\s*[,，}]+$', '', reason).strip()
            if len(reason) >= 2:
                result["reason"] = reason
        if result and all(k in result for k in required_keys):
            logger.info(f"[extract_json_object] regex fallback 成功: {list(result.keys())}")
            return result

        # 盘中买入等执行类节点常输出字段行，而不是完整 JSON。这里把字段行修复成结构化对象。
        if "action" in required_keys:
            action_match = re.search(
                r'(?:["\'])?action(?:["\'])?\s*[:=：]\s*["\']?([A-Z_]+|[\u4e00-\u9fff]{1,12})["\']?',
                text,
                re.IGNORECASE,
            )
            if action_match:
                action_raw = action_match.group(1).strip()
                action_alias = {
                    "买入": "BUY_NOW",
                    "立即买入": "BUY_NOW",
                    "直接买入": "BUY_NOW",
                    "追入": "BUY_NOW",
                    "等待": "WAIT",
                    "观望": "WAIT",
                    "继续观察": "WAIT",
                    "跳过": "SKIP_TODAY",
                    "今日跳过": "SKIP_TODAY",
                    "保留挂单": "KEEP_ORDER",
                    "撤单等待": "CANCEL_WAIT",
                    "撤单重报": "CANCEL_REBUY",
                    "撤单追高": "CANCEL_REBUY",
                    "撤单跳过": "CANCEL_SKIP_TODAY",
                }
                result = {"action": action_alias.get(action_raw, action_raw.upper())}
                price_mode_match = re.search(
                    r'(?:["\'])?price_mode(?:["\'])?\s*[:=：]\s*["\']?([A-Z_]+|[\u4e00-\u9fff]{1,12})["\']?',
                    text,
                    re.IGNORECASE,
                )
                price_mode_raw = price_mode_match.group(1).strip() if price_mode_match else "NONE"
                price_mode_alias = {"无": "NONE", "跟随": "FOLLOW", "追价": "FOLLOW", "低吸": "DIP", "被动": "PASSIVE"}
                result["price_mode"] = price_mode_alias.get(price_mode_raw, price_mode_raw.upper())
                limit_match = re.search(
                    r'(?:["\'])?limit_price(?:["\'])?\s*[:=：]\s*["\']?(null|none|无|[-+]?[0-9]*\.?[0-9]+)',
                    text,
                    re.IGNORECASE,
                )
                result["limit_price"] = None
                if limit_match and limit_match.group(1).lower() not in {"null", "none", "无"}:
                    result["limit_price"] = float(limit_match.group(1))
                premium_match = re.search(
                    r'(?:["\'])?max_premium_pct(?:["\'])?\s*[:=：]\s*["\']?([-+]?[0-9]*\.?[0-9]+)',
                    text,
                    re.IGNORECASE,
                )
                result["max_premium_pct"] = float(premium_match.group(1)) if premium_match else 0.0
                conf_match = re.search(r'(?:["\'])?confidence(?:["\'])?\s*[:=：]\s*["\']?(\d+)', text, re.IGNORECASE)
                result["confidence"] = int(conf_match.group(1)) if conf_match else 0
                reason_match = re.search(
                    r'(?:["\'])?reason(?:["\'])?\s*[:=：]\s*(?:"([^"]+)"|\'([^\']+)\'|([^\n\r]+))',
                    text,
                    re.IGNORECASE,
                )
                reason = ""
                if reason_match:
                    reason = next((g for g in reason_match.groups() if g), "").strip()
                    reason = re.sub(r'\s*[,，}]+$', '', reason).strip()
                result["reason"] = reason
                if all(k in result for k in required_keys):
                    logger.info(f"[extract_json_object] action regex fallback 成功: {list(result.keys())}")
                    return result

    return None if required_keys else fallback


def _thinking_payload(max_tokens: int, budget_tokens: Optional[int], provider: str = "") -> Optional[dict]:
    """Build a thinking payload that stays below the response token cap.
    MiniMax portal 走 OpenClaw CLI 的 adaptive thinking；原生直连仍保持保守。
    """
    if not budget_tokens or max_tokens <= 1024:
        return None
    safe_budget = min(int(budget_tokens), max_tokens - 512)
    if safe_budget < 1024:
        return None
    return {"type": "enabled", "budget_tokens": safe_budget}


def _load_models_config() -> None:
    """从 OpenClaw 配置加载 provider 信息（models.json + auth-profiles.json）"""
    global _PROVIDER_MAP
    _load_project_env()
    if _PROVIDER_MAP:
        return
    with _CONFIG_LOAD_LOCK:
        if _PROVIDER_MAP:
            return
        loaded: dict = {}
        try:
            cfg_path = Path.home() / ".openclaw" / "agents" / "main" / "agent" / "models.json"
            with open(cfg_path) as f:
                models = json.load(f)
            for provider, info in models.get("providers", {}).items():
                api = info.get("api", "")
                base_url = info.get("baseUrl", "") or info.get("base_url", "")
                api_key = info.get("apiKey", "") or info.get("key", "") or ""
                if base_url and api:
                    loaded[provider] = {
                        "api": api,
                        "baseUrl": base_url.rstrip("/"),
                        "apiKey": api_key,
                    }
        except Exception as e:
            logger.warning(f"[providers] models.json 读取失败: {e}")

        legacy_defaults = {
            "volcengine-plan": {
                "api": "openai-completions",
                "baseUrl": "https://ark.cn-beijing.volces.com/api/coding/v3",
                "apiKey": "",
            },
            "minimax-portal": {
                "api": "anthropic-messages",
                "baseUrl": "https://api.minimaxi.com/anthropic/v1",
                "apiKey": "",
            },
        }
        for provider, cfg in legacy_defaults.items():
            loaded.setdefault(provider, cfg)
        _PROVIDER_MAP = loaded


def _get_api_key(provider_name: str) -> str:
    """获取指定 provider 的实际 API key，优先从 auth-profiles.json 读取"""
    _load_project_env()
    _load_models_config()
    cfg = _PROVIDER_MAP.get(provider_name, {})
    api_key = cfg.get("apiKey", "")
    if api_key and re.fullmatch(r"[A-Z][A-Z0-9_]{5,}", api_key):
        api_key = os.environ.get(api_key, "")

    # 掩码检测：models.json 的 key 可能是掩码，尝试从 auth-profiles.json 还原
    if not api_key or api_key.startswith("VOLCANO") or api_key.startswith("***") or len(api_key) < 20:
        try:
            profile_path = Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth-profiles.json"
            with open(profile_path) as pf:
                profiles = json.load(pf)
            if provider_name == "volcengine-plan":
                profile_keys = (f"{provider_name}:default", "volcengine:default")
            elif provider_name == "minimax-portal":
                profile_keys = (f"{provider_name}:default", "minimax:default", "minimax-cn:default")
            elif provider_name in {"openai-codex", "codex", "openai"}:
                profile_keys = (
                    "openai-codex:default",
                    "openai:default",
                    "codex:default",
                )
            else:
                profile_keys = (f"{provider_name}:default",)
            for pk in profile_keys:
                entry = profiles.get("profiles", {}).get(pk, {})
                key = entry.get("key") or entry.get("access", "")
                if key and len(key) > 20:
                    return key
        except Exception:
            pass

    # 环境变量 fallback：按 provider 区分，避免火山引擎误拿 MiniMax/MX key
    if not api_key:
        if provider_name == "volcengine-plan":
            api_key = (
                os.environ.get("VOLCAN_API_KEY")
                or os.environ.get("VOLCANO_API_KEY")
                or os.environ.get("VOLCANO_ENGINE_API_KEY")
                or os.environ.get("VOLCAN_ENGINE_API_KEY", "")
            )
        elif provider_name == "minimax-portal":
            api_key = os.environ.get("MINIMAX_API_KEY") or os.environ.get("MINIMAX_NATIVE_KEY", "")
            if not api_key and os.environ.get("MINIMAX_ALLOW_MX_DIRECT_KEY") == "1":
                api_key = os.environ.get("MX_DIRECT_KEY", "")
        elif provider_name in {"openai-codex", "codex", "openai"}:
            api_key = os.environ.get("OPENAI_API_KEY", "")
    return api_key


def _call_minimax_portal_cli(
    prompt: str,
    system: str = "",
    timeout: int = 120,
    max_tokens: int = 12000,
    thinking: str = "",
) -> str:
    """Use OpenClaw's MiniMax portal OAuth path."""
    import subprocess
    import uuid

    message = f"<system>{system}</system>\n{prompt}" if system else prompt
    cmd = [
        "openclaw",
        "agent",
        "--local",
        "--session-id",
        f"minimax-portal-{uuid.uuid4().hex[:10]}",
        "--model",
        "minimax-portal/MiniMax-M3",
        "--timeout",
        str(max(30, int(timeout))),
    ]
    if thinking:
        cmd.extend(["--thinking", thinking])
    cmd.extend(["--message", message])
    env = os.environ.copy()
    env.setdefault("OPENCLAW_WORKSPACE", ".")
    proc = subprocess.run(
        cmd,
        cwd=str(Path(__file__).resolve().parents[1]),
        env=env,
        capture_output=True,
        text=True,
        timeout=max(45, int(timeout) + 30),
    )
    output = (proc.stdout or "").strip()
    if proc.returncode != 0:
        err = (proc.stderr or output or "").strip()
        raise RuntimeError(f"MiniMax portal CLI failed: {err[:500]}")
    lines = [
        line
        for line in output.splitlines()
        if line.strip() and not line.lstrip().startswith("[")
    ]
    return "\n".join(lines).strip() or output


# ── 核心调用函数 ────────────────────────────────────────────

def call_llm(
    prompt: str,
    system: str = "",
    model: str = "openai/gpt-5.6-sol",
    timeout: int = 120,
    retries: int = 3,
    max_tokens: int = 12000,
    thinking_budget: Optional[int] = None,
    temperature: float = 0.3,
    return_thinking: bool = False,
) -> str:
    """
    统一 LLM 调用，支持多 provider 自动路由。

    Args:
        prompt: 用户 prompt
        system: 系统提示（可选）
        model: 模型名，格式 "provider/model" 或 "model"
        timeout: 超时秒数
        retries: 重试次数
        max_tokens: 最大输出 token 数
        thinking_budget: thinking 预算；None=自动（按 TA_THINKING_BUDGET_* 配置）
        temperature: 温度
    """
    global _volcan_circuit_broken
    _load_models_config()

    # 解析 provider 和 model
    provider_name, model_name = _split_model_ref(model)

    if not provider_name or provider_name not in _PROVIDER_MAP:
        raise ValueError(f"未找到模型 {model} 的 provider（可用: {list(_PROVIDER_MAP.keys())})")

    cfg = _PROVIDER_MAP[provider_name]
    base_url = cfg["baseUrl"]
    api_type = cfg["api"]
    last_err = None

    for attempt in range(retries):
        try:
            # 熔断检查：volcengine 当日已熔断后，全局后续请求直接切 MiniMax
            if provider_name == "volcengine-plan" and _is_volcan_circuit_open():
                raise urllib.error.HTTPError(None, 429, "Circuit Open", {}, None)
            if provider_name == "volcengine-plan":
                _wait_for_volcan_request_slot()

            if provider_name == "openai-codex" and api_type == "openai-codex-responses":
                api_key = _get_api_key(provider_name)
                if not api_key:
                    raise RuntimeError(f"{provider_name} API key 未配置")
                base = base_url.rstrip("/")
                if re.match(r"^https://chatgpt\.com/backend-api/?$", base):
                    base = "https://chatgpt.com/backend-api/codex"
                url = f"{base}/responses"
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                }
                body = {
                    "model": model_name,
                    "stream": True,
                    "store": False,
                    "instructions": system or "You are a precise financial analysis assistant.",
                    "input": [{"role": "user", "content": prompt}],
                    "reasoning": {
                        "context": "current_turn",
                        "effort": _reasoning_effort_for_model(model, "high"),
                    },
                }
                req = urllib.request.Request(
                    url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                    headers=headers, method="POST",
                )
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    raw = resp.read().decode("utf-8")

                text = ""
                reasoning_text = ""
                current_event = None
                for line in raw.splitlines():
                    if line.startswith("event:"):
                        current_event = line[6:].strip()
                        continue
                    if not line.startswith("data:"):
                        continue
                    try:
                        data = json.loads(line[5:].strip())
                    except Exception:
                        continue
                    if current_event in ("response.output_text.delta", "response.output_text.done"):
                        text += data.get("text", "") or data.get("delta", "") or ""
                    elif current_event in ("response.reasoning_text.delta", "response.reasoning_text.done"):
                        reasoning_text += data.get("text", "") or data.get("delta", "") or ""
                    elif isinstance(data, dict):
                        if data.get("type") in ("response.output_text.delta", "response.output_text.done"):
                            text += data.get("text", "") or data.get("delta", "") or ""
                        elif data.get("type") in ("response.reasoning_text.delta", "response.reasoning_text.done"):
                            reasoning_text += data.get("text", "") or data.get("delta", "") or ""
                if return_thinking:
                    return (reasoning_text.strip(), text.strip() or reasoning_text.strip())
                if text.strip():
                    return text.strip()
                if reasoning_text.strip():
                    return reasoning_text.strip()

            elif provider_name == "minimax-portal":
                text = _call_minimax_portal_cli(
                    prompt=prompt,
                    system=system,
                    timeout=timeout,
                    max_tokens=max_tokens,
                    thinking="adaptive" if thinking_budget else "",
                )
                if return_thinking:
                    return ("", text)
                if text:
                    return text

            elif api_type == "anthropic-messages":
                api_key = _get_api_key(provider_name)
                if not api_key:
                    raise RuntimeError(f"{provider_name} API key 未配置")
                url = f"{base_url}/messages"
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "anthropic-version": "2023-06-01",
                }
                messages = []
                if system:
                    messages.append({"role": "user", "content": f"<system>{system}</system>\n{prompt}"})
                else:
                    messages.append({"role": "user", "content": prompt})

                body = {
                    "model": model_name,
                    "messages": messages,
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                }
                budget = thinking_budget
                if budget is None:
                    budget = THINKING_BUDGET_VOLCAN if provider_name == "volcengine-plan" else THINKING_BUDGET_MINIMAX
                thinking = _thinking_payload(max_tokens, budget, provider_name)
                if thinking:
                    body["thinking"] = thinking

                req = urllib.request.Request(
                    url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                    headers=headers, method="POST",
                )
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    raw = resp.read().decode("utf-8")
                result = json.loads(raw)
                content_blocks = result.get("content", [])
                if return_thinking:
                    thinking_text = "\n".join(
                        b.get("thinking", "").strip() or b.get("text", "").strip()
                        for b in content_blocks
                        if b.get("type") == "thinking" and b.get("thinking")
                    ).strip()
                    text = next((b.get("text", "").strip() for b in content_blocks if b.get("type") == "text" and b.get("text")), None) or ""
                    return (thinking_text, text)
                text = next((b.get("text", "").strip() for b in content_blocks if b.get("type") == "text"), None)
                if not text:
                    text = " ".join(b.get("text", "") for b in content_blocks if b.get("type") == "text").strip()
                if text:
                    return text

            else:  # openai-completions / openai-chat
                api_key = _get_api_key(provider_name)
                if not api_key:
                    raise RuntimeError(f"{provider_name} API key 未配置")
                url = f"{base_url}/chat/completions"
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                }
                messages = []
                if system:
                    messages.append({"role": "system", "content": system})
                messages.append({"role": "user", "content": prompt})

                body = {
                    "model": model_name,
                    "messages": messages,
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                }
                if provider_name in {"volcengine-plan", "openai-codex"}:
                    _budget = thinking_budget if thinking_budget else (THINKING_BUDGET_VOLCAN if provider_name == "volcengine-plan" else 16000)
                    thinking = _thinking_payload(max_tokens, _budget, provider_name)
                    if thinking:
                        body["thinking"] = thinking

                req = urllib.request.Request(
                    url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                    headers=headers, method="POST",
                )
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    raw = resp.read().decode("utf-8")
                result = json.loads(raw)
                msg = result.get("choices", [{}])[0].get("message", {})
                content = msg.get("content", "") or ""
                think_blob = msg.get("thinking", "") or ""
                # volcengine/codex 的 thinking 在顶层 msg.thinking（字符串）
                # MiniMax 的 thinking 在 content blocks 里（已在上方 anthropic 分支处理）
                if return_thinking:
                    # volcengine: thinking blob may come WITHOUT content — both are partial.
                    # kimi-k2-thinking uses reasoning_content (not msg.thinking) for thinking output.
                    reasoning = msg.get("reasoning_content", "") or ""
                    # Two channels: reasoning for logging, content for decision parsing
                    return (reasoning, content.strip() if content.strip() else reasoning)
                if content:
                    return content
                if think_blob:
                    return think_blob

            last_err = RuntimeError(f"{model} returned empty output")
            if attempt < retries - 1:
                time.sleep(30 * (2 ** attempt))

        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in (401, 403):
                if provider_name == "volcengine-plan":
                    logger.warning(f"[providers] volcengine {e.code} 认证/权限失败，立即熔断本次任务主模型")
                    _trip_volcan_circuit()
                    break
            elif e.code == 429:
                if provider_name == "volcengine-plan":
                    logger.warning(f"[providers] volcengine {e.code} (attempt {attempt+1}/{retries})")
                    if attempt >= retries - 1:
                        logger.warning("[providers] volcengine 重试耗尽，熔断火山引擎，切 MiniMax")
                        _trip_volcan_circuit()
                # MiniMax 429/403 → 等退避重试
                if attempt < retries - 1:
                    time.sleep(30 * (2 ** attempt))
            elif e.code == 400 or e.code == 500:
                if attempt < retries - 1:
                    time.sleep(10 * (attempt + 1))
        except Exception as e:
            last_err = e
            err_class = classify_llm_error(e)
            if provider_name == "volcengine-plan" and err_class == LLM_ERROR_AUTH:
                logger.warning(f"[providers] volcengine 认证/配置失败，立即熔断本次任务主模型: {e}")
                _trip_volcan_circuit()
                break
            # Connection refused / timeout / reset → 熔断 volcengine，切 MiniMax
            err_str = str(e).lower()
            is_conn_err = any(x in err_str for x in ["connection refused", "connection abort", "timed out", "timeout", "reset by peer"])
            if is_conn_err and provider_name == "volcengine-plan" and attempt >= retries - 1:
                logger.warning(f"[providers] volcengine 连接失败({type(e).__name__})且重试耗尽；按瞬时网络错误处理，不触发全局熔断")
            if attempt < retries - 1:
                time.sleep(10 * (attempt + 1))

    logger.error(f"[_call_llm] 全部失败 {model}: {last_err}")
    return ""


# ── Structured Output ───────────────────────────────────────────

from typing import Literal, Optional


class EvidenceRef(BaseModel):
    field: str = Field(description="引用的数据字段路径，例如 money_flow.main_net_flow 或 kline_summary.ma_system")
    value: str = Field(description="该字段在数据包中的原始值，统一转成字符串；缺失字段不得引用")
    claim: str = Field(description="基于该字段支持的具体判断，不能超出字段含义")


MissingDataCategory = Literal["kline", "money_flow", "financial", "sector", "news"]


class PortfolioManagerOutput(BaseModel):
    signal: Literal["BUY", "WATCH", "AVOID"] = Field(description="最终信号")
    buy_score: Optional[int] = Field(default=None, ge=0, le=100, description="未来1-3个交易日短线做多吸引力评分 0-100，会参与最终综合排序但不是唯一排序依据")
    confidence: int = Field(ge=0, le=100, description="对 signal 与 buy_score 可靠程度的置信度 0-100")
    position_ratio: float = Field(ge=0.0, le=1.0, description="建议仓位比例")
    allow_direct_buy: Optional[bool] = Field(default=None, description="是否允许早报信号进入直接买入口径；若需要盘中确认则为 false")
    needs_intraday_confirmation: Optional[bool] = Field(default=None, description="是否必须等盘中技术/量能/承接确认后才允许买入")
    entry_condition: Optional[str] = Field(default="", description="盘中买入应等待的具体条件；可直接买入时写'开盘强势/盘中强势可买'等简短条件")
    block_buy_reason: Optional[str] = Field(default="", description="不能直接BUY的核心阻断理由；没有则为空字符串")
    reason: str = Field(description="核心理由，2-3句话")
    evidence_refs: list[EvidenceRef] = Field(default_factory=list, description="支撑 reason 的证据引用；每条必须来自数据包真实字段")
    missing_data_used: list[MissingDataCategory] = Field(default_factory=list, description="只能填写数据合同中的缺失大类: kline, money_flow, financial, sector, news；没有则为空数组")
    unsupported_claims: list[str] = Field(default_factory=list, description="无法由数据包字段支持的表述；没有则为空数组")

    @field_validator("missing_data_used", mode="before")
    @classmethod
    def _filter_missing_data_used(cls, value):
        allowed = {"kline", "money_flow", "financial", "sector", "news"}
        if value is None:
            return []
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, list):
            return []
        return [str(item).strip() for item in value if str(item).strip() in allowed]


class ResearchManagerOutput(BaseModel):
    winner: Literal["bull", "bear", "tie"] = Field(description="辩论裁决方")
    consensus: str = Field(description="双方共识")
    direction: Literal["偏多", "偏空", "中性"] = Field(description="最终方向")
    confidence: int = Field(ge=0, le=10, description="置信度 0-10")
    reason: str = Field(description="裁决理由")


class BullResearcherOutput(BaseModel):
    bull_thesis: str = Field(description="核心看多逻辑")
    target_price: Optional[float] = Field(description="目标价")
    confidence: int = Field(ge=1, le=10, description="信心评分 1-10")
    key_risks: list[str] = Field(default_factory=list, description="主要风险")


class BearResearcherOutput(BaseModel):
    bear_thesis: str = Field(description="核心看空逻辑")
    worst_case_price: Optional[float] = Field(description="最坏情况价格")
    confidence: int = Field(ge=1, le=10, description="信心评分 1-10")
    key_concerns: list[str] = Field(default_factory=list, description="主要担忧")


def call_structured(
    prompt: str,
    schema: Type[BaseModel],
    system: str = "",
    model: str = "openai/gpt-5.6-sol",
    timeout: int = 120,
    retries: int = 3,
    thinking_budget: int = 50000,
    max_tokens: int = 1500,
    allow_fallback: bool = True,
    fallback_model: str = "",
    reasoning_effort: str = "max",
) -> Optional[BaseModel]:
    """
    强制 LLM 输出纯 JSON 并解析为 Pydantic 模型。
    支持 volcengine（response_format json_object）和 MiniMax。
    主模型失败自动切换到备用。
    """

    schema_name = schema.__name__
    required_keys = _schema_field_names(schema)
    schema_fields = []
    for fname, ftype in getattr(schema, "__annotations__", {}).items():
        ftype_name = ftype.__name__ if hasattr(ftype, "__name__") else str(ftype)
        schema_fields.append(f'  "{fname}": <{ftype_name}>')
    schema_lines = "\n".join(schema_fields)
    json_only_prompt = (
        f"{prompt}\n\n"
        f"## JSON 输出要求\n"
        f"只输出一个可被 json.loads 解析的 JSON 对象。第一个字符必须是 {{，最后一个字符必须是 }}。\n"
        f"不要输出 markdown、代码块、解释文字，也不要包裹外层模型名。字段如下：\n"
        f"""
{{
{schema_lines}
}}"""
    )

    provider_name, _model_name = _split_model_ref(model)

    # 根据 model 决定主/备 provider；fallback_model 只在调用方显式指定时覆盖默认兜底。
    if "volcengine" in model:
        primary, fallback = ("volcengine-plan/ark-code-latest", fallback_model or FALLBACK_MODEL)
    elif provider_name == "openai-codex":
        primary, fallback = (model, fallback_model or FALLBACK_MODEL)
    elif provider_name == "minimax-portal":
        primary, fallback = ("minimax-portal/MiniMax-M3", fallback_model or DEFAULT_MODEL)
    else:
        primary, fallback = (model, fallback_model or FALLBACK_MODEL)
    if fallback == primary:
        fallback = ""

    retries = effective_llm_retries("structured", retries)
    last_err = None
    tried_fallback = False
    attempt_count = retries + 1 if allow_fallback and fallback else retries
    for attempt in range(attempt_count):
        try:
            provider_name, _model_name = _split_model_ref(model)
            # 熔断检查：volcengine 当日触发过 429 后，直接切 MiniMax
            if allow_fallback and attempt == 0 and "volcengine" in model and _is_volcan_circuit_open():
                tried_fallback = True
                model = fallback
                provider_name, _model_name = _split_model_ref(model)

            if allow_fallback and attempt > 0 and not tried_fallback:
                tried_fallback = True
                logger.warning(f"[call_structured] 主模型 {primary} 失败，切备用 {fallback}")
                model = fallback
                provider_name, _model_name = _split_model_ref(model)

            cfg = _PROVIDER_MAP.get(provider_name, {})
            api_type = cfg.get("api", "")
            if provider_name == "openai-codex":
                data = _call_structured_openai_responses(
                    model,
                    json_only_prompt,
                    schema,
                    timeout,
                    max_tokens,
                    reasoning_effort=reasoning_effort,
                )
            elif provider_name == "minimax-portal":
                text = _call_minimax_portal_cli(
                    prompt=json_only_prompt,
                    timeout=timeout,
                    max_tokens=max_tokens,
                    thinking="adaptive" if thinking_budget else "",
                )
                data = extract_json_object(
                    text,
                    required_keys=_schema_field_names(schema),
                    wrapper_key=schema.__name__,
                )
            elif "volcengine" in model:
                data = _call_structured_volcengine(json_only_prompt, schema, timeout, thinking_budget, max_tokens)
            elif api_type in {"openai-completions", "openai-chat"}:
                data = _call_structured_openai_chat(model, json_only_prompt, schema, timeout, max_tokens)
            else:
                data = _call_structured_minimax(json_only_prompt, schema, timeout, thinking_budget, max_tokens)

            if data is not None:
                if isinstance(data, dict) and isinstance(data.get(schema_name), dict):
                    data = data[schema_name]
                return schema(**data)

        except Exception as e:
            last_err = e
            err_class = classify_llm_error(e)
            if "volcengine" in model and err_class == LLM_ERROR_AUTH:
                logger.warning("[call_structured] volcengine 认证/配置失败，立即熔断本次任务主模型")
                _trip_volcan_circuit()
            elif isinstance(e, urllib.error.HTTPError) and e.code == 429 and "volcengine" in model and attempt >= retries - 1:
                _trip_volcan_circuit()
            logger.warning(f"[call_structured] {model} attempt {attempt+1} failed: {e}")

        if not allow_fallback:
            if attempt < retries:
                import time as _time
                _time.sleep(5 * (attempt + 1))
            continue
        if not tried_fallback and fallback:
            continue  # try fallback next
        else:
            if attempt < retries:
                import time as _time
                _time.sleep(10 * (attempt + 1))
            continue

    logger.warning(f"[call_structured] 全部失败: {last_err}")
    return None


def _call_structured_openai_chat(
    model: str,
    prompt: str,
    schema: Type[BaseModel],
    timeout: int,
    max_tokens: int,
) -> Optional[dict]:
    """Generic OpenAI-chat compatible structured output via JSON-only prompt."""
    text = call_llm(
        prompt=prompt,
        model=model,
        timeout=timeout,
        retries=1,
        max_tokens=max_tokens,
        thinking_budget=0,
        temperature=0,
    )
    return extract_json_object(
        text,
        required_keys=_schema_field_names(schema),
        wrapper_key=schema.__name__,
    )


def _call_structured_volcengine(
    prompt: str,
    schema: Type[BaseModel],
    timeout: int,
    thinking_budget: int,
    max_tokens: int,
) -> Optional[dict]:
    """volcengine structured output via response_format + raw_decode"""
    _load_models_config()
    api_key = _get_api_key("volcengine-plan")
    if not api_key or api_key.startswith("VOLCANO"):
        api_key = os.environ.get("VOLCAN_API_KEY", "")
    if not api_key:
        raise RuntimeError("volcengine-plan API key 未配置")

    url = "https://ark.cn-beijing.volces.com/api/coding/v3/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": "ark-code-latest",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
    }
    req = urllib.request.Request(
        url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers=headers, method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
    result = json.loads(raw)
    msg = result.get("choices", [{}])[0].get("message", {})
    content = msg.get("content", "") or ""
    if not content:
        content = msg.get("reasoning_content", "") or msg.get("thinking", "") or ""
    return extract_json_object(
        content,
        required_keys=_schema_field_names(schema),
        wrapper_key=schema.__name__,
    )


def _extract_responses_text(result: dict) -> str:
    """Best-effort text extraction for OpenAI Responses style payloads."""
    if not isinstance(result, dict):
        return ""
    if isinstance(result.get("output_text"), str):
        return result["output_text"].strip()
    texts = []
    for item in result.get("output", []) or []:
        for content in item.get("content", []) or []:
            if isinstance(content.get("text"), str):
                texts.append(content["text"])
            elif isinstance(content.get("json"), dict):
                return json.dumps(content["json"], ensure_ascii=False)
    if texts:
        return "\n".join(t.strip() for t in texts if t).strip()
    msg = result.get("choices", [{}])[0].get("message", {}) if result.get("choices") else {}
    return (msg.get("content") or "").strip()


def _call_structured_openai_responses(
    model: str,
    prompt: str,
    schema: Type[BaseModel],
    timeout: int,
    max_tokens: int,
    reasoning_effort: str = "max",
) -> Optional[dict]:
    """OpenAI/Codex Responses structured output via json_schema."""
    _load_models_config()
    provider_name, model_name = _split_model_ref(model)
    cfg = _PROVIDER_MAP.get(provider_name, {})
    if not cfg:
        raise ValueError(f"未找到 provider: {provider_name}")
    api_key = _get_api_key(provider_name)
    base_url = cfg.get("baseUrl", "").rstrip("/")
    if not base_url:
        raise ValueError(f"{provider_name} 未配置 baseUrl")
    if not api_key:
        raise RuntimeError(f"{provider_name} API key 未配置")
    if cfg.get("api") == "openai-codex-responses" and re.match(r"^https://chatgpt\.com/backend-api/?$", base_url):
        base_url = "https://chatgpt.com/backend-api/codex"

    url = f"{base_url}/responses"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    schema_name = schema.__name__
    body = {
        "model": model_name,
        "stream": True,
        "store": False,
        "instructions": "You are a portfolio manager. Output ONLY valid JSON matching the schema exactly. No extra text.",
        "input": [{"role": "user", "content": prompt}],
        "reasoning": {
            "context": "current_turn",
            "effort": _reasoning_effort_for_model(model, reasoning_effort),
        },
        "text": {
            "format": {
                "type": "json_schema",
                "name": schema_name,
                "schema": _schema_for_json_schema(schema),
                "strict": True,
            }
        },
    }
    req = urllib.request.Request(
        url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers=headers, method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")

    # Parse SSE stream: collect output_text from response.output_text.done events
    text = ""
    current_event = None
    for line in raw.split("\n"):
        if line.startswith("event:"):
            current_event = line[6:].strip()
        elif line.startswith("data:"):
            try:
                data = json.loads(line[5:])
                # Collect output_text from delta/done events
                if current_event in ("response.output_text.delta", "response.output_text.done"):
                    text += data.get("text", "") or ""
            except Exception:
                pass

    if not text:
        logger.warning(f"[_call_structured_openai_responses] 未找到 output_text 内容")
        return None

    return extract_json_object(
        text,
        required_keys=_schema_field_names(schema),
        wrapper_key=schema_name,
    )


def _call_structured_minimax(
    prompt: str,
    schema: Type[BaseModel],
    timeout: int,
    thinking_budget: int,
    max_tokens: int,
) -> Optional[dict]:
    """MiniMax structured output via API-key anthropic messages API."""
    api_key = _get_api_key("minimax-portal")
    if not api_key:
        raise RuntimeError("minimax-portal API key 未配置")
    url = "https://api.minimaxi.com/anthropic/v1/messages"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01",
    }
    body = {
        "model": "MiniMax-M3",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0,
        "max_tokens": max_tokens,
    }
    req = urllib.request.Request(
        url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers=headers, method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
    result = json.loads(raw)
    content_blocks = result.get("content", [])
    text = "\n".join(
        b.get("text", "").strip() or b.get("thinking", "").strip()
        for b in content_blocks
        if b.get("type") in ("text", "thinking") and (b.get("text") or b.get("thinking"))
    )
    return extract_json_object(
        text,
        required_keys=_schema_field_names(schema),
        wrapper_key=schema.__name__,
    )

def call_llm_structured(
    prompt: str,
    schema: Type[T],
    system: str = "",
    model: str = "openai/gpt-5.6-sol",
    timeout: int = 120,
    retries: int = 3,
    thinking_budget: int = 50000,
) -> Optional[T]:
    """
    使用统一 structured output 返回 Pydantic 模型。
    默认 GPT-5.6 Sol，失败时由 call_structured 路径切 MiniMax。
    """

    return call_structured(
        prompt=prompt,
        schema=schema,
        system=system,
        model=model,
        timeout=timeout,
        retries=retries,
        thinking_budget=thinking_budget,
        max_tokens=12000,
        allow_fallback=True,
        fallback_model=FALLBACK_MODEL,
    )

    provider_name = "minimax-portal"
    _load_models_config()
    cfg = _PROVIDER_MAP.get(provider_name, {})
    base_url = cfg.get("baseUrl", "https://api.minimaxi.com/anthropic/v1")
    api_key = _get_api_key(provider_name)

    # 构建 schema 描述（简化版，直接用 schema 名+字段）
    schema_name = schema.__name__
    fields = []
    for fname, ftype in getattr(schema, "__annotations__", {}).items():
        fields.append(f"  {fname}: {ftype.__name__ if hasattr(ftype, '__name__') else str(ftype)}")
    schema_desc = f"{schema_name}\n" + "\n".join(fields)

    for attempt in range(retries):
        try:
            url = f"{base_url}/messages"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "anthropic-version": "2023-06-01",
            }
            messages = []
            if system:
                messages.append({"role": "user", "content": f"<system>{system}</system>\n{prompt}"})
            else:
                messages.append({"role": "user", "content": prompt})

            body = {
                "model": "MiniMax-M3",
                "messages": messages,
                "temperature": 0.3,
                "max_tokens": 12000,
                "thinking_type": "advanced",  # MiniMax structured output hint
            }
            # MiniMax 不用 thinking enabled（会导致 JSON 碎片）
            # thinking = _thinking_payload(body["max_tokens"], thinking_budget)
            # if thinking:
            #     body["thinking"] = thinking

            req = urllib.request.Request(
                url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                headers=headers, method="POST",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
            result = json.loads(raw)
            content_blocks = result.get("content", [])
            text = next((b.get("text", "").strip() for b in content_blocks if b.get("type") == "text"), None)

            # 日志：记录 MiniMax 原始响应
            logger.info(f"[MiniMax raw] text length={len(text) if text else 0}, text[:200]={text[:200] if text else 'empty'}")
            if text:
                data = extract_json_object(
                    text,
                    required_keys=_schema_field_names(schema),
                    wrapper_key=schema_name,
                )
                logger.info(f"[MiniMax parsed] data={data}")
                if data is not None:
                    return schema(**data)

        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(10 * (attempt + 1))

    logger.warning(f"[call_llm_structured] 失败: {last_err}")
    return None


if __name__ == "__main__":
    # 简单测试
    print("测试 providers.py...")
    r = call_llm("1+1=?", model="openai/gpt-5.6-sol", max_tokens=10, thinking_budget=16384)
    print(f"结果: {r[:100]}")


DEFAULT_MODEL = os.environ.get(
    "TA_DEFAULT_MODEL",
    "volcengine-plan/ark-code-latest",
)
FALLBACK_MODEL = os.environ.get(
    "TA_FALLBACK_MODEL",
    "openai/gpt-5.6-sol",
)
SECONDARY_FALLBACK_MODEL = os.environ.get(
    "TA_SECONDARY_FALLBACK_MODEL",
    "minimax-portal/MiniMax-M3",
)
MAX_DEBATE_ROUNDS = int(os.environ.get("TA_MAX_DEBATE_ROUNDS", "2"))  # 6-04 老板拍板：多空各 2 轮（原来是 1）
DEFAULT_TIMEOUT = int(os.environ.get("TA_TIMEOUT", "120"))
ROLE_MAX_TOKENS = int(os.environ.get("TA_ROLE_MAX_TOKENS", "12288"))
THINKING_BUDGET_VOLCAN = int(os.environ.get("TA_THINKING_BUDGET_VOLCAN", "8192"))
THINKING_BUDGET_MINIMAX = int(os.environ.get("TA_THINKING_BUDGET_MINIMAX", "8000"))

# MiniMax 兜底并发控制：限制最多同时 3 个兜底请求，避免雪崩
_MINIMAX_SEMAPHORE = threading.Semaphore(3)

# volcengine 熔断：主模型重试耗尽后，之后所有请求直接走 MiniMax，直至进程结束


# ── 带 fallback 的封装（兼容 debate_engine.py）──────────────


def call_llm_with_fallback(
    prompt: str,
    system: str = "",
    model: str = "",
    fallback_model: str = "",
    secondary_fallback_model: str = "",
    timeout: int = 120,
    retries: int = 3,
    thinking_budget: Optional[int] = None,
    fallback_thinking_budget: int = 0,
    temperature: float = 0.3,
    max_tokens: int = 12000,
    actual_model_out: Optional[list] = None,
    node_name: str = "default",
) -> str:
    """主模型失败后自动切换备用，model/fallback_model 为空则用默认值。

    actual_model_out: 可选，调用方传一个长度为1的 list，函数会把实际响应的
    模型名（primary 或 fallback）写入 actual_model_out[0]，用于早报卡片
    显示真实跑的是哪个模型。仅当返回值非空时写入。
    """
    retries = effective_llm_retries(node_name, retries)
    route = resolve_model_route(model or DEFAULT_MODEL, fallback_model or FALLBACK_MODEL, secondary_fallback_model or SECONDARY_FALLBACK_MODEL)
    primary = route.primary
    fallback = route.fallback
    secondary = route.secondary
    if primary.startswith("volcengine-plan/") and _is_volcan_circuit_open():
        logger.info(f"主模型 {primary} 当日已熔断，直接使用备用 {fallback}")
        result = call_llm(prompt=prompt, system=system, model=fallback,
                          timeout=timeout, retries=2,
                          max_tokens=max_tokens,
                          thinking_budget=fallback_thinking_budget,
                          temperature=temperature)
        if result:
            if actual_model_out is not None:
                actual_model_out[0] = fallback
            return result
        if secondary:
            result = call_llm(prompt=prompt, system=system, model=secondary,
                              timeout=timeout, retries=2,
                              max_tokens=max_tokens,
                              thinking_budget=fallback_thinking_budget,
                              temperature=temperature)
            if result:
                if actual_model_out is not None:
                    actual_model_out[0] = secondary
                return result
        raise RuntimeError("fallback empty response")
    primary_err = None
    try:
        result = call_llm(prompt=prompt, system=system, model=primary,
                          timeout=timeout, retries=max(1, retries),
                          max_tokens=max_tokens,
                          thinking_budget=thinking_budget,
                          temperature=temperature)
        if result:
            if actual_model_out is not None:
                actual_model_out[0] = primary
            return result
        primary_err = RuntimeError(f"empty response after {max(1, retries)} retries")
    except Exception as err:
        primary_err = err
    logger.warning(f"主模型 {primary} 重试{max(1, retries)}次仍失败，切备用 {fallback}: {primary_err}")
    try:
        with _MINIMAX_SEMAPHORE:
            result = call_llm(prompt=prompt, system=system, model=fallback,
                              timeout=timeout, retries=2,
                              max_tokens=max_tokens,
                              thinking_budget=fallback_thinking_budget,
                              temperature=temperature)
        if result:
            if actual_model_out is not None:
                actual_model_out[0] = fallback
            return result
        raise RuntimeError("fallback empty response")
    except Exception as fallback_err:
        if secondary:
            logger.warning(f"第一备用模型 {fallback} 失败，切第二备用 {secondary}: {fallback_err}")
            try:
                with _MINIMAX_SEMAPHORE:
                    result = call_llm(prompt=prompt, system=system, model=secondary,
                                      timeout=timeout, retries=2,
                                      max_tokens=max_tokens,
                                      thinking_budget=fallback_thinking_budget,
                                      temperature=temperature)
                if result:
                    if actual_model_out is not None:
                        actual_model_out[0] = secondary
                    return result
                raise RuntimeError("secondary fallback empty response")
            except Exception as secondary_err:
                raise RuntimeError(f"主备模型均失败\n主: {primary_err}\n备1: {fallback_err}\n备2: {secondary_err}")
        raise RuntimeError(f"主备模型均失败\n主: {primary}\n备: {fallback_err}")
