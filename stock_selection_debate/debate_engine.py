"""
选股辩论引擎 v2
===============
基于 LangGraph 的 TradingAgents-style 对抗辩论

架构：7节点 + 2阶段辩论
  阶段1（投资辩论）：
    Bull Researcher → Bear Researcher → Research Manager
    → [count < 2×max_debate_rounds] → 回到 Bull Researcher 继续
    → [count >= 2×max_debate_rounds] → 进入阶段2

  阶段2（风险辩论）：
    Aggressive Analyst → Conservative Analyst → Neutral Analyst
    → Portfolio Manager（最终裁决）

信号：BUY(≥70) / WATCH(40-69) / AVOID(<40)
"""

from __future__ import annotations

import json
import logging
from .tech_scoring_engine import compute_tech_score
import time
import os
import sys
import re
import threading
import urllib.error
from pathlib import Path
from typing import Dict, List, Any, Optional, Annotated
from typing_extensions import TypedDict
from pm_schema_docs import pm_json_field_instructions, pm_text_field_instructions
from dataclasses import dataclass, field

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from .data_fetcher import build_debate_packet, load_phase1_cache

logger = logging.getLogger("daily_stock_workflow.debate")

# ── 修复：JsonPlusSerializer 的 msgpack 序列化器不处理 numpy 标量，
#    导致 MemorySaver checkpoint 失败。patch _msgpack_default 解决。
#
# 注意：MemorySaver 在 StockDebateEngine.__init__ 中实际未作为 checkpointer 使用。
# 真正的断点续跑依赖 workflow.py 中的外部 JSON 文件（checkpoint_cb）。
# 此 patch 为保险代码，不影响功能，但保留以备将来使用 MemorySaver。
try:
    from langgraph.checkpoint.serde.jsonplus import _msgpack_default as _orig_default
    _numpy_msgpack_patched = False

    def _safe_msgpack_default(obj):
        if hasattr(obj, "item"):
            return obj.item()
        return _orig_default(obj)

    from langgraph.checkpoint.serde import jsonplus as _jp_mod
    _jp_mod._msgpack_default = _safe_msgpack_default
    _numpy_msgpack_patched = True
except Exception as e:
    _numpy_msgpack_patched = False

if _numpy_msgpack_patched:
    logger.info("已 patch msgpack serializer 支持 numpy 标量")

# ── 默认配置（部分从 providers.py 导入，部分本地）───
# DEFAULT_MODEL / FALLBACK_MODEL / MAX_DEBATE_ROUNDS → 从 providers 导入
RISK_DEBATE_ROUNDS = 1         # 各风险分析师只说1次
SIGNAL_BUY_THRESHOLD = 70
SIGNAL_WATCH_THRESHOLD = 40

# 技术形态分析师配置（环境变量可覆盖；基金经理裁决不走这组默认值）
TECH_ANALYST_MODEL = os.environ.get("TECH_ANALYST_MODEL", "volcengine-plan/ark-code-latest")
TECH_ANALYST_FALLBACK_MODEL = os.environ.get("TECH_ANALYST_FALLBACK_MODEL", "openai/gpt-5.6-sol")
TECH_ANALYST_TIMEOUT = 90

# 基金经理裁决专用：
# GPT-5.6 Sol(max) -> GPT-5.6 Sol(max) -> MiniMax M3(adaptive JSON) -> MiniMax text/repair
PORTFOLIO_MANAGER_PRIMARY_MODEL = os.environ.get("PORTFOLIO_MANAGER_PRIMARY_MODEL", "openai/gpt-5.6-sol")
PORTFOLIO_MANAGER_PRIMARY_ENABLED = os.environ.get(
    "PORTFOLIO_MANAGER_PRIMARY_ENABLED",
    os.environ.get("PORTFOLIO_MANAGER_SOL_ENABLED", "1"),
) != "0"
PORTFOLIO_MANAGER_PRIMARY_REASONING_EFFORT = os.environ.get("PORTFOLIO_MANAGER_PRIMARY_REASONING_EFFORT", "max")
PORTFOLIO_MANAGER_TIMEOUT = int(os.environ.get(
    "PORTFOLIO_MANAGER_TIMEOUT",
    os.environ.get("PORTFOLIO_MANAGER_SOL_TIMEOUT", "300"),
))
PORTFOLIO_MANAGER_SECONDARY_MODEL = os.environ.get("PORTFOLIO_MANAGER_SECONDARY_MODEL", "openai/gpt-5.6-sol")
PORTFOLIO_MANAGER_SECONDARY_REASONING_EFFORT = os.environ.get("PORTFOLIO_MANAGER_SECONDARY_REASONING_EFFORT", "max")
PORTFOLIO_MANAGER_SECONDARY_FALLBACK_MODEL = os.environ.get("PORTFOLIO_MANAGER_SECONDARY_FALLBACK_MODEL", "")
PORTFOLIO_MANAGER_TERTIARY_MODEL = os.environ.get("PORTFOLIO_MANAGER_TERTIARY_MODEL", "minimax-portal/MiniMax-M3")
PORTFOLIO_MANAGER_MINIMAX_ENABLED = os.environ.get("PORTFOLIO_MANAGER_MINIMAX_ENABLED", "1") != "0"
# MiniMax portal 只支持 off/adaptive；这里的非零预算映射为 adaptive。
PORTFOLIO_MANAGER_MINIMAX_BUDGET = int(os.environ.get("PORTFOLIO_MANAGER_MINIMAX_BUDGET", "16000"))

_pm_primary_lock = threading.Lock()
_pm_primary_broken = False
_pm_primary_failure_reason = ""
_pm_primary_fallback_count = 0
_secondary_broken = False
_secondary_failure_reason = ""

# ── LLM 调用 ──────────────────────────────────────────────

def _get_api_key() -> str:
    # 优先用 MX_DIRECT_KEY（OpenAI兼容格式，sk-开头）
    return os.environ.get("MX_DIRECT_KEY") or os.environ.get("MINIMAX_API_KEY") or os.environ.get("MX_APIKEY", "")

# ── LLM Provider（统一抽象层）───────────────────────────────────
from .providers import (
    call_llm as _call_llm,
    call_llm_with_fallback as _call_llm_with_fallback,
    extract_json_object as _extract_json_object,
    call_structured as _call_structured,
    PortfolioManagerOutput,
    DEFAULT_MODEL,
    MAX_DEBATE_ROUNDS,
    DEFAULT_TIMEOUT,
    ROLE_MAX_TOKENS,
    THINKING_BUDGET_VOLCAN,
    THINKING_BUDGET_MINIMAX,
)
from . import providers as _providers
def _extract_json(text: str) -> Optional[str]:
    """从 LLM 输出中提取 JSON 块。"""
    data = _extract_json_object(text)
    if data is None:
        return None
    return json.dumps(data, ensure_ascii=False)


def _call_role(
    system: str,
    user: str,
    model: str = DEFAULT_MODEL,
    timeout: int = 120,
    max_tokens: int = ROLE_MAX_TOKENS,
    actual_model_out: Optional[list] = None,
) -> str:
    """带备用机制的 LLM 调用：主模型失败自动切换备用模型。

    actual_model_out: 可选，调用方传 [None] 进来，函数会把实际响应的
    模型名（primary 或 fallback）写入 actual_model_out[0]，用于早报卡片
    显示真实跑的是哪个模型。
    """
    return _call_llm_with_fallback(
        prompt=user,
        system=system,
        model=model,
        timeout=timeout,
        thinking_budget=THINKING_BUDGET_VOLCAN,
        fallback_thinking_budget=THINKING_BUDGET_MINIMAX,
        temperature=0.3,
        max_tokens=max_tokens,
        actual_model_out=actual_model_out,
    )


def _normalize_position_ratio(value, default: float = 0.15) -> float:
    """Normalize position ratios from either decimal or percent form."""
    try:
        ratio = float(value)
    except (TypeError, ValueError):
        ratio = default
    if ratio > 1:
        ratio = ratio / 100
    return max(0.0, min(1.0, ratio))


def _fallback_buy_score(signal: str, confidence: int) -> int:
    """Compatibility score for old PM outputs that do not include buy_score."""
    sig = str(signal or "WATCH").upper()
    try:
        conf = int(confidence)
    except (TypeError, ValueError):
        conf = 0
    conf = max(0, min(100, conf))
    if sig == "BUY":
        return max(70, conf)
    if sig == "WATCH":
        return min(max(55, conf), 69)
    return min(conf, 54)


def _signal_from_buy_score(buy_score: int) -> str:
    try:
        score = int(buy_score)
    except (TypeError, ValueError):
        score = 55
    if score >= 70:
        return "BUY"
    if score >= 55:
        return "WATCH"
    return "AVOID"


# ── Prompt 模板 ───────────────────────────────────────────

SYSTEM_PROMPT = """你是一位客观、专业、数据驱动的A股分析师。你的所有判断必须基于提供的实际数据，不主观臆断，不编造数据。
证据约束：只能使用数据包中 status=ok 的字段作为正面或负面论据；类别为 partial 时，仅可使用其 field_status=ok 的具体字段；missing/unknown 字段只能写“无法验证”，不得据此推断。每个核心结论都要能对应到具体字段。输出要简洁、有条理、用数据说话。"""

# ── 辩论包渲染 ────────────────────────────────────────────

def _contract_category_for_field(field: str) -> str:
    field = str(field or "")
    if field.startswith("knowledge_rule") or field.startswith("verified_market_snapshot."):
        return "kline"
    if field.startswith("money_flow."):
        return "money_flow"
    if field.startswith("kline_summary.") or field.startswith("indicators.") or field in {"kline_raw", "kline_count"}:
        return "kline"
    if field.startswith("financial."):
        return "financial"
    if field == "sector" or field.startswith("sector."):
        return "sector"
    if field.startswith("news"):
        return "news"
    return field.split(".", 1)[0] if field else "unknown"


def _field_contract_status(packet: Dict[str, Any], field: str) -> str:
    category = _contract_category_for_field(field)
    item = ((packet.get("data_contract") or {}).get(category) or {})
    field_status = item.get("field_status") or {}
    short_field = field.split(".", 1)[1] if "." in field else field
    return str(field_status.get(field) or field_status.get(short_field) or item.get("status") or "unknown")


def _get_nested_value(data: Dict[str, Any], field: str):
    cur: Any = data
    for part in str(field or "").split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None, False
    return cur, True


def _is_missing_value(value: Any) -> bool:
    return value is None or value == "" or value == [] or value == {} or str(value).upper() in {"N/A", "NONE", "NULL"}


def _render_data_contract(packet: Dict[str, Any]) -> str:
    contract = packet.get("data_contract") or {}
    if not contract:
        return "数据合同: 未提供；所有未明确给出的字段都视为不可用，不得推断。"
    lines = []
    labels = {"kline": "K线", "money_flow": "资金流", "financial": "财务", "sector": "板块", "news": "新闻"}
    for key in ("kline", "money_flow", "financial", "sector", "news"):
        item = contract.get(key) or {}
        status = item.get("status") or "unknown"
        source = item.get("source") or "none"
        error = item.get("error") or ""
        flags = ",".join(str(x) for x in (item.get("quality_flags") or []))
        extra = []
        if error:
            extra.append(f"error={error}")
        if flags:
            extra.append(f"flags={flags}")
        field_status = item.get("field_status") or {}
        if field_status:
            available = [name for name, value in field_status.items() if value == "ok"]
            missing = [name for name, value in field_status.items() if value != "ok"]
            extra.append(f"available_fields={','.join(available) or 'none'}")
            if missing:
                extra.append(f"missing_fields={','.join(missing)}")
        suffix = f" ({'; '.join(extra)})" if extra else ""
        lines.append(f"- {labels.get(key, key)}: status={status}, source={source}{suffix}")
    return "\n".join(lines)


def _available_evidence_fields(packet: Dict[str, Any]) -> str:
    contract = packet.get("data_contract") or {}
    candidates = [
        "kline_summary.latest_close", "kline_summary.ma_system", "kline_summary.trend_pct_5d",
        "kline_summary.trend_pct_10d", "kline_summary.trend_pct_20d", "kline_summary.vol_5avg_vs_20avg",
        "kline_summary.vol_trend", "kline_summary.close_position_20d", "indicators.rsi_14",
        "indicators.macd", "indicators.macd_dea", "indicators.macd_hist", "indicators.macd_state",
        "indicators.macd_cross_event", "money_flow.main_net_flow",
        "money_flow.super_net_flow", "money_flow.ddx_5", "money_flow.ddy_10",
        "financial.roe", "financial.revenue_growth", "financial.net_profit_growth",
        "financial.pe_ttm", "financial.pb", "sector",
        "knowledge_rule_summary", "knowledge_rule_score_adjustment",
        "verified_market_snapshot.latest_close", "verified_market_snapshot.trend_state",
        "verified_market_snapshot.ma_alignment", "verified_market_snapshot.volume_ratio_5_20",
        "verified_market_snapshot.rsi14", "verified_market_snapshot.macd.state",
        "verified_market_snapshot.macd.cross_event",
        "verified_market_snapshot.kdj.signal", "verified_market_snapshot.close_position_20d",
    ]
    refs = []
    for field in candidates:
        status = _field_contract_status(packet, field)
        value, exists = _get_nested_value(packet, field)
        if exists and not _is_missing_value(value) and status == "ok":
            refs.append(f"- {field} = {value}")
    return "\n".join(refs[:24]) if refs else "无可用证据字段；只能给低置信 WATCH/AVOID。"


def _render_pm_knowledge_rules(packet: Dict[str, Any]) -> str:
    hits = packet.get("knowledge_rule_hits") or []
    summary = str(packet.get("knowledge_rule_summary") or "").strip()
    if not hits and not summary:
        return "未命中明确本地证券知识规则。"
    lines = []
    if summary:
        lines.append(f"摘要: {summary}")
    for item in hits[:5]:
        try:
            effect = float(item.get("effect") or 0)
            effect_text = f"{effect:+.1f}"
        except (TypeError, ValueError):
            effect_text = str(item.get("effect") or "")
        gate = "；需盘中确认" if item.get("watch_only") else ""
        lines.append(
            f"- {item.get('rule_id')}: {item.get('claim')} ({effect_text}{gate}; source={item.get('source')})"
        )
    return "\n".join(lines[:6])


def _normalize_evidence_refs(value: Any) -> list[dict]:
    refs = []
    for item in value or []:
        if hasattr(item, "model_dump"):
            item = item.model_dump()
        elif hasattr(item, "dict"):
            item = item.dict()
        if isinstance(item, dict):
            refs.append({
                "field": str(item.get("field") or ""),
                "value": item.get("value"),
                "claim": str(item.get("claim") or ""),
            })
    return [x for x in refs if x.get("field") or x.get("claim")]


_MISSING_CATEGORY_TERMS = {
    "money_flow": ["资金", "主力", "超大单", "DDX", "DDY", "净流入", "流入", "流出", "承接", "吸筹"],
    "kline": ["K线", "均线", "MA", "RSI", "MACD", "突破", "趋势", "放量", "缩量", "回踩", "高位", "低位"],
    "financial": ["财务", "PE", "PB", "ROE", "估值", "利润", "营收", "负债", "毛利"],
    "sector": ["板块", "行业", "赛道"],
    "news": ["新闻", "消息", "政策", "题材", "催化", "舆情"],
}

_VALID_MISSING_DATA_CATEGORIES = {"kline", "money_flow", "financial", "sector", "news"}


def _uses_missing_category_as_fact(text: str, terms: list[str]) -> bool:
    for sentence in re.split(r"[。！？；;\n]", str(text or "")):
        if not any(term in sentence for term in terms):
            continue
        if any(marker in sentence for marker in ("无法验证", "不可验证", "缺失", "不可用", "未提供", "无数据")):
            continue
        return True
    return False


def _normalize_missing_data_used(value: Any, packet: Dict[str, Any] | None = None) -> list[str]:
    contract = (packet or {}).get("data_contract") or {}
    missing_categories = {
        key for key, item in contract.items()
        if key in _VALID_MISSING_DATA_CATEGORIES and (item or {}).get("status") not in (None, "", "ok")
    }
    normalized = []
    for item in value or []:
        if hasattr(item, "value"):
            item = item.value
        category = str(item or "").strip()
        if category in _VALID_MISSING_DATA_CATEGORIES and category in missing_categories:
            normalized.append(category)
    return list(dict.fromkeys(normalized))


def _validate_pm_evidence(
    packet: Dict[str, Any],
    reason: str,
    evidence_refs: list[dict],
    missing_data_used: list[str] | None = None,
    unsupported_claims: list[str] | None = None,
) -> dict:
    contract = packet.get("data_contract") or {}
    missing_categories = {
        key for key, item in contract.items()
        if (item or {}).get("status") not in (None, "", "ok")
    }
    errors: list[str] = []
    warnings: list[str] = []
    normalized_refs = _normalize_evidence_refs(evidence_refs)

    if not normalized_refs:
        errors.append("PM未返回evidence_refs")

    for ref in normalized_refs:
        field = ref.get("field", "")
        value, exists = _get_nested_value(packet, field)
        if not exists or _is_missing_value(value):
            errors.append(f"证据字段不存在或为空: {field}")
            continue
        if _field_contract_status(packet, field) != "ok":
            errors.append(f"证据字段属于缺失/部分缺失类别: {field}")
            continue
        claimed_value = ref.get("value")
        if not _evidence_values_equal(value, claimed_value):
            errors.append(f"证据值不一致: {field} 实际={value} 引用={claimed_value}")
        if not str(ref.get("claim") or "").strip():
            errors.append(f"证据缺少claim: {field}")

    reason_text = str(reason or "")
    errors.extend(_role_evidence_errors(packet, reason_text))
    for category in missing_categories:
        if str((contract.get(category) or {}).get("status") or "") == "partial":
            continue
        terms = _MISSING_CATEGORY_TERMS.get(category, [])
        if _uses_missing_category_as_fact(reason_text, terms):
            errors.append(f"理由疑似使用缺失数据类别: {category}")

    normalized_missing_data_used = _normalize_missing_data_used(missing_data_used, packet)
    for category in normalized_missing_data_used:
        if category not in missing_categories:
            warnings.append(f"missing_data_used标记了非缺失类别: {category}")

    if unsupported_claims:
        errors.extend([f"LLM自报不支持表述: {x}" for x in unsupported_claims if x])

    status = "fail" if errors else "warn" if warnings else "pass"
    return {
        "status": status,
        "errors": list(dict.fromkeys(errors))[:8],
        "warnings": list(dict.fromkeys(warnings))[:8],
        "evidence_refs_count": len(normalized_refs),
        "missing_categories": sorted(missing_categories),
        "missing_data_used": normalized_missing_data_used,
    }


def _evidence_values_equal(actual: Any, claimed: Any) -> bool:
    if _is_missing_value(claimed):
        return False
    try:
        actual_num = float(actual)
        claimed_num = float(str(claimed).replace("%", "").strip())
        tolerance = max(1e-4, abs(actual_num) * 0.005)
        return abs(actual_num - claimed_num) <= tolerance
    except (TypeError, ValueError):
        return str(actual).strip().lower() == str(claimed).strip().lower()


def _phrase_is_asserted(text: str, phrase: str) -> bool:
    """Return True only when a phrase is stated as a current fact.

    Research roles often mention an indicator in a negation or a future trigger,
    such as "尚未形成MACD金叉" or "若MACD金叉再确认". Those are not factual
    contradictions and must not poison the node checkpoint.
    """
    for sentence in re.split(r"[。！？；;\n]", str(text or "")):
        start = 0
        while True:
            idx = sentence.find(phrase, start)
            if idx < 0:
                break
            before = sentence[max(0, idx - 56):idx]
            after = sentence[idx + len(phrase):idx + len(phrase) + 40]
            negated_before = re.search(
                r"(?:尚未|未见|未现|未形成|未发生|未触发|无新|无明显|无|没有|不存在|并非|不属于|不构成|不能确认|无法确认|暂无|等待|待)[^，,]{0,10}$",
                before,
            )
            conditional_before = re.search(r"(?:若|如果|一旦|除非|只有)[^，,]{0,24}$", before)
            negated_after = re.match(r"(?:尚未|未|并未|没有|并不|不成立|不明显|待确认|才|再)", after)
            # Bull/Bear roles must quote and rebut each other.  A phrase inside
            # "多方声称..." or "反驳空方观点..." is an attributed claim, not
            # the current role asserting that technical state as fact.
            attributed_before = re.search(
                r"(?:多方|空方|对方|上一轮|前述|此前|原(?:观点|论点)|该(?:观点|论点)|所谓)"
                r"[^，,]{0,28}(?:认为|声称|宣称|主张|提出|强调|判断|假设|称|观点|论点|说法)"
                r"[^，,]{0,14}$",
                before,
            ) or re.search(
                r"(?:反驳|质疑|否定)(?:多方|空方|对方)?[^，,]{0,18}$",
                before,
            )
            refuted_after = re.match(
                r"[”’\"']?\s*(?:(?:这一|该)?(?:说法|观点|论点|判断))?\s*"
                r"(?:并?不成立|错误|不实|无法验证|未获验证|与实际[^，,]{0,12}不一致)",
                after,
            )
            endorsed_after = re.match(
                r"[”’\"']?\s*(?:(?:这一|该)?(?:说法|观点|论点|判断))?\s*"
                r"(?:确实|明确|已经)?(?:成立|属实|得到确认|被数据证实)",
                after,
            )
            if endorsed_after or not (
                negated_before
                or conditional_before
                or negated_after
                or attributed_before
                or refuted_after
            ):
                return True
            start = idx + len(phrase)
    return False


def _macd_event_is_asserted(text: str, event: str) -> bool:
    """Recognize formatted MACD event claims such as 'MACD动量：金叉有效'."""
    for sentence in re.split(r"[。！？；;\n]", str(text or "")):
        for match in re.finditer(re.escape(event), sentence):
            before = sentence[max(0, match.start() - 48):match.start()]
            after = sentence[match.end():match.end() + 24]
            macd_context = (
                re.search(r"(?:MACD|DIF|DEA|零轴)[^，,]{0,30}$", before, flags=re.IGNORECASE)
                or re.match(r"[^，,]{0,16}(?:MACD|DIF|DEA)", after, flags=re.IGNORECASE)
            )
            if macd_context and _phrase_is_asserted(sentence, event):
                return True
    return False


def _ma_direct_claim_pattern(direction: str) -> re.Pattern:
    """Match common compact MA-system claims emitted by different providers."""
    compact = "多排" if direction == "多头" else "空排"
    return re.compile(
        rf"均线(?:系统)?\s*"
        rf"(?:(?:当前|目前|整体|总体|结构|形态|走势)\s*)?"
        rf"(?:[:：=]\s*)?"
        rf"(?:(?:呈现?|为|是|处于|维持|保持|转为|形成|构成)\s*)?"
        rf"(?:(?:明显|典型|标准|完整)\s*)?"
        rf"(?:{re.escape(direction)}(?:排列|格局|趋势|形态|结构)?|{compact})"
    )


def _ma_arrangement_is_asserted(text: str, direction: str) -> bool:
    """Recognize MA arrangement claims without confusing them with MACD wording."""
    direct_pattern = _ma_direct_claim_pattern(direction)
    phrase = f"{direction}排列"
    ma_token = re.compile(r"MA(?:5|10|20|30|60|120|250)\b", flags=re.IGNORECASE)
    for sentence in re.split(r"[。！？；;\n]", str(text or "")):
        for match in direct_pattern.finditer(sentence):
            if _phrase_is_asserted(sentence, match.group(0)):
                return True
        for match in re.finditer(re.escape(phrase), sentence):
            before = sentence[max(0, match.start() - 56):match.start()]
            if ("均线" in before or ma_token.search(before)) and _phrase_is_asserted(sentence, phrase):
                return True
    return False


_MONEY_FLOW_INDEX_VALUE_RE = re.compile(
    r"(?P<label>5日DDX|10日DDY|10日DDX|5日DDY)"
    r"\s*(?:为|是|=|:|：)?\s*[（(]?\s*"
    r"(?P<value>[+-]?\d+(?:\.\d+)?)",
    flags=re.IGNORECASE,
)
_MONEY_FLOW_COMPOSITE_INDEX_RE = re.compile(
    r"(?:5\s*[/／、]\s*10日|5日\s*[/／、]\s*10日)\s*"
    r"(?:DDX|DDY)"
    r"(?:\s*(?:为|是|=|:|：)?\s*[（(]?\s*[+-]?\d+(?:\.\d+)?\s*[）)]?)?"
    r"(?:\s*[/／、]\s*(?:DDX|DDY)"
    r"(?:\s*(?:为|是|=|:|：)?\s*[（(]?\s*[+-]?\d+(?:\.\d+)?\s*[）)]?)?"
    r"(?:\s*(?:深度|明显)?\s*(?:正|负)?值)?)?",
    flags=re.IGNORECASE,
)
_MONEY_FLOW_NET_VALUE_RE = re.compile(
    r"(?P<horizon>(?:5|10)日)?\s*"
    r"(?P<actor>主力(?:资金)?|超大单)(?:资金)?\s*(?:净)?\s*"
    r"(?:(?:单日|当日|明显|大幅|持续)\s*){0,3}"
    r"(?P<direction>流入|流出)"
    r"\s*(?:为|是|=|:|：)?\s*[（(]?\s*"
    r"(?P<value>[+-]?\d+(?:\.\d+)?)\s*(?:亿元|亿)",
    flags=re.IGNORECASE,
)
_MONEY_FLOW_INDEX_FIELDS = {
    "5日DDX": "ddx_5",
    "10日DDY": "ddy_10",
}
_UNSUPPORTED_MONEY_FLOW_INDEX_FIELDS = {
    "10日DDX": ("10日DDY", "ddy_10"),
    "5日DDY": ("5日DDX", "ddx_5"),
}


def _money_flow_claim_tolerance(actual: float) -> float:
    return max(0.01, abs(float(actual)) * 0.03)


def _money_flow_net_field(match: re.Match) -> str:
    horizon = str(match.group("horizon") or "")
    actor = str(match.group("actor") or "")
    if actor.startswith("主力"):
        return {
            "": "main_net_flow",
            "5日": "main_net_flow_5d",
            "10日": "main_net_flow_10d",
        }.get(horizon, "")
    return "super_net_flow" if not horizon else ""


def _money_flow_evidence_errors(packet: Dict[str, Any], text: str) -> list[str]:
    """Validate exact money-flow metric names and numeric claims."""
    money_flow = packet.get("money_flow") or {}
    errors: list[str] = []
    for sentence in re.split(r"[。！？；;\n]", str(text or "")):
        for match in _MONEY_FLOW_COMPOSITE_INDEX_RE.finditer(sentence):
            if _phrase_is_asserted(sentence, match.group(0)):
                errors.append("资金流复合指标写法不明确")

        matched_labels: set[str] = set()
        for match in _MONEY_FLOW_INDEX_VALUE_RE.finditer(sentence):
            if not _phrase_is_asserted(sentence, match.group(0)):
                continue
            label = str(match.group("label") or "").upper().replace("日DD", "日DD")
            matched_labels.add(label)
            if label in _UNSUPPORTED_MONEY_FLOW_INDEX_FIELDS:
                errors.append(f"资金流字段{label}不可用")
                continue
            field = _MONEY_FLOW_INDEX_FIELDS.get(label)
            actual = money_flow.get(field) if field else None
            if not field or actual is None or _field_contract_status(packet, f"money_flow.{field}") != "ok":
                errors.append(f"资金流字段{label}不可用")
                continue
            try:
                claimed = float(match.group("value"))
                actual_num = float(actual)
            except (TypeError, ValueError):
                continue
            if abs(claimed - actual_num) > _money_flow_claim_tolerance(actual_num):
                errors.append(
                    f"{label}引用{_format_score(claimed)}与实际{_format_score(actual_num)}不一致"
                )

        for label in _UNSUPPORTED_MONEY_FLOW_INDEX_FIELDS:
            if label not in matched_labels and _phrase_is_asserted(sentence, label):
                errors.append(f"资金流字段{label}不可用")

        for match in _MONEY_FLOW_NET_VALUE_RE.finditer(sentence):
            if not _phrase_is_asserted(sentence, match.group(0)):
                continue
            field = _money_flow_net_field(match)
            actual = money_flow.get(field) if field else None
            if not field or actual is None or _field_contract_status(packet, f"money_flow.{field}") != "ok":
                errors.append(f"资金流字段{field or match.group(0)}不可用")
                continue
            try:
                claimed = float(match.group("value"))
                if match.group("direction") == "流出" and claimed > 0:
                    claimed = -claimed
                actual_num = float(actual)
            except (TypeError, ValueError):
                continue
            if abs(claimed - actual_num) > _money_flow_claim_tolerance(actual_num):
                errors.append(
                    f"资金流净额引用{_format_score(claimed)}与实际"
                    f"{_format_score(actual_num)}不一致:{field}"
                )
    return errors


_TECH_SCORE_CLAIM_RE = re.compile(
    r"(?P<label>(?:否决后|规则|量化|综合|原始)?\s*技术(?:形态|面)?(?:评分|得分|分数|分))"
    r"\s*(?:为|是|=|:|：)?\s*"
    r"(?P<score>\d+(?:\.\d+)?)\s*(?P<unit>/\s*100|分)?"
    r"(?:\s*(?P<op><=|>=|<|>|≤|≥|低于|高于|不高于|不低于)\s*"
    r"(?P<threshold>\d+(?:\.\d+)?))?",
    flags=re.IGNORECASE,
)
_TECH_SCORE_THRESHOLD_RE = re.compile(
    r"(?P<label>(?:否决后|规则|量化|综合|原始)?\s*技术(?:形态|面)?(?:评分|得分|分数|分))"
    r"\s*(?P<op><=|>=|<|>|≤|≥|低于|高于|不高于|不低于)\s*"
    r"(?P<threshold>\d+(?:\.\d+)?)",
    flags=re.IGNORECASE,
)


def _role_evidence_packet(state: Dict[str, Any]) -> Dict[str, Any]:
    """Attach node-produced technical facts without mutating the market packet."""
    packet = dict(state.get("debate_packet") or {})
    workflow_evidence = dict(packet.get("_workflow_evidence") or {})
    for key in (
        "tech_pattern_score",
        "tech_raw_score",
        "tech_max_score",
        "tech_rule_signal",
        "tech_veto_reasons",
    ):
        if key in state:
            workflow_evidence[key] = state.get(key)
    if workflow_evidence:
        packet["_workflow_evidence"] = workflow_evidence
    return packet


def _tech_score_actual(packet: Dict[str, Any], label: str) -> Optional[float]:
    evidence = packet.get("_workflow_evidence") or {}
    key = "tech_raw_score" if "原始" in str(label or "") else "tech_pattern_score"
    value = evidence.get(key)
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _comparison_holds(left: float, operator: str, right: float) -> bool:
    if operator in {"<", "低于"}:
        return left < right
    if operator in {"<=", "≤", "不高于"}:
        return left <= right
    if operator in {">", "高于"}:
        return left > right
    if operator in {">=", "≥", "不低于"}:
        return left >= right
    return True


def _format_score(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else f"{value:g}"


def _role_evidence_errors(packet: Dict[str, Any], response: str) -> list[str]:
    """Catch deterministic fact contradictions before one role can poison later nodes."""
    text = str(response or "")
    if not text.strip():
        return ["模型输出为空"]
    errors: list[str] = []
    contract = packet.get("data_contract") or {}
    for category, item in contract.items():
        if category not in _MISSING_CATEGORY_TERMS or (item or {}).get("status") in {"ok", "partial"}:
            continue
        if _uses_missing_category_as_fact(text, _MISSING_CATEGORY_TERMS.get(category, [])):
            errors.append(f"使用了不可用数据类别:{category}")

    indicators = packet.get("indicators") or {}
    macd_state = str(indicators.get("macd_state") or "")
    macd_cross = str(indicators.get("macd_cross_event") or "")
    if _macd_event_is_asserted(text, "金叉") and macd_cross != "金叉":
        errors.append(f"MACD金叉与实际事件{macd_cross or '无'}不一致")
    if _macd_event_is_asserted(text, "死叉") and macd_cross != "死叉":
        errors.append(f"MACD死叉与实际事件{macd_cross or '无'}不一致")
    if _phrase_is_asserted(text, "MACD多头") and macd_state != "多头":
        errors.append(f"MACD多头与实际状态{macd_state or '未知'}不一致")
    if _phrase_is_asserted(text, "MACD空头") and macd_state != "空头":
        errors.append(f"MACD空头与实际状态{macd_state or '未知'}不一致")

    ma_system = str((packet.get("kline_summary") or {}).get("ma_system") or "")
    if _ma_arrangement_is_asserted(text, "多头") and "多头" not in ma_system:
        errors.append(f"均线多头排列与实际{ma_system or '未知'}不一致")
    if _ma_arrangement_is_asserted(text, "空头") and "空头" not in ma_system:
        errors.append(f"均线空头排列与实际{ma_system or '未知'}不一致")

    actual_rsi = indicators.get("rsi_14")
    if actual_rsi is not None:
        # Only validate an explicit current value. Thresholds such as RSI>75
        # are trading rules, not claims that the current RSI equals 75.
        for match in re.finditer(
            r"RSI(?:\(14\))?\s*(?:=|为|约|:|：)\s*(\d+(?:\.\d+)?)",
            text,
            flags=re.IGNORECASE,
        ):
            try:
                if abs(float(match.group(1)) - float(actual_rsi)) > 1.0:
                    errors.append(f"RSI引用{match.group(1)}与实际{actual_rsi}不一致")
                    break
            except (TypeError, ValueError):
                pass

    errors.extend(_money_flow_evidence_errors(packet, text))

    for sentence in re.split(r"[。！？；;\n]", text):
        for match in _TECH_SCORE_CLAIM_RE.finditer(sentence):
            if not _phrase_is_asserted(sentence, match.group(0)):
                continue
            claimed = float(match.group("score"))
            actual = _tech_score_actual(packet, match.group("label"))
            if actual is not None and abs(claimed - actual) > 0.5:
                errors.append(
                    f"技术分引用{_format_score(claimed)}与实际{_format_score(actual)}不一致"
                )
            operator = match.group("op")
            threshold = match.group("threshold")
            if operator and threshold is not None:
                left = actual if actual is not None else claimed
                right = float(threshold)
                if not _comparison_holds(left, operator, right):
                    errors.append(
                        f"技术分阈值比较{_format_score(left)}{operator}{_format_score(right)}不成立"
                    )

        for match in _TECH_SCORE_THRESHOLD_RE.finditer(sentence):
            if not _phrase_is_asserted(sentence, match.group(0)):
                continue
            actual = _tech_score_actual(packet, match.group("label"))
            if actual is None:
                continue
            right = float(match.group("threshold"))
            operator = match.group("op")
            if not _comparison_holds(actual, operator, right):
                errors.append(
                    f"技术分阈值比较{_format_score(actual)}{operator}{_format_score(right)}不成立"
                )

    tech_evidence = packet.get("_workflow_evidence") or {}
    if "tech_veto_reasons" in tech_evidence and not (tech_evidence.get("tech_veto_reasons") or []):
        technical_context = re.compile(r"技术|均线|MACD|RSI|量价|规则引擎|形态", flags=re.IGNORECASE)
        for sentence in re.split(r"[。！？；;\n]", text):
            hard_veto = any(
                _phrase_is_asserted(sentence, phrase)
                for phrase in ("硬否决", "强制否决")
            ) and technical_context.search(sentence)
            explicit_veto = any(
                _phrase_is_asserted(sentence, phrase)
                for phrase in ("触发技术否决", "触发规则否决", "触发量化否决")
            )
            if hard_veto or explicit_veto:
                errors.append("声称触发技术硬否决但实际无否决原因")
                break
    return list(dict.fromkeys(errors))[:8]


class RoleEvidenceValidationError(RuntimeError):
    """A role response remained contradictory after one evidence repair."""


def _replace_macd_event_claims(text: str, event: str, replacement: str) -> str:
    patterns = (
        re.compile(
            rf"MACD[^，,。！？；;\n]{{0,30}}?{event}(?:已经|已)?(?:形成|出现|确认|有效)?",
            flags=re.IGNORECASE,
        ),
        re.compile(
            rf"(?:DIF(?:与|和|/)?DEA|零轴(?:上|下)?)"
            rf"[^，,。！？；;\n]{{0,20}}?{event}(?:已经|已)?(?:形成|出现|确认|有效)?",
            flags=re.IGNORECASE,
        ),
    )
    repaired = str(text or "")
    for pattern in patterns:
        repaired = pattern.sub(replacement, repaired)
    return repaired


def _replace_ma_arrangement_claims(text: str, direction: str, replacement: str) -> str:
    direct_pattern = _ma_direct_claim_pattern(direction)
    phrase_pattern = re.compile(rf"(?:完美|典型|标准|完整)?{direction}排列")
    ma_token = re.compile(r"MA(?:5|10|20|30|60|120|250)\b", flags=re.IGNORECASE)
    parts = re.split(r"([。！？；;\n])", str(text or ""))
    for index in range(0, len(parts), 2):
        sentence = parts[index]
        if not _ma_arrangement_is_asserted(sentence, direction):
            continue

        direct_matches = list(direct_pattern.finditer(sentence))
        for match in reversed(direct_matches):
            if not _phrase_is_asserted(sentence, match.group(0)):
                continue
            sentence = sentence[:match.start()] + replacement + sentence[match.end():]

        matches = list(phrase_pattern.finditer(sentence))
        for match in reversed(matches):
            if not _phrase_is_asserted(sentence, match.group(0)):
                continue
            local_before = sentence[max(0, match.start() - 40):match.start()]
            if "MACD" in local_before and "均线" not in local_before and not ma_token.search(local_before):
                continue
            replace_start = match.start()
            ma_prefix = re.search(
                r"均线(?:系统)?(?:当前|呈|为|是|已呈|形成|处于)?\s*[:：]?\s*$",
                sentence[:match.start()],
            )
            if ma_prefix:
                replace_start = ma_prefix.start()
            sentence = sentence[:replace_start] + replacement + sentence[match.end():]
        parts[index] = sentence
    return "".join(parts)


def _replace_money_flow_claims(packet: Dict[str, Any], text: str) -> str:
    money_flow = packet.get("money_flow") or {}
    parts = re.split(r"([。！？；;\n])", str(text or ""))
    for index in range(0, len(parts), 2):
        sentence = parts[index]

        def _replace_composite_indexes(match: re.Match) -> str:
            if not _phrase_is_asserted(sentence, match.group(0)):
                return match.group(0)
            canonical: list[str] = []
            for label, field in _MONEY_FLOW_INDEX_FIELDS.items():
                actual = money_flow.get(field)
                if actual is not None and _field_contract_status(
                    packet, f"money_flow.{field}"
                ) == "ok":
                    canonical.append(f"{label}={_format_score(float(actual))}")
                else:
                    canonical.append(f"{label}数据不可用")
            return "、".join(canonical)

        sentence = _MONEY_FLOW_COMPOSITE_INDEX_RE.sub(
            _replace_composite_indexes,
            sentence,
        )

        for wrong_label, (right_label, right_field) in _UNSUPPORTED_MONEY_FLOW_INDEX_FIELDS.items():
            pattern = re.compile(
                rf"{re.escape(wrong_label)}"
                r"(?:\s*(?:为|是|=|:|：)?\s*[（(]?\s*[+-]?\d+(?:\.\d+)?\s*[）)]?)?",
                flags=re.IGNORECASE,
            )
            for match in reversed(list(pattern.finditer(sentence))):
                if not _phrase_is_asserted(sentence, match.group(0)):
                    continue
                actual = money_flow.get(right_field)
                if actual is not None and _field_contract_status(packet, f"money_flow.{right_field}") == "ok":
                    replacement = f"{right_label}={_format_score(float(actual))}"
                else:
                    replacement = f"{right_label}数据不可用"
                sentence = sentence[:match.start()] + replacement + sentence[match.end():]

        def _replace_index_value(match: re.Match) -> str:
            if not _phrase_is_asserted(sentence, match.group(0)):
                return match.group(0)
            label = str(match.group("label") or "").upper().replace("日DD", "日DD")
            field = _MONEY_FLOW_INDEX_FIELDS.get(label)
            actual = money_flow.get(field) if field else None
            if not field or actual is None or _field_contract_status(packet, f"money_flow.{field}") != "ok":
                return f"{label}数据不可用"
            try:
                claimed = float(match.group("value"))
                actual_num = float(actual)
            except (TypeError, ValueError):
                return match.group(0)
            if abs(claimed - actual_num) <= _money_flow_claim_tolerance(actual_num):
                return match.group(0)
            return f"{label}={_format_score(actual_num)}"

        sentence = _MONEY_FLOW_INDEX_VALUE_RE.sub(_replace_index_value, sentence)

        def _replace_net_value(match: re.Match) -> str:
            if not _phrase_is_asserted(sentence, match.group(0)):
                return match.group(0)
            field = _money_flow_net_field(match)
            actual = money_flow.get(field) if field else None
            horizon = str(match.group("horizon") or "")
            actor = "主力" if str(match.group("actor") or "").startswith("主力") else "超大单"
            if not field or actual is None or _field_contract_status(packet, f"money_flow.{field}") != "ok":
                return f"{horizon}{actor}净额数据不可用"
            try:
                claimed = float(match.group("value"))
                if match.group("direction") == "流出" and claimed > 0:
                    claimed = -claimed
                actual_num = float(actual)
            except (TypeError, ValueError):
                return match.group(0)
            if abs(claimed - actual_num) <= _money_flow_claim_tolerance(actual_num):
                return match.group(0)
            direction = "流入" if actual_num >= 0 else "流出"
            return f"{horizon}{actor}净{direction}{_format_score(abs(actual_num))}亿元"

        sentence = _MONEY_FLOW_NET_VALUE_RE.sub(_replace_net_value, sentence)
        parts[index] = sentence
    return "".join(parts)


def _deterministic_role_evidence_repair(
    packet: Dict[str, Any],
    response: str,
    errors: list[str],
) -> str:
    """Correct only contradictions whose true value is explicit in the packet."""
    text = str(response or "")
    error_text = "\n".join(str(item) for item in errors)
    indicators = packet.get("indicators") or {}
    macd_state = str(indicators.get("macd_state") or "")
    macd_cross = str(indicators.get("macd_cross_event") or "")

    def _macd_replacement(event: str) -> str:
        if macd_cross in {"金叉", "死叉"}:
            return f"MACD{macd_cross}"
        if macd_state in {"多头", "空头"}:
            return f"MACD{macd_state}状态（本日无新{event}事件）"
        return f"MACD本日无新{event}事件"

    if "MACD金叉与实际事件" in error_text:
        text = _replace_macd_event_claims(text, "金叉", _macd_replacement("金叉"))
    if "MACD死叉与实际事件" in error_text:
        text = _replace_macd_event_claims(text, "死叉", _macd_replacement("死叉"))

    if "MACD多头与实际状态" in error_text:
        replacement = f"MACD{macd_state}状态" if macd_state else "MACD状态未知"
        text = text.replace("MACD多头", replacement)
    if "MACD空头与实际状态" in error_text:
        replacement = f"MACD{macd_state}状态" if macd_state else "MACD状态未知"
        text = text.replace("MACD空头", replacement)

    ma_system = str((packet.get("kline_summary") or {}).get("ma_system") or "")
    if "多头" in ma_system:
        ma_replacement = "均线多头排列"
    elif "空头" in ma_system:
        ma_replacement = "均线空头排列"
    elif ma_system:
        ma_replacement = f"均线系统{ma_system}"
    else:
        ma_replacement = "均线状态未知"
    if "均线多头排列与实际" in error_text:
        text = _replace_ma_arrangement_claims(text, "多头", ma_replacement)
    if "均线空头排列与实际" in error_text:
        text = _replace_ma_arrangement_claims(text, "空头", ma_replacement)

    if (
        "资金流字段" in error_text
        or "DDX引用" in error_text
        or "DDY引用" in error_text
        or "资金流净额引用" in error_text
    ):
        text = _replace_money_flow_claims(packet, text)

    actual_rsi = indicators.get("rsi_14")
    if actual_rsi is not None and "RSI引用" in error_text:
        rsi_pattern = re.compile(
            r"(RSI(?:\(14\))?\s*(?:=|为|约|:|：)\s*)(\d+(?:\.\d+)?)",
            flags=re.IGNORECASE,
        )

        def _replace_rsi(match: re.Match) -> str:
            try:
                if abs(float(match.group(2)) - float(actual_rsi)) <= 1.0:
                    return match.group(0)
            except (TypeError, ValueError):
                return match.group(0)
            return f"{match.group(1)}{actual_rsi}"

        text = rsi_pattern.sub(_replace_rsi, text)

    if "技术分引用" in error_text or "技术分阈值比较" in error_text:
        def _replace_score_claim(match: re.Match) -> str:
            actual = _tech_score_actual(packet, match.group("label"))
            claimed = float(match.group("score"))
            if actual is None:
                actual = claimed
            operator = match.group("op")
            threshold = match.group("threshold")
            invalid_comparison = bool(
                operator
                and threshold is not None
                and not _comparison_holds(actual, operator, float(threshold))
            )
            invalid_value = abs(claimed - actual) > 0.5
            if not invalid_comparison and not invalid_value:
                return match.group(0)
            return f"{match.group('label')}{_format_score(actual)}/100"

        text = _TECH_SCORE_CLAIM_RE.sub(_replace_score_claim, text)

        def _replace_threshold_claim(match: re.Match) -> str:
            actual = _tech_score_actual(packet, match.group("label"))
            if actual is None or _comparison_holds(
                actual,
                match.group("op"),
                float(match.group("threshold")),
            ):
                return match.group(0)
            return f"{match.group('label')}{_format_score(actual)}/100"

        text = _TECH_SCORE_THRESHOLD_RE.sub(_replace_threshold_claim, text)

    if "声称触发技术硬否决" in error_text:
        text = re.sub(
            r"(?:构成|形成|属于|命中|触发)?\s*(?:技术|规则|量化)?硬否决(?:条件|项|信号)?",
            "未触发技术硬否决",
            text,
        )
        text = re.sub(
            r"触发(?:技术|规则|量化)?否决",
            "未触发技术硬否决",
            text,
        )
        text = re.sub(
            r"(?:技术面?|规则引擎|量化)?\s*强制否决",
            "未触发技术硬否决",
            text,
        )
    return text


def _call_role_guarded(
    system: str,
    prompt: str,
    *,
    packet: Dict[str, Any],
    model: str,
    timeout: int,
    max_tokens: int,
    actual_model_out: Optional[list] = None,
) -> str:
    response = _call_role(
        system,
        prompt,
        model=model,
        timeout=timeout,
        max_tokens=max_tokens,
        actual_model_out=actual_model_out,
    )
    errors = _role_evidence_errors(packet, response)
    if not errors:
        return response
    # Cheap, fully deterministic corrections should happen before spending a
    # second model call.  The corrected text still has to pass the exact same
    # validator; anything not provable from the packet falls through to the
    # model rewrite path below.
    deterministic = _deterministic_role_evidence_repair(packet, response, errors)
    deterministic_errors = _role_evidence_errors(packet, deterministic)
    if not deterministic_errors:
        stock_label = (
            packet.get("stock_name")
            or packet.get("name")
            or packet.get("stock_code")
            or "unknown"
        )
        logger.warning(
            "[%s] 分析节点初次输出确定性证据纠偏后通过: %s",
            stock_label,
            errors,
        )
        return deterministic
    errors = deterministic_errors
    repair_prompt = (
        prompt
        + "\n\n【证据校验失败，必须重写】\n"
        + "\n".join(f"- {error}" for error in errors)
        + "\n只能使用数据包真实字段和值；无法验证的内容明确写无法验证。"
    )
    repaired = _call_role(
        system,
        repair_prompt,
        model=model,
        timeout=timeout,
        max_tokens=max_tokens,
        actual_model_out=actual_model_out,
    )
    repaired_errors = _role_evidence_errors(packet, repaired)
    if not repaired_errors:
        return repaired
    repaired_deterministic = _deterministic_role_evidence_repair(packet, repaired, repaired_errors)
    repaired_deterministic_errors = _role_evidence_errors(packet, repaired_deterministic)
    if not repaired_deterministic_errors:
        stock_label = (
            packet.get("stock_name")
            or packet.get("name")
            or packet.get("stock_code")
            or "unknown"
        )
        logger.warning(
            "[%s] 分析节点确定性证据纠偏后通过: %s",
            stock_label,
            repaired_errors,
        )
        return repaired_deterministic
    logger.warning("分析节点证据校验失败，标记待重试: %s", repaired_errors)
    raise RoleEvidenceValidationError("分析节点证据校验失败: " + "；".join(repaired_deterministic_errors))


def _repair_portfolio_evidence(
    original_prompt: str,
    validation: dict,
    call_structured,
    PortfolioManagerOutput,
):
    repair_prompt = f"""上一轮基金经理裁决存在证据约束问题，请仅基于同一事实材料重写一次结构化裁决。

证据校验问题：
{json.dumps(validation, ensure_ascii=False)}

重写要求：
- 不要新增事实，不要使用缺失/partial/unknown字段做正面或负面推断。
- 若资金流/K线/财务/板块/新闻缺失，只能写“无法验证”，不能推断。
- evidence_refs 必须只引用真实存在且 status=ok 的字段。
- unsupported_claims 必须为空；如果做不到，降低 buy_score 和 confidence。
- 只输出 schema JSON。

原始事实材料：
{original_prompt}
"""
    return call_structured(
        prompt=repair_prompt,
        schema=PortfolioManagerOutput,
        model=PORTFOLIO_MANAGER_PRIMARY_MODEL,
        timeout=PORTFOLIO_MANAGER_TIMEOUT,
        retries=1,
        thinking_budget=0,
        max_tokens=4096,
        allow_fallback=True,
        fallback_model=PORTFOLIO_MANAGER_SECONDARY_MODEL,
        reasoning_effort=PORTFOLIO_MANAGER_PRIMARY_REASONING_EFFORT,
    )


def _render_debate_packet(packet: Dict) -> str:
    """将辩论包渲染为 Prompt 友好格式。"""
    fin = packet.get("financial", {})
    mf = packet.get("money_flow", {})
    kls = packet.get("kline_summary", {})
    ind = packet.get("indicators", {})
    snap = packet.get("verified_market_snapshot") or {}
    source_pools = packet.get("source_pools") or ([packet.get("pool")] if packet.get("pool") else [])
    strategy_types = packet.get("strategy_types") or ([packet.get("strategy_type")] if packet.get("strategy_type") else [])
    entry_biases = packet.get("entry_biases") or ([packet.get("entry_bias")] if packet.get("entry_bias") else [])
    source_reasons = packet.get("source_reasons") or ([packet.get("screening_reason")] if packet.get("screening_reason") else [])
    pool_score = packet.get("pool_score")
    pool_rank = packet.get("pool_rank")
    pool_total = packet.get("pool_scored_candidates") or packet.get("pool_total_candidates")

    source_lines = []
    if source_pools or strategy_types or entry_biases:
        source_lines = [
            "",
            "### 入池逻辑（第一阶段筛选来源）",
            f"- 来源池: {'、'.join([str(x) for x in source_pools if x]) or 'N/A'}",
            f"- 策略类型: {'、'.join([str(x) for x in strategy_types if x]) or 'N/A'}",
            f"- 买入偏好: {'；'.join([str(x) for x in entry_biases if x]) or 'N/A'}",
        ]
        if pool_score is not None or pool_rank is not None:
            rank_text = f"{pool_rank}/{pool_total}" if pool_rank and pool_total else str(pool_rank or "N/A")
            score_text = f"{float(pool_score):.1f}/100" if pool_score is not None else "N/A"
            source_lines.append(f"- 池内排序: 第{rank_text}，本地评分 {score_text}")
        if source_reasons:
            source_lines.append(f"- 入池原因: {'；'.join([str(x) for x in source_reasons if x])[:220]}")

    knowledge_lines = []
    knowledge_text = _render_pm_knowledge_rules(packet)
    if knowledge_text:
        knowledge_lines = ["", "### 本地证券知识规则命中", knowledge_text]

    # Pre-compute conditionals for f-strings
    chk_5d = "V" if (kls.get('trend_pct_5d') or 0) > 0 else "X"
    chk_vol = "V" if (kls.get('vol_5avg_vs_20avg') or 0) > 0.8 else "X"
    chk_rsi = "V" if (ind.get('rsi_14') or 0) < 75 else "W 超买"
    chk_ma = "V" if kls.get('ma_system') == '多头排列' else "X"
    chk_vol_trend = "V" if kls.get('vol_trend') == '逐日递增' else "X"
    warn_20d = "W>30%已涨多" if (kls.get('trend_pct_20d') or 0) > 30 else "V"
    warn_rsi_pos = "W>80高位" if (ind.get('rsi_position') or 0) > 80 else "V"
    lines = [
        f"## {packet.get('name', packet.get('stock_code', 'N/A'))}（{packet.get('stock_code', 'N/A')}）",
        f"**所属行业**: {packet.get('sector', 'N/A')}",
        *source_lines,
        *knowledge_lines,
        "",
        "### 数据可用性合同",
        _render_data_contract(packet),
        "",
        "### 统一行情事实快照",
        f"- status={snap.get('status', 'N/A')} source={snap.get('source', 'N/A')} bars={snap.get('bar_count', 'N/A')} as_of={snap.get('as_of', 'N/A')}",
        f"- 收盘={snap.get('latest_close', 'N/A')} 涨跌1D={snap.get('pct_change_1d', 'N/A')}% 5D={snap.get('pct_change_5d', 'N/A')}% 20D={snap.get('pct_change_20d', 'N/A')}%",
        f"- 趋势={snap.get('trend_state', 'N/A')} 均线={snap.get('ma_alignment', 'N/A')} 20日位置={snap.get('close_position_20d', 'N/A')} 量比={snap.get('volume_ratio_5_20', 'N/A')}",
        f"- RSI14={snap.get('rsi14', 'N/A')} MACD状态={((snap.get('macd') or {}).get('state', 'N/A'))} MACD交叉事件={((snap.get('macd') or {}).get('cross_event', '无'))} KDJ={((snap.get('kdj') or {}).get('signal', 'N/A'))} ATR14={snap.get('atr14', 'N/A')}",
        "",
        "### 证据使用规则",
        "- status=ok 的字段才可作为判断依据；partial/missing/unknown 只能写无法验证，不得推断。",
        "- 每个核心论点必须引用数据包中的具体字段和值。",
        "",
        "### 财务数据",
        f"- ROE(年): {fin.get('roe', 'N/A')}%",
        f"- ROE(季度,最新): {fin.get('roe_quarter', 'N/A')}%",
        f"- 营收增速: {fin.get('revenue_growth', 'N/A')}%",
        f"- 净利润增速: {fin.get('net_profit_growth', 'N/A')}%",
        f"- 毛利率: {fin.get('gross_margin', 'N/A')}%",
        f"- PE(TTM): {fin.get('pe_ttm', 'N/A')}",
        f"- PB: {fin.get('pb', 'N/A')}",
        f"- 负债率: {fin.get('debt_ratio', 'N/A')}%",
        f"- 营收现金含量: {fin.get('cash_flow_ratio', 'N/A')}%",
        "",
        "### 资金流",
        f"- 主力净流入: {mf.get('main_net_flow', 'N/A')}亿元",
        f"- 超大单净流入: {mf.get('super_net_flow', 'N/A')}亿元",
        f"- 5日DDX: {mf.get('ddx_5', 'N/A')}",
        f"- 10日DDY: {mf.get('ddy_10', 'N/A')}",
        "",
        "### K线",
        f"- 最新收盘价: {kls.get('latest_close', 'N/A')}",
        f"- MA系统: {kls.get('ma_system', 'N/A')}",
        f"- 近期趋势: {kls.get('recent_trend', 'N/A')}",
        f"- 成交量状态: {kls.get('volume_state', 'N/A')}",
        "",
        "### 短线动量（短线股筛选关键指标）",
        f"- 5日涨幅: {kls.get('trend_pct_5d', 'N/A')}%",
        f"- 10日涨幅: {kls.get('trend_pct_10d', 'N/A')}%",
        f"- 20日涨幅: {kls.get('trend_pct_20d', 'N/A')}%",
        f"- 均线系统: {kls.get('ma_system', 'N/A')}",
        f"- 量比（5日均量/20日均量）: {kls.get('vol_5avg_vs_20avg', 'N/A')}x",
        f"- 量能趋势: {kls.get('vol_trend', 'N/A')}",
        "",
        "### 技术指标",
        f"- RSI(14): {ind.get('rsi_14', 'N/A')}",
        f"- MACD DIF/DEA/柱: {ind.get('macd', 'N/A')}/{ind.get('macd_dea', 'N/A')}/{ind.get('macd_hist', 'N/A')}",
        f"- MACD状态: {ind.get('macd_state', 'N/A')}；交叉事件: {ind.get('macd_cross_event', '无')}；柱斜率: {ind.get('macd_breadth', 'N/A')}",
        f"- 量价信号: {kls.get('vol_signal', 'N/A')}",
        "",
        "### 短线参考门槛（必须结合入池逻辑解释，不能机械否决）",

        f"- 5日涨幅 > 0%: {chk_5d} {kls.get('trend_pct_5d', 'N/A')}%",
        f"- 量比 > 0.8x: {chk_vol} {kls.get('vol_5avg_vs_20avg', 'N/A')}x",
        f"- RSI < 75: {chk_rsi} {ind.get('rsi_14', 'N/A')}",
        f"- 均线多头: {chk_ma} {kls.get('ma_system', 'N/A')}",
        f"- 量能逐增: {chk_vol_trend} {kls.get('vol_trend', 'N/A')}",
        "- 准备启动/资金异动候选：若尚未突破，不因单项动量不足直接否决；重点看资金背离、止跌企稳、回踩承接和日内低吸条件。",

        "### 短线评分（综合短线质量，满分100）",
        f"- 近20日涨幅: {kls.get('trend_pct_20d', 'N/A')}%  {warn_20d}",
        f"- RSI位置(20日): {ind.get('rsi_position', 'N/A')}/100  {warn_rsi_pos}",
        f"- 20日高低位: {kls.get('close_position_20d', 'N/A')}%  (0%=低点,100%=高点)",
    ]

    # 个股新闻（SentimentGrounding 用）
    news_items = packet.get("news", [])
    if news_items:
        news_lines = ["", "### 个股新闻"]
        for item in news_items[:10]:
            title = item.get("title", "")[:60]
            content = item.get("content", "")[:150]
            src = item.get("source", "")
            tm = item.get("time", "")
            news_lines.append(f"- 【{tm}】{title}: {content}")
        lines.extend(news_lines)

    return "\n".join(lines)


# ── LangGraph 状态定义 ────────────────────────────────────

class InvestDebateState(TypedDict, total=False):
    """阶段1（投资辩论）的状态。"""
    stock_code: str
    stock_name: str
    bull_history: Annotated[str, "Bull 方辩论历史"]
    bear_history: Annotated[str, "Bear 方辩论历史"]
    history: Annotated[str, "完整辩论历史"]
    current_response: Annotated[str, "最新一轮的发言"]
    count: Annotated[int, "辩论轮次计数"]
    research_plan: Annotated[str, "Research Manager 的综合判断"]
    # 技术形态分析师
    tech_analyst_verdict: Annotated[str, "技术形态分析师的综合裁决"]
    tech_pattern_score: Annotated[int, "技术形态综合评分(-20~20)"]
    tech_signals_summary: Annotated[str, "各信号库汇总"]
    tech_rule_signal: str
    tech_raw_score: int
    tech_max_score: int
    tech_veto_reasons: list[str]
    data_quality_flags: Annotated[list[str], "数据质量缺口标记"]
    # 跨节点传递的数据
    debate_packet: Dict
    market_context: str
    max_rounds: int
    model: str
    node_models_log: list[Dict[str, str]]
    _node_resume: Dict[str, Dict[str, Any]]


class RiskDebateState(TypedDict, total=False):
    """阶段2（风险辩论）的状态。"""
    stock_code: str
    stock_name: str
    aggressive_history: Annotated[str, "激进风险分析师发言历史"]
    conservative_history: Annotated[str, "保守风险分析师发言历史"]
    neutral_history: Annotated[str, "中性风险分析师发言历史"]
    history: Annotated[str, "完整辩论历史"]
    latest_speaker: Annotated[str, "最后发言的分析师"]
    research_plan: Annotated[str, "Research Manager 的投资建议"]
    count: Annotated[int, "风险辩论轮次计数"]
    final_decision: Annotated[str, "Portfolio Manager 的最终裁决"]
    # 跨节点传递的数据
    debate_packet: Dict
    market_context: str
    model: str
    signal: str
    buy_score: int
    confidence: int
    position_ratio: float
    allow_direct_buy: bool
    needs_intraday_confirmation: bool
    entry_condition: str
    block_buy_reason: str
    reason: str
    decision_source: str
    raw_final_decision: str
    decision_models: Dict[str, str]
    evidence_refs: list[dict]
    missing_data_used: list[str]
    unsupported_claims: list[str]
    evidence_validation: dict
    node_models_log: list[Dict[str, str]]
    data_quality_flags: list[str]
    tech_pattern_score: int
    tech_rule_signal: str
    tech_raw_score: int
    tech_max_score: int
    tech_veto_reasons: list[str]
    _node_resume: Dict[str, Dict[str, Any]]


_NODE_CHECKPOINT_LOCAL = threading.local()


def _node_checkpoint_key(phase: str, node_name: str, state: Dict[str, Any]) -> str:
    count = int(state.get("count", 0) or 0)
    # Only the research manager can legitimately run twice at the same count:
    # once after Bull/Bear and once after the technical analyst.  Applying the
    # marker to every node creates bogus keys such as bull_researcher.0.tech
    # when LangGraph resumes after a downstream failure.
    marker = (
        "tech"
        if node_name == "research_manager" and state.get("tech_analyst_verdict")
        else "debate"
    )
    return f"{phase}.{node_name}.{count}.{marker}"


def _checkpointed_node(phase: str, node_name: str, node_fn):
    def wrapped(state):
        key = _node_checkpoint_key(phase, node_name, state)
        resume_nodes = state.get("_node_resume") or {}
        cached = resume_nodes.get(key)
        if isinstance(cached, dict) and cached:
            logger.info(f"[{state.get('stock_name', '')}] 节点断点复用: {key}")
            restored = dict(state)
            restored.update(cached)
            restored["_node_resume"] = resume_nodes
            return restored

        result = node_fn(state)
        snapshot = {
            k: _sanitize(v)
            for k, v in dict(result or {}).items()
            if k not in {"debate_packet", "market_context", "_node_resume"}
        }
        callback = getattr(_NODE_CHECKPOINT_LOCAL, "callback", None)
        if callback:
            callback(str(state.get("stock_code") or ""), key, snapshot)
        return result

    wrapped.__name__ = f"checkpointed_{phase}_{node_name}"
    return wrapped


# ── 状态更新辅助 ─────────────────────────────────────────

def _sanitize(obj):
    """将 numpy 类型转换为原生 Python 类型，防止 msgpack 序列化失败。"""
    if hasattr(obj, "item"):  # numpy scalar
        return obj.item()
    if hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, dict)):
        try:
            return [_sanitize(v) for v in obj]
        except TypeError:
            pass
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    return obj

def _update_invest(state: InvestDebateState, **updates) -> InvestDebateState:
    """生成新的投资辩论状态，numpy 类型全部转原生。"""
    return {**dict(state), **{k: _sanitize(v) for k, v in updates.items()}}

def _update_risk(state: RiskDebateState, **updates) -> RiskDebateState:
    """生成新的风险辩论状态，numpy 类型全部转原生。"""
    sanitized_state = {k: _sanitize(v) for k, v in dict(state).items()}
    result = {**sanitized_state, **{k: _sanitize(v) for k, v in updates.items()}}
    # 日志：记录 history 写入前后内容
    hist = result.get("history", "")
    if "history" in updates:
        old_hist = updates.get("history", "")[:100]
        result_hist = result.get("history", "")[:100]
        logger.info(f"[_update_risk] history 写入: old={old_hist!r} -> new={result_hist!r}")
        ns = '\n'
        # 防止前导 \n 导致 LLM 输出碎片化 JSON
        if result_hist.startswith(ns):
            clean_hist = result_hist.lstrip(ns)
            result["history"] = clean_hist
            logger.warning(f"[_update_risk] 去除前导 \\n: {result['history'][:80]!r}")
    return result


import json as _stdlib_json

def _safe_json_dumps(obj, **kwargs) -> str:
    try:
        return _stdlib_json.dumps(obj, ensure_ascii=False, indent=None, default=str)
    except Exception:
        return str(obj)


# ── 节点：Bull Researcher ──────────────────────────────────

BULL_RESEARCHER_PROMPT = """你是**短线多方分析师（Bull Analyst）**，负责为候选股票构建未来1-3个交易日的看多论证。

## 数据包
{debate_packet}

## 市场整体环境
{market_context}

## 辩论规则
1. 先认真阅读数据包，重点关注"入池逻辑"、"短线动量"和与来源策略匹配的确认条件
2. 看空方（Bear Analyst）已发表观点（见下方"空方最新论点"）
3. 你需要：基于数据提出看多论据，并针对空方观点逐条反驳
4. 所有论证必须围绕未来1-3个交易日的短线机会；长期基本面只能作为辅助，不能替代短线触发条件
5. 数据证据边界：只允许引用数据可用性合同中 status=ok 的字段；partial/missing/unknown 只能写“无法验证”，不得据此推断

## 短线股评分重点（多方必须论证的核心）
- **动量强度**：5日涨幅 5-15% 说明刚启动，>15% 可能已走完；20日涨幅 >30% 说明已是高位，多方需解释为何还能追
- **均线多头排列**：5>10>20日均线多头排列是强势信号
- **量能配合**：量比>1.0x 且量能逐日递增是主力介入信号
- **DDX/DDY**：5日主力净流入持续为正，是机构进场信号
- **板块动量**：所属板块近5日涨幅与大盘对比
- **来源适配**：准备启动/资金异动候选重点论证资金吸筹、止跌企稳和低吸胜率；突破新高/首板候选才重点论证追涨持续性

## 短线加分项（满足越多越好）
- 5日涨幅 5-15%（刚启动）→ 强势信号
- 均线多头排列 + 量能逐增 → 主力控盘
- DDX 5日持续净流入 → 机构信号
- 近5日量能逐日递增 → 资金持续流入
- MACD动能"扩张" → 上涨动能仍在增强

## 蜡烛图形态核查（在数据包中寻找以下形态）
【多方必查形态】逐一检查数据包是否存在以下买入信号：
- 锤子线（Hammer）：出现在下跌后，下影线≥2×实体，收盘接近高点
- 蜻蜓十字（Dragonfly Doji）：出现在下跌后，只有下影线，开=收=最高
- 多头吞噬（Bullish Engulfing）：下降趋势中，长白实体包裹前一根小黑实体
- 启明星（Morning Star）：黑→星线→白，第三根切入黑实体中点以上
- 刺穿形态（Piercing）：黑→白，第二根切入黑实体中点以上
- 孕线十字底部（Harami Cross Bottom）：长黑后紧跟十字星

【否决项——出现以下任一形态，即使多方论据强也必须降级】
- RSI>75 + 流星线 → 超买共振，立即降级至WATCH或AVOID
- 放量不涨（资金流出但价格小涨）→ 主力出货信号
- 价涨量缩 + 处于相对高位 → 上冲乏力，可能是顶部

## 短线惩罚项（需解释的风险）
- 20日涨幅 >30% → 已涨多，追高风险大
- RSI > 75 或 RSI位置>80 → 超买/近20日高位风险
- 量比 < 0.8 → 缩量，无主力关注
- MACD动能"收缩" → 上涨动能衰减
- 近20日价格位置>85% → 向上空间有限

## 量价背离核查
- 是否有"放量不涨"信号（资金大流出但价格小涨=主力出货）？
- 是否有"缩量不跌"信号（资金小流出但价格小跌=主力控盘）？
- 价涨量缩是否发生在相对高位（>80%位置）？

## 海龟突破核查
- 是否突破20日最高点（短线做多信号）？
- 是否突破55日最高点（更强趋势确认）？
- 是否为假突破（突破后2-3日跌回）？

## 波浪位置核查（如可判断）
- 当前处于推动浪第几浪？
- 是否可能处于第5浪（衰竭信号）？连续3根阳线后出现反转K线=第5浪警告
- 调整浪A/B/C完成后是否有入场机会？

## 江恩回调位核查
- 当前价格是否在关键回调位（33%/50%/67%/75%）附近？
- 是否在重要支撑/压力位获得企稳？

## 空方最新论点
{current_response}

## 你的任务
1. 提出**3-5条**有数据支撑的短线看多论据（具体数字，来源自数据包）
2. 明确给出未来1-3个交易日的上涨触发条件：突破追随、回踩低吸、放量确认或资金继续流入
3. 判断这只股票更适合追随突破、等待确认，还是回踩低吸
4. 针对空方论点进行**逐条反驳**（说清楚空方的哪个假设是错误的，或为什么不是1-3日硬风险）
5. 给出你的结论：当前短线形势对多方是否有利

## 格式要求
- 语气：专业、有数据、简洁
- 不要编造数据，所有数字必须来自数据包
- 反驳时直接点名空方的哪个论点有问题
- 不要泛泛说"长期价值好"；必须落到短线触发、买点类型和风险边界

## 输出
直接输出你的分析内容，不需要JSON格式。"""

def bull_researcher_node(state: InvestDebateState) -> InvestDebateState:
    stock_name = state["stock_name"]
    packet_text = _render_debate_packet(state["debate_packet"])
    current = state.get("current_response", "（空方尚未发言，先陈述你的观点）")

    prompt = BULL_RESEARCHER_PROMPT.format(
        debate_packet=packet_text,
        current_response=current,
        market_context=state.get('market_context', '') or '（暂无市场整体信息）',
    )

    used_holder: List[Optional[str]] = [None]
    response = _call_role_guarded(SYSTEM_PROMPT, prompt, packet=_role_evidence_packet(state), model=state.get("model", DEFAULT_MODEL), timeout=120, max_tokens=ROLE_MAX_TOKENS, actual_model_out=used_holder)
    argument = f"【多方分析师】{response}"

    new_history = state.get("history", "") + f"\n{argument}"
    new_bull_history = state.get("bull_history", "") + f"\n{argument}"

    logger.info(f"[{stock_name}] 多方发言 (count={state.get('count', 0)})")

    node_models_log = list(state.get("node_models_log") or [])
    node_models_log.append({"node": "bull", "model": used_holder[0] or state.get("model") or DEFAULT_MODEL})

    return _update_invest(
        state,
        bull_history=new_bull_history,
        history=new_history,
        current_response=argument,
        count=state.get("count", 0) + 1,
        node_models_log=node_models_log,
    )


TECH_ANALYST_PROMPT = """你是**短线技术形态分析师**，负责解读量化打分结果并向研究总监汇报未来1-3个交易日的技术可买性。

## 量化打分结果（由规则引擎计算）
{tech_score_result}

## 你的任务
1. 解读分项得分，找出最强维度和最弱维度
2. 判断短线趋势、均线、量能、突破/回踩、资金承接是否支持次日进入观察或买入
3. 识别是否有明确技术硬否决：趋势破位、放量不涨/下跌、严重超买叠加反转形态、数据严重缺失
4. 用1-2句话向研究总监说明技术面综合判断，不做长期基本面评价

## 输出格式（简短）
**技术面综合**: [BUY/WATCH/AVOID]
**核心依据**: [1-2句话，说明最强短线信号和适合的买点类型]
**风险提示**: [如有硬否决，说明原因；如无，写"无明显技术硬否决"]
"""


# ── 节点：Bear Researcher ──────────────────────────────────

BEAR_RESEARCHER_PROMPT = """你是**短线空方分析师（Bear Analyst）**，负责为候选股票构建未来1-3个交易日的看空论证。

## 数据包
{debate_packet}

## 市场整体环境
{market_context}

## 辩论规则
1. 先认真阅读数据包，重点关注"入池逻辑"、"短线动量"和与来源策略匹配的风险
2. 多方（Bull Analyst）已发表观点（见下方"多方最新论点"）
3. 你需要：基于数据提出看空论据，并针对多方观点逐条反驳
4. 只攻击未来1-3个交易日会压制股价的风险；长期估值、行业竞争、基本面平庸不能单独作为硬否决
5. 数据证据边界：只允许引用数据可用性合同中 status=ok 的字段；partial/missing/unknown 只能写“无法验证”，不得据此推断

## 短线股空方重点攻击方向
- **高位追涨**：20日涨幅>30%且5日涨幅仍>5%的股票，多方在说"还能涨"时要高度警惕
- **超买风险**：RSI>75的股票，任何利好都可能变成借机出货的理由；RSI位置>80（近20日高位）更是明显警示
- **量能萎缩**：量比<0.8且量能逐日递减的股票，无主力关注的股票容易阴跌
- **均线破坏**：5日均线已转头向下，即使10/20日均线还在多头也要警惕
- **MACD收缩**：MACD动能"收缩"说明上涨动能衰减，反弹难以持续
- **板块轮动**：若板块已连续3日走弱，则个股独立行情难持续
- **低吸失败**：准备启动/资金异动候选若只是资金短暂托底、价格继续破位或无承接，不能因为资金流入就给高仓位

## 短线做空信号（满足任一就值得警惕）
- 20日涨幅 >30% → 获利盘压力大
- RSI > 80 或 RSI位置>80 → 严重超买/近20日高位
- 量能逐日递减 → 主力撤退
- 5日均线已死叉10日均线 → 短线趋势破坏
- MACD动能收缩 → 上涨缺乏持续性
- 近20日价格位置>85%（逼近高点）→ 向上空间有限

## 蜡烛图空方必查形态
- 流星线（Shooting Star）：上升后，上影线≥2×实体
- 墓碑十字（Grave Doji）：上升后，开=收=最低，只有上影线
- 空头吞噬（Bearish Engulfing）：上升趋势中，长黑包裹小白
- 黄昏星（Evening Star）：白→星线→黑，第三根切入白实体中点以下
- 乌云盖顶（Dark Cloud）：白→黑，第二根切入白实体50%以上

【否决项——满足以下任一，即使空方论据强也需谨慎降级】
- RSI<30 + 锤子线 → 超卖共振，可能只是正常反弹
- 缩量不跌 + 处于低位 → 主力控盘，不是真跌
- 放量下跌（价格暴跌+巨量）→ 可能是恐慌抛售底部

## 量价背离核查
- 是否有"放量不涨"信号（资金大流出但价格小涨=主力出货）？
- 是否有"缩量不跌"信号（资金小流出但价格小跌=主力控盘）？
- 价涨量缩是否发生在相对高位（>80%位置）？

## 海龟突破核查
- 是否突破20日最高点（短线做多信号）？
- 是否突破55日最高点（更强趋势确认）？
- 是否为假突破（突破后2-3日跌回）？

## 波浪位置核查（如可判断）
- 当前处于推动浪第几浪？
- 是否可能处于第5浪（衰竭信号）？连续3根阳线后出现反转K线=第5浪警告
- 调整浪A/B/C完成后是否有入场机会？

## 江恩回调位核查
- 当前价格是否在关键回调位（33%/50%/67%/75%）附近？
- 是否在重要支撑/压力位获得企稳？

## 多方最新论点
{current_response}

## 你的任务
1. 提出**3-5条**有数据支撑的短线看空论据（具体数字，来源自数据包）
2. 重点说明：这只股票未来1-3个交易日的追涨风险、低吸失败风险或资金背离失效风险
3. 针对多方论点进行**逐条反驳**（说清楚多方的哪个短线假设站不住脚）
4. 明确区分"硬否决风险"和"普通波动风险"；普通波动风险只能支持WATCH，不能直接推成AVOID
5. 给出你的结论：当前短线形势对空方是否更有利

## 格式要求
- 语气：专业、有数据、简洁
- 不要编造数据，所有数字必须来自数据包
- 反驳时直接点名多方的哪个论点有问题
- 不要用泛泛长期风险替代短线风险；必须说明风险如何在1-3个交易日内体现

## 输出
直接输出你的分析内容，不需要JSON格式。"""

def bear_researcher_node(state: InvestDebateState) -> InvestDebateState:
    stock_name = state["stock_name"]
    packet_text = _render_debate_packet(state["debate_packet"])
    current = state.get("current_response", "（多方尚未发言，先陈述你的空方论点）")

    prompt = BEAR_RESEARCHER_PROMPT.format(
        debate_packet=packet_text,
        current_response=current,
        market_context=state.get('market_context', '') or '（暂无市场整体信息）',
    )

    used_holder: List[Optional[str]] = [None]
    response = _call_role_guarded(SYSTEM_PROMPT, prompt, packet=_role_evidence_packet(state), model=state.get("model", DEFAULT_MODEL), timeout=120, max_tokens=ROLE_MAX_TOKENS, actual_model_out=used_holder)
    argument = f"【空方分析师】{response}"

    new_history = state.get("history", "") + f"\n{argument}"
    new_bear_history = state.get("bear_history", "") + f"\n{argument}"

    logger.info(f"[{stock_name}] 空方发言 (count={state.get('count', 0)})")

    node_models_log = list(state.get("node_models_log") or [])
    node_models_log.append({"node": "bear", "model": used_holder[0] or state.get("model") or DEFAULT_MODEL})

    return _update_invest(
        state,
        bear_history=new_bear_history,
        history=new_history,
        current_response=argument,
        count=state.get("count", 0) + 1,
        node_models_log=node_models_log,
    )
def tech_analyst_node(state: InvestDebateState) -> InvestDebateState:
    """技术形态分析师节点：调用规则引擎计算量化分 + LLM解读"""
    stock_name = state["stock_name"]
    packet = state.get("debate_packet", {})
    
    # ── Step 1: 规则引擎计算量化分（不调LLM）──
    try:
        score_result = compute_tech_score(packet)
        tech_score_json = _safe_json_dumps(score_result, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"[{stock_name}] 规则引擎失败: {e}")
        score_result = {"total_score": 50, "veto": False, "signal": "WATCH", "confidence": 50, "breakdown": {}}
        tech_score_json = "{}"

    signal = score_result.get("signal", "WATCH")
    confidence = score_result.get("confidence", 50)
    raw_total = score_result.get("raw_total", 0)
    max_total = score_result.get("max_total", 100)
    veto_reasons = score_result.get("veto_reasons", []) or []
    tech_validation_state = dict(state)
    tech_validation_state.update({
        "tech_pattern_score": score_result.get("total_score", 50),
        "tech_rule_signal": signal,
        "tech_raw_score": raw_total,
        "tech_max_score": max_total,
        "tech_veto_reasons": veto_reasons,
    })
    
    # ── Step 2: LLM解读量化分（简短）──
    prompt = TECH_ANALYST_PROMPT.format(tech_score_result=tech_score_json)
    tech_flags = list(dict.fromkeys((state.get("data_quality_flags") or []) + (packet.get("data_quality_flags") or [])))
    used_holder: List[Optional[str]] = [None]
    try:
        response = _call_role_guarded(
            SYSTEM_PROMPT,
            prompt,
            packet=_role_evidence_packet(tech_validation_state),
            model=state.get("model", TECH_ANALYST_MODEL),
            timeout=60,
            max_tokens=ROLE_MAX_TOKENS,
            actual_model_out=used_holder,
        )
        if not response:
            raise RuntimeError("技术形态分析师LLM返回空响应")
    except Exception as e:
        logger.warning(f"[{stock_name}] Tech Analyst LLM解读失败: {e}")
        response = f"技术面信号: {score_result.get('signal', 'WATCH')}，置信度: {score_result.get('confidence', 50)}"
        if "TECH_ANALYSIS_MISSING" not in tech_flags:
            tech_flags.append("TECH_ANALYSIS_MISSING")
    
    verdict = f"【技术形态分析师】{response}"
    
    new_history = state.get("history", "") + f"\n{verdict}"
    node_models_log = list(state.get("node_models_log") or [])
    node_models_log.append({
        "node": "tech",
        "model": used_holder[0] or "rule-fallback",
    })
    
    logger.info(f"[{stock_name}] Tech Analyst: {signal}={confidence}%, raw={raw_total}/{max_total}, veto={veto_reasons}")

    return _update_invest(
        state,
        tech_analyst_verdict=verdict,
        tech_pattern_score=score_result.get("total_score", 50),
        tech_signals_summary=response,
        tech_rule_signal=signal,
        tech_raw_score=raw_total,
        tech_max_score=max_total,
        tech_veto_reasons=veto_reasons,
        data_quality_flags=tech_flags,
        node_models_log=node_models_log,
        history=new_history,
        current_response=verdict,
    )


# ── 节点：Research Manager ─────────────────────────────────

RESEARCH_MANAGER_PROMPT = """你是**短线研究总监（Research Manager）**，负责综合多方/空方的辩论，输出未来1-3个交易日的新开仓方向建议。

## 技术形态量化打分结果（来自规则引擎，非LLM主观评分）
{tech_score_summary}

## 完整辩论历史
{history}

## 技术形态分析师结果
{tech_analyst_result}

## 你的任务
综合分析辩论双方的观点和技术形态评分，给出**明确的短线方向建议**，并决定辩论是否需要继续。
你不是长期价值审查员；你的任务是判断候选股是否值得继续进入风险评估和Top5排序。
普通估值、长期竞争、基本面平庸只能作为降级参考，不能单独把短线机会判为AVOID。
如果多方或空方使用了缺失/partial/unknown字段作为论据，必须指出并降低该方论据权重。

## 技术形态否决层（必须执行）
即使多空论据看似均衡，以下情况必须调整裁决：

【必须降级 BUY→WATCH】
- 量化总分≥70，但出现"放量不涨"（DDX大负+价格平）→ 主力出货
- 量化总分≥70，但第5浪警告 + 空头形态 → 衰竭确认

【必须降级 WATCH/BUY→AVOID】
- 量化总分<35 → 技术面整体偏弱
- 量化总分≥50，RSI>75 + 流星线/墓碑十字 → 超买共振
- 量化总分任何值，PE>100 + ROE<0 且缺少短线资金/技术共振 → 财务硬伤
- 量化总分任何值，ST/*ST → 高风险否决

【趋势强烈信号（可提升置信度）】
- 量化总分≥75 + 55日突破 → BUY置信度+15
- 量化总分≥70 + 吸筹完成 + RSI<50 → BUY置信度+10

## 评分标准（3档）
- **BUY**: 多方论据明显更强，技术/资金/题材/策略回测支持未来1-3日上涨机会
- **WATCH**: 具备做多线索，但买点、量能、持续性或普通风险仍需盘中确认
- **AVOID**: 存在明确硬否决，或核心看多逻辑被数据证伪

## 决策规则
- count >= {max_rounds} × 2 → 辩论结束，进入风险评估阶段
- count < {max_rounds} × 2 → 可以让双方继续辩论一轮
- 若多方和空方各有道理但没有硬否决，优先给 WATCH，不要直接 AVOID
- 若技术分较高且风险只是一般性长期担忧，不能大幅压低短线方向

## 输出格式
直接输出以下4行，不使用JSON，不加标题、表格、列表或额外段落。裁决理由最多2句话，总字数控制在220字以内：

**综合判断**: [BUY / WATCH / AVOID]
**裁决理由**: [最多2句话，说明哪方短线论点更有说服力，为什么]
**下一步**: [继续辩论 / 进入风险评估]
**信心度**: [0-100，表示你对裁决的确信程度]"""


def _compact_research_manager_response(response: str) -> str:
    """研究总监是中间文本节点；缺字段时本地补成稳定短格式。"""
    text = (response or "").strip()
    required = ("综合判断", "裁决理由", "下一步", "信心度")
    has_confidence_number = bool(re.search(r"信心度\**\s*[：:]\s*\d{1,3}", text))
    if text and all(k in text for k in required) and has_confidence_number and len(text) <= 900:
        return text

    signal = "WATCH"
    m = re.search(r"\b(BUY|WATCH|AVOID)\b", text, flags=re.IGNORECASE)
    if m:
        signal = m.group(1).upper()
    reason_match = re.search(r"裁决理由\**\s*[：:]\s*(.+?)(?:\n\s*\*\*?下一步|\n\s*\*\*?信心度|$)", text, flags=re.DOTALL)
    reason = reason_match.group(1).strip() if reason_match else text
    reason = re.sub(r"\s+", " ", reason).strip(" -*：:")
    if len(reason) > 220:
        reason = reason[:220].rstrip() + "..."
    if not reason:
        reason = "模型中间裁决文本不完整，按观望处理并进入风险评估。"
    confidence_match = re.search(r"信心度\**\s*[：:]\s*(\d{1,3})", text)
    confidence = int(confidence_match.group(1)) if confidence_match else 50
    confidence = max(0, min(100, confidence))
    return (
        f"**综合判断**: {signal}\n"
        f"**裁决理由**: {reason}\n"
        f"**下一步**: 进入风险评估\n"
        f"**信心度**: {confidence}"
    )


def research_manager_node(state: InvestDebateState) -> InvestDebateState:
    stock_name = state["stock_name"]
    history = state.get("history", "（暂无辩论历史）")
    tech_result = state.get("tech_signals_summary") or "（暂无技术形态分析）"
    count = state.get("count", 0)
    max_rounds = state.get("max_rounds", MAX_DEBATE_ROUNDS)

    # 新增：构建tech_score_summary供Research Manager参考
    tech_score = state.get("tech_pattern_score", 50)  # 否决后0-100标准化分
    tech_raw_score = state.get("tech_raw_score", tech_score)
    tech_max_score = state.get("tech_max_score", 100)
    tech_rule_signal = state.get("tech_rule_signal", "WATCH")
    tech_veto_reasons = state.get("tech_veto_reasons", []) or []
    veto = any([
        "RSI>75" in tech_result and "流星" in tech_result,
        "放量不涨" in tech_result,
        "第5浪" in tech_result,
        "PE>100" in tech_result,
        "ROE<0" in tech_result,
        "ST" in state.get("debate_packet", {}).get("name", ""),
    ])
    if tech_veto_reasons:
        veto = True
    tech_score_summary = (
        f"规则技术信号: {tech_rule_signal}\n"
        f"否决后技术分: {tech_score}/100\n"
        f"原始技术分: {tech_raw_score}/{tech_max_score}\n"
        f"否决检查: {'有：' + '；'.join(map(str, tech_veto_reasons)) if veto else '无'}\n"
        f"LLM解读: {tech_result[-200:] if tech_result else '无'}"
    )

    prompt = RESEARCH_MANAGER_PROMPT.format(
        history=history,
        max_rounds=max_rounds,
        tech_analyst_result=tech_result,
        tech_score_summary=tech_score_summary,
    )

    used_holder: List[Optional[str]] = [None]
    response = _call_role_guarded(SYSTEM_PROMPT, prompt, packet=_role_evidence_packet(state), model=state.get("model", DEFAULT_MODEL), timeout=120, max_tokens=ROLE_MAX_TOKENS, actual_model_out=used_holder)
    response = _compact_research_manager_response(response)
    logger.info(f"[{stock_name}] 研究总监原始响应 (len={len(response)}): {response[:300]}")
    logger.info(f"[{stock_name}] 研究总监裁决: {response[:100]}...")

    node_models_log = list(state.get("node_models_log") or [])
    node_models_log.append({"node": "judge", "model": used_holder[0] or state.get("model") or DEFAULT_MODEL})

    return _update_invest(
        state,
        research_plan=response,
        current_response=response,
        count=count,
        node_models_log=node_models_log,
    )


# ── 条件路由：投资辩论阶段 ────────────────────────────────

def route_invest_debate(state: InvestDebateState) -> str:
    count = state.get("count", 0)
    max_rounds = state.get("max_rounds", MAX_DEBATE_ROUNDS)
    last_speaker = state.get("current_response", "")

    if count < 2 * max_rounds:
        if "【多方分析师】" in last_speaker:
            return "bear_researcher"
        else:
            return "bull_researcher"
    
    # 达到最大轮次后，进入技术形态分析
    if not state.get("tech_analyst_verdict"):
        return "tech_analyst"
    
    return "risk_debate"


# ── 节点：Aggressive Risk Analyst ─────────────────────────

AGGRESSIVE_ANALYST_PROMPT = """你是**短线激进风险分析师**，代表高风险高收益的立场，专门评估未来1-3个交易日是否值得积极做多。

## 研究总监投资建议
{research_plan}

## 完整辩论历史
{history}

## 重要前提
- **假设当前没有任何持仓**
- 你的任务是判断：**这只候选股未来1-3个交易日是否值得积极新开仓做多**
- 不要出现"已有持仓如何处理"的分析
- 不要输出具体仓位比例；实际仓位由系统根据最终 signal + confidence 分档决定

## 你的任务
作为激进风险分析师，你需要：
1. 列举支持短线做多的**最有力论据**（3条）
2. 指出潜在风险，但说明为什么这些风险不是未来1-3日硬否决
3. 给出积极做多结论：BUY倾向、WATCH倾向或不支持激进做多

## 语气
- 自信、有数据、敢于下结论
- 不要回避风险，但要说明为什么值得冒险
- 只讨论短线有效机会和风险，不做长期价值宣传

## 输出
直接输出你的分析内容。"""

def aggressive_analyst_node(state: RiskDebateState) -> RiskDebateState:
    stock_name = state["stock_name"]
    history = state.get("history", "")
    plan = state.get("research_plan", "")

    prompt = AGGRESSIVE_ANALYST_PROMPT.format(
        research_plan=plan,
        history=history,
    )

    used_holder: List[Optional[str]] = [None]
    response = _call_role_guarded(SYSTEM_PROMPT, prompt, packet=_role_evidence_packet(state), model=state.get("model", DEFAULT_MODEL), timeout=120, max_tokens=ROLE_MAX_TOKENS, actual_model_out=used_holder)
    argument = f"【激进风险分析师】{response}"

    new_history = state.get("history", "") + f"\n{argument}"
    new_aggressive = state.get("aggressive_history", "") + f"\n{argument}"

    logger.info(f"[{stock_name}] 激进风险分析师发言")

    node_models_log = list(state.get("node_models_log") or [])
    node_models_log.append({"node": "aggressive", "model": used_holder[0] or state.get("model") or DEFAULT_MODEL})

    return _update_risk(
        state,
        aggressive_history=new_aggressive,
        history=new_history,
        latest_speaker="Aggressive",
        count=state.get("count", 0) + 1,
        node_models_log=node_models_log,
    )


# ── 节点：Conservative Risk Analyst ───────────────────────

CONSERVATIVE_ANALYST_PROMPT = """你是**短线保守风险分析师**，代表谨慎防守的立场，专门识别未来1-3个交易日会导致买入失败的风险。

## 激进分析师观点
{aggressive_view}

## 完整辩论历史
{history}

## 重要前提
- **假设当前没有任何持仓**
- 你的任务是判断：**这只候选股未来1-3个交易日是否值得新买入做多**
- 不要出现"已有持仓如何处理"的分析
- 不要输出具体仓位比例；实际仓位由系统根据最终 signal + confidence 分档决定
- 不能用长期估值、行业竞争、基本面平庸直接否决短线机会，除非它们会在1-3日内明显压制股价

## 你的任务
作为保守风险分析师，你需要：
1. 认真回应激进分析师的论点
2. 指出**最被你关注的1-2个短线风险点**（为什么这些风险被低估了）
3. 明确风险等级：硬否决、降为WATCH、或只是盘中买点风险

## 语气
- 谨慎、数据驱动、强调风险控制
- 不要完全否定激进观点，但要说明你的担忧
- 若没有硬否决，应建议等待盘中确认，而不是直接回避

## 输出
直接输出你的分析内容。"""

def conservative_analyst_node(state: RiskDebateState) -> RiskDebateState:
    stock_name = state["stock_name"]
    history = state.get("history", "")
    aggressive = state.get("aggressive_history", "")

    prompt = CONSERVATIVE_ANALYST_PROMPT.format(
        aggressive_view=aggressive,
        history=history,
    )

    used_holder: List[Optional[str]] = [None]
    response = _call_role_guarded(SYSTEM_PROMPT, prompt, packet=_role_evidence_packet(state), model=state.get("model", DEFAULT_MODEL), timeout=120, max_tokens=ROLE_MAX_TOKENS, actual_model_out=used_holder)
    argument = f"【保守风险分析师】{response}"

    new_history = state.get("history", "") + f"\n{argument}"
    new_conservative = state.get("conservative_history", "") + f"\n{argument}"

    logger.info(f"[{stock_name}] 保守风险分析师发言")

    node_models_log = list(state.get("node_models_log") or [])
    node_models_log.append({"node": "conservative", "model": used_holder[0] or state.get("model") or DEFAULT_MODEL})

    return _update_risk(
        state,
        conservative_history=new_conservative,
        history=new_history,
        latest_speaker="Conservative",
        count=state.get("count", 0) + 1,
        node_models_log=node_models_log,
    )


# ── 节点：Neutral Risk Analyst ────────────────────────────

NEUTRAL_ANALYST_PROMPT = """你是**短线中性风险分析师**，负责平衡双方观点，找出未来1-3个交易日最客观的新开仓结论。

## 激进分析师观点
{aggressive_view}

## 保守分析师观点
{conservative_view}

## 完整辩论历史
{history}

## 重要前提
- **假设当前没有任何持仓**
- 你的任务是判断：**这只候选股未来1-3个交易日是否值得新买入做多**
- 不要出现"已有持仓如何处理"的分析
- 不要输出具体仓位比例；实际仓位由系统根据最终 signal + confidence 分档决定

## 你的任务
作为中性分析师，你需要：
1. 指出激进和保守双方**各自最有力的一个论点**
2. 给出一个**平衡的、客观的**短线风险评估
3. 判断更合理的结论：BUY、WATCH 或 AVOID，并说明是否存在硬否决
4. 若没有硬否决但买点不够清晰，优先给 WATCH，而不是直接 AVOID

## 语气
- 客观、冷静、不偏不倚
- 强调数据和逻辑，不情绪化
- 区分短线有效风险和长期泛风险

## 输出
直接输出你的分析内容。"""

def neutral_analyst_node(state: RiskDebateState) -> RiskDebateState:
    stock_name = state["stock_name"]
    history = state.get("history", "")
    aggressive = state.get("aggressive_history", "")
    conservative = state.get("conservative_history", "")

    prompt = NEUTRAL_ANALYST_PROMPT.format(
        aggressive_view=aggressive,
        conservative_view=conservative,
        history=history,
    )

    used_holder: List[Optional[str]] = [None]
    response = _call_role_guarded(SYSTEM_PROMPT, prompt, packet=_role_evidence_packet(state), model=state.get("model", DEFAULT_MODEL), timeout=120, max_tokens=ROLE_MAX_TOKENS, actual_model_out=used_holder)
    # 日志：记录中性分析师原始响应
    logger.info(f"[{stock_name}] 中性分析师原始响应 (len={len(response)}): {response[:300]}")
    argument = f"【中性风险分析师】{response}"

    new_history = state.get("history", "") + f"\n{argument}"
    new_neutral = state.get("neutral_history", "") + f"\n{argument}"

    logger.info(f"[{stock_name}] 中性风险分析师发言")

    node_models_log = list(state.get("node_models_log") or [])
    node_models_log.append({"node": "neutral", "model": used_holder[0] or state.get("model") or DEFAULT_MODEL})

    return _update_risk(
        state,
        neutral_history=new_neutral,
        history=new_history,
        latest_speaker="Neutral",
        count=state.get("count", 0) + 1,
        node_models_log=node_models_log,
    )


# ── 条件路由：风险辩论阶段 ────────────────────────────────

def route_risk_debate(state: RiskDebateState) -> str:
    """
    风险辩论阶段的路由逻辑（TradingAgents-style）：
    - count >= 3 × RISK_DEBATE_ROUNDS → 结束辩论
    - 否则根据刚发言的分析师（latest_speaker）决定下一位
    """
    count = state.get("count", 0)
    speaker = state.get("latest_speaker", "")

    if count >= 3 * RISK_DEBATE_ROUNDS:
        return "portfolio_manager"

    if speaker == "Aggressive":
        return "conservative_analyst"
    elif speaker == "Conservative":
        return "neutral_analyst"
    else:
        return "aggressive_analyst"


# ── 节点：Portfolio Manager ────────────────────────────────

PORTFOLIO_MANAGER_PROMPT = """你是**短线机会基金经理（Portfolio Manager）**，负责综合所有分析师的观点，对"这只股票未来1-3个交易日是否值得新买入（做多）"给出最终判断。

## 研究总监投资建议
{research_plan}

## 风险分析师辩论
{history}

## 近期复盘记忆
{selection_memory}

## 当前股票数据可用性合同
{evidence_contract}

## 可引用证据字段
{available_evidence_refs}

## 本地证券知识规则命中
{knowledge_rules}

## 证据约束
- reason 中每个核心判断必须能对应 evidence_refs 中至少一个字段。
- evidence_refs 只能引用上方可引用证据字段或数据包中真实存在且 status=ok 的字段。
- evidence_refs 不得为空，且每项 value 必须逐字对应数据包中的真实原值，不能只写方向性概括。
- 类别 status=partial 时仅可引用 field_status=ok 的具体字段；field_status=missing 以及类别 missing/unknown 只能写“无法验证”。
- 如果发现某个分析师使用了缺失字段，应在 reason 中说明已降低该论据权重。
- unsupported_claims 应为空；若无法为空，请降低 buy_score 与 confidence。

## 重要前提
- **假设当前没有任何持仓**
- 你的任务是判断：**这只候选股是否值得进入短线新开仓观察/买入池**
- 不要出现"减仓"、"持有"、"已持仓"、"若已有仓位"等基于已有持仓的管理建议
- 系统会另行用技术量化分、池内分、资金流、次日可买性和已成熟的历史复盘规则计算最终做多分；当前股票不使用向后回放收益。你给出的 buy_score 仅作为短线机会修正依据。
- 你的角色不是长期价值审查员，而是短线机会裁判：在候选池内，优先识别未来1-3个交易日的上涨机会，同时校准明显风险。
- 基本面平庸不能单独否决短线机会；只有短期会直接压制股价的基本面/消息风险才应明显扣分。
- 只要短线技术、资金、题材催化、策略回测中至少两项形成有效共振，应倾向 BUY 或 WATCH；只有出现明确硬否决项才给 AVOID。

## 分数口径（必须严格区分）

### buy_score：未来1-3个交易日短线做多吸引力
| buy_score | 含义 |
|-----------|------|
| 85-100 | 强做多，技术趋势、资金流、题材催化、策略回测至少两项强共振，且没有明显硬伤 |
| 70-84 | 可买入，短线胜率较高，风险可通过盘中买点控制 |
| 55-69 | 可观察，具备做多线索但买点、量能、风险或持续性仍需盘中确认 |
| <55 | 不适合进入短线买入池，通常应存在明确硬否决或核心看多逻辑被证伪 |

### confidence：你对 signal 与 buy_score 的可靠程度
- confidence 不是仓位建议，也不是最终Top5排序分；它表示你对本次短线裁决是否可靠的把握。
- 短线票即使波动较大，只要买入逻辑清晰、证据一致，也可以给较高 confidence。
- AVOID 也可以有高 confidence，表示你很确定不该买；但 AVOID 必须基于明确硬否决。

### signal：由 buy_score 决定
- BUY: buy_score >= 70
- WATCH: 55 <= buy_score < 70
- AVOID: buy_score < 55，或存在明确硬否决项

### position_ratio
- 兼容字段，保留输出即可；早报不展示，盘中实际买入仓位不使用它。
- AVOID 输出 0.0；BUY/WATCH 可按你的裁决保守填写 0.1-0.4。

## 短线加减分规则
- 技术趋势走强、放量突破、均线多头、资金净流入、题材催化、策略回测有效，应提高 buy_score。
- 量化分高但风险分析师只指出一般性估值/基本面瑕疵时，不要大幅压低短线 buy_score。
- 资金流强但价格滞涨、冲高回落、放量下跌，应降低 buy_score。
- 重大负面新闻、趋势破位、资金明显出逃、核心看多逻辑被证伪、数据严重缺失无法判断，才应给 AVOID。

## 输出要求
只输出一个 JSON 对象。第一个字符必须是 {{，最后一个字符必须是 }}。
不要输出 markdown、代码块、解释文字、前缀或后缀。
字段：
{pm_json_fields}
"""

PORTFOLIO_MANAGER_THINKING_PROMPT = """你是**短线机会基金经理（Portfolio Manager）**，负责综合所有分析师的观点，对"这只股票未来1-3个交易日是否值得新买入（做多）"给出最终判断。

## 研究总监投资建议
{research_plan}

## 风险分析师辩论
{history}

## 近期复盘记忆
{selection_memory}

## 当前股票数据可用性合同
{evidence_contract}

## 可引用证据字段
{available_evidence_refs}

## 本地证券知识规则命中
{knowledge_rules}

## 证据约束
- reason 中每个核心判断必须能对应 evidence_refs 中至少一个字段。
- evidence_refs 只能引用上方可引用证据字段或数据包中真实存在且 status=ok 的字段。
- evidence_refs 不得为空，且每项 value 必须逐字对应数据包中的真实原值，不能只写方向性概括。
- 类别 status=partial 时仅可引用 field_status=ok 的具体字段；field_status=missing 以及类别 missing/unknown 只能写“无法验证”。
- missing_data_used 只能填写 data_contract 中实际 status!=ok 的大类，允许值仅为 kline、money_flow、financial、sector、news；不得填写公告原文、龙虎榜、盘中数据、指标名等细分项。
- 如果发现某个分析师使用了缺失字段，应在 reason 中说明已降低该论据权重。
- unsupported_claims 应为空；若无法为空，请降低 buy_score 与 confidence。

## 重要前提
- **假设当前没有任何持仓**
- 你的任务是判断：**这只候选股是否值得进入短线新开仓观察/买入池**
- 不要出现"减仓"、"持有"、"已持仓"、"若已有仓位"等基于已有持仓的管理建议
- 系统会另行用技术量化分、池内分、资金流、次日可买性和已成熟的历史复盘规则计算最终做多分；当前股票不使用向后回放收益。你给出的 buy_score 仅作为短线机会修正依据。
- 你的角色不是长期价值审查员，而是短线机会裁判：在候选池内，优先识别未来1-3个交易日的上涨机会，同时校准明显风险。
- 基本面平庸不能单独否决短线机会；只有短期会直接压制股价的基本面/消息风险才应明显扣分。
- 只要短线技术、资金、题材催化、策略回测中至少两项形成有效共振，应倾向 BUY 或 WATCH；只有出现明确硬否决项才给 AVOID。

## 分数口径（必须严格区分）

### buy_score：未来1-3个交易日短线做多吸引力
- 85-100：强做多，技术趋势、资金流、题材催化、策略回测至少两项强共振，且没有明显硬伤
- 70-84：可买入，短线胜率较高，风险可通过盘中买点控制
- 55-69：可观察，具备做多线索但买点、量能、风险或持续性仍需盘中确认
- <55：不适合进入短线买入池，通常应存在明确硬否决或核心看多逻辑被证伪

### confidence：你对 signal 与 buy_score 的可靠程度
- confidence 不是仓位建议，也不是最终Top5排序分；它表示你对本次短线裁决是否可靠的把握。
- 短线票即使波动较大，只要买入逻辑清晰、证据一致，也可以给较高 confidence。
- AVOID 也可以有高 confidence，表示你很确定不该买；但 AVOID 必须基于明确硬否决。

### signal：由 buy_score 决定
- BUY: buy_score >= 70
- WATCH: 55 <= buy_score < 70
- AVOID: buy_score < 55，或存在明确硬否决项

### position_ratio
- 兼容字段，保留输出即可；早报不展示，盘中实际买入仓位不使用它。
- AVOID 输出 0.0；BUY/WATCH 可按你的裁决保守填写 0.1-0.4。

## 短线加减分规则
- 技术趋势走强、放量突破、均线多头、资金净流入、题材催化、策略回测有效，应提高 buy_score。
- 量化分高但风险分析师只指出一般性估值/基本面瑕疵时，不要大幅压低短线 buy_score。
- 资金流强但价格滞涨、冲高回落、放量下跌，应降低 buy_score。
- 重大负面新闻、趋势破位、资金明显出逃、核心看多逻辑被证伪、数据严重缺失无法判断，才应给 AVOID。

## 输出格式
先用中文写出完整分析思路（2-4段话），综合各方观点给出判断逻辑。
然后最后一行必须按以下格式输出结构化决策：
{pm_text_fields}
evidence_refs: [{{"field":"kline_summary.ma_system","value":"多头排列","claim":"均线趋势偏强"}}]
missing_data_used: []
unsupported_claims: []"""


PORTFOLIO_MANAGER_TEXT_PROMPT = """你是**短线机会基金经理（Portfolio Manager）**，负责综合所有分析师的观点，对"这只股票未来1-3个交易日是否值得新买入（做多）"给出最终判断。

## 研究总监投资建议
{research_plan}

## 风险分析师辩论
{history}

## 重要前提
- 假设当前没有任何持仓
- 你的任务是判断：这只候选股是否值得进入短线新开仓观察/买入池
- 不要出现"减仓"、"持有"、"已持仓"、"若已有仓位"等基于已有持仓的管理建议
- 系统会另行用技术量化分、池内分、资金流、次日可买性和已成熟的历史复盘规则计算最终做多分；当前股票不使用向后回放收益。你给出的 buy_score 仅作为短线机会修正依据。
- 优先判断未来1-3个交易日的短线机会，不做长期价值审查。
- 基本面平庸不能单独否决短线机会；只有重大负面、趋势破位、资金明显出逃、核心看多逻辑被证伪或数据严重缺失时才给 AVOID。
- 只要短线技术、资金、题材催化、策略回测中至少两项有效共振，应倾向 BUY 或 WATCH。

## 输出格式
输出纯文本，不要输出 JSON，不要 markdown 代码块。
必须包含以下字段行：
最终信号: BUY / WATCH / AVOID
做多分: 0-100
置信度: 0-100
新开仓仓位上限: 0%-40%
allow_direct_buy: true/false
needs_intraday_confirmation: true/false
entry_condition: 盘中买入条件
block_buy_reason: 不能直接BUY的原因，没有则空
核心理由: 2-3句话"""


def _parse_portfolio_manager_text(text: str) -> Optional[dict]:
    """Parse PM output from thinking-mode text response."""
    if not text:
        return None
    # ──优先尝试 JSON 解析（GPT-5.6 Sol 等模型可能返回 JSON 格式）──
    import json
    json_text = text.strip()
    # 尝试提取 JSON 对象（可能有前缀/后缀文字）
    json_start = json_text.find('{')
    if json_start >= 0:
        json_str = json_text[json_start:]
        # 找到匹配的结束 brace
        brace_count = 0
        json_end = -1
        for i, c in enumerate(json_str):
            if c == '{':
                brace_count += 1
            elif c == '}':
                brace_count -= 1
                if brace_count == 0:
                    json_end = i + 1
                    break
        if json_end > 0:
            try:
                obj = json.loads(json_str[:json_end])
                sig = obj.get("signal", "").upper()
                if sig in {"BUY", "WATCH", "AVOID"}:
                    try:
                        conf = int(obj.get("confidence", 0))
                    except (TypeError, ValueError):
                        conf = 0
                    buy_score = obj.get("buy_score")
                    if buy_score is None:
                        buy_score = _fallback_buy_score(sig, conf)
                    try:
                        buy_score = int(buy_score)
                    except (TypeError, ValueError):
                        buy_score = _fallback_buy_score(sig, conf)
                    sig = _signal_from_buy_score(buy_score)
                    ratio = obj.get("position_ratio", 0.0)
                    try:
                        ratio = float(ratio)
                    except (TypeError, ValueError):
                        ratio = 0.0
                    if ratio > 1.0:
                        ratio = ratio / 100.0
                    reason = obj.get("reason", "")
                    return {
                        "signal": sig,
                        "buy_score": max(0, min(100, int(buy_score))),
                        "confidence": max(0, min(100, conf)),
                        "position_ratio": round(ratio, 4),
                        "allow_direct_buy": obj.get("allow_direct_buy"),
                        "needs_intraday_confirmation": obj.get("needs_intraday_confirmation"),
                        "entry_condition": obj.get("entry_condition", ""),
                        "block_buy_reason": obj.get("block_buy_reason", ""),
                        "reason": reason,
                    }
            except (json.JSONDecodeError, ValueError, KeyError):
                pass  # JSON 解析失败，继续尝试正则

    # ──正则解析（原始逻辑，兼容纯文本格式）──
    cleaned = re.sub(r'PortfolioManagerOutput\s*\([^)]*\)', '', text)
    m = re.search(r"(?:signal|最终信号)\s*[:：]\s*(\w+)", cleaned, re.IGNORECASE)
    signal = m.group(1).strip().upper() if m else None
    if signal not in {"BUY", "WATCH", "AVOID"}:
        return None
    m = re.search(r"(?:buy_score|做多分|做多评分)\s*[:：]\s*([0-9]+)", cleaned, re.IGNORECASE)
    buy_score = int(m.group(1)) if m else None
    m = re.search(r"(?:confidence|置信度)\s*[:：]\s*([0-9]+)", cleaned, re.IGNORECASE)
    confidence = int(m.group(1)) if m else 0
    if buy_score is None:
        buy_score = _fallback_buy_score(signal, confidence)
    signal = _signal_from_buy_score(buy_score)
    m = re.search(r"(?:新开仓仓位上限|position_ratio)\s*[:：]\s*([0-9.]+)", cleaned, re.IGNORECASE)
    position_ratio = float(m.group(1)) if m else 0.0
    if position_ratio > 1.0:
        position_ratio = position_ratio / 100.0
    entry_m = re.search(r"(?:entry_condition|入场条件)\s*[:：]\s*(.+?)(?:\n|$)", cleaned, re.IGNORECASE)
    block_m = re.search(r"(?:block_buy_reason|阻断理由)\s*[:：]\s*(.+?)(?:\n|$)", cleaned, re.IGNORECASE)
    m = re.search(r"(?:reason|核心理由)\s*[:：]\s*(.+?)(?:\n|$)", cleaned, re.IGNORECASE)
    reason = m.group(1).strip() if m else ""
    if re.search(r"PortfolioManagerOutput|\b\w+\s*\(.*\)", reason):
        reason = re.sub(r"PortfolioManagerOutput[^,\n]*", "", reason).strip() or ""

    def _regex_bool(field: str):
        match = re.search(rf"{re.escape(field)}\s*[:：]\s*(true|false|1|0|yes|no)", cleaned, re.IGNORECASE)
        if not match:
            return None
        return match.group(1).lower() in {"true", "1", "yes"}

    return {
        "signal": signal,
        "buy_score": max(0, min(100, buy_score)),
        "confidence": max(0, min(100, confidence)),
        "position_ratio": round(position_ratio, 4),
        "allow_direct_buy": _regex_bool("allow_direct_buy"),
        "needs_intraday_confirmation": _regex_bool("needs_intraday_confirmation"),
        "entry_condition": entry_m.group(1).strip() if entry_m else "",
        "block_buy_reason": block_m.group(1).strip() if block_m else "",
        "reason": reason,
    }

def _repair_portfolio_json(raw_text: str, call_structured, PortfolioManagerOutput):
    """Repair a text-only PM decision into schema JSON without re-judging."""
    repair_prompt = f"""下面是一只股票的基金经理最终裁决文本。你的任务只是把原文中的裁决抽取成 JSON，不要重新分析、不要改变判断、不要新增事实。

如果原文没有明确给出某个字段，返回：
{{"signal":"WATCH","buy_score":55,"confidence":0,"position_ratio":0.0,"allow_direct_buy":false,"needs_intraday_confirmation":true,"entry_condition":"盘中重新确认","block_buy_reason":"最终裁决文本缺少必要字段","reason":"最终裁决文本缺少必要字段，JSON修复失败，转为观察候选。","evidence_refs":[],"missing_data_used":[],"unsupported_claims":["最终裁决文本缺少必要字段"]}}

原文：
{raw_text}

只输出一个 JSON 对象，字段要求：
{pm_json_field_instructions()}
buy_score 和 confidence 必须是0-100整数，position_ratio 必须是 0.0-1.0。missing_data_used 只能填写实际缺失的大类；evidence_refs 无法从原文中抽取时，必须把原因写入 unsupported_claims，不能伪造证据。"""
    return call_structured(
        repair_prompt,
        PortfolioManagerOutput,
        model="openai/gpt-5.6-sol",
        fallback_model="minimax-portal/MiniMax-M3",
        thinking_budget=0,
        max_tokens=1000,
    )

def _repair_empty_portfolio_structured(
    original_prompt: str,
    call_openai_responses,
    PortfolioManagerOutput,
    model: str,
    reasoning_effort: str,
) -> Optional[dict]:
    """Give the current OpenAI PM model one concise retry after empty JSON."""
    repair_prompt = f"""上一轮基金经理结构化裁决没有返回可解析 JSON。请重新基于同一事实材料裁决，不要新增事实，不要输出解释文字，只输出符合 schema 的 JSON。

必须包含字段：
{pm_json_field_instructions()}

原始事实材料：
{original_prompt}
"""
    return call_openai_responses(
        prompt=repair_prompt,
        model=model,
        schema=PortfolioManagerOutput,
        timeout=PORTFOLIO_MANAGER_TIMEOUT,
        max_tokens=4096,
        reasoning_effort=reasoning_effort,
    )


def _coerce_portfolio_fields(parsed: dict, default_ratio: float = 0.0) -> tuple[str, int, int, float, str]:
    sig = str(parsed.get("signal", "WATCH")).upper()
    if sig not in {"BUY", "WATCH", "AVOID"}:
        sig = "WATCH"
    try:
        conf = int(parsed.get("confidence", 0))
    except (TypeError, ValueError):
        conf = 0
    conf = max(0, min(100, conf))
    try:
        buy_score = int(parsed.get("buy_score", _fallback_buy_score(sig, conf)))
    except (TypeError, ValueError):
        buy_score = _fallback_buy_score(sig, conf)
    buy_score = max(0, min(100, buy_score))
    sig = _signal_from_buy_score(buy_score)
    ratio = _normalize_position_ratio(parsed.get("position_ratio", default_ratio), default=default_ratio)
    reason = str(parsed.get("reason", "")).strip()
    return sig, buy_score, conf, ratio, reason




def _coerce_bool_field(value, default=None):
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return default
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "是", "允许", "可直接买入"}:
        return True
    if text in {"false", "0", "no", "n", "否", "不允许", "需要确认"}:
        return False
    return default


def _default_execution_gate(sig: str, buy_score: int) -> tuple[bool, bool, str, str]:
    sig = str(sig or "WATCH").upper()
    if sig == "BUY" and buy_score >= 70:
        return True, False, "开盘强势或盘中强势可买", ""
    if sig == "WATCH" and buy_score >= 55:
        return False, True, "盘中放量突破或回踩承接确认", "需要盘中确认"
    return False, True, "", "信号不足或存在风险"


def _extract_execution_gate(obj, sig: str, buy_score: int):
    allow_default, confirm_default, entry_default, block_default = _default_execution_gate(sig, buy_score)
    allow = _coerce_bool_field(getattr(obj, "allow_direct_buy", None) if not isinstance(obj, dict) else obj.get("allow_direct_buy"), allow_default)
    confirm = _coerce_bool_field(getattr(obj, "needs_intraday_confirmation", None) if not isinstance(obj, dict) else obj.get("needs_intraday_confirmation"), confirm_default)
    entry = getattr(obj, "entry_condition", None) if not isinstance(obj, dict) else obj.get("entry_condition")
    block = getattr(obj, "block_buy_reason", None) if not isinstance(obj, dict) else obj.get("block_buy_reason")
    return bool(allow), bool(confirm), str(entry or entry_default or "")[:160], str(block or block_default or "")[:160]

def _trip_portfolio_primary(reason: str) -> None:
    global _pm_primary_broken, _pm_primary_failure_reason
    _pm_primary_broken = True
    _pm_primary_failure_reason = str(reason)[:300]


def _portfolio_primary_is_available() -> bool:
    return (
        PORTFOLIO_MANAGER_PRIMARY_ENABLED
        and bool(PORTFOLIO_MANAGER_PRIMARY_MODEL)
        and not _pm_primary_broken
    )


def _trip_secondary(reason: str) -> None:
    global _secondary_broken, _secondary_failure_reason
    _secondary_broken = True
    _secondary_failure_reason = str(reason)[:300]


def _secondary_is_available() -> bool:
    return not _secondary_broken


def _is_minimax_model(model: str) -> bool:
    return str(model or "").startswith("minimax-portal/") or str(model or "") == "MiniMax-M3"


def _pm_model_label(model: str) -> str:
    name = str(model or "").split("/", 1)[-1]
    labels = {
        "gpt-5.6-sol": "GPT-5.6-Sol",
        "MiniMax-M3": "MiniMax-M3",
    }
    return labels.get(name, name or "unknown")


def _load_selection_memory_rules(limit: int = 5) -> str:
    """Read compact lessons from previous Top5 reviews without bloating prompts."""
    path = Path(__file__).resolve().parents[1] / "output" / "selection_memory.jsonl"
    if not path.exists():
        return "暂无可用复盘记忆。"
    try:
        lines = path.read_text(encoding="utf-8").splitlines()[-300:]
    except Exception:
        return "暂无可用复盘记忆。"

    rules: List[str] = []
    seen = set()
    for line in reversed(lines):
        try:
            item = json.loads(line)
        except Exception:
            continue
        labels = item.get("attribution_labels") or []
        primary = item.get("primary_attribution") or (labels[0] if labels else "")
        if not any(item.get(f"return_{h}_complete") is True for h in ("d1", "d3", "d5", "d10")):
            continue
        if not primary:
            continue
        signal = item.get("signal") or ""
        buy_score = item.get("buy_score")
        stock = item.get("stock") or ""
        name = item.get("name") or ""
        ret = None
        for horizon in ("d3", "d1", "d5"):
            if item.get(f"return_{horizon}_complete") is True:
                ret = item.get(f"return_{horizon}_pct")
                if ret is not None:
                    break
        ret_text = f"，后续收益{float(ret):+.2f}%" if isinstance(ret, (int, float)) else ""
        rule = f"{stock}{name} {signal} 做多分{buy_score} 归因{primary}{ret_text}"
        if rule in seen:
            continue
        seen.add(rule)
        rules.append(f"- {rule}")
        if len(rules) >= limit:
            break
    if not rules:
        return "暂无可用复盘记忆。"
    return "\n".join(rules)



def _call_portfolio_manager_structured(prompt: str, call_structured, PortfolioManagerOutput, stock_name: str):
    """
    基金经理三级结构化裁决：GPT-5.6 Sol(max) -> GPT-5.6 Sol(max) -> MiniMax M3。
    每层按节点重试预算执行；只有认证、额度或限流错误才触发任务级熔断。
    """
    global _pm_primary_fallback_count
    from .providers import _call_structured_openai_responses
    import time as _time

    RETRIES = _providers.effective_llm_retries("pm", 3)
    RETRY_INTERVAL = 1  # 秒

    class _Mock:
        def __init__(self, d):
            for k, v in d.items():
                setattr(self, k, v)

    # Dependency-injected tests and recovery tools need a deterministic one-shot
    # path instead of the full direct GPT/secondary cascade.
    if call_structured is not _call_structured:
        injected_model = PORTFOLIO_MANAGER_TERTIARY_MODEL or PORTFOLIO_MANAGER_SECONDARY_MODEL
        structured = call_structured(
            prompt=prompt,
            schema=PortfolioManagerOutput,
            model=injected_model,
            timeout=PORTFOLIO_MANAGER_TIMEOUT,
            retries=1,
            thinking_budget=PORTFOLIO_MANAGER_MINIMAX_BUDGET if _is_minimax_model(injected_model) else 0,
            max_tokens=16384,
            allow_fallback=False,
        )
        if structured is not None:
            return structured, f"Structured:{_pm_model_label(injected_model)}"
        raise RuntimeError("injected structured call returned None")

    # ── 第1层：GPT-5.6 Sol max structured reasoning ──
    if _portfolio_primary_is_available():
        with _pm_primary_lock:
            if _portfolio_primary_is_available():
                last_err = None
                empty_structured_seen = False
                primary_label = _pm_model_label(PORTFOLIO_MANAGER_PRIMARY_MODEL)
                for attempt in range(RETRIES):
                    try:
                        data = _call_structured_openai_responses(
                            prompt=prompt,
                            model=PORTFOLIO_MANAGER_PRIMARY_MODEL,
                            schema=PortfolioManagerOutput,
                            timeout=PORTFOLIO_MANAGER_TIMEOUT,
                            max_tokens=16384,
                            reasoning_effort=PORTFOLIO_MANAGER_PRIMARY_REASONING_EFFORT,
                        )
                        if data is not None:
                            logger.info(
                                f"[{stock_name}] PM {primary_label} "
                                f"effort={PORTFOLIO_MANAGER_PRIMARY_REASONING_EFFORT} structured成功: "
                                f"signal={data.get('signal')}"
                            )
                            return _Mock(data), f"Structured:{primary_label}"
                        logger.warning(f"[{stock_name}] PM {primary_label} structured返回None (attempt {attempt+1}/{RETRIES})")
                        empty_structured_seen = True
                        last_err = RuntimeError(f"{primary_label} structured返回None")
                    except urllib.error.HTTPError as e:
                        last_err = e
                        if e.code in (429, 403):
                            logger.warning(f"[{stock_name}] PM {primary_label} HTTP {e.code} 限流/配额 (attempt {attempt+1}/{RETRIES}): {e}")
                        else:
                            logger.warning(f"[{stock_name}] PM {primary_label} HTTP错误 (attempt {attempt+1}/{RETRIES}): {e}")
                    except Exception as e:
                        last_err = e
                        logger.warning(f"[{stock_name}] PM {primary_label} structured失败 (attempt {attempt+1}/{RETRIES}): {e}")
                    if attempt < RETRIES - 1:
                        _time.sleep(RETRY_INTERVAL)

                if empty_structured_seen:
                    try:
                        data = _repair_empty_portfolio_structured(
                            prompt,
                            _call_structured_openai_responses,
                            PortfolioManagerOutput,
                            PORTFOLIO_MANAGER_PRIMARY_MODEL,
                            PORTFOLIO_MANAGER_PRIMARY_REASONING_EFFORT,
                        )
                        if data is not None:
                            logger.info(f"[{stock_name}] PM {primary_label} structured空结果修复成功: signal={data.get('signal')}")
                            return _Mock(data), f"Structured:{primary_label}Repair"
                        last_err = RuntimeError(f"{primary_label} structured空结果修复返回None")
                        logger.warning(f"[{stock_name}] PM {primary_label} structured空结果修复返回None")
                    except Exception as e:
                        last_err = e
                        logger.warning(f"[{stock_name}] PM {primary_label} structured空结果修复失败: {e}")

                # 重试全部失败后切 secondary model。只有认证/额度/限流类错误才全局熔断；
                # SSL EOF/timeout 等瞬时网络错误只影响当前股票，避免一只股票把整批 PM 切走。
                if _portfolio_primary_is_available():
                    err_class = _providers.classify_llm_error(last_err)
                    if _providers.is_global_model_failure(last_err):
                        _trip_portfolio_primary(last_err)
                        scope = "本次任务全局fallback"
                    else:
                        scope = "单票fallback"
                    _pm_primary_fallback_count += 1
                    logger.warning(
                        f"[{stock_name}] PM {primary_label} 重试{RETRIES}次均失败，"
                        f"{scope}到{PORTFOLIO_MANAGER_SECONDARY_MODEL} "
                        f"err_class={err_class}: {last_err}"
                    )

    # ── 第2层：GPT-5.6 Sol max structured 兜底 ──
    last_err = None
    if (
        _secondary_is_available()
        and PORTFOLIO_MANAGER_SECONDARY_MODEL
        and PORTFOLIO_MANAGER_SECONDARY_MODEL != PORTFOLIO_MANAGER_PRIMARY_MODEL
    ):
        secondary_label = _pm_model_label(PORTFOLIO_MANAGER_SECONDARY_MODEL)
        for attempt in range(RETRIES):
            try:
                data = _call_structured_openai_responses(
                    prompt=prompt,
                    model=PORTFOLIO_MANAGER_SECONDARY_MODEL,
                    schema=PortfolioManagerOutput,
                    timeout=PORTFOLIO_MANAGER_TIMEOUT,
                    max_tokens=16384,
                    reasoning_effort=PORTFOLIO_MANAGER_SECONDARY_REASONING_EFFORT,
                )
                if data is not None:
                    logger.info(
                        f"[{stock_name}] PM {secondary_label} "
                        f"effort={PORTFOLIO_MANAGER_SECONDARY_REASONING_EFFORT} structured成功: "
                        f"signal={data.get('signal')}"
                    )
                    return _Mock(data), f"Structured:{secondary_label}"
                logger.warning(f"[{stock_name}] PM {secondary_label} structured返回None (attempt {attempt+1}/{RETRIES})")
                last_err = RuntimeError(f"{secondary_label} structured返回None")
            except Exception as e:
                last_err = e
                logger.warning(f"[{stock_name}] PM {secondary_label} structured失败 (attempt {attempt+1}/{RETRIES}): {e}")
            if attempt < RETRIES - 1:
                _time.sleep(RETRY_INTERVAL)
        if _providers.is_global_model_failure(last_err):
            _trip_secondary(last_err)
        logger.warning(
            f"[{stock_name}] PM {secondary_label} 重试{RETRIES}次均失败，"
            f"切换到{PORTFOLIO_MANAGER_TERTIARY_MODEL}: {last_err}"
        )

    # ── 第3层：MiniMax M3 portal OAuth + adaptive thinking + JSON ──
    if (
        PORTFOLIO_MANAGER_MINIMAX_ENABLED
        and PORTFOLIO_MANAGER_TERTIARY_MODEL
        and PORTFOLIO_MANAGER_TERTIARY_MODEL != PORTFOLIO_MANAGER_SECONDARY_MODEL
    ):
        tertiary_label = _pm_model_label(PORTFOLIO_MANAGER_TERTIARY_MODEL)
        for attempt in range(RETRIES):
            try:
                structured = call_structured(
                    prompt=prompt,
                    schema=PortfolioManagerOutput,
                    model=PORTFOLIO_MANAGER_TERTIARY_MODEL,
                    timeout=PORTFOLIO_MANAGER_TIMEOUT,
                    retries=1,
                    thinking_budget=PORTFOLIO_MANAGER_MINIMAX_BUDGET,
                    max_tokens=16384,
                    allow_fallback=False,
                )
                if structured is not None:
                    logger.info(f"[{stock_name}] PM {tertiary_label} structured成功: signal={structured.signal}")
                    return structured, f"Structured:{tertiary_label}"
                logger.warning(f"[{stock_name}] PM {tertiary_label} structured返回None (attempt {attempt+1}/{RETRIES})")
                last_err = RuntimeError(f"{tertiary_label} structured返回None")
            except Exception as e:
                last_err = e
                logger.warning(f"[{stock_name}] PM {tertiary_label} structured失败 (attempt {attempt+1}/{RETRIES}): {e}")
            if attempt < RETRIES - 1:
                _time.sleep(RETRY_INTERVAL)

    # 重试全部失败，抛异常
    raise RuntimeError(f"[{stock_name}] PM 所有模型重试{RETRIES}次均失败: {last_err}")


def portfolio_manager_node(state: RiskDebateState) -> RiskDebateState:
    stock_name = state["stock_name"]
    # 防止前导 \n 导致 LLM 输出碎片化 JSON
    history_raw = state.get("history", "")
    history = history_raw.lstrip('\n') if history_raw.startswith('\n') else history_raw
    plan = state.get("research_plan", "")
    selection_memory = _load_selection_memory_rules()

    packet = state.get("debate_packet", {}) or {}
    validation_packet = _role_evidence_packet(state)
    evidence_contract = _render_data_contract(packet)
    available_evidence_refs = _available_evidence_fields(packet)

    prompt = PORTFOLIO_MANAGER_PROMPT.format(
        research_plan=plan,
        history=history,
        selection_memory=selection_memory,
        evidence_contract=evidence_contract,
        available_evidence_refs=available_evidence_refs,
        knowledge_rules=_render_pm_knowledge_rules(packet),
        pm_json_fields=pm_json_field_instructions(),
        pm_text_fields=pm_text_field_instructions(),
    )

    # 基金经理优先走 structured；失败时回到纯文本裁决并修复成结构化字段。
    raw_final_decision = ""
    evidence_refs: list[dict] = []
    missing_data_used: list[str] = []
    unsupported_claims: list[str] = []
    allow_direct_buy, needs_intraday_confirmation, entry_condition, block_buy_reason = _default_execution_gate("WATCH", 55)
    evidence_validation: dict = {"status": "unknown", "errors": [], "warnings": []}
    evidence_failed = False
    try:
        structured, structured_source = _call_portfolio_manager_structured(
            prompt, _providers.call_structured, _providers.PortfolioManagerOutput, stock_name
        )
        sig = structured.signal
        conf = int(structured.confidence)
        raw_buy_score = getattr(structured, "buy_score", None)
        buy_score = _fallback_buy_score(sig, conf) if raw_buy_score is None else int(raw_buy_score)
        buy_score = max(0, min(100, buy_score))
        sig = _signal_from_buy_score(buy_score)
        ratio = _normalize_position_ratio(structured.position_ratio)
        reason = structured.reason
        allow_direct_buy, needs_intraday_confirmation, entry_condition, block_buy_reason = _extract_execution_gate(structured, sig, buy_score)
        evidence_refs = _normalize_evidence_refs(getattr(structured, "evidence_refs", []))
        missing_data_used = _normalize_missing_data_used(getattr(structured, "missing_data_used", []) or [], packet)
        unsupported_claims = list(getattr(structured, "unsupported_claims", []) or [])
        evidence_validation = _validate_pm_evidence(validation_packet, reason, evidence_refs, missing_data_used, unsupported_claims)
        missing_data_used = list(evidence_validation.get("missing_data_used") or missing_data_used)
        if evidence_validation.get("status") == "fail":
            logger.warning(f"[{stock_name}] PM证据校验失败，尝试同材料重写: {evidence_validation}")
            try:
                repaired_structured = _repair_portfolio_evidence(
                    prompt, evidence_validation, _providers.call_structured, _providers.PortfolioManagerOutput
                )
            except Exception as repair_err:
                repaired_structured = None
                logger.warning(f"[{stock_name}] PM证据重写失败: {repair_err}")
            if repaired_structured is not None:
                r_sig = repaired_structured.signal
                r_conf = int(repaired_structured.confidence)
                r_buy_score = getattr(repaired_structured, "buy_score", None)
                r_buy_score = _fallback_buy_score(r_sig, r_conf) if r_buy_score is None else int(r_buy_score)
                r_buy_score = max(0, min(100, r_buy_score))
                r_sig = _signal_from_buy_score(r_buy_score)
                r_ratio = _normalize_position_ratio(repaired_structured.position_ratio)
                r_reason = repaired_structured.reason
                r_allow, r_confirm, r_entry, r_block = _extract_execution_gate(repaired_structured, r_sig, r_buy_score)
                r_refs = _normalize_evidence_refs(getattr(repaired_structured, "evidence_refs", []))
                r_missing = _normalize_missing_data_used(getattr(repaired_structured, "missing_data_used", []) or [], packet)
                r_unsupported = list(getattr(repaired_structured, "unsupported_claims", []) or [])
                r_validation = _validate_pm_evidence(validation_packet, r_reason, r_refs, r_missing, r_unsupported)
                r_missing = list(r_validation.get("missing_data_used") or r_missing)
                if r_validation.get("status") != "fail":
                    sig, conf, buy_score, ratio, reason = r_sig, r_conf, r_buy_score, r_ratio, r_reason
                    allow_direct_buy, needs_intraday_confirmation, entry_condition, block_buy_reason = r_allow, r_confirm, r_entry, r_block
                    evidence_refs, missing_data_used, unsupported_claims = r_refs, r_missing, r_unsupported
                    evidence_validation = r_validation
                    structured_source = f"{structured_source}+EvidenceRepair"
                    logger.info(f"[{stock_name}] PM证据重写通过: signal={sig}")
                else:
                    evidence_validation = r_validation
                    evidence_failed = True
            else:
                evidence_failed = True
        if evidence_failed:
            buy_score = min(buy_score, 69)
            sig = _signal_from_buy_score(buy_score)
            conf = max(0, conf - 12)
            ratio = min(ratio, 0.10)
            unsupported_claims = list(dict.fromkeys((unsupported_claims or []) + (evidence_validation.get("errors") or [])))
            allow_direct_buy, needs_intraday_confirmation = False, True
            block_buy_reason = "证据校验未通过"
            entry_condition = "盘中重新确认"
            reason = f"证据校验未通过，已禁止进入BUY；{reason}"[:260]
        elif evidence_validation.get("status") == "warn":
            conf = max(0, conf - 5)
        decision_text = f"[{structured_source}] signal={sig} buy_score={buy_score} confidence={conf} position_ratio={ratio*100:.0f}% reason={reason}"
        decision_source = structured_source
        raw_final_decision = f"PM thinking成功: {structured_source} signal={sig} buy_score={buy_score} conf={conf} evidence={evidence_validation.get('status')}"
        logger.info(f"[{stock_name}] 基金经理 {decision_source}: {sig} 做多{buy_score}分 置信{conf}分 证据={evidence_validation.get('status')}")
    except Exception as e:
        logger.warning(f"[{stock_name}] 基金经理structured模式失败，尝试MiniMax deep-thinking文本裁决: {e}")
        text_prompt = PORTFOLIO_MANAGER_THINKING_PROMPT.format(
            research_plan=plan,
            history=history,
            selection_memory=selection_memory,
            evidence_contract=evidence_contract,
            available_evidence_refs=available_evidence_refs,
            knowledge_rules=_render_pm_knowledge_rules(packet),
            pm_json_fields=pm_json_field_instructions(),
            pm_text_fields=pm_text_field_instructions(),
        )
        try:
            text_model = PORTFOLIO_MANAGER_TERTIARY_MODEL or PORTFOLIO_MANAGER_SECONDARY_MODEL or DEFAULT_MODEL
            raw_text = _providers.call_llm(
                text_prompt,
                model=text_model,
                timeout=PORTFOLIO_MANAGER_TIMEOUT,
                max_tokens=4096,
                thinking_budget=PORTFOLIO_MANAGER_MINIMAX_BUDGET if _is_minimax_model(text_model) else 0,
                temperature=0.3,
            )
        except Exception as text_err:
            logger.error(f"[{stock_name}] 基金经理纯文本裁决也失败: {text_err}")
            raw_text = ""

        parsed_text = _parse_portfolio_manager_text(raw_text)
        repaired = None
        if raw_text and not parsed_text:
            try:
                repaired = _repair_portfolio_json(raw_text, _providers.call_structured, _providers.PortfolioManagerOutput)
            except Exception as repair_err:
                logger.warning(f"[{stock_name}] PM 文本修复失败: {repair_err}")

        if parsed_text:
            sig, buy_score, conf, ratio, reason = _coerce_portfolio_fields(parsed_text)
            allow_direct_buy, needs_intraday_confirmation, entry_condition, block_buy_reason = _extract_execution_gate(parsed_text, sig, buy_score)
            decision_source = "MiniMaxThinkingText"
            decision_text = f"[MiniMaxThinkingText] signal={sig} buy_score={buy_score} confidence={conf} position_ratio={ratio*100:.0f}% reason={reason}"
            raw_final_decision = raw_text
            logger.info(f"[{stock_name}] 基金经理MiniMax thinking文本解析成功: {sig} 做多{buy_score}分 置信{conf}分")
        elif repaired is not None:
            parsed = {
                "signal": repaired.signal,
                "buy_score": getattr(repaired, "buy_score", None),
                "confidence": repaired.confidence,
                "position_ratio": repaired.position_ratio,
                "reason": repaired.reason,
            }
            evidence_refs = _normalize_evidence_refs(getattr(repaired, "evidence_refs", []))
            missing_data_used = _normalize_missing_data_used(getattr(repaired, "missing_data_used", []) or [], packet)
            unsupported_claims = list(getattr(repaired, "unsupported_claims", []) or [])
            sig, buy_score, conf, ratio, reason = _coerce_portfolio_fields(parsed)
            allow_direct_buy, needs_intraday_confirmation, entry_condition, block_buy_reason = _extract_execution_gate(repaired, sig, buy_score)
            decision_source = "Repaired"
            decision_text = f"[Repaired] signal={sig} buy_score={buy_score} confidence={conf} position_ratio={ratio*100:.0f}% reason={reason}"
            raw_final_decision = raw_text
            logger.info(f"[{stock_name}] 基金经理文本修复成功: {sig} 做多{buy_score}分 置信{conf}分")
        else:
            sig, buy_score, conf, ratio = "WATCH", 55, 0, 0.0
            allow_direct_buy, needs_intraday_confirmation = False, True
            entry_condition, block_buy_reason = "盘中重新确认", "基金经理结构化裁决失败"
            reason = "基金经理结构化裁决失败，纯文本也缺少明确字段；不允许新开仓。"
            decision_source = "TextOnly"
            decision_text = f"[TextOnly] signal={sig} buy_score={buy_score} confidence={conf} position_ratio=0% reason={reason}"
            raw_final_decision = raw_text or str(e)
            logger.warning(f"[{stock_name}] 基金经理转 TextOnly 零仓位")

    if not evidence_validation or evidence_validation.get("status") == "unknown":
        evidence_validation = _validate_pm_evidence(validation_packet, reason, evidence_refs, missing_data_used, unsupported_claims)
        missing_data_used = list(evidence_validation.get("missing_data_used") or missing_data_used)
        if evidence_validation.get("status") == "fail":
            buy_score = min(buy_score, 69)
            sig = _signal_from_buy_score(buy_score)
            conf = max(0, conf - 12)
            ratio = min(ratio, 0.10)
            unsupported_claims = list(dict.fromkeys((unsupported_claims or []) + (evidence_validation.get("errors") or [])))
            allow_direct_buy, needs_intraday_confirmation = False, True
            block_buy_reason = "证据校验未通过"
            entry_condition = "盘中重新确认"
            reason = f"证据校验未通过，已禁止进入BUY；{reason}"[:260]
            decision_text = f"[{decision_source}] signal={sig} buy_score={buy_score} confidence={conf} position_ratio={ratio*100:.0f}% reason={reason}"
        elif evidence_validation.get("status") == "warn":
            conf = max(0, conf - 5)
            decision_text = f"[{decision_source}] signal={sig} buy_score={buy_score} confidence={conf} position_ratio={ratio*100:.0f}% reason={reason}"

    if evidence_validation.get("status") == "fail":
        errors = evidence_validation.get("errors") or ["未知证据错误"]
        logger.warning("[%s] PM最终证据仍未通过，标记节点待重试: %s", stock_name, errors)
        raise RoleEvidenceValidationError(
            "基金经理证据校验失败: " + "；".join(str(item) for item in errors)
        )

    # ── 聚合各节点实际跑的模型（用于早报卡片真实显示）──
    # 同一节点多次出现时只保留最后一次（辩论多轮），按节点出现顺序输出
    # PM 走 structured 路径时 decision_source 是 "Structured:<model>"，拆出真实模型名；
    # 走文本兜底时 (Repaired/TextOnly/MiniMaxThinkingText) 用 decision_source 原值标注
    pm_model_name = decision_source.split(":", 1)[-1] if ":" in decision_source else decision_source
    node_models_log = list(state.get("node_models_log") or [])
    node_models_log.append({"node": "pm", "model": pm_model_name or decision_source})
    seen: Dict[str, str] = {}
    order: List[str] = []
    for entry in node_models_log:
        node = entry.get("node", "")
        if not node:
            continue
        if node not in seen:
            order.append(node)
        seen[node] = entry.get("model", "unknown")
    decision_models = {n: seen[n] for n in order}

    data_quality_flags = list(state.get("data_quality_flags") or [])
    if evidence_validation.get("status") == "fail" and "MODEL_EVIDENCE_FAILED" not in data_quality_flags:
        data_quality_flags.append("MODEL_EVIDENCE_FAILED")

    return _update_risk(
        state,
        final_decision=decision_text,
        signal=sig,
        buy_score=buy_score,
        confidence=conf,
        position_ratio=ratio,
        allow_direct_buy=allow_direct_buy,
        needs_intraday_confirmation=needs_intraday_confirmation,
        entry_condition=entry_condition,
        block_buy_reason=block_buy_reason,
        reason=reason,
        decision_source=decision_source,
        raw_final_decision=raw_final_decision,
        decision_models=decision_models,
        evidence_refs=evidence_refs,
        missing_data_used=missing_data_used,
        unsupported_claims=unsupported_claims,
        evidence_validation=evidence_validation,
        data_quality_flags=data_quality_flags,
    )


def rerun_portfolio_manager_from_checkpoint(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """只重跑基金经理裁决，用于恢复 StructuredFailed 的 checkpoint 结果。"""
    history = result.get("debate_history") or result.get("history") or ""
    research_plan = result.get("research_plan") or ""
    if not history or not research_plan:
        return None

    state: RiskDebateState = {
        "stock_code": result.get("stock_code", ""),
        "stock_name": result.get("stock_name", result.get("name", "")),
        "history": history,
        "research_plan": research_plan,
        "debate_packet": {},
        "market_context": "",
        "model": result.get("model", DEFAULT_MODEL),
        "signal": result.get("signal", "WATCH"),
        "confidence": int(result.get("confidence") or 0),
        "position_ratio": 0.0,
        "allow_direct_buy": False,
        "needs_intraday_confirmation": True,
        "entry_condition": result.get("entry_condition", "盘中重新确认"),
        "block_buy_reason": result.get("block_buy_reason", "断点重跑基金经理裁决"),
        "reason": result.get("reason", ""),
        "decision_source": result.get("decision_source", ""),
        "raw_final_decision": result.get("raw_final_decision", ""),
        "final_decision": result.get("final_decision", ""),
        "node_models_log": result.get("node_models_log", []),
        "decision_models": result.get("decision_models", {}),
    }
    updated = portfolio_manager_node(state)
    recovered = dict(result)
    for key in (
        "signal",
        "confidence",
        "position_ratio",
        "reason",
        "decision_source",
        "raw_final_decision",
        "final_decision",
        "decision_models",
    ):
        recovered[key] = updated.get(key, recovered.get(key))
    return recovered


# ── 状态合并（投资辩论 → 风险辩论）──────────────────────

def merge_to_risk_state(invest_state: InvestDebateState) -> RiskDebateState:
    """投资辩论结束后，将状态合并到风险辩论状态。"""
    return {
        "stock_code": invest_state["stock_code"],
        "stock_name": invest_state["stock_name"],
        "debate_packet": invest_state["debate_packet"],
        "market_context": invest_state.get("market_context", ""),
        "research_plan": invest_state.get("research_plan", ""),
        "history": invest_state.get("history", ""),
        # ★ 6-04 修复：传递 invest 阶段累积的 bull/bear_history，避免 risk 阶段读到空
        "bull_history": invest_state.get("bull_history", ""),
        "bear_history": invest_state.get("bear_history", ""),
        "aggressive_history": "",
        "conservative_history": "",
        "neutral_history": "",
        "latest_speaker": "",
        "count": 0,
        "signal": "WATCH",
        "confidence": 50,
        "position_ratio": 0.15,
        "allow_direct_buy": False,
        "needs_intraday_confirmation": True,
        "entry_condition": "盘中确认",
        "block_buy_reason": "默认观察",
        "reason": "",
        "decision_source": "",
        "raw_final_decision": "",
        "final_decision": "",
        "evidence_refs": [],
        "missing_data_used": [],
        "unsupported_claims": [],
        "evidence_validation": {},
        "model": invest_state.get("model", DEFAULT_MODEL),
        "data_quality_flags": invest_state.get("data_quality_flags", []),
        "tech_pattern_score": invest_state.get("tech_pattern_score", 50),
        "tech_rule_signal": invest_state.get("tech_rule_signal", "WATCH"),
        "tech_raw_score": invest_state.get("tech_raw_score", invest_state.get("tech_pattern_score", 50)),
        "tech_max_score": invest_state.get("tech_max_score", 100),
        "tech_veto_reasons": invest_state.get("tech_veto_reasons", []),
        "node_models_log": invest_state.get("node_models_log", []),
        "decision_models": {},
    }


# ── 构建投资辩论图 ────────────────────────────────────────

def _build_invest_debate_graph(checkpointer):
    """构建阶段1（投资辩论）的 LangGraph。"""
    g = StateGraph(InvestDebateState)

    g.add_node("bull_researcher", _checkpointed_node("invest", "bull_researcher", bull_researcher_node))
    g.add_node("bear_researcher", _checkpointed_node("invest", "bear_researcher", bear_researcher_node))
    g.add_node("tech_analyst", _checkpointed_node("invest", "tech_analyst", tech_analyst_node))
    g.add_node("research_manager", _checkpointed_node("invest", "research_manager", research_manager_node))

    g.set_entry_point("bull_researcher")

    # Bull → Bear → Research Manager 的普通边
    g.add_edge("bull_researcher", "bear_researcher")
    g.add_edge("bear_researcher", "research_manager")
    # tech_analyst → research_manager（路由函数控制）
    g.add_edge("tech_analyst", "research_manager")

    # 条件边：路由到 bear / research_manager / risk_debate / tech_analyst
    g.add_conditional_edges(
        "research_manager",
        route_invest_debate,
        {
            "bull_researcher": "bull_researcher",
            "bear_researcher": "bear_researcher",
            "tech_analyst": "tech_analyst",
            "risk_debate": END,
        },
    )

    return g.compile(checkpointer=checkpointer)


# ── 构建风险辩论图 ────────────────────────────────────────

def _build_risk_debate_graph(checkpointer):
    """构建阶段2（风险辩论）的 LangGraph。"""
    g = StateGraph(RiskDebateState)

    g.add_node("aggressive_analyst", _checkpointed_node("risk", "aggressive_analyst", aggressive_analyst_node))
    g.add_node("conservative_analyst", _checkpointed_node("risk", "conservative_analyst", conservative_analyst_node))
    g.add_node("neutral_analyst", _checkpointed_node("risk", "neutral_analyst", neutral_analyst_node))
    g.add_node("portfolio_manager", _checkpointed_node("risk", "portfolio_manager", portfolio_manager_node))

    g.set_entry_point("aggressive_analyst")

    # 条件边路由
    g.add_conditional_edges(
        "aggressive_analyst",
        route_risk_debate,
        {
            "conservative_analyst": "conservative_analyst",
            "neutral_analyst": "neutral_analyst",
            "portfolio_manager": "portfolio_manager",
        },
    )
    g.add_conditional_edges(
        "conservative_analyst",
        route_risk_debate,
        {
            "aggressive_analyst": "aggressive_analyst",
            "neutral_analyst": "neutral_analyst",
            "portfolio_manager": "portfolio_manager",
        },
    )
    g.add_conditional_edges(
        "neutral_analyst",
        route_risk_debate,
        {
            "aggressive_analyst": "aggressive_analyst",
            "conservative_analyst": "conservative_analyst",
            "portfolio_manager": "portfolio_manager",
        },
    )

    g.add_edge("portfolio_manager", END)

    return g.compile(checkpointer=checkpointer)


# ── 主入口：辩论引擎 ─────────────────────────────────────

class StockDebateEngine:
    """
    选股辩论引擎 v2
    基于 LangGraph 的对抗辩论，两个阶段：
      阶段1：Bull/Bear 辩论 → Research Manager 裁决
      阶段2：风险分析师辩论 → Portfolio Manager 最终决策
    """

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        max_debate_rounds: int = MAX_DEBATE_ROUNDS,
        timeout: int = 120,
    ):
        self.model = model
        self.max_debate_rounds = max_debate_rounds
        self.timeout = timeout

        # 持久化 checkpoint（断点续跑）
        self._checkpointer = MemorySaver()

        # 构建两个阶段图
        self._invest_graph = _build_invest_debate_graph(self._checkpointer)
        self._risk_graph = _build_risk_debate_graph(self._checkpointer)

    def run(
        self,
        debate_packets: List[Dict],
        market_context: str = "",
        stock_scores: Optional[Dict[str, float]] = None,
        max_parallel: int = 3,  # ★ 6-04 老板拍板：ark-code 无 QPS 限流（不像 MiniMax 有 Semaphore(3)），并发 ≤ 3 防 5-hour quota 熔断
        top_n: Optional[int] = None,
        checkpoint_cb: Optional[callable] = None,
        node_checkpoint_cb: Optional[callable] = None,
        resume_node_states: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,
    ) -> List[Dict]:
        """
        对候选股票池运行完整辩论流程。

        Args:
            debate_packets: build_debate_packet() 返回的候选股数据包列表
            market_context: 市场整体环境描述（可选）
            stock_scores: 候选股Phase1评分（用于排序）
            max_parallel: 最大并行辩论数（默认3；ark-code 无 QPS 限流保护，并发不宜 > 3）
            top_n: 最多辩论股数（默认None=全部候选股进入辩论）
        """
        stock_scores = stock_scores or {}
        resume_node_states = resume_node_states or {}

        # 按 Phase1 评分排序，top_n=None 表示全部进入辩论
        sorted_packets = sorted(
            debate_packets,
            key=lambda p: -(stock_scores.get(p.get("stock_code", ""), 0)),
        )
        debate_list = sorted_packets[:top_n] if top_n else sorted_packets

        logger.info(f"辩论引擎启动，候选={len(debate_packets)}只，{'全部' if not top_n else 'Top' + str(len(debate_list))}进入辩论，并行={max_parallel}")

        # ── checkpoint callback（线程安全：每完成一只写一次）────────
        def make_checkpoint_cb():
            _cp_lock = __import__("threading").Lock()
            def _cb(code, result):
                if not checkpoint_cb:
                    return
                with _cp_lock:
                    checkpoint_cb(code, result)
            return _cb

        _checkpoint_cb = make_checkpoint_cb()
        _node_cp_lock = threading.Lock()

        def _node_cb(code: str, node_key: str, state_snapshot: Dict[str, Any]) -> None:
            if not node_checkpoint_cb:
                return
            with _node_cp_lock:
                node_checkpoint_cb(code, node_key, state_snapshot)

        def run_one(packet: Dict) -> Dict:
            code = packet.get("stock_code", "N/A")
            name = packet.get("name", packet.get("stock_name", code))
            phase1_score = stock_scores.get(code, 0)
            stock_resume_nodes = dict(resume_node_states.get(code, {}) or {})

            def _stock_node_cb(
                callback_code: str,
                node_key: str,
                state_snapshot: Dict[str, Any],
            ) -> None:
                # Keep the just-completed node available to retries in this
                # same process.  Otherwise a later evidence failure restarts
                # the stock from its first Bull node even though that node was
                # already durably checkpointed.
                stock_resume_nodes[node_key] = dict(state_snapshot)
                _node_cb(callback_code, node_key, state_snapshot)

            try:
                _NODE_CHECKPOINT_LOCAL.callback = _stock_node_cb
                result = self._run_single(
                    code,
                    name,
                    packet,
                    market_context,
                    resume_nodes=stock_resume_nodes,
                )
                result["phase1_score"] = phase1_score
                if _checkpoint_cb:
                    _checkpoint_cb(code, result)
                return result
            except Exception as e:
                import traceback
                logger.error(f"[{name}] 辩论异常: {e}\n{traceback.format_exc()}")
                if _checkpoint_cb:
                    _checkpoint_cb(code, None)
                flags = list(dict.fromkeys((packet.get("data_quality_flags", []) or []) + ["MODEL_FAILED"]))
                return {
                    "stock_code": code, "stock_name": name,
                    "signal": "MODEL_FAILED", "buy_score": 0, "confidence": 0,
                    "position_ratio": 0.0,
                    "final_decision": f"模型失败/待重试: {str(e)[:100]}", "error": str(e),
                    "decision_source": "MODEL_FAILED",
                    "data_quality_flags": flags,
                    "phase1_score": phase1_score,
                }
            finally:
                _NODE_CHECKPOINT_LOCAL.callback = None

        # 分批执行，每批10个，批次间隔5秒
        from concurrent.futures import ThreadPoolExecutor, as_completed
        results = []
        batch_size = max_parallel
        for batch_start in range(0, len(debate_list), batch_size):
            batch = debate_list[batch_start:batch_start + batch_size]
            batch_num = batch_start // batch_size + 1
            total_batches = (len(debate_list) + batch_size - 1) // batch_size
            logger.info(f"批次 {batch_num}/{total_batches}: 提交 {len(batch)} 只")
            with ThreadPoolExecutor(max_workers=batch_size) as pool:
                futures = {pool.submit(run_one, p): p for p in batch}
                for future in as_completed(futures):
                    try:
                        results.append(future.result())
                    except Exception as e:
                        logger.error(f"批次 {batch_num} 中某只股票结果获取异常: {e}")
            if batch_start + batch_size < len(debate_list):
                logger.info(f"批次间隔 5 秒...")
                time.sleep(5)

        # 未进入辩论的候选股（top_n 模式下才执行）
        for packet in (sorted_packets[top_n:] if top_n else []):
            code = packet.get("stock_code", "N/A")
            name = packet.get("name", packet.get("stock_name", code))
            results.append({
                "stock_code": code, "stock_name": name,
                "signal": "WATCH", "confidence": 0,
                "final_decision": f"Phase1评分不足，未进入辩论",
                "phase1_score": stock_scores.get(code, 0),
            })

        # 汇总
        buy_list = [r for r in results if r.get("signal") == "BUY"]
        watch_list = [r for r in results if r.get("signal") == "WATCH"]
        avoid_list = [r for r in results if r.get("signal") == "AVOID"]
        logger.info(f"辩论完成: BUY={len(buy_list)} WATCH={len(watch_list)} AVOID={len(avoid_list)}")

        return results

    def _run_single(
        self,
        stock_code: str,
        stock_name: str,
        packet: Dict,
        market_context: str,
        resume_nodes: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict:
        """对单只股票运行完整两阶段辩论。"""

        resume_nodes = resume_nodes if isinstance(resume_nodes, dict) else {}

        # ── 阶段1：投资辩论 ──────────────────────────────
        invest_initial: InvestDebateState = {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "debate_packet": _sanitize(packet),
            "market_context": market_context,
            "bull_history": "",
            "bear_history": "",
            "history": "",
            "current_response": "",
            "count": 0,
            "research_plan": "",
            "max_rounds": self.max_debate_rounds,
            "model": self.model,
            "node_models_log": [],
            "_node_resume": resume_nodes,
        }

        # 运行投资辩论图（使用 checkpointer 支持断点）
        invest_state = None
        for attempt in range(3):
            try:
                invest_state = self._invest_graph.invoke(
                    invest_initial if attempt == 0 else None,
                    config={"configurable": {"thread_id": f"invest_{stock_code}"}},
                )
                break
            except Exception as e:
                logger.warning(f"投资辩论重试 {attempt+1}: {e}")
                if attempt == 2:
                    raise
                time.sleep(5)

        # ── 阶段2：风险辩论 ──────────────────────────────
        risk_initial = merge_to_risk_state(invest_state)
        risk_initial["_node_resume"] = resume_nodes

        risk_state = None
        for attempt in range(3):
            try:
                risk_state = self._risk_graph.invoke(
                    risk_initial if attempt == 0 else None,
                    config={"configurable": {"thread_id": f"risk_{stock_code}"}},
                )
                break
            except Exception as e:
                import traceback
                logger.warning(f"风险辩论重试 {attempt+1}: {e}")
                logger.warning(f"风险辩论异常 state keys: {list(risk_initial.keys()) if risk_state is None else list(risk_state.keys())}")
                if attempt == 2:
                    raise
                time.sleep(5)

        # ── 汇总结果 ────────────────────────────────────
        return {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "pool": packet.get("pool", ""),
            "source_pools": packet.get("source_pools", []),
            "source_queries": packet.get("source_queries", []),
            "source_reasons": packet.get("source_reasons", []),
            "screen_id": packet.get("screen_id", ""),
            "screen_ids": packet.get("screen_ids", []),
            "strategy_type": packet.get("strategy_type", ""),
            "strategy_types": packet.get("strategy_types", []),
            "entry_bias": packet.get("entry_bias", ""),
            "entry_biases": packet.get("entry_biases", []),
            "screening_reason": packet.get("screening_reason", ""),
            "pool_score": packet.get("pool_score"),
            "pool_rank": packet.get("pool_rank"),
            "pool_score_detail": packet.get("pool_score_detail", {}),
            "pool_total_candidates": packet.get("pool_total_candidates"),
            "pool_scored_candidates": packet.get("pool_scored_candidates"),
            "source_score_records": packet.get("source_score_records", []),
            "sector": packet.get("sector", ""),
            "money_flow": packet.get("money_flow", {}),
            "data_contract": packet.get("data_contract", {}),
            "verified_market_snapshot": packet.get("verified_market_snapshot", {}),
            "market_snapshot_version": packet.get("market_snapshot_version", ""),
            "data_router_version": packet.get("data_router_version", ""),
            "data_router_summary": packet.get("data_router_summary", {}),
            "debate_rounds": self.max_debate_rounds,
            "knowledge_rule_hits": packet.get("knowledge_rule_hits", []),
            "knowledge_rule_score_adjustment": packet.get("knowledge_rule_score_adjustment", 0),
            "knowledge_rule_summary": packet.get("knowledge_rule_summary", ""),
            "knowledge_rule_watch_only": packet.get("knowledge_rule_watch_only", False),
            "knowledge_rule_hard_blocker": packet.get("knowledge_rule_hard_blocker", False),
            "knowledge_rule_version": packet.get("knowledge_rule_version", ""),
            "data_quality_flags": list(dict.fromkeys(
                (packet.get("data_quality_flags", []) or [])
                + (invest_state.get("data_quality_flags", []) or [])
                + (risk_state.get("data_quality_flags", []) or [])
            )),
            "signal": risk_state.get("signal", "WATCH"),
            "buy_score": risk_state.get("buy_score", _fallback_buy_score(risk_state.get("signal", "WATCH"), risk_state.get("confidence", 50))),
            "confidence": risk_state.get("confidence", 50),
            "position_ratio": risk_state.get("position_ratio", 0.15),
            "allow_direct_buy": risk_state.get("allow_direct_buy"),
            "needs_intraday_confirmation": risk_state.get("needs_intraday_confirmation"),
            "entry_condition": risk_state.get("entry_condition", ""),
            "block_buy_reason": risk_state.get("block_buy_reason", ""),
            "reason": risk_state.get("reason", ""),
            "decision_source": risk_state.get("decision_source", ""),
            "raw_final_decision": risk_state.get("raw_final_decision", ""),
            "final_decision": risk_state.get("final_decision", ""),
            "research_plan": invest_state.get("research_plan", ""),
            "debate_history": risk_state.get("history", ""),
            # ★ 6-11 修复：多节点实际跑模型汇总（早报卡片显示真实使用模型，不再误显火山引擎）
            "decision_models": risk_state.get("decision_models", {}),
            "evidence_refs": risk_state.get("evidence_refs", []),
            "missing_data_used": risk_state.get("missing_data_used", []),
            "unsupported_claims": risk_state.get("unsupported_claims", []),
            "evidence_validation": risk_state.get("evidence_validation", {}),
            # ★ 6-04 修复：bull/bear_history 来自 invest 阶段（risk 阶段不会有 Bull/Bear 发言）
            "bull_history": invest_state.get("bull_history", ""),
            "bear_history": invest_state.get("bear_history", ""),
        }

    def run_from_phase1(
        self,
        phase1_output_path: str = "daily-stock-workflow/output",
        market_context: str = "",
    ) -> List[Dict]:
        """
        从 Phase1 输出文件加载候选股，直接运行完整辩论。
        """
        from pathlib import Path

        output_dir = Path(phase1_output_path)

        # 找到最新的 phase1 输出
        phase1_files = sorted(output_dir.glob("phase1_*.json"))
        if not phase1_files:
            raise FileNotFoundError(f"未找到 Phase1 输出文件: {output_dir}/phase1_*.json")

        latest = phase1_files[-1]
        logger.info(f"加载 Phase1 输出: {latest}")

        with open(latest, encoding="utf-8") as f:
            phase1_data = json.load(f)

        candidates = phase1_data.get("candidates", [])
        ranked = phase1_data.get("ranked_stocks", candidates)

        # 评分映射
        stock_scores = {c["stock_code"]: c.get("final_score", c.get("score", 0)) for c in candidates}

        # Top 20 候选股构建辩论包
        top_stocks = ranked[:20]
        debate_packets = []
        for stock in top_stocks:
            code = stock["stock_code"]
            name = stock.get("name", code)
            phase1_cache = load_phase1_cache(output_dir)
            klines = self._fetch_klines(code)
            packet = build_debate_packet(code, name, phase1_cache, klines)
            debate_packets.append(packet)

        return self.run(debate_packets, market_context, stock_scores)

    def _fetch_klines(self, stock_code: str, days: int = 120):
        """获取K线数据（通过HTTP QMT API）。"""
        try:
            from .data_fetcher import get_kline_via_http
            return get_kline_via_http(stock_code, days) or []
        except Exception as e:
            logger.warning(f"K线获取失败 {stock_code}: {e}")
            return []


# ── 便捷入口 ──────────────────────────────────────────────

def run_debate(
    candidates: List[Dict],
    market_context: str = "",
    model: str = DEFAULT_MODEL,
    max_debate_rounds: int = MAX_DEBATE_ROUNDS,
) -> List[Dict]:
    """
    便捷入口：对候选股票列表运行辩论引擎。

    Args:
        candidates: Phase1 输出的候选股列表（每项含 stock_code / name）
        market_context: 市场整体描述
        model: LLM 模型
        max_debate_rounds: 辩论轮数（默认1=说1次+反驳1次）
    """
    engine = StockDebateEngine(model=model, max_debate_rounds=max_debate_rounds)

    # 构建辩论包
    phase1_cache = {}
    debate_packets = []
    for c in candidates:
        code = c["stock_code"]
        name = c.get("name", code)
        klines = engine._fetch_klines(code)
        packet = build_debate_packet(code, name, phase1_cache, klines)
        debate_packets.append(packet)

    stock_scores = {c["stock_code"]: c.get("final_score", c.get("score", 0)) for c in candidates}
    return engine.run(debate_packets, market_context, stock_scores)
