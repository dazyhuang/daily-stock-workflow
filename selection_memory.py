"""Persistent selection memory for the daily stock workflow.

The file is append-friendly JSONL, but daily writes replace the same report_date
to avoid duplicate memories after resume/re-push.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
MEMORY_PATH = OUTPUT_DIR / "selection_memory.jsonl"


def _read_jsonl(path: Path = MEMORY_PATH) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    items: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            data = json.loads(line)
        except Exception:
            continue
        if isinstance(data, dict):
            items.append(data)
    return items


def _write_jsonl(items: Iterable[Dict[str, Any]], path: Path = MEMORY_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
        f.flush()
    tmp.replace(path)


def _stock_key(item: Dict[str, Any]) -> str:
    raw = str(item.get("stock") or item.get("stock_code") or "").strip()
    return raw.zfill(6) if raw else ""


def _date_key(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[:8] if len(digits) >= 8 else ""


def _extract_candidates(phase2: Dict[str, Any]) -> List[Dict[str, Any]]:
    ranked = phase2.get("ranked_candidates") or phase2.get("candidates") or []
    if ranked:
        return [x for x in ranked if isinstance(x, dict)]
    return [x for x in (phase2.get("top_picks") or []) if isinstance(x, dict)]


def _backtest_maps(backtest_selection: Dict[str, Any], backtest_strategy: Dict[str, Any]) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    selection = {
        _stock_key(x): x
        for x in (backtest_selection or {}).get("stocks", [])
        if isinstance(x, dict) and _stock_key(x)
    }
    strategy = {
        _stock_key(x): x
        for x in (backtest_strategy or {}).get("trades", [])
        if isinstance(x, dict) and _stock_key(x)
    }
    return selection, strategy


def append_daily_selection_memory(
    report: Dict[str, Any],
    backtest_selection: Dict[str, Any] | None = None,
    backtest_strategy: Dict[str, Any] | None = None,
    path: Path = MEMORY_PATH,
) -> int:
    phase2 = report.get("phase2") or {}
    ranked = _extract_candidates(phase2)
    if not ranked:
        return 0
    report_date = _date_key(report.get("date") or report.get("report_date"))
    generated_at = report.get("timestamp") or phase2.get("timestamp") or ""
    top_codes = [_stock_key(x) for x in (phase2.get("top_picks") or []) if isinstance(x, dict)]
    top_rank = {code: idx + 1 for idx, code in enumerate(top_codes) if code}
    selection_map, strategy_map = _backtest_maps(backtest_selection or report.get("phase3_selection") or {}, backtest_strategy or report.get("phase3_strategy") or {})

    new_items: List[Dict[str, Any]] = []
    for idx, item in enumerate(ranked, 1):
        if not isinstance(item, dict):
            continue
        code = _stock_key(item)
        if not code:
            continue
        strategy = strategy_map.get(code, {})
        selection = selection_map.get(code, {})
        decision_models = item.get("decision_models") or {}
        new_items.append({
            "report_date": report_date,
            "generated_at": generated_at,
            "stock": code,
            "name": item.get("name") or item.get("stock_name") or "",
            "rank": idx,
            "top5_rank": top_rank.get(code),
            "signal": item.get("signal"),
            "pm_signal": item.get("pm_signal") or item.get("llm_signal"),
            "pm_score": item.get("pm_score") or item.get("llm_buy_score"),
            "pm_confidence": item.get("pm_confidence") or item.get("llm_confidence"),
            "pm_reason": item.get("pm_reason"),
            "final_reason": item.get("final_reason") or item.get("reason"),
            "buy_score": item.get("buy_score"),
            "confidence": item.get("confidence"),
            "ranking_score": item.get("ranking_score"),
            "quant_base_score": item.get("quant_base_score"),
            "llm_risk_adjustment": item.get("llm_risk_adjustment"),
            "historical_edge_score": item.get("historical_edge_score"),
            "historical_weakness_penalty": item.get("historical_weakness_penalty"),
            "pool": item.get("pool"),
            "source": item.get("source"),
            "source_pools": item.get("source_pools") or [],
            "strategy_type": item.get("strategy_type"),
            "strategy_types": item.get("strategy_types") or [],
            "entry_bias": item.get("entry_bias"),
            "entry_biases": item.get("entry_biases") or [],
            "pool_score": item.get("pool_score"),
            "money_flow": item.get("money_flow") or {},
            "money_flow_source": item.get("money_flow_source") or (item.get("money_flow") or {}).get("source"),
            "data_quality_flags": item.get("data_quality_flags") or [],
            "data_contract": item.get("data_contract") or {},
            "decision_models": decision_models,
            "pm_model": decision_models.get("pm") or item.get("decision_source"),
            "selection_backtest": selection,
            "strategy_backtest": strategy,
            "primary_attribution": item.get("primary_attribution"),
            "attribution_labels": item.get("attribution_labels") or [],
        })

    existing = [x for x in _read_jsonl(path) if _date_key(x.get("report_date")) != report_date]
    _write_jsonl(existing + new_items, path)
    return len(new_items)


def quarantine_unverified_selection_memory(path: Path = MEMORY_PATH) -> int:
    items = _read_jsonl(path)
    changed = 0
    for item in items:
        for horizon in ("d1", "d3", "d5", "d10"):
            if item.get(f"return_{horizon}_complete") is True:
                continue
            if item.get(f"return_{horizon}_pct") is not None or item.get(f"alpha_{horizon}_pct") is not None:
                changed += 1
            item[f"return_{horizon}_pct"] = None
            item[f"alpha_{horizon}_pct"] = None
    if changed:
        _write_jsonl(items, path)
    return changed


def update_selection_memory_from_top5_review(review: Dict[str, Any], path: Path = MEMORY_PATH) -> int:
    items = _read_jsonl(path)
    if not items:
        return 0
    # Legacy reviews wrote last-available or embedded backtest values into D+N
    # without proving that the horizon had matured.  Quarantine all such values.
    quarantined = 0
    for item in items:
        for horizon in ("d1", "d3", "d5", "d10"):
            if item.get(f"return_{horizon}_complete") is not True:
                if item.get(f"return_{horizon}_pct") is not None or item.get(f"alpha_{horizon}_pct") is not None:
                    quarantined += 1
                item[f"return_{horizon}_pct"] = None
                item[f"alpha_{horizon}_pct"] = None
    index = {(_date_key(x.get("report_date")), _stock_key(x)): x for x in items}
    updated = 0
    for review_item in review.get("items") or []:
        if not isinstance(review_item, dict):
            continue
        key = (_date_key(review_item.get("report_date") or review_item.get("date")), _stock_key(review_item))
        target = index.get(key)
        if not target:
            continue
        future_returns = review_item.get("future_returns_pct") or {}
        future_complete = review_item.get("future_return_complete") or {}
        future_dates = review_item.get("future_return_dates") or {}
        alpha = review_item.get("alpha_pct") or {}
        for horizon in ("d1", "d3", "d5", "d10"):
            is_complete = future_complete.get(horizon) is True
            target[f"return_{horizon}_complete"] = is_complete
            target[f"return_{horizon}_date"] = future_dates.get(horizon) if is_complete else None
            if is_complete:
                target[f"return_{horizon}_pct"] = future_returns.get(horizon)
                target[f"alpha_{horizon}_pct"] = alpha.get(horizon)
            else:
                # Clear legacy values that were written from an incomplete
                # horizon or embedded strategy simulation.
                target[f"return_{horizon}_pct"] = None
                target[f"alpha_{horizon}_pct"] = None
        target["review_price_data_quality"] = review_item.get("price_data_quality")
        target["review_reference_source"] = review_item.get("reference_source")
        target["review_updated_at"] = review.get("generated_at")
        target["was_bought"] = bool(review_item.get("actual_bought"))
        target["was_filled"] = bool(review_item.get("filled"))
        for field in (
            "primary_attribution",
            "attribution_labels",
            "return_d1_pct",
            "return_d3_pct",
            "return_d5_pct",
            "return_d10_pct",
            "alpha_d1_pct",
            "alpha_d3_pct",
            "alpha_d5_pct",
            "alpha_d10_pct",
            "was_bought",
            "was_filled",
            "not_bought_reason",
        ):
            if field in review_item:
                target[field] = review_item.get(field)
        updated += 1
    if updated or quarantined:
        _write_jsonl(items, path)
    return updated
