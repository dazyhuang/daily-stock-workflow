#!/usr/bin/env python3
"""Run a focused full workflow test with one stock from each xuangu pool."""

import csv
import json
import logging
import os
import random
import shutil
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

if os.environ.get("RUN_LIVE_WORKFLOW_TESTS") != "1":
    print("skipped: set RUN_LIVE_WORKFLOW_TESTS=1 to run live pool full workflow test")
    raise SystemExit(0)

import llm_scorer
import workflow


def _candidate_from_ranked_row(row, cfg, stats, sample_mode: str):
    stock = llm_scorer._normalize_stock_code(row.get("股票代码") or row.get("代码"))
    name = row.get("股票名称") or row.get("名称") or stock
    score = row.get("_pool_score", 0)
    rank = row.get("_pool_rank", 0)
    reason = f"[{cfg['pool']}]池内评分{score:.1f}/100 排名{rank}: {cfg['query'][:18]}"
    return {
        "stock": stock,
        "name": str(name).strip(),
        "reason": reason,
        "source": f"xuangu_pool_{sample_mode}_test",
        "pool": cfg["pool"],
        "screen_id": cfg["screen_id"],
        "query": cfg["query"],
        "strategy_type": cfg["strategy_type"],
        "entry_bias": cfg["entry_bias"],
        "priority": cfg.get("priority", 99),
        "pool_score": score,
        "pool_rank": rank,
        "pool_score_detail": row.get("_pool_score_detail", {}),
        "pool_total_candidates": stats.get("raw", 0),
        "pool_scored_candidates": stats.get("scored", 0),
        "source_pools": [cfg["pool"]],
        "source_queries": [cfg["query"]],
        "source_reasons": [reason],
        "screen_ids": [cfg["screen_id"]],
        "strategy_types": [cfg["strategy_type"]],
        "entry_biases": [cfg["entry_bias"]],
        "screening_reason": reason,
    }


def _choose_ranked_row(ranked, seen, sample_mode: str, rng: random.Random):
    available = [
        row for row in ranked
        if llm_scorer._normalize_stock_code(row.get("股票代码") or row.get("代码"))
    ]
    unique_rows = [
        row for row in available
        if llm_scorer._normalize_stock_code(row.get("股票代码") or row.get("代码")) not in seen
    ]
    source_rows = unique_rows or available
    if not source_rows:
        return None, []
    if sample_mode == "random":
        return rng.choice(source_rows), [
            llm_scorer._normalize_stock_code(row.get("股票代码") or row.get("代码"))
            for row in available
            if llm_scorer._normalize_stock_code(row.get("股票代码") or row.get("代码")) in seen
        ]
    chosen = source_rows[0]
    duplicate_skips = []
    for row in available:
        stock = llm_scorer._normalize_stock_code(row.get("股票代码") or row.get("代码"))
        if stock in seen:
            duplicate_skips.append(stock)
            continue
        break
    return chosen, duplicate_skips


def build_pool_candidates(test_output: Path, sample_mode: str, rng: random.Random):
    skills_dir = Path(os.environ.get("OPENCLAW_WORKSPACE", "./workspace")) / "skills"
    gen = llm_scorer.CandidateGenerator(skills_dir, test_output)
    generated = gen._run_xuangu_screening()

    xuangu_dir = test_output / "xuangu"
    selected = []
    issues = []
    seen = set()
    per_pool = []

    for cfg in llm_scorer.XUANGU_SCREEN_CONFIGS:
        safe = llm_scorer._xuangu_safe_filename(cfg["query"])
        csv_file = xuangu_dir / f"mx_xuangu_{safe}.csv"
        if not csv_file.exists():
            matches = sorted(xuangu_dir.glob(f"mx_xuangu_{safe[:32]}*.csv"))
            csv_file = matches[-1] if matches else csv_file

        if csv_file.exists():
            with open(csv_file, encoding="utf-8-sig") as f:
                rows = list(csv.DictReader(f))
            ranked, stats = llm_scorer._rank_xuangu_rows_for_pool(rows, cfg, top_n=20)
            chosen_row, duplicate_skips = _choose_ranked_row(ranked, seen, sample_mode, rng)
            if not chosen_row and ranked:
                chosen_row = rng.choice(ranked) if sample_mode == "random" else ranked[0]
                issues.append({
                    "severity": "medium",
                    "area": "candidate_selection",
                    "message": f"{cfg['pool']} 前20与其他池全部重复，只能复用 {chosen_row.get('代码') or chosen_row.get('股票代码')}",
                })
            if not chosen_row:
                issues.append({
                    "severity": "high",
                    "area": "candidate_selection",
                    "message": f"{cfg['pool']} 没有可选候选股",
                    "stats": stats,
                })
                continue
            candidate = _candidate_from_ranked_row(chosen_row, cfg, stats, sample_mode)
            if duplicate_skips:
                candidate["pool_rank_note"] = f"跳过重复Top: {','.join(duplicate_skips[:5])}"
            if sample_mode == "random":
                candidate["sample_note"] = "从每池排序前20只随机抽样"
            selected.append(candidate)
            seen.add(candidate["stock"])
            per_pool.append({
                "pool": cfg["pool"],
                "stock": candidate["stock"],
                "name": candidate["name"],
                "pool_rank": candidate["pool_rank"],
                "pool_score": candidate["pool_score"],
                "raw": stats.get("raw", 0),
                "scored": stats.get("scored", 0),
                "filtered": stats.get("filtered", 0),
                "note": candidate.get("sample_note") or candidate.get("pool_rank_note", ""),
            })
            continue

        fallback = [
            c for c in generated
            if cfg["pool"] in (c.get("source_pools") or [c.get("pool")])
            and c.get("stock") not in seen
        ]
        fallback.sort(key=lambda c: (c.get("pool_rank") or 9999, -(c.get("pool_score") or 0)))
        if fallback:
            chosen = dict(rng.choice(fallback[:20]) if sample_mode == "random" else fallback[0])
            chosen["source"] = f"xuangu_pool_{sample_mode}_test"
            selected.append(chosen)
            seen.add(chosen["stock"])
            per_pool.append({
                "pool": cfg["pool"],
                "stock": chosen.get("stock"),
                "name": chosen.get("name"),
                "pool_rank": chosen.get("pool_rank"),
                "pool_score": chosen.get("pool_score"),
                "raw": None,
                "scored": None,
                "filtered": None,
                "note": "CSV缺失，使用生成器合并结果兜底",
            })
            issues.append({
                "severity": "medium",
                "area": "candidate_selection",
                "message": f"{cfg['pool']} CSV缺失，使用生成器合并结果兜底",
            })
        else:
            issues.append({
                "severity": "high",
                "area": "candidate_selection",
                "message": f"{cfg['pool']} CSV缺失且无兜底候选",
            })

    return selected, per_pool, issues


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    sample_mode = os.getenv("POOL_WORKFLOW_SAMPLE_MODE", "top1").strip().lower()
    if sample_mode not in {"top1", "random"}:
        raise SystemExit(f"unsupported POOL_WORKFLOW_SAMPLE_MODE={sample_mode!r}; use top1 or random")
    seed = int(os.getenv("POOL_WORKFLOW_RANDOM_SEED") or datetime.now().strftime("%Y%m%d%H%M%S"))
    rng = random.Random(seed)
    test_output = BASE_DIR / "output" / f"pool_{sample_mode}_full_workflow_{run_id}"
    test_output.mkdir(parents=True, exist_ok=True)

    src_cache = BASE_DIR / "output" / "fundamental_cache" / "all_stocks_financial.json"
    if src_cache.exists():
        dst_cache = test_output / "fundamental_cache" / "all_stocks_financial.json"
        dst_cache.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_cache, dst_cache)

    workflow.OUTPUT_DIR = test_output
    workflow.SKILLS_DIR = Path(os.environ.get("OPENCLAW_WORKSPACE", "./workspace")) / "skills"

    selected, per_pool, issues = build_pool_candidates(test_output, sample_mode, rng)
    if len(selected) != 6:
        issues.append({
            "severity": "high",
            "area": "candidate_selection",
            "message": f"预期6只候选，实际{len(selected)}只",
        })

    phase1_results = [
        {
            "status": "success",
            "name": "新闻分析师",
            "color": "📰",
            "findings": f"1. 【测试上下文】本次为6池{sample_mode}完整流程测试，不注入额外利好。 | 来源:本地测试",
        },
        {
            "status": "success",
            "name": "市场情绪分析师",
            "color": "🌡️",
            "findings": "测试上下文：以个股自身K线、资金与财务数据为主。",
        },
    ]
    gen = SimpleNamespace(
        candidates=selected,
        screening_signature=llm_scorer._screening_signature() + f"_pool_{sample_mode}_test",
    )

    phase2 = workflow.run_phase2_debate(
        phase1_results,
        gen=gen,
        dry_run=True,
        model=os.getenv("POOL_TOP1_TEST_MODEL", "volcengine-plan/ark-code-latest"),
        resume=False,
    )
    top_picks = phase2.get("top_picks", [])
    backtest_selection = workflow.run_backtest_selection(top_picks) if top_picks else {"status": "no_candidates"}
    backtest_strategy = workflow.run_backtest_strategy(top_picks) if top_picks else {"status": "no_candidates"}

    import intraday_executor
    report = {
        "date": datetime.now().date().isoformat(),
        "phase1": phase1_results,
        "phase2": phase2,
        "phase3_selection": backtest_selection,
        "phase3_strategy": backtest_strategy,
    }
    intraday_signals = intraday_executor._select_intraday_buy_signals(report, 5)

    ranked = phase2.get("ranked_candidates", [])
    if len(ranked) != len(selected):
        issues.append({
            "severity": "high",
            "area": "debate",
            "message": f"辩论结果数量不一致：候选{len(selected)}，结果{len(ranked)}",
        })
    if not top_picks:
        issues.append({
            "severity": "high",
            "area": "top_picks",
            "message": "phase2.top_picks 为空",
        })
    for item in ranked:
        if not item.get("source_pools") or item.get("pool_score") is None:
            issues.append({
                "severity": "medium",
                "area": "metadata",
                "stock": item.get("stock"),
                "message": "辩论结果缺少第一阶段来源或池内评分",
            })
        flags = item.get("data_quality_flags") or []
        if "KLINE_MISSING" in flags:
            issues.append({
                "severity": "high",
                "area": "data_quality",
                "stock": item.get("stock"),
                "message": "K线缺失，无法可靠辩论/入选",
                "flags": flags,
            })
        elif "KLINE_SHORT" in flags:
            issues.append({
                "severity": "medium",
                "area": "data_quality",
                "stock": item.get("stock"),
                "message": "K线偏短，Top5与盘中买入会过滤",
                "flags": flags,
            })
        if item.get("decision_source") in {"TextOnly", "Repaired"}:
            issues.append({
                "severity": "low",
                "area": "llm_output",
                "stock": item.get("stock"),
                "message": f"裁决来源为 {item.get('decision_source')}，说明结构化输出未一次成功",
            })

    summary = {
        "run_id": run_id,
        "output_dir": str(test_output),
        "sample_mode": sample_mode,
        "random_seed": seed,
        "selected_candidates": per_pool,
        "ranked": [
            {
                "stock": c.get("stock"),
                "name": c.get("name"),
                "pool": c.get("pool"),
                "signal": c.get("signal"),
                "confidence": c.get("confidence"),
                "position_ratio": c.get("position_ratio"),
                "pool_rank": c.get("pool_rank"),
                "pool_score": c.get("pool_score"),
                "decision_source": c.get("decision_source"),
                "flags": c.get("data_quality_flags", []),
                "reason": c.get("reason", "")[:180],
            }
            for c in ranked
        ],
        "top_picks": [
            {
                "stock": c.get("stock"),
                "name": c.get("name"),
                "signal": c.get("signal"),
                "confidence": c.get("total_score") or c.get("confidence"),
                "position_ratio": c.get("position_ratio"),
                "pool": c.get("pool"),
            }
            for c in top_picks
        ],
        "intraday_buy_signals": [
            {
                "stock": c.get("stock"),
                "name": c.get("name"),
                "signal": c.get("signal"),
                "confidence": c.get("confidence"),
                "position_ratio": c.get("position_ratio"),
            }
            for c in intraday_signals
        ],
        "backtest_selection": backtest_selection,
        "backtest_strategy": backtest_strategy,
        "issues": issues,
    }

    out_file = test_output / "summary.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
