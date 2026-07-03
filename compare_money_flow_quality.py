#!/usr/bin/env python3
"""Compare money-flow quality between two daily reports."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


BASE = Path(__file__).resolve().parent / "output"


def load(day: str) -> dict:
    path = BASE / f"daily_report_{day}.json"
    if not path.exists():
        raise FileNotFoundError(f"report not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    phase2 = data.get("phase2", {}) or {}
    ranked = phase2.get("ranked_candidates") or phase2.get("full_ranked_candidates") or []
    counts = (phase2.get("data_quality_summary") or {}).get("flag_counts") or {}
    total = len(ranked)
    missing = int(counts.get("MONEY_FLOW_MISSING", 0) or 0)
    partial = int(counts.get("MONEY_FLOW_PARTIAL", 0) or 0)
    fetch_failed = int(counts.get("MONEY_FLOW_FETCH_FAILED", 0) or 0)
    missing_rows = [
        r for r in ranked
        if "MONEY_FLOW_MISSING" in (r.get("data_quality_flags") or [])
    ]
    seedable_missing = sum(
        1 for r in missing_rows
        if isinstance(r.get("pool_score_detail"), dict)
        and r["pool_score_detail"].get("main_flow_value") is not None
    )
    post_seed_missing = max(0, missing - seedable_missing)
    ratio = (missing + partial) / total if total else 0.0
    post_seed_ratio = (post_seed_missing + partial) / total if total else 0.0
    return {
        "day": day,
        "path": str(path),
        "total": total,
        "missing": missing,
        "partial": partial,
        "fetch_failed": fetch_failed,
        "seedable_missing": seedable_missing,
        "post_seed_missing": post_seed_missing,
        "ratio": ratio,
        "post_seed_ratio": post_seed_ratio,
    }


def fmt(x: dict) -> str:
    return (
        f"{x['day']} total={x['total']} "
        f"missing={x['missing']} partial={x['partial']} fetch_failed={x['fetch_failed']} "
        f"ratio={x['ratio']:.1%} seedable_missing={x['seedable_missing']} "
        f"post_seed_ratio={x['post_seed_ratio']:.1%}"
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--before", required=True, help="YYYYMMDD")
    p.add_argument("--after", required=True, help="YYYYMMDD")
    args = p.parse_args()

    b = load(args.before)
    a = load(args.after)
    print("before:", fmt(b))
    print("after :", fmt(a))
    delta = a["ratio"] - b["ratio"]
    post_seed_delta = a["post_seed_ratio"] - b["post_seed_ratio"]
    print(f"delta : {delta:+.1%} (after-before)")
    print(f"delta(post-seed): {post_seed_delta:+.1%} (after-before)")


if __name__ == "__main__":
    main()
