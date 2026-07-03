#!/usr/bin/env python3
"""
统计 daily_report 中资金流数据质量趋势。

用法:
  python3 check_money_flow_quality.py
  python3 check_money_flow_quality.py --days 14
"""

from __future__ import annotations

import argparse
import glob
import json
import re
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"


def report_files() -> list[Path]:
    files = [
        Path(p)
        for p in glob.glob(str(OUTPUT_DIR / "daily_report_*.json"))
        if "daily_report_push_" not in p
    ]
    files.sort()
    return files


def parse_day(path: Path) -> str:
    m = re.search(r"daily_report_(\d{8})\.json$", path.name)
    return m.group(1) if m else path.name


def summarize(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    phase2 = data.get("phase2", {}) or {}
    ranked = phase2.get("ranked_candidates") or phase2.get("full_ranked_candidates") or []
    total = len(ranked)

    flag_counts = (phase2.get("data_quality_summary") or {}).get("flag_counts") or {}
    missing = int(flag_counts.get("MONEY_FLOW_MISSING", 0) or 0)
    partial = int(flag_counts.get("MONEY_FLOW_PARTIAL", 0) or 0)
    fetch_failed = int(flag_counts.get("MONEY_FLOW_FETCH_FAILED", 0) or 0)
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

    if total <= 0:
        miss_ratio = 0.0
        post_seed_ratio = 0.0
    else:
        miss_ratio = (missing + partial) / total
        post_seed_ratio = (post_seed_missing + partial) / total

    return {
        "day": parse_day(path),
        "total": total,
        "missing": missing,
        "partial": partial,
        "fetch_failed": fetch_failed,
        "seedable_missing": seedable_missing,
        "post_seed_missing": post_seed_missing,
        "ratio": miss_ratio,
        "post_seed_ratio": post_seed_ratio,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=10, help="展示最近 N 天")
    args = parser.parse_args()

    files = report_files()
    if not files:
        print("未找到 daily_report_*.json")
        return

    rows = [summarize(p) for p in files[-max(1, args.days):]]
    print("资金流质量趋势 (MONEY_FLOW_MISSING + MONEY_FLOW_PARTIAL):")
    for row in rows:
        print(
            f"{row['day']} total={row['total']} "
            f"missing={row['missing']} partial={row['partial']} fetch_failed={row['fetch_failed']} "
            f"ratio={row['ratio']:.1%} "
            f"seedable_missing={row['seedable_missing']} "
            f"post_seed_ratio={row['post_seed_ratio']:.1%}"
        )

    latest = rows[-1]
    print(
        "\n最新报告: "
        f"{latest['day']} ratio={latest['ratio']:.1%} "
        f"(missing={latest['missing']} partial={latest['partial']} fetch_failed={latest['fetch_failed']} total={latest['total']} "
        f"seedable_missing={latest['seedable_missing']} post_seed_ratio={latest['post_seed_ratio']:.1%})"
    )


if __name__ == "__main__":
    main()
