#!/usr/bin/env python3
"""
诊断候选股资金流多源覆盖情况。

用法:
  python3 diagnose_money_flow_sources.py --limit 10
  python3 diagnose_money_flow_sources.py --report output/daily_report_20260529.json --limit 20
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List

from stock_selection_debate.data_fetcher import (
    _fetch_money_flow_via_mx,
    _fetch_money_flow_via_eastmoney_direct,
    _fetch_money_flow_via_akshare,
    _fetch_money_flow_via_akshare_rank,
    _merge_money_flow,
    _money_flow_coverage,
)


BASE = Path(__file__).resolve().parent
DEFAULT_REPORT = BASE / "output" / "daily_report_20260529.json"


def load_candidates(report_file: Path, limit: int) -> List[Dict[str, Any]]:
    data = json.loads(report_file.read_text(encoding="utf-8"))
    phase2 = data.get("phase2") or {}
    rows = phase2.get("ranked_candidates") or phase2.get("full_ranked_candidates") or []
    rows = [r for r in rows if r.get("stock")]
    return rows[: max(1, limit)]


def has_main(mf: Dict[str, Any]) -> bool:
    return (mf or {}).get("main_net_flow") is not None


def diagnose_one(stock: str) -> Dict[str, Any]:
    # ★ QMT HTTP 资金流已移除（6-04 实测不可用）
    mx = _fetch_money_flow_via_mx(stock) or {}
    em = _fetch_money_flow_via_eastmoney_direct(stock) or {}
    ak = _fetch_money_flow_via_akshare(stock) or {}
    rank = _fetch_money_flow_via_akshare_rank(stock) or {}
    merged = _merge_money_flow({}, mx, em, ak, rank)
    return {
        "qmt": {},
        "mx": mx,
        "eastmoney": em,
        "ak": ak,
        "ak_rank": rank,
        "merged": merged,
    }


def diagnose_one_fast(stock: str, enable_mx: bool = True) -> Dict[str, Any]:
    # ★ QMT HTTP 资金流已移除（6-04 实测不可用）
    mx = _fetch_money_flow_via_mx(stock) or {} if enable_mx else {}
    merged = _merge_money_flow({}, mx)
    if has_main(merged):
        return {"qmt": {}, "mx": mx, "eastmoney": {}, "ak": {}, "ak_rank": {}, "merged": merged}

    # 快速诊断优先尝试 rank（通常可命中缓存），命中则跳过更慢的东财/ak个股接口。
    rank = _fetch_money_flow_via_akshare_rank(stock) or {}
    merged = _merge_money_flow({}, mx, rank)
    if has_main(merged):
        return {"qmt": {}, "mx": mx, "eastmoney": {}, "ak": {}, "ak_rank": rank, "merged": merged}

    em = _fetch_money_flow_via_eastmoney_direct(stock) or {}
    merged = _merge_money_flow({}, mx, rank, em)
    if has_main(merged):
        return {"qmt": {}, "mx": mx, "eastmoney": em, "ak": {}, "ak_rank": rank, "merged": merged}

    ak = _fetch_money_flow_via_akshare(stock) or {}
    merged = _merge_money_flow({}, mx, rank, em, ak)
    return {"qmt": {}, "mx": mx, "eastmoney": em, "ak": ak, "ak_rank": rank, "merged": merged}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--report", default=str(DEFAULT_REPORT))
    p.add_argument("--limit", type=int, default=10)
    p.add_argument("--fast", action="store_true", help="主力资金命中后停止继续探测后续数据源")
    p.add_argument("--rank-cache-only", action="store_true", help="资金流排名只读本地缓存，不触发 akshare 网络请求")
    args = p.parse_args()

    if args.rank_cache_only:
        os.environ["MONEY_FLOW_RANK_CACHE_ONLY"] = "1"

    report_file = Path(args.report)
    if not report_file.exists():
        raise FileNotFoundError(f"report not found: {report_file}")

    rows = load_candidates(report_file, args.limit)
    if not rows:
        print("no candidates")
        return

    src_main_hits = {"qmt": 0, "mx": 0, "eastmoney": 0, "ak": 0, "ak_rank": 0}
    merged_main = 0
    merged_full = 0
    enable_mx = bool(os.getenv("MX_APIKEY") or os.getenv("MINIMAX_API_KEY"))

    for i, r in enumerate(rows, 1):
        stock = str(r.get("stock")).zfill(6)
        name = r.get("name", "")
        d = diagnose_one_fast(stock, enable_mx=enable_mx) if args.fast else diagnose_one(stock)
        for k in src_main_hits:
            if has_main(d[k]):
                src_main_hits[k] += 1
        if has_main(d["merged"]):
            merged_main += 1
        if _money_flow_coverage(d["merged"]) >= 4:
            merged_full += 1

        print(
            f"[{i:02d}] {stock} {name} | "
            f"QMT={'Y' if has_main(d['qmt']) else 'N'} "
            f"MX={'Y' if has_main(d['mx']) else 'N'} "
            f"EM={'Y' if has_main(d['eastmoney']) else 'N'} "
            f"AK={'Y' if has_main(d['ak']) else 'N'} "
            f"RANK={'Y' if has_main(d['ak_rank']) else 'N'} "
            f"=> merged_main={d['merged'].get('main_net_flow')} coverage={_money_flow_coverage(d['merged'])}/4"
        )

    n = len(rows)
    print("\n--- summary ---")
    print(f"stocks={n} merged_main={merged_main}/{n} merged_full={merged_full}/{n}")
    print("main hits by source:", src_main_hits)


if __name__ == "__main__":
    main()
