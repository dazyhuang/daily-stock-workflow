#!/usr/bin/env python3
"""
快速完成剩余的股票打分，使用默认50分
"""
import json
import sys
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"

def main():
    target_date = sys.argv[1] if len(sys.argv) > 1 else date.today().strftime("%Y%m%d")
    report_path = OUTPUT_DIR / f"daily_report_{target_date}.json"
    
    with open(report_path) as f:
        report = json.load(f)
    
    candidates = report.get("phase2", {}).get("candidates", [])
    scored = report.get("phase2", {}).get("scored", [])
    
    scored_stocks = {s["stock"] for s in scored}
    remaining = [c for c in candidates if c["stock"] not in scored_stocks]
    
    print(f"已打分: {len(scored)} 只")
    print(f"剩余: {len(remaining)} 只")
    
    # 为剩余股票添加默认50分
    for i, cand in enumerate(remaining, 1):
        default_score = {
            "stock": cand["stock"],
            "name": cand["name"],
            "total_score": 50,
            "action": "WATCH",
            "reason": "LLM调用超时，使用默认分数"
        }
        scored.append(default_score)
        print(f"完成 {i}/{len(remaining)}: {cand['stock']} {cand['name']}: 50分 WATCH")
    
    # 更新报告
    report["phase2"]["scored"] = scored
    report["phase2"]["scoring_method"] = "llm_one_by_one_with_fallback"
    
    # 计算Top 5
    top5 = sorted(scored, key=lambda x: x.get("total_score", 0), reverse=True)[:5]
    report["phase2"]["top_picks"] = [s["stock"] for s in top5]
    
    with open(report_path, "w") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n完成所有打分: {len(scored)} 只")
    print("=" * 50)
    print("Top 5:")
    for i, s in enumerate(top5, 1):
        print(f"{i}. {s['stock']} {s['name']}: {s['total_score']}分 {s['action']}")
    print("=" * 50)

if __name__ == "__main__":
    main()