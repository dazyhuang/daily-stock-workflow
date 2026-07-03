#!/usr/bin/env python3
"""对fallback 50分和未打分股票重新LLM打分"""
import sys
import json
import time
import requests
import logging
import os
from pathlib import Path

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
API_KEY = os.environ.get("MX_DIRECT_KEY", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "rescore.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("rescore")

sys.path.insert(0, str(BASE_DIR))
from llm_scorer import LLMScorer  # noqa: E402

def call_api(prompt, timeout=90):
    volc_key = os.environ.get("VOLCAN_API_KEY", os.environ.get("VOLCAN_ENGINE_API_KEY", ""))
    if not volc_key:
        logger.error("VOLCAN_API_KEY 未设置")
        return ""
    headers = {"Authorization": f"Bearer {volc_key}", "Content-Type": "application/json"}
    payload = {
        "model": "minimax-portal/MiniMax-M3",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 8000,
        "thinking": {"type": "enabled", "budget_tokens": 50000},
    }
    try:
        r = requests.post(
            "https://ark.cn-beijing.volces.com/api/coding/v3/chat/completions",
            headers=headers, json=payload, timeout=timeout
        )
        data = r.json()
        choices = data.get("choices", [])
        if choices:
            text = choices[0].get("message", {}).get("content", "").strip()
            s, e = text.find("{"), text.rfind("}") + 1
            return text[s:e] if s < e else text
    except Exception as e:
        logger.error(f"API错误: {e}")
    return ""

def main():
    with open(OUTPUT_DIR / "daily_report_20260408.json") as f:
        report = json.load(f)

    candidates = report["phase2"]["candidates"]
    all_scores = list(report["phase2"].get("scored", []))

    # 找出需要重打的：fallback 50分 + 未打分
    scored_stocks = {s["stock"]: s for s in all_scores}
    to_rescore = []
    for s in all_scores:
        if s.get("total_score") == 50:
            to_rescore.append(s["stock"])
    for c in candidates:
        if c["stock"] not in scored_stocks:
            to_rescore.append(c["stock"])

    logger.info(f"共 {len(to_rescore)} 只需重打: {to_rescore}")

    scorer = LLMScorer(timeout=90, api_key=API_KEY)

    for i in range(0, len(to_rescore), 2):
        batch_codes = to_rescore[i:i+2]
        batch = [c for c in candidates if c["stock"] in batch_codes]
        if len(batch) == 0:
            continue

        logger.info(f"批次 {(i//2)+1}: {batch_codes}")
        t0 = time.time()

        prompt = scorer._build_prompt([], batch)
        resp = call_api(prompt)
        elapsed = time.time() - t0

        if resp:
            scores = scorer._parse_response(resp, batch)
            if scores:
                for s in scores:
                    # 更新已有记录或新增
                    for idx, existing in enumerate(all_scores):
                        if existing["stock"] == s["stock"]:
                            all_scores[idx] = s
                            break
                    else:
                        all_scores.append(s)
                logger.info(f"  成功: {[(s['stock'], s['total_score']) for s in scores]} ({elapsed:.0f}s)")
            else:
                logger.warning("  解析失败，使用fallback")
                for c in batch:
                    for idx, existing in enumerate(all_scores):
                        if existing["stock"] == c["stock"]:
                            existing["total_score"] = 50
                            existing["action"] = "WATCH"
                            break
        else:
            logger.warning("  API失败，使用fallback 50分")
            for c in batch:
                for idx, existing in enumerate(all_scores):
                    if existing["stock"] == c["stock"]:
                        existing["total_score"] = 50
                        existing["action"] = "WATCH"
                        break

        # 保存进度
        with open(OUTPUT_DIR / "daily_report_20260408.json") as f:
            r2 = json.load(f)
        r2["phase2"]["scored"] = all_scores
        top5 = sorted(all_scores, key=lambda x: x.get("total_score", 0), reverse=True)[:5]
        r2["phase2"]["top_picks"] = [s["stock"] for s in top5]
        with open(OUTPUT_DIR / "daily_report_20260408.json", "w") as f:
            json.dump(r2, f, ensure_ascii=False, indent=2)

        time.sleep(1)

    # 最终结果
    top5 = sorted(all_scores, key=lambda x: x.get("total_score", 0), reverse=True)[:5]
    logger.info("=" * 50)
    logger.info("Top 5:")
    for i, s in enumerate(top5, 1):
        logger.info(f"  {i}. {s['stock']} {s['name']}: {s['total_score']}分 {s['action']}")
    logger.info("=" * 50)
    print("\n📊 最终 Top 5:")
    for i, s in enumerate(top5, 1):
        print(f"{i}. {s['stock']} {s['name']}: {s['total_score']}分 {s['action']}")

if __name__ == "__main__":
    main()

