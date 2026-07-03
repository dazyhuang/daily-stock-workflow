#!/usr/bin/env python3
"""
独立LLM打分脚本 - 批量打分 + 增量保存
用法: python3 run_llm_scoring.py [date]
"""
import sys
import json
import logging
import time
import os
from pathlib import Path
from datetime import date

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
SKILLS_DIR = Path.home() / ".openclaw/workspace/skills"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / f"llm_score_{date.today().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("llm_score")

sys.path.insert(0, str(BASE_DIR))
from llm_scorer import LLMScorer  # noqa: E402

BATCH_SIZE = 4  # 每批4只，减少openclaw启动开销


def main():
    target_date = sys.argv[1] if len(sys.argv) > 1 else date.today().strftime("%Y%m%d")
    report_path = OUTPUT_DIR / f"daily_report_{target_date}.json"

    if not report_path.exists():
        logger.error(f"报告不存在: {report_path}")
        sys.exit(1)

    with open(report_path) as f:
        report = json.load(f)

    candidates = report.get("phase2", {}).get("candidates", [])
    if not candidates:
        logger.error("无候选股票")
        sys.exit(1)

    # 加载已有的打分结果，避免重复打分
    existing_scores = {}
    if report.get("phase2", {}).get("scored"):
        for s in report["phase2"]["scored"]:
            # 缓存加载时也做 action 阈值校验（防止旧缓存分数不合规）
            score = s.get("adjusted_score", s.get("total_score", 0))
            action = s.get("action", "")
            expected = "BUY" if score >= 70 else ("WATCH" if score >= 50 else "AVOID")
            if action != expected:
                logger.warning(f"  缓存修正: {s.get('stock','?')} score={score} {action}→{expected}")
                s["action"] = expected
            existing_scores[s["stock"]] = s
        logger.info(f"已有 {len(existing_scores)} 只股票已打分，跳过")

    all_scores = list(existing_scores.values())
    remaining = [c for c in candidates if c["stock"] not in existing_scores]
    total = len(candidates)
    logger.info(f"候选股票共 {total} 只，剩余 {len(remaining)} 只待打分（每批{BATCH_SIZE}只）")

    if not remaining:
        logger.info("全部股票已打完，输出Top 5")
        output_top5(all_scores, report_path)
        return

    # 使用直接API（如果提供key）
    api_key = os.environ.get("MX_DIRECT_KEY", "")
    if api_key:
        logger.info(f"使用直接API (MiniMax), key长度: {len(api_key)}")
        scorer = LLMScorer(timeout=60, api_key=api_key)
    else:
        logger.info("使用openclaw agent --local")
        scorer = LLMScorer(timeout=300)

    # 分批处理：每批BATCH_SIZE只
    for batch_start in range(0, len(remaining), BATCH_SIZE):
        batch = remaining[batch_start:batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE
        stocks = [c["stock"] for c in batch]
        logger.info(f"批次 {batch_num}/{total_batches}: {len(batch)} 只 {stocks}")

        start = time.time()
        prompt = scorer._build_prompt([], batch)
        resp = scorer._call_llm(prompt)
        elapsed = time.time() - start

        if resp:
            batch_scores = scorer._parse_response(resp, batch)
            if batch_scores:
                all_scores.extend(batch_scores)
                for s in batch_scores:
                    logger.info(f"  → {s['stock']} {s['name']}: {s['total_score']}分 {s['action']} ({elapsed:.0f}s)")
            else:
                logger.warning("  → JSON解析失败，使用默认50分")
                for c in batch:
                    fallback = scorer._fallback_scores([c])
                    all_scores.extend(fallback)
        else:
            logger.warning("  → LLM调用失败，使用默认50分")
            for c in batch:
                all_scores.extend(scorer._fallback_scores([c]))

        # 增量保存
        with open(report_path) as f:
            _report = json.load(f)
        _report["phase2"]["scored"] = all_scores
        top5 = sorted(all_scores, key=lambda x: x.get("total_score", 0), reverse=True)[:5]
        _report["phase2"]["top_picks"] = [s["stock"] for s in top5]
        with open(report_path, "w") as f:
            json.dump(_report, f, ensure_ascii=False, indent=2)
        logger.info(f"  ↳ 进度已存: {len(all_scores)}/{total}")

        if len(all_scores) % 10 == 0:
            logger.info(f"★★★ 已完成 {len(all_scores)}/{total} 只 ★★★")

    output_top5(all_scores, report_path)


def output_top5(all_scores, report_path):
    top5 = sorted(all_scores, key=lambda x: x.get("total_score", 0), reverse=True)[:5]
    with open(report_path) as f:
        report = json.load(f)
    report["phase2"]["scored"] = all_scores
    report["phase2"]["top_picks"] = [s["stock"] for s in top5]
    with open(report_path, "w") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    logger.info(f"打分完成: {len(all_scores)} 只")
    logger.info("=" * 50)
    logger.info("Top 5:")
    for s in top5:
        logger.info(f"  {s['stock']} {s['name']}: {s['total_score']}分 {s['action']} - {s.get('reason', '')[:40]}")
    logger.info("=" * 50)

    print("\n📊 Top 5 选股结果:")
    for i, s in enumerate(top5, 1):
        print(f"{i}. {s['stock']} {s['name']}: {s['total_score']}分 {s['action']}")


if __name__ == "__main__":
    main()


