#!/usr/bin/env python3
"""
测试选股工作流：从六个池子各选1只股票，跑完整流程。
六个池子：准备启动、突破新高、首板追击、热点龙头、强势反包、资金异动
"""

import os, sys, json, logging, traceback
from pathlib import Path
from datetime import datetime, date

if os.environ.get("RUN_LIVE_WORKFLOW_TESTS") != "1":
    print("skipped: set RUN_LIVE_WORKFLOW_TESTS=1 to run live full workflow test")
    raise SystemExit(0)

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / f"test_workflow_{date.today().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("test_workflow")

# ── 测试股票：从六个池子各选1只 ─────────────────────────────
# 每只股票附带 pool 来源，用于验证知识库注入
TEST_CANDIDATES = [
    {"stock": "002463", "name": "沪电股份", "pool": "准备启动", "screen_id": "startup_setup", "reason": "均线粘合突破，资金持续净流入"},
    {"stock": "002594", "name": "比亚迪", "pool": "突破新高", "screen_id": "breakout_high", "reason": "股价突破20日高点，量能放大"},
    {"stock": "600519", "name": "贵州茅台", "pool": "首板追击", "screen_id": "first_limit", "reason": "首板涨停，封单稳健"},
    {"stock": "300750", "name": "宁德时代", "pool": "热点龙头", "screen_id": "sector_leader", "reason": "新能源板块涨幅前3，资金流入"},
    {"stock": "688041", "name": "龙芯中科", "pool": "强势反包", "screen_id": "strong_reversal", "reason": "前两日下跌，昨日反包上涨8%"},
    {"stock": "000001", "name": "平安银行", "pool": "资金异动", "screen_id": "capital_absorption", "reason": "近三日下跌但主力资金每日净流入"},
]

# Phase 1 简化为占位结果（主要测 Phase 2 辩论）
def run_phase1():
    logger.info("=== Phase 1: 模拟分析师结果（仅作候选股来源验证）===")
    return [
        {"name": "技术分析师", "status": "success", "candidates": TEST_CANDIDATES[:3]},
        {"name": "新闻分析师", "status": "success", "candidates": TEST_CANDIDATES[3:]},
    ]

def run_phase2_with_candidates(candidates):
    """Phase 2: 辩论选股（6只股票）"""
    from stock_selection_debate.run_debate_phase import run_debate_phase

    logger.info(f"=== Phase 2: 辩论选股 ({len(candidates)} 只) ===")
    try:
        result = run_debate_phase(
            candidates=candidates,
            model="volcengine-plan/ark-code-latest",
            output_dir=OUTPUT_DIR,
        )
        logger.info(f"辩论完成: {len(result.get('ranked_candidates', []))} 只候选")
        return result
    except Exception as e:
        logger.error(f"辩论失败: {e}\n{traceback.format_exc()}")
        return {"error": str(e)}

def show_phase2_summary(result):
    """展示 Phase 2 结果摘要"""
    ranked = result.get("ranked_candidates", [])
    logger.info("\n===== Phase 2 结果 =====")
    for i, c in enumerate(ranked[:6], 1):
        signal = c.get("signal", "?")
        conf = c.get("confidence", c.get("final_score", "?"))
        reason = (c.get("reason") or c.get("final_decision", ""))[:80]
        logger.info(f"  {i}. {c['stock']} {c['name']} [{signal}] conf={conf}")
        logger.info(f"       {reason}")
        logger.info(f"       pool={c.get('pool')} screen={c.get('screen_id')}")
    return ranked[:5]

def run_phase3_backtest(top_picks):
    """Phase 3: 回测验证——使用 QMT HTTP API 获取本地K线，无外部网络依赖"""
    logger.info(f"\n===== Phase 3: 回测验证 ({len(top_picks)} 只) =====")
    from stock_selection_debate.data_fetcher import _fetch_kline_via_http

    for c in top_picks:
        stock = c["stock"]
        name = c.get("name", stock)
        try:
            kline_raw = _fetch_kline_via_http(stock, days=30)
            if not kline_raw:
                logger.warning(f"  {stock} {name}: 无K线数据")
                c["backtest_change_5d"] = None
                continue

            # 取最近5个交易日计算涨跌
            closes = [float(b.get("close", 0)) for b in kline_raw if b.get("close")]
            if len(closes) >= 5:
                change_5d = (closes[-1] - closes[-5]) / closes[-5] * 100
                c["backtest_change_5d"] = round(change_5d, 2)
                c["backtest_latest_price"] = closes[-1]
                logger.info(f"  {stock} {name}: 5日涨跌 {change_5d:.2f}% (现价{closes[-1]})")
            else:
                logger.warning(f"  {stock} {name}: K线不足5天 (仅{len(closes)}天)")
                c["backtest_change_5d"] = None
        except Exception as e:
            logger.warning(f"  {stock} {name}: 回测失败: {e}")
            c["backtest_change_5d"] = None

def main():
    logger.info("===== 测试选股工作流开始 =====")
    logger.info(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"候选股: {[c['stock'] for c in TEST_CANDIDATES]}")

    # Phase 1 - 技术分析（简化，不跑完整5个分析师）
    phase1_results = run_phase1()

    # Phase 2 - 辩论
    phase2_result = run_phase2_with_candidates(TEST_CANDIDATES)
    if phase2_result.get("error"):
        logger.error(f"Phase 2 失败，终止: {phase2_result['error']}")
        return

    top5 = show_phase2_summary(phase2_result)

    # Phase 3 - 回测
    run_phase3_backtest(top5)

    # 保存结果
    output_file = OUTPUT_DIR / f"test_workflow_result_{date.today().strftime('%Y%m%d')}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump({
            "date": date.today().strftime("%Y-%m-%d"),
            "test_candidates": TEST_CANDIDATES,
            "phase2": phase2_result,
            "top5": top5,
        }, f, ensure_ascii=False, indent=2)
    logger.info(f"\n结果已保存: {output_file}")
    logger.info("===== 测试选股工作流完成 =====")

if __name__ == "__main__":
    main()
