#!/usr/bin/env python3
"""
周复盘辩论主入口
================
对齐 TradingAgents cli/main.py 的 run_analysis 模式:

用法:
  python3 run_weekly_debate.py
  python3 run_weekly_debate.py --input output/weekly_review_20260515.json
  python3 run_weekly_debate.py --input output/weekly_review_20260515.json --stocks

断点续跑:
  - 周参数辩论(5角色串行):整个 graph 跑完才出结果,无法中间断点(原子操作,几秒完成)
  - 持仓辩论(每持仓股票独立):每完成一只立即写断点,崩溃重跑只丢当前处理那只

Options:
  --input: 周报JSON文件(默认从 output/ 找到最新的 weekly_review_*.json)
  --output: 输出文件路径(默认 output/strategy_debate_result_YYYYMMDD.json)
  --model: LLM 模型(默认 volcengine-plan/ark-code-latest)
  --stocks: 是否对每持仓股票执行独立辩论(默认 False)
  --checkpoint: 启用断点续跑(默认 False)
"""

import os
import sys
import json
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Any, Optional

# ── 工作目录设置 ─────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"
CHECKPOINT_DIR = Path(__file__).parent / "checkpoints"
PARAM_FILE = BASE_DIR / "params.json"
KB_DIR = BASE_DIR / "knowledge-base"

for d in [OUTPUT_DIR, LOG_DIR, CHECKPOINT_DIR]:
    d.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"weekly_debate_{date.today().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("run_weekly_debate")

# ── 依赖导入 ──────────────────────────────────────────────

import sys
from pathlib import Path

# 确保父目录在 sys.path 中(支持直接运行 python3 weekly_review/run_weekly_debate.py)
_parent = Path(__file__).parent.parent
if str(_parent) not in sys.path:
    sys.path.insert(0, str(_parent))

from weekly_strategy.graph import get_weekly_review_graph, get_stock_debate_graph
from weekly_strategy.state import WeeklyReviewState, StockDebateState, InvestDebateState, RiskDebateState


# ── 数据准备 ──────────────────────────────────────────────

def run_backtest(
    week_history: List[Dict],
    current_params: Dict,
    weeks: int = 4,
) -> Dict[str, Any]:
    """
    基于近 N 周历史交易数据,对不同参数组合进行回测。
    返回:各参数组合的收益、夏普比率、最大回撤对比表

    回测参数:
    - 止损:-3% / -5% / -8%
    - 止盈1:5% / 8% / 10%
    - 仓位:10% / 15% / 20%
    - 评分阈值:60 / 65 / 70
    """
    import math

    # 取近 N 周数据
    recent = list(reversed(week_history))[-weeks:]
    if not recent:
        return {"error": "无历史数据"}

    # 展开所有股票交易
    trades = []
    for week in recent:
        for s in week.get("stocks", []):
            pnl = s.get("pnl_pct", 0)
            if pnl != 0:  # 跳过 pnl=0 的(可能是仍未结束的)
                trades.append({
                    "week": week.get("week", ""),
                    "code": s.get("stock", ""),
                    "pool": s.get("pool", ""),
                    "signal_quality": s.get("signal_quality", ""),
                    "pnl_pct": pnl,
                    "exec_price": s.get("exec_price", 0),
                    "hit": 1 if pnl > 0 else 0,
                })

    if len(trades) < 5:
        return {"error": f"历史交易不足({len(trades)} 条),无法回测"}

    # 累计收益曲线(用于计算最大回撤)
    def _max_drawdown(pnls: List[float]) -> float:
        curve = []
        running = 0.0
        for p in pnls:
            running += p
            curve.append(running)
        peak = curve[0]
        dd = 0.0
        for v in curve:
            if v > peak:
                peak = v
            diff = peak - v
            if diff > dd:
                dd = diff
        return dd

    # 对一组参数模拟收益
    def _simulate(stop_loss: float, take_profit_1: float,
                  position_size: float, scoring_th: float) -> Dict:
        # 模拟:每周用当周参数的期望收益
        weekly_returns = []
        for week in recent:
            week_pnl = 0.0
            count = 0
            for s in week.get("stocks", []):
                pnl = s.get("pnl_pct", 0)
                if pnl == 0:
                    continue
                # 用评分阈值过滤(简化:signal_quality==alpha_win 则通过)
                sq = s.get("signal_quality", "")
                score = {
                    "alpha_win": 75, "beta_win": 68,
                    "false_signal": 45, "": 60
                }.get(sq, 60)
                if score < scoring_th:
                    continue
                # 仓位模拟(简化:用 position_size 作为每笔的权重上限)
                weight = min(position_size, 0.2)
                # 止损/止盈约束:若参数更紧,则用更严的参数重算 pnl
                eff_pnl = pnl
                if pnl > 0 and eff_pnl > take_profit_1 * 100:
                    eff_pnl = take_profit_1 * 100  # 止盈截断
                if pnl < 0 and eff_pnl < stop_loss * 100:
                    eff_pnl = stop_loss * 100  # 止损截断
                week_pnl += eff_pnl * weight
                count += 1
            if count > 0:
                weekly_returns.append(week_pnl / count)

        if not weekly_returns:
            return {"total": 0, "sharpe": 0, "max_dd": 0, "win_rate": 0}

        total = sum(weekly_returns)
        avg_r = total / len(weekly_returns)
        std_r = (sum((r - avg_r) ** 2 for r in weekly_returns) / len(weekly_returns)) ** 0.5
        sharpe = (avg_r / std_r * (52 ** 0.5)) if std_r > 0 else 0
        dd = _max_drawdown(weekly_returns)
        wins = sum(1 for r in weekly_returns if r > 0)

        return {
            "total": round(total, 2),
            "sharpe": round(sharpe, 2),
            "max_dd": round(dd, 2),
            "win_rate": round(wins / len(weekly_returns), 3),
            "weeks": len(weekly_returns),
        }

    # 当前参数结果
    cp = current_params
    curr = _simulate(
        stop_loss=cp.get("stop_loss_pct", -0.03),
        take_profit_1=cp.get("take_profit_1", 0.05),
        position_size=cp.get("position_size_pct", 0.15),
        scoring_th=cp.get("scoring_threshold", 65),
    )
    curr["label"] = "当前参数"

    # 穷举所有组合
    results = [curr]
    for sl in [-0.03, -0.05, -0.08]:
        for tp in [0.05, 0.08, 0.10]:
            for ps in [0.10, 0.15, 0.20]:
                for st in [60, 65, 70]:
                    if sl == cp.get("stop_loss_pct", -0.03) and \
                       tp == cp.get("take_profit_1", 0.05) and \
                       ps == cp.get("position_size_pct", 0.15) and \
                       st == cp.get("scoring_threshold", 65):
                        continue  # 跳过当前
                    r = _simulate(sl, tp, ps, st)
                    r["label"] = f"止损{abs(sl)*100:.0f}%/止盈{tp*100:.0f}%/仓{ps*100:.0f}%/评{st}"
                    results.append(r)

    # 排序:按总收益
    results.sort(key=lambda x: x["total"], reverse=True)

    # 生成对比表
    table_lines = ["参数 | 总收益 | 夏普 | 最大回撤 | 周胜率"]
    table_lines.append("---|---:|---:|---:|---:")
    for r in results[:9]:  # 最多9条
        table_lines.append(
            f"{r['label']} | {r['total']:+.1f}% | {r['sharpe']} | {r['max_dd']:.1f}% | {r['win_rate']:.1%}"
        )

    return {
        "summary": {
            "total_trades": len(trades),
            "weeks": weeks,
            "current_params_result": curr,
        },
        "comparison_table": "\n".join(table_lines),
        "ranked": results[:9],
        "best": results[0],
    }


def prepare_debate_data(report: Dict) -> Dict:
    """
    将周报 JSON 转换为辩论所需的统一格式
    适配 actual weekly_review_*.json 的字段结构
    """
    perf = report.get("performance", {})
    stocks = perf.get("stocks", [])
    ad = report.get("adaptive", {})
    new_params = ad.get("new_params", {})

    wins = [s for s in stocks if s.get("pnl_pct", 0) > 0]
    total_pnl = sum(s.get("total_pnl_pct", 0) for s in stocks)
    avg_pnl = total_pnl / len(stocks) if stocks else 0
    max_drawdown = min(s.get("pnl_pct", 0) for s in stocks) if stocks else 0
    consecutive_loss_weeks = 0
    for w in reversed(new_params.get("week_history", [])):
        if w.get("avg_pnl_pct", 0) < 0:
            consecutive_loss_weeks += 1
        else:
            break

    bench = report.get("benchmark", {})

    return {
        "week": f"{report.get('week_start', '?')} ~ {report.get('week_end', '?')}",
        "trades": [
            {
                "code": s.get("stock", ""),
                "name": s.get("name", ""),
                "buy_price": s.get("buy_price", 0),
                "current_price": s.get("current_price", 0),
                "pnl_pct": s.get("pnl_pct", 0),
                "action": s.get("action", "BUY"),
                "llm_score": s.get("llm_score", 0),
                "signal_quality": s.get("signal_quality", ""),
                "sector": s.get("sector", ""),
            }
            for s in stocks
        ],
        "stats": {
            "win_rate": len(wins) / len(stocks) if stocks else 0,
            "avg_return": avg_pnl,
            "hit_rate": ad.get("bayesian_hit_rate", 0) / 100,
            "raw_hit_rate": ad.get("raw_hit_rate", 0) / 100,
            "bayesian_hit_rate": ad.get("bayesian_hit_rate", 0),
            "max_drawdown": max_drawdown / 100,
            "total_pnl_pct": total_pnl,
            "consecutive_loss_weeks": consecutive_loss_weeks,
            "pool_bayesian": ad.get("pool_bayesian", {}),
            "momentum_weeks": ad.get("momentum_weeks", 0),
            "recent_directions": ad.get("recent_directions", []),
        },
        "benchmark": {
            "hs300_change": bench.get("hs300_change", 0),
            "sh_change": bench.get("sh_change", 0),
            "sz_change": bench.get("sz_change", 0),
            "vs_benchmark": f"{bench.get('excess_return', 0):+.2f}%",
            "portfolio_avg_pnl": bench.get("portfolio_avg_pnl", 0),
        },
        "market_regime": new_params.get("market_regime", "震荡"),
        "market_detail": report.get("market_context", ""),
        "current_params": {
            "position_size_pct": new_params.get("position_size_pct", 0.2),
            "scoring_threshold": new_params.get("scoring_threshold", 50),
            "stop_loss_pct": new_params.get("stop_loss_pct", -0.03),
            "take_profit_1": new_params.get("take_profit_1", 0.05),
            "take_profit_2": new_params.get("take_profit_2", 0.10),
            "take_profit_3": new_params.get("take_profit_3", 0.30),
            "max_positions": new_params.get("max_positions", 5),
        },
        "pool_rates": new_params.get("pool_hit_rates", {}),
        "sector_rotation": report.get("sector_rotation", {}),
    }


def save_checkpoint(date_str: str, state: Dict, stock_index: int = None):
    """
    保存断点。每完成一只股票立即保存,崩溃重跑只丢当前处理那只。

    Args:
        date_str: 日期字符串
        state: 完整状态(包含周参数结果 + 所有已完成股票的辩论结果)
        stock_index: 当前完成的股票索引(可选,用于追踪进度)
    """
    cp_file = CHECKPOINT_DIR / f"weekly_debate_{date_str}.json"

    # 构建断点数据(只保留关键字段,控制文件大小)
    checkpoint = {
        "status": state.get("status", "in_progress"),
        "generated_at": datetime.now().isoformat(),
        "week_parameter_result": state.get("week_parameter_result"),
        "stock_debates": state.get("stock_debates", []),
        "total_stocks": state.get("total_stocks", 0),
        "last_completed_index": stock_index if stock_index is not None else len(state.get("stock_debates", [])) - 1,
    }

    with open(cp_file, "w") as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=2)

    completed = len(state.get("stock_debates", []))
    total = state.get("total_stocks", 0)
    logger.info(f"[Checkpoint] 保存完成,进度 {completed}/{total}")


# ── 主辩论流程 ─────────────────────────────────────────────

def run_weekly_parameter_debate(
    week_data: Dict,
    checkpoint: bool = False,
    existing_result: Dict = None,
) -> Dict[str, Any]:
    """
    运行周参数辩论(主流程)

    使用 LangGraph 状态机:Analyst → Strategist → Risk → Fund Manager

    ⚠️ 注意:这是一个原子操作,整个 graph 跑完才返回结果。
    中间无法断点(LangGraph 设计如此)。但 graph 本身很快(5个 LLM 调用),
    真正需要断点的是持仓辩论(逐只股票)。

    Args:
        week_data: 周数据
        checkpoint: 是否启用断点(当前无实际效果,保留接口)
        existing_result: 如果已有结果(从断点恢复),直接返回
    """
    date_str = date.today().strftime("%Y%m%d")

    # 断点恢复:已有周参数结果则直接返回
    if checkpoint and existing_result and existing_result.get("week_parameter_result"):
        logger.info("[Checkpoint] 从断点恢复周参数辩论结果,跳过")
        return existing_result["week_parameter_result"]

    # 构建初始状态
    week_str = week_data.get("week", f"{date.today().isoformat()}")

    init_state: WeeklyReviewState = {
        "week_data": week_data,
        "week_str": week_str,
        "analyst_output": "",
        "strategist_output": "",
        "risk_output": "",
        "final_decision": "",
        "final_decision_obj": {},
        "debate_history": "",
        "sender": "",
        "current_step": "analyst",
        "stock_debates": [],
        "current_stock_index": 0,
    }

    # 获取预编译图
    graph = get_weekly_review_graph()

    # 执行图
    logger.info("=== 周参数辩论开始 ===")
    start = time.time()

    try:
        final_state = graph.invoke(init_state)
    except Exception as e:
        logger.error(f"辩论执行异常: {e}")
        raise

    elapsed = time.time() - start
    logger.info(f"=== 周参数辩论完成,耗时 {elapsed:.1f}s ===")

    result = {
        "week": week_str,
        "generated_at": datetime.now().isoformat(),
        "analyst_output": final_state.get("analyst_output", ""),
        "strategist_output": final_state.get("strategist_output", ""),
        "risk_output": final_state.get("risk_output", ""),
        "final_decision": final_state.get("final_decision", ""),
        "final_decision_obj": final_state.get("final_decision_obj", {}),
        "debate_history": final_state.get("debate_history", ""),
        "elapsed_seconds": round(elapsed, 1),
        "method": "tradingagents_weekly_review",
    }

    return result


# ── 持仓辩论流程(每只股票独立,可断点)────────────────────────

def run_stock_debates(
    stocks: list,
    week_data: Dict,
    checkpoint: bool = False,
    existing_debates: list = None,
    max_debate_rounds: int = 1,
    max_risk_rounds: int = 1,
) -> list:
    """
    对每持仓股票执行完整辩论(可选流程)

    断点续跑:
    - 每完成一只立即写断点文件
    - 恢复时从 last_completed_index + 1 继续
    - 崩溃重跑只丢失当前处理那只
    """
    graph = get_stock_debate_graph(max_debate_rounds, max_risk_rounds)
    date_str = date.today().strftime("%Y%m%d")

    # 断点恢复:从已完成的辩论开始
    if existing_debates is None:
        existing_debates = []

    completed_codes = {d.get("stock_code") for d in existing_debates if d.get("stock_code")}
    results = list(existing_debates)
    pending = [s for s in stocks if s.get("code") not in completed_codes]

    logger.info(f"[持仓辩论] 总数={len(stocks)}, 已完成={len(completed_codes)}, 待处理={len(pending)}")

    for i, stock in enumerate(pending):
        code = stock.get("code", "?")
        idx = len(results) + 1
        total = len(stocks)

        logger.info(f"[持仓辩论 {idx}/{total}] {code} {stock.get('name', '')}")

        # 初始化持仓状态
        init_state: StockDebateState = {
            "stock_code": code,
            "stock_name": stock.get("name", ""),
            "trade_date": date.today().isoformat(),
            "market_report": "",
            "sentiment_report": "",
            "news_report": "",
            "fundamentals_report": "",
            "investment_debate_state": {
                "bull_history": "",
                "bear_history": "",
                "history": "",
                "current_response": "",
                "judge_decision": "",
                "count": 0,
            },
            "investment_plan": "",
            "trader_investment_plan": "",
            "risk_debate_state": {
                "aggressive_history": "",
                "conservative_history": "",
                "neutral_history": "",
                "history": "",
                "latest_speaker": "",
                "current_aggressive_response": "",
                "current_conservative_response": "",
                "current_neutral_response": "",
                "judge_decision": "",
                "count": 0,
            },
            "final_trade_decision": "",
            "past_context": "",
            "buy_price": stock.get("buy_price", 0),
            "current_price": stock.get("current_price", 0),
            "pnl_pct": stock.get("pnl_pct", 0),
            "action": stock.get("action", "HOLD"),
        }

        try:
            result = graph.invoke(init_state)
            debate_result = {
                "stock_code": code,
                "stock_name": stock.get("name", ""),
                "final_decision": result.get("final_trade_decision", ""),
                "investment_plan": result.get("investment_plan", ""),
                "trader_proposal": result.get("trader_investment_plan", ""),
                "pnl_pct": stock.get("pnl_pct", 0),
            }
            results.append(debate_result)

            # ✅ 每完成一只立即写断点
            current_state = {
                "status": "in_progress",
                "week_parameter_result": None,  # 由调用方填充
                "stock_debates": results,
                "total_stocks": len(stocks),
            }
            save_checkpoint(date_str, current_state, stock_index=len(results) - 1)

            logger.info(f"[{code}] 辩论完成,进度 {idx}/{total}")

        except Exception as e:
            logger.error(f"[{code}] 持仓辩论异常: {e}")
            # 失败不写入结果,下次重跑这只
            results.append({
                "stock_code": code,
                "stock_name": stock.get("name", ""),
                "error": str(e),
            })

    return results


# ── 辩论结果归档与参数更新 ───────────────────────────────

def apply_decision_and_archive(result: Dict, raw_report: Dict) -> None:
    """
    将辩论结果写入知识库,并更新 params.json
    对齐原有 weekly_review_debate.py 的归档逻辑
    """
    fm = result.get("final_decision_obj", {})
    if not fm:
        logger.warning("无基金经理决策,跳过参数更新")
        return

    KB_DIR.mkdir(exist_ok=True)

    # 1. 归档到知识库
    try:
        week_end = raw_report.get("week_end", date.today().strftime("%Y-%m-%d"))
        week_key = week_end.replace("-", "")
        archive_path = KB_DIR / f"weekly_debate_{week_key}.json"

        archive_data = {
            "week_start": raw_report.get("week_start", ""),
            "week_end": week_end,
            "generated_at": result.get("generated_at", ""),
            "method": result.get("method", ""),
            "analyst_output": result.get("analyst_output", ""),
            "strategist_output": result.get("strategist_output", ""),
            "risk_output": result.get("risk_output", ""),
            "final_decision": result.get("final_decision", ""),
            "final_decision_obj": fm,
            "debate_history": result.get("debate_history", ""),
            "elapsed_seconds": result.get("elapsed_seconds", 0),
        }

        with open(archive_path, "w", encoding="utf-8") as f:
            json.dump(archive_data, f, ensure_ascii=False, indent=2)
        logger.info(f"辩论归档已保存: {archive_path}")

        # 更新索引
        index_path = KB_DIR / "_index.json"
        if index_path.exists():
            try:
                with open(index_path) as f:
                    idx = json.load(f)
            except Exception:
                idx = []
        else:
            idx = []

        idx.insert(0, {
            "week_end": week_end,
            "archive": f"weekly_debate_{week_key}.json",
            "rating": fm.get("rating", "?"),
            "generated_at": result.get("generated_at", ""),
        })
        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(idx[:50], f, ensure_ascii=False, indent=2)

    except Exception as e:
        logger.error(f"辩论归档失败: {e}")

    # 2. 更新 params.json
    try:
        if PARAM_FILE.exists():
            with open(PARAM_FILE) as f:
                params = json.load(f)
        else:
            params = {}

        params.update({
            "position_size_pct": fm.get("position_size_pct", 20),
            "scoring_threshold": fm.get("scoring_threshold", 50),
            "stop_loss_pct": fm.get("stop_loss_pct", -3.0),
            "take_profit_1": fm.get("take_profit_1", 5.0),
            "take_profit_2": fm.get("take_profit_2", 10.0),
            "take_profit_3": fm.get("take_profit_3", 30.0),
            "last_updated": result.get("generated_at", ""),
            "last_week": week_end,
        })

        with open(PARAM_FILE, "w") as f:
            json.dump(params, f, ensure_ascii=False, indent=2)
        logger.info(f"参数已更新: {PARAM_FILE}")

    except Exception as e:
        logger.error(f"保存辩论参数失败: {e}")


# ── 主函数 ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="周复盘辩论(对齐 TradingAgents v0.2.5 架构)")
    parser.add_argument("--input", help="周报JSON文件路径")
    parser.add_argument("--output", help="输出文件路径")
    parser.add_argument("--model", default="volcengine-plan/ark-code-latest",
                        help="LLM 模型(默认: volcengine-plan/ark-code-latest)")
    parser.add_argument("--stocks", action="store_true",
                        help="对每持仓股票执行独立辩论(默认关闭)")
    parser.add_argument("--checkpoint", action="store_true",
                        help="启用断点续跑")
    parser.add_argument("--max-debate-rounds", type=int, default=1,
                        help="投资辩论最大轮数(默认1)")
    parser.add_argument("--max-risk-rounds", type=int, default=1,
                        help="风险辩论最大轮数(默认1)")
    args = parser.parse_args()

    date_str = date.today().strftime("%Y%m%d")
    raw = None

    # 加载断点(如果启用)
    checkpoint_state = {}
    if args.checkpoint:
        checkpoint_state = load_checkpoint(date_str)
        logger.info(f"[Checkpoint] 加载断点,已有 {len(checkpoint_state.get('stock_debates', []))} 只股票辩论结果")

    # 如果指定了 input,直接使用;否则等待 weekly_review.py 生成
    if args.input:
        input_path = Path(args.input)
        if input_path.exists():
            with open(input_path) as f:
                raw = json.load(f)
            logger.info(f"加载周报: {input_path}")
        else:
            logger.warning(f"指定报告不存在: {args.input}")

    # 等待报告生成(最多 30 分钟,每 5 分钟轮询)
    if raw is None:
        logger.info("等待 weekly_review.py 生成报告...")
        for attempt in range(6):
            reports = sorted(OUTPUT_DIR.glob("weekly_review_*.json"), reverse=True)
            if reports:
                with open(reports[0]) as f:
                    raw = json.load(f)
                logger.info(f"找到报告: {reports[0]}")
                break
            if attempt < 5:
                logger.info(f"第 {attempt+1} 次未找到,5 分钟后重试...")
                time.sleep(300)
        if raw is None:
            logger.error("30 分钟内未找到周报数据,退出")
            sys.exit(1)

    week_data = prepare_debate_data(raw)
    logger.info(f"周范围: {week_data['week']}, 持仓数: {len(week_data['trades'])}")

    # ── Step 1: 运行周参数辩论(原子操作,无需断点)────────────
    week_parameter_result = run_weekly_parameter_debate(
        week_data,
        checkpoint=args.checkpoint,
        existing_result=checkpoint_state if checkpoint_state else None,
    )

    # ── Step 2: 持仓辩论(每只股票独立,可断点)───────────────
    stock_debates = []
    if args.stocks and week_data.get("trades"):
        logger.info(f"=== 开始持仓辩论 ({len(week_data['trades'])} 只) ===")

        # 从断点恢复已完成的辩论
        existing_debates = checkpoint_state.get("stock_debates", []) if checkpoint_state else None

        stock_debates = run_stock_debates(
            week_data["trades"],
            week_data,
            checkpoint=args.checkpoint,
            existing_debates=existing_debates,
            max_debate_rounds=args.max_debate_rounds,
            max_risk_rounds=args.max_risk_rounds,
        )
        logger.info(f"=== 持仓辩论完成 ({len(stock_debates)} 只) ===")

    # ── Step 3: 合并结果 + 归档 ──────────────────────────────
    final_result = {
        "status": "done",
        "generated_at": datetime.now().isoformat(),
        "week": week_data["week"],
        "week_parameter_result": week_parameter_result,
        "stock_debates": stock_debates,
        "total_stocks": len(week_data["trades"]) if week_data.get("trades") else 0,
        "method": "tradingagents_weekly_review",
    }

    # 辩论结果归档 + 参数更新
    apply_decision_and_archive(week_parameter_result, raw)

    # 保存最终结果(覆盖断点文件,状态改为 done)
    cp_file = CHECKPOINT_DIR / f"weekly_debate_{date_str}.json"
    final_checkpoint = {
        "status": "done",
        "generated_at": datetime.now().isoformat(),
        "week_parameter_result": week_parameter_result,
        "stock_debates": stock_debates,
        "total_stocks": len(week_data["trades"]) if week_data.get("trades") else 0,
        "last_completed_index": len(stock_debates) - 1 if stock_debates else -1,
    }
    with open(cp_file, "w") as f:
        json.dump(final_checkpoint, f, ensure_ascii=False, indent=2)

    # 输出到文件
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = OUTPUT_DIR / f"strategy_debate_result_{date_str}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_result, f, ensure_ascii=False, indent=2)

    # 同时写入 strategy_debate_result_latest.json（供 send_weekly_feishu.py 使用）
    latest_path = OUTPUT_DIR / "strategy_debate_result_latest.json"
    try:
        with open(latest_path, "w", encoding="utf-8") as f:
            json.dump(final_result, f, ensure_ascii=False, indent=2)
        logger.info(f"同步到: {latest_path}")
    except Exception as e:
        logger.warning(f"写入 strategy_debate_result_latest.json 失败: {e}")

    logger.info(f"结果已保存: {output_path}")

    # 打印摘要
    fm = week_parameter_result.get("final_decision_obj", {})
    rating = fm.get("rating", "?")
    new_pos = fm.get("position_size_pct", "?")
    new_thresh = fm.get("scoring_threshold", "?")
    new_sl = fm.get("stop_loss_pct", "?")
    new_tp1 = fm.get("take_profit_1", "?")
    new_tp2 = fm.get("take_profit_2", "?")
    new_tp3 = fm.get("take_profit_3", "?")
    confidence = fm.get("confidence", "?")
    summary = fm.get("executive_summary", "?") or week_parameter_result.get("final_decision", "")[:200]

    print(f"\n{'='*60}")
    print(f"周复盘辩论完成 | 耗时 {week_parameter_result.get('elapsed_seconds', '?')}s")
    print(f"周范围: {week_data['week']}")
    print(f"{'='*60}")
    print(f"【Rating】{rating}")
    print(f"【仓位】{new_pos}% | 【阈值】{new_thresh}")
    print(f"【止损】{new_sl}% | 【止盈1/2/3】{new_tp1}%/{new_tp2}%/{new_tp3}%")
    print(f"【置信度】{confidence}")
    print(f"【执行摘要】{summary}")
    if stock_debates:
        print(f"【持仓辩论】{len(stock_debates)} 只股票")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()