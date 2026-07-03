#!/usr/bin/env python3
"""
周复盘 + 辩论结果 飞书推送
================================
支持新旧两种辩论结果格式：
- 新格式：week_parameter_result.final_decision_obj（TradingAgents架构）
- 旧格式：fund_manager（原有格式）

6个优化方向：
① Schema兼容修复（读新格式字段）
② 板块强弱注入（hot/cold sectors）
③ 信号质量+各池命中率汇总
④ 参数前后对比展示
⑤ 三方分歧展示
⑥ 原始vs贝叶斯命中率对比
"""

import os
import json
import requests
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"


# ── 数据读取工具 ──────────────────────────────────────────

def load_debate_result(path: Path) -> Dict[str, Any]:
    """读取辩论结果，兼容新旧格式"""
    with open(path, "r", encoding="utf-8") as f:
        d = json.load(f)

    # 新格式：run_weekly_debate.py（TradingAgents架构）
    # 结构：{week_parameter_result: {final_decision_obj: {...}}}
    if "week_parameter_result" in d:
        wpr = d["week_parameter_result"]
        fm = wpr.get("final_decision_obj", {})
        return {
            "rating": fm.get("rating", "Hold"),
            "executive_summary": fm.get("executive_summary", ""),
            "investment_thesis": fm.get("investment_thesis", ""),
            "analyst_view": fm.get("analyst_view", ""),
            "strategist_view": fm.get("strategist_view", ""),
            "risk_view": fm.get("risk_view", ""),
            "disagreements": fm.get("disagreements", ""),
            "confidence": fm.get("confidence", "中"),
            "position_size_pct": fm.get("position_size_pct"),
            "scoring_threshold": fm.get("scoring_threshold"),
            "stop_loss_pct": fm.get("stop_loss_pct"),
            "take_profit_1": fm.get("take_profit_1"),
            "take_profit_2": fm.get("take_profit_2"),
            "take_profit_3": fm.get("take_profit_3"),
            "week": wpr.get("week", ""),
            "analyst_output": wpr.get("analyst_output", ""),
            "strategist_output": wpr.get("strategist_output", ""),
            "risk_output": wpr.get("risk_output", ""),
            "method": wpr.get("method", ""),
            "elapsed_seconds": wpr.get("elapsed_seconds", 0),
        }

    # 旧格式：原 send_weekly_feishu.py 格式
    # 结构：{fund_manager: {decision, final_position_pct, ...}}
    fm = d.get("fund_manager", {})
    return {
        "rating": None,
        "executive_summary": fm.get("summary", ""),
        "investment_thesis": "",
        "analyst_view": fm.get("analyst_view", ""),
        "strategist_view": fm.get("strategist_view", ""),
        "risk_view": fm.get("risk_view", ""),
        "disagreements": fm.get("disagreements", ""),
        "confidence": fm.get("confidence", "中"),
        "position_size_pct": fm.get("final_position_pct"),
        "scoring_threshold": fm.get("final_threshold"),
        "stop_loss_pct": fm.get("final_stop_loss"),
        "take_profit_1": fm.get("final_take_profit_1"),
        "take_profit_2": fm.get("final_take_profit_2"),
        "take_profit_3": None,
        "week": d.get("week", ""),
        "analyst_output": d.get("analyst", ""),
        "strategist_output": d.get("strategist", ""),
        "risk_output": d.get("risk_officer", ""),
        "method": "",
        "elapsed_seconds": 0,
    }


def load_review_data(path: Path) -> Dict[str, Any]:
    """读取周报数据"""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ── 字段提取工具 ─────────────────────────────────────────

def _safe(v, suffix="?"):
    """安全获取值，避免 None"""
    return v if v is not None else suffix


def _pct(v, default="?"):
    """格式化百分比"""
    if v is None:
        return default
    try:
        return f"{float(v):+.2f}%"
    except (ValueError, TypeError):
        return default


# ── 卡片构建 ─────────────────────────────────────────────

def build_weekly_report_card(
    review_data: Dict,
    debate_data: Dict,
) -> Dict:
    """
    生成优化后的周复盘 + 辩论结果飞书卡片

    包含6个优化方向：
    ① Schema兼容（读新格式）
    ② 板块强弱
    ③ 信号质量+各池命中率
    ④ 参数前后对比
    ⑤ 三方分歧展示
    ⑥ 原始vs贝叶斯命中率
    """
    # ── 提取关键数据 ─────────────────────────────────────
    week_period = debate_data.get("week", "未知周期")
    analyst_view = debate_data.get("analyst_view", "")
    strategist_view = debate_data.get("strategist_view", "")
    risk_view = debate_data.get("risk_view", "")
    disagreements = debate_data.get("disagreements", "")

    # 决策
    rating = debate_data.get("rating") or debate_data.get("decision", "维持")
    rating_display = rating.value if hasattr(rating, 'value') else str(rating)
    confidence = debate_data.get("confidence", "中")

    # 参数（新格式）
    # 注意：辩论结果里的值可能是旧 bug（100倍），需要修正
    # position: 1500→15, stop_loss: -300→-3, take_profit: 1000→10/2000→20/5000→50
    pos_new = debate_data.get("position_size_pct")
    if pos_new is not None and pos_new > 100:
        pos_new = pos_new / 100  # 1500 → 15
    thresh_new = debate_data.get("scoring_threshold")
    sl_new = debate_data.get("stop_loss_pct")
    if sl_new is not None and abs(sl_new) > 50:  # -300 → -3
        sl_new = sl_new / 100
    tp1_new = debate_data.get("take_profit_1")
    if tp1_new is not None and tp1_new > 100:  # 1000 → 10
        tp1_new = tp1_new / 100
    tp2_new = debate_data.get("take_profit_2")
    if tp2_new is not None and tp2_new > 100:  # 2000 → 20
        tp2_new = tp2_new / 100
    tp3_new = debate_data.get("take_profit_3")
    if tp3_new is not None and tp3_new > 100:  # 5000 → 50
        tp3_new = tp3_new / 100

    # 当前参数（从周报读取，用于前后对比）
    # 注意：new_params 里存的是绝对值（如 15 表示 15%，-3 表示 -3%），不是小数
    current_params = review_data.get("adaptive", {}).get("new_params", {})
    pos_old = current_params.get("position_size_pct", 15)  # 已是百分比值（如 15）
    thresh_old = current_params.get("scoring_threshold", 65)
    sl_old = current_params.get("stop_loss_pct", -3)  # 已是百分比值（如 -3）
    tp1_old = current_params.get("take_profit_1", 10)
    tp2_old = current_params.get("take_profit_2", 20)
    tp3_old = current_params.get("take_profit_3", 50)

    # ── 大盘环境 ─────────────────────────────────────────
    mc = review_data.get("market_context", {})
    sh_change = mc.get("000001", {}).get("change_pct", 0)
    sz_change = mc.get("399001", {}).get("change_pct", 0)
    cyb_change = mc.get("399006", {}).get("change_pct", 0)
    hs300_change = mc.get("000300", {}).get("change_pct", 0)

    # ── 板块强弱（方向②）────────────────────────────────
    sr = review_data.get("sector_rotation", {})

    def _extract_sectors(sector_list, max_count=4):
        """从 sector list 提取板块名称列表，支持 dict 或 str"""
        if not sector_list:
            return []
        result = []
        for item in sector_list:
            if isinstance(item, dict):
                result.append(item.get("sector", "未知"))
            elif isinstance(item, str):
                result.append(item)
        return result[:max_count]

    # 优先用 strongest_sectors / weakest_sectors（all_sectors 格式）
    strongest = sr.get("strongest_sectors", [])
    weakest = sr.get("weakest_sectors", [])
    hot_list = _extract_sectors(strongest, 4)
    cold_list = _extract_sectors(weakest, 4)

    # 兼容 hot_sectors / cold_sectors（字符串列表格式）
    if not hot_list:
        hot_list = _extract_sectors(sr.get("hot_sectors", []) or [], 4)
    if not cold_list:
        cold_list = _extract_sectors(sr.get("cold_sectors", []) or [], 4)

    hot_str = "、".join(hot_list) if hot_list else "暂无"
    cold_str = "、".join(cold_list) if cold_list else "暂无"

    # ── 统计摘要（方向③+⑥）──────────────────────────────
    perf = review_data.get("performance", {})
    stocks = perf.get("stocks", [])
    ad = review_data.get("adaptive", {})
    bench = review_data.get("benchmark", {})

    # 原始 vs 贝叶斯命中率
    raw_hit = ad.get("raw_hit_rate", 0)
    bayes_hit = ad.get("bayesian_hit_rate", 0)
    pool_bay = ad.get("pool_bayesian", {})
    momentum = ad.get("momentum_weeks", 0)
    recent_dirs = ad.get("recent_directions", [])
    dirs_str = "→".join(recent_dirs[-6:]) if recent_dirs else "无"

    # 信号质量统计
    alpha_cnt = sum(1 for s in stocks if s.get("signal_quality") == "alpha_win")
    false_cnt = sum(1 for s in stocks if s.get("signal_quality") == "false_signal")
    beta_cnt = sum(1 for s in stocks if s.get("signal_quality") == "beta_win")
    total_signals = len(stocks)

    # 各池子命中率
    pool_lines = []
    for pool_name in ["选股池", "A", "趋势型", "成长型", "逆向型", "强势型"]:
        v = pool_bay.get(pool_name)
        if v is not None:
            try:
                v_f = float(v)
                pool_lines.append(f"{pool_name} {v_f:.1f}%")
            except (ValueError, TypeError):
                pool_lines.append(f"{pool_name} {v}")
    pool_summary = "、".join(pool_lines) if pool_lines else "暂无数据"

    # 胜率
    wins = [s for s in stocks if s.get("pnl_pct", 0) > 0]
    win_rate_pct = len(wins) / len(stocks) if stocks else 0
    avg_pnl = bench.get("portfolio_avg_pnl", 0)
    excess = bench.get("excess_return", 0)

    # ── 决策emoji ────────────────────────────────────────
    decision_emoji = {"Buy": "🟡", "Overweight": "🟢", "Hold": "🟢",
                      "Underweight": "🔴", "Sell": "🔴", "维持": "🟢",
                      "上调": "🟡", "下调": "🔴"}.get(rating_display, "⚪")
    confidence_emoji = {"高": "🔵", "中": "🟡", "低": "🟠"}.get(str(confidence), "⚪")

    # ── 参数变化「前后对比」（方向④）────────────────────
    def _fmt_pct(x):
        return f"{float(x):.0f}%"

    def _diff(old, new, fmt=None):
        """生成变化对比字符串"""
        fmt = fmt or _fmt_pct
        if new is None:
            return f"{fmt(old)} → ?"
        old_v, new_v = float(old), float(new)
        if abs(old_v - new_v) < 0.001:
            return f"**{fmt(old_v)}** → {fmt(new_v)}（维持）"
        arrow = "↑" if new_v > old_v else "↓"
        return f"**{fmt(old_v)}** → {fmt(new_v)}（{arrow}）"

    pos_diff = _diff(pos_old, pos_new)
    thresh_diff = _diff(thresh_old, thresh_new, fmt=lambda x: f"{x:.0f}")
    sl_diff = _diff(sl_old, sl_new)
    tp1_diff = _diff(tp1_old, tp1_new)
    tp2_diff = _diff(tp2_old, tp2_new)
    tp3_diff = _diff(tp3_old, tp3_new)

    # ── 三方分歧（方向⑤）────────────────────────────────
    disagree_text = "无分歧，三方一致"
    if disagreements and str(disagreements).strip() not in ["", "无", "none", "[]"]:
        disagree_text = str(disagreements)[:300]

    # ── 拼装卡片 ─────────────────────────────────────────
    executive_summary = debate_data.get("executive_summary", "") or ""

    card = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": f"📊 周复盘决策报告 {week_period}"},
                "template": "blue",
            },
            "elements": [
                # ━━━━ 大盘环境 ━━━━
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "**━━━━━━━━ 大盘环境 ━━━━━━━━**\n"
                            f"🟢 上证: {sh_change:+.2f}%  "
                            f"深证: {sz_change:+.2f}%  "
                            f"创业板: {cyb_change:+.2f}%\n"
                            f"📊 沪深300: {hs300_change:+.2f}%\n"
                            f"📈 市场判断: **{review_data.get('market_regime', '未知')}**\n"
                            f"📉 动量方向: {dirs_str}（{momentum}周动量）"
                        ),
                    }
                },
                {"tag": "hr"},

                # ━━━━ 信号质量统计（方向③） ━━━━
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "**━━━━━━━━ 信号质量统计 ━━━━━━━━**\n"
                            f"✅ Alpha信号: {alpha_cnt}条  "
                            f"❌ 假信号: {false_cnt}条  "
                            f"🔄 Beta信号: {beta_cnt}条\n"
                            f"📊 本周胜率: {win_rate_pct:.0%}（{len(wins)}/{total_signals}）\n"
                            f"📈 组合收益: {avg_pnl:+.2f}%  "
                            f"🏆 超额收益: {excess:+.2f}%"
                        ),
                    }
                },
                {"tag": "hr"},

                # ━━━━ 命中率分析（方向⑥） ━━━━
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "**━━━━━━━━ 命中率分析 ━━━━━━━━**\n"
                            f"🎯 原始命中率: {raw_hit:.1f}%\n"
                            f"🧠 贝叶斯校正: {bayes_hit:.1f}%\n"
                            f"📦 各池子命中率: {pool_summary}"
                        ),
                    }
                },
                {"tag": "hr"},

                # ━━━━ 板块强弱（方向②） ━━━━
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "**━━━━━━━━ 板块强弱 ━━━━━━━━**\n"
                            f"🔥 强势板块: {hot_str}\n"
                            f"❄️ 弱势板块: {cold_str}"
                        ),
                    }
                },
                {"tag": "hr"},

                # ━━━━ 参数调整（方向④） ━━━━
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "**━━━━━━━━ 参数调整 ━━━━━━━━**\n"
                            f"📦 仓位: {pos_diff}\n"
                            f"🎯 选股阈值: {thresh_diff}\n"
                            f"🛡️ 止损: {sl_diff}\n"
                            f"📊 止盈1/2/3: {tp1_diff} / {tp2_diff} / {tp3_diff}"
                        ),
                    }
                },
                {"tag": "hr"},

                # ━━━━ 基金经理决策 ━━━━
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "**━━━━━━━━ 基金经理决策 ━━━━━━━━**\n"
                            f"{decision_emoji} 评级: **{rating_display}**\n"
                            f"{confidence_emoji} 置信度: **{confidence}**\n\n"
                            f"💡 {executive_summary}"
                        ),
                    }
                },
                {"tag": "hr"},

                # ━━━━ 三方观点（方向⑤） ━━━━
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "**━━━━━━━━ 三方观点 ━━━━━━━━**\n\n"
                            f"🔬 **分析师**: {analyst_view[:200] if analyst_view else '暂无'}\n\n"
                            f"📈 **策略师**: {strategist_view[:200] if strategist_view else '暂无'}\n\n"
                            f"🛡️ **风控官**: {risk_view[:200] if risk_view else '暂无'}"
                        ),
                    }
                },

                # ━━━━ 分歧点（方向⑤） ━━━━
                *([{"tag": "hr"}, {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "**━━━━━━━━ 三方分歧 ━━━━━━━━**\n"
                            f"{disagree_text}"
                        ),
                    }
                }] if disagree_text != "无分歧，三方一致" else []),

                # ━━━━ 操作提示 ━━━━
                {"tag": "hr"},
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "**━━━━━━━━ 下周操作提示 ━━━━━━━━**\n"
                            f"✅ 参数已自动同步，系统按节奏运行\n"
                            f"📌 重点关注板块: {hot_str}\n"
                            f"⚠️ 规避板块: {cold_str}"
                        ),
                    }
                },
            ],
        }
    }

    return card


def send_feishu(card: dict, webhook_url: str = None) -> bool:
    """发送飞书卡片"""
    if not webhook_url:
        webhook_url = os.getenv("FEISHU_WEBHOOK_URL")
    if not webhook_url:
        print("❌ 未设置 FEISHU_WEBHOOK_URL 环境变量")
        return False

    try:
        resp = requests.post(webhook_url, json=card, timeout=15)
        if resp.status_code == 200:
            print("✅ 飞书消息发送成功")
            return True
        else:
            print(f"❌ 飞书消息发送失败: {resp.status_code} {resp.text}")
            return False
    except Exception as e:
        print(f"❌ 发送异常: {e}")
        return False


def main():
    review_path = OUTPUT_DIR / "weekly_review_latest.json"
    debate_path = OUTPUT_DIR / "strategy_debate_result_latest.json"

    # 尝试找最新的辩论结果（可能是新格式日期文件）
    import glob
    debate_candidates = [debate_path]
    debate_candidates += sorted(
        glob.glob(str(OUTPUT_DIR / "strategy_debate_result_2026*.json")),
        reverse=True
    )

    # 选择存在的最新文件
    debate_path_selected = None
    for p in debate_candidates:
        p = Path(p)
        if p.exists():
            debate_path_selected = p
            break

    if not review_path.exists():
        print(f"❌ 周报文件不存在: {review_path}")
        return 1
    if not debate_path_selected:
        print(f"❌ 辩论结果文件不存在")
        return 1

    print(f"读取周报: {review_path}")
    print(f"读取辩论: {debate_path_selected}")

    review_data = load_review_data(review_path)
    debate_data = load_debate_result(debate_path_selected)

    card = build_weekly_report_card(review_data, debate_data)
    send_feishu(card)

    return 0


if __name__ == "__main__":
    exit(main())