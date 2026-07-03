"""
飞书卡片生成
"""

import os
import requests
from datetime import date
from typing import Dict, List, Any


def build_debate_card(debate_result: Dict, market_context: str = "") -> dict:
    """
    生成选股辩论报告飞书卡片

    Args:
        debate_result: StockSelectionDebate.run() 的返回值
        market_context: 市场背景（可选）

    Returns:
        飞书 interactive card payload
    """
    ranked = debate_result.get("ranked_candidates", [])
    buy_list = debate_result.get("buy_list", [])
    watch_list = debate_result.get("watch_list", [])
    avoid_list = debate_result.get("avoid_list", [])
    portfolio = debate_result.get("portfolio_suggestion", "")
    summary = debate_result.get("summary", "")

    today = date.today().strftime("%Y-%m-%d")

    # BUY 列表（最多5只）
    buy_elements = []
    for i, c in enumerate(buy_list[:5], 1):
        conviction_emoji = {"高": "🟢", "中": "🟡", "低": "🟠"}.get(c.get("conviction", "中"), "⚪")
        signal_emoji = "🟢"
        rank_emoji = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"][i-1] if i <= 5 else f"{i}️⃣"
        scores = c.get("scores", {})
        bull = c.get("bull_argument", "—")
        bear = c.get("bear_argument", "—")
        verdict = c.get("verdict", "")

        buy_elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"{rank_emoji} **{c.get('name', '?')} {c.get('stock', '?')}** "
                    f"| 综合分: **{c.get('final_score', 0)}** "
                    f"| 置信度: {conviction_emoji}{c.get('conviction', '?')} "
                    f"| 仓位: **{c.get('position_ratio', '?')}**\n"
                    f"　　信号: {signal_emoji}BUY\n"
                    f"　　Bull: {bull}\n"
                    f"　　Bear: {bear}\n"
                    f"　　⚖️ {verdict}"
                ),
            }
        })

    if not buy_elements:
        buy_elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "今日无 BUY 信号"},
        })

    # WATCH 列表
    watch_elements = []
    for i, c in enumerate(watch_list[:3], 1):
        conviction_emoji = {"高": "🟢", "中": "🟡", "低": "🟠"}.get(c.get("conviction", "中"), "⚪")
        watch_elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"{i}. **{c.get('name', '?')} {c.get('stock', '?')}** "
                    f"| 分:{c.get('final_score', 0)} "
                    f"| {conviction_emoji}{c.get('conviction', '?')} "
                    f"| 仓位:{c.get('position_ratio', '?')}\n"
                    f"　　{c.get('verdict', '')}"
                ),
            }
        })

    # AVOID 列表
    avoid_elements = []
    for c in avoid_list[:3]:
        avoid_elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"🔴 **{c.get('name', '?')} {c.get('stock', '?')}** "
                    f"| 分:{c.get('final_score', 0)} "
                    f"| {c.get('bear_argument', '')}"
                ),
            }
        })

    card = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": f"📊 选股辩论报告 {today}"},
                "template": "purple",
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            "━━━━━━━━ 辩论阵容 ━━━━━━━━\n"
                            "🔬 行业研究员：成长性/催化剂\n"
                            "📈 技术分析师：趋势/形态/指标\n"
                            "🛡️ 量化风控师：财务/估值风险\n"
                            "💹 市场情绪官：资金/筹码/情绪\n"
                            "⚖️ 基金经理：综合裁判"
                        ),
                    }
                },
                {"tag": "hr"},
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": "**━━━━━━━━ BUY 信号（Top 5）━━━━━━━━**"},
                },
                *buy_elements,
                {"tag": "hr"},
            ],
        },
    }

    if watch_elements:
        card["card"]["elements"].extend([
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": "**━━━━━━━━ WATCH 观察 ━━━━━━━━**"},
            },
            *watch_elements,
            {"tag": "hr"},
        ])

    if avoid_elements:
        card["card"]["elements"].extend([
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": "**━━━━━━━━ AVOID 回避 ━━━━━━━━**"},
            },
            *avoid_elements,
            {"tag": "hr"},
        ])

    if portfolio:
        card["card"]["elements"].append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"**━━━━━━━━ 组合建议 ━━━━━━━━**\n{portfolio}"},
        })

    if summary:
        card["card"]["elements"].append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"**裁判总结：** {summary}"},
        })

    return card


def send_feishu(card: dict, webhook_url: str = None) -> bool:
    """发送飞书卡片"""
    if not webhook_url:
        webhook_url = os.getenv("FEISHU_WEBHOOK_URL")
    if not webhook_url:
        return False

    try:
        resp = requests.post(webhook_url, json=card, timeout=15)
        return resp.status_code == 200
    except Exception:
        return False
