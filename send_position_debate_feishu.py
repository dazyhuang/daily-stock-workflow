#!/usr/bin/env python3
"""
周持仓辩论结果飞书推送
=====================
读取 position_debate_result.json，生成格式化卡片并通过 webhook 发送。
"""

import os, json, requests, sys, re, subprocess
from pathlib import Path

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
WEBHOOK_URL = os.getenv("FEISHU_WEBHOOK_URL", "")

RATING_MAP = {
    "Buy": "加仓", "Add": "加仓",
    "Hold": "持有", "Neutral": "持有",
    "Sell": "减仓", "Reduce": "减仓", "Clear": "清仓",
    "Overweight": "加仓", "Underweight": "减仓",
}

def rating_cn(rating):
    return RATING_MAP.get(rating, rating)


def load_debate_result():
    path = OUTPUT_DIR / "position_debate_result.json"
    if not path.exists():
        print(f"文件不存在: {path}")
        return None
    with open(path) as f:
        return json.load(f)


def require_webhook_url():
    if not WEBHOOK_URL:
        print("FEISHU_WEBHOOK_URL is not configured")
        return False
    return True


def get_market_data():
    try:
        mx_script = BASE_DIR.parent / "skills/mx-data/mx_data.py"
        result = subprocess.run(
            [sys.executable, str(mx_script), "000001.SH 399001.SZ 399006.SZ 000300.SH 今日行情"],
            capture_output=True, text=True, timeout=30
        )
        changes = {}
        for code in ["000001", "399001", "399006", "000300"]:
            m = re.search(rf'{code}[^.]*[-+]([\d.]+)%', result.stdout)
            if m:
                changes[code] = float(m.group(1))
        return changes
    except Exception as e:
        print(f"获取大盘数据失败: {e}")
        return {}


def stock_lines(items):
    """生成每只股票的完整段落"""
    if not items:
        return "无"
    lines = []
    for r in items:
        # 字段名修正：run_position_debate.py 输出的是 stock_code / stock_name
        stock = r.get("stock_code", "")
        name = r.get("stock_name", stock)
        pnl = r.get("pnl_pct", 0)
        price = r.get("current_price") or r.get("buy_price", 0) or 0
        reason = str(r.get("final_decision", "") or r.get("investment_plan", "") or "")[:200]
        # 替换英文评级为中文
        reason = re.sub(r'\*\*Rating\*\*:\s*Underweight', '评级: 减仓', reason)
        reason = re.sub(r'\*\*Rating\*\*:\s*Overweight', '评级: 加仓', reason)
        reason = re.sub(r'\*\*Rating\*\*:\s*Hold', '评级: 持有', reason)
        reason = re.sub(r'\*\*Rating\*\*:\s*Neutral', '评级: 持有', reason)
        reason = re.sub(r'\*\*Rating\*\*:\s*Sell', '评级: 减仓', reason)
        # 去掉 markdown 标题标记
        reason = re.sub(r'\*\*Rationale\*\*:\s*', '', reason)
        reason = re.sub(r'\*\*Strategic Actions\*\*:\s*', '操作: ', reason)
        reason = re.sub(r'\*\*', '', reason)
        reason = reason[:150]
        tech = r.get("technicals") or {}
        pe_val = r.get("pe")
        
        # RSI 处理
        rsi_val = tech.get("rsi")
        if rsi_val is not None:
            rsi_str = f"{rsi_val:.0f}"
            rsi_em = "📈" if rsi_val > 60 else ("📉" if rsi_val < 40 else "➡️")
        else:
            rsi_str = "-"
            rsi_em = ""
        
        # PE 处理
        pe_str = f"{pe_val:.1f}" if pe_val is not None else "-"
        
        # 涨跌颜色
        pnl_em = "🟢" if pnl >= 0 else "🔴"
        
        # 均线状态
        ma_order = tech.get("ma_order", "")
        trend = tech.get("trend", "")
        ma_info = f" MA({ma_order}/{trend})" if ma_order else ""
        
        lines.append(
            f"**{stock}** {name} {pnl_em}{pnl:+.2f}%\n"
            f"   现价:{price:.2f} RSI:{rsi_str}{rsi_em} PE:{pe_str}{ma_info}\n"
            f"   {reason}"
        )
    return "\n\n".join(lines)


def build_card(data):
    results = data.get("results", [])
    if not results:
        return None

    market = get_market_data()
    
    # 按 rating 分类（清仓单独列出）
    add    = [r for r in results if r.get("rating") in ("Buy", "Add")]
    hold   = [r for r in results if r.get("rating") == "Hold"]
    reduce = [r for r in results if r.get("rating") in ("Sell", "Reduce")]
    clear  = [r for r in results if r.get("rating") == "Clear"]

    # 大盘
    sh = market.get("000001", 0)
    sz = market.get("399001", 0)
    cy = market.get("399006", 0)
    hs300 = market.get("000300", 0)
    market_str = f"📊 上证 {sh:+.2f}%  深证 {sz:+.2f}%  创业板 {cy:+.2f}%  沪深300 {hs300:+.2f}%"

    summary = f"🟢 加仓 {len(add)}  |  🟡 持有 {len(hold)}  |  🟠 减仓 {len(reduce)}  |  🔴 清仓 {len(clear)}"

    card = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": "📊 周持仓辩论结果"},
                "template": "blue",
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": market_str}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**━━━━━━━━ 汇总 ━━━━━━━━**\n{summary}"}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**━━━━━━━━ 🟢 加仓（{len(add)}只）━━━━━━━━**\n{stock_lines(add)}"}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**━━━━━━━━ 🟡 持有（{len(hold)}只）━━━━━━━━**\n{stock_lines(hold)}"}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**━━━━━━━━ 🟠 减仓（{len(reduce)}只）━━━━━━━━**\n{stock_lines(reduce)}"}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**━━━━━━━━ 🔴 清仓（{len(clear)}只）━━━━━━━━**\n{stock_lines(clear)}"}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": "**━━━━━━━━ 操作提示 ━━━━━━━━**\n" + _op_tips(add, sell, hold)}},
            ],
        },
    }
    return card


def _op_tips(add, sell, hold):
    parts = []
    if add:
        names = ", ".join(f"{r.get('stock_code','')}" for r in add[:3])
        parts.append(f"✅ 加仓: {names}")
    if reduce:
        names = ", ".join(f"{r.get('stock_code','')}" for r in reduce[:3])
        parts.append(f"🟠 减仓: {names}")
    if clear:
        names = ", ".join(f"{r.get('stock_code','')}" for r in clear[:3])
        parts.append(f"🔴 清仓: {names}")
    if hold:
        names = ", ".join(f"{r.get('stock_code','')}" for r in hold[:3])
        parts.append(f"🔄 观望: {names}")
    return "\n".join(parts)


def send_feishu(card):
    if not require_webhook_url():
        return False
    resp = requests.post(WEBHOOK_URL, json=card, timeout=15)
    result = json.loads(resp.text)
    if result.get("code") == 0:
        print("✅ 飞书消息发送成功")
        return True
    else:
        print(f"❌ 飞书消息发送失败: {result}")
        return False


def archive():
    src = OUTPUT_DIR / "position_debate_result.json"
    dst = BASE_DIR / "knowledge-base" / "_position_debate_prev.json"
    try:
        with open(src) as f:
            data = json.load(f)
        with open(dst, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"✅ 已备份为上周辩论结果")
    except Exception as e:
        print(f"备份失败: {e}")


def main():
    print("读取周持仓辩论结果...")
    data = load_debate_result()
    if not data:
        sys.exit(1)
    results = data.get("results", [])
    print(f"共 {len(results)} 只股票")
    for r in results:
        print(f"  {r.get('stock_code')} {r.get('stock_name')} → {rating_cn(r.get('rating',''))}  PE:{r.get('pe')}  RSI:{r.get('technicals',{}).get('rsi')}")
    card = build_card(data)
    if card:
        if send_feishu(card):
            archive()
    else:
        print("无结果可推送")
        sys.exit(1)


if __name__ == "__main__":
    main()
