#!/usr/bin/env python3
"""
检查 mx-moni 持仓与 trades.json 是否一致。
模拟账户持仓是权威源；可通过 --fix 自动校准 trades.json。
"""

import os
import sys
import json
import logging
from datetime import date
from pathlib import Path

# Setup path
BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
TRADES_FILE = OUTPUT_DIR / "trades.json"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
sys.path.insert(0, str(BASE_DIR))

from trade_position_sync import (
    aggregate_mock_positions,
    aggregate_trades_positions,
    fetch_mock_positions,
    load_local_env,
    load_trades,
    reconcile_trades_file_with_account,
)

load_local_env()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"check_sync_{date.today().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("check_sync")

# ── Feishu Webhook ────────────────────────────────────────
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK_URL")

def push_feishu(msg: str):
    if not FEISHU_WEBHOOK:
        logger.warning("未配置 FEISHU_WEBHOOK，跳过推送")
        return
    import requests
    payload = {"msg_type": "text", "content": {"text": msg}}
    try:
        r = requests.post(FEISHU_WEBHOOK, json=payload, timeout=10)
        logger.info(f"飞书推送: {r.status_code}")
    except Exception as e:
        logger.error(f"飞书推送失败: {e}")

# ── 加载 trades.json 汇总 ────────────────────────────────
def load_trades_summary():
    return aggregate_trades_positions(load_trades(TRADES_FILE))

# ── 加载 mx-moni 持仓 ──────────────────────────────────
def load_mx_positions():
    return aggregate_mock_positions(fetch_mock_positions())

# ── 主逻辑 ──────────────────────────────────────────────
def main():
    fix = "--fix" in sys.argv
    logger.info("持仓一致性检查启动")

    trades = load_trades_summary()
    try:
        mx = load_mx_positions()
    except Exception as e:
        logger.error(f"获取 mx-moni 持仓失败: {e}")
        push_feishu(f"⚠️ 持仓同步检查失败：无法获取 mx-moni 持仓\n错误: {e}")
        return

    mx_codes = set(mx.keys())
    tr_codes = set(trades.keys())
    all_codes = mx_codes | tr_codes

    diff_lines = []
    for code in sorted(all_codes):
        m = mx.get(code, {})
        t = trades.get(code, {})
        mq = m.get("qty", 0)
        tq = t.get("qty", 0)
        if mq != tq:
            diff_lines.append(f"  • {code} {m.get('name') or t.get('name')}: trades={tq}股 vs 模拟账户={mq}股")
        elif mq > 0:
            m_cost = float(m.get("cost_price", 0) or 0)
            t_cost = float(t.get("cost_price", 0) or 0)
            if m_cost > 0 and abs(m_cost - t_cost) > 0.0005:
                diff_lines.append(
                    f"  • {code} {m.get('name') or t.get('name')}: trades成本={t_cost:.4f} vs 模拟账户成本={m_cost:.4f}"
                )

    missing_in_trades = [f"  • {c} {mx[c]['name']}（模拟账户有，trades.json无）" for c in sorted(mx_codes - tr_codes)]
    missing_in_mx = [f"  • {c} {trades[c]['name']}（trades.json有，模拟账户无）" for c in sorted(tr_codes - mx_codes)]

    if not diff_lines and not missing_in_trades and not missing_in_mx:
        msg = f"✅ 持仓一致检查完成\ntrades.json 共 {len(trades)} 只，与模拟账户 {len(mx)} 只完全匹配"
        logger.info(msg)
        push_feishu(msg)
        return

    lines = []
    if missing_in_trades:
        lines.append("🔴 缺失（模拟账户有，trades.json无）：")
        lines.extend(missing_in_trades)
    if missing_in_mx:
        lines.append("🟠 多余（trades.json有，模拟账户无）：")
        lines.extend(missing_in_mx)
    if diff_lines:
        lines.append("🟡 数量差异：")
        lines.extend(diff_lines)

    if fix:
        try:
            report = reconcile_trades_file_with_account(source="check_positions_sync")
            msg = (
                "✅ 持仓不一致已自动同步\n\n"
                + "\n".join(lines)
                + f"\n\n同步结果: fixed={len(report.get('fixed', []))}, created={len(report.get('created', []))}, "
                  f"consistent={report.get('is_consistent')}"
            )
            logger.warning(msg)
            push_feishu(msg)
            return
        except Exception as e:
            logger.error(f"自动同步失败: {e}")
            push_feishu(f"❌ 持仓自动同步失败\n错误: {e}")
            return

    msg = "⚠️ 持仓不一致，需要运行 check_positions_sync.py --fix 自动同步\n\n" + "\n".join(lines)
    logger.warning(msg)
    push_feishu(msg)

if __name__ == "__main__":
    main()
