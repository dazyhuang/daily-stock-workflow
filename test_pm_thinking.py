#!/usr/bin/env python3
"""5只候选股辩论流程测试 - 仅测PM节点"""
import sys, os, json, logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
os.environ["PYTHONUNBUFFERED"] = "1"
if os.environ.get("RUN_LIVE_LLM_TESTS") != "1":
    print("skipped: set RUN_LIVE_LLM_TESTS=1 to run live PM thinking probe")
    raise SystemExit(0)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("test_pm")

TEST_STOCKS = [
    {"stock": "000001", "name": "平安银行", "reason": "银行板块低估值"},
    {"stock": "002594", "name": "比亚迪", "reason": "新能源龙头"},
    {"stock": "600519", "name": "贵州茅台", "reason": "消费白酒龙头"},
    {"stock": "300750", "name": "宁德时代", "reason": "动力电池龙头"},
    {"stock": "688041", "name": "龙芯中科", "reason": "CPU概念"},
]

def make_mock_phase1(stock, name, reason):
    return {
        "stock": stock,
        "name": name,
        "news": {"signal": "正面", "key_events": [f"{name}相关利好公告"], "summary": f"{name}基本面相好，{reason}。"},
        "technical": {"ma_score": 75, "rsi": 55, "boll_position": "中轨附近", "trend": "震荡向上", "recommendation": "BUY"},
        "sentiment": {"limit_up_count": 80, "board_leaders": 5, "chess_spirit": "偏多", "market_sentiment_score": 65},
    }

def test_pm(stock, name, reason):
    from stock_selection_debate.debate_engine import portfolio_manager_node, _parse_portfolio_manager_text

    state = {
        "stock_name": f"{name}({stock})",
        "stock": stock,
        "phase1_result": make_mock_phase1(stock, name, reason),
        "research_plan": f"{name}值得买入，{reason}，技术面良好，建议积极关注。置信度75分。",
        "history": "风控分析师A：看好，建议25%仓位，多方占优。\n风控分析师B：谨慎，提醒大盘风险，建议10%仓位。\n研究总监：偏多，置信度75分。",
        "debate_winner": "bull",
        "debate_consensus": f"{name}存在买入机会，但需控制仓位",
        "direction": "偏多",
        "confidence": 75,
    }

    print(f"\n{'='*50}")
    print(f"测试: {name}({stock})")
    print(f"{'='*50}")

    # 解析器测试
    test_text = f"""最终信号: BUY
置信度: 75
新开仓仓位上限: 25%
核心理由: {name}技术面良好，{reason}，研究总监偏多建议持仓。"""
    parsed = _parse_portfolio_manager_text(test_text)
    print(f"  [1] 解析器: {'✅' if parsed else '❌'} {json.dumps(parsed, ensure_ascii=False) if parsed else '解析失败'}")

    # PM thinking
    print(f"  [2] PM thinking调用...")
    try:
        result_state = portfolio_manager_node(state)
        sig = result_state.get("signal", "N/A")
        conf = result_state.get("confidence", "N/A")
        ratio = result_state.get("position_ratio", "N/A")
        reason_out = result_state.get("reason", "")
        src = result_state.get("decision_source", "N/A")
        print(f"  [2] ✅ 来源={src}, signal={sig}, confidence={conf}, ratio={ratio}")
        print(f"  [2] reason: {reason_out[:100] if reason_out else 'N/A'}")
        return True, src, result_state
    except Exception as e:
        print(f"  [2] ❌ 失败: {e}")
        import traceback
        traceback.print_exc()
        return False, str(e), None

if __name__ == "__main__":
    results = []
    ok_count = 0
    for stk in TEST_STOCKS:
        ok, source, _ = test_pm(stk["stock"], stk["name"], stk["reason"])
        if ok:
            ok_count += 1
            results.append(f"✅ {stk['name']}({stk['stock']}) - {source}")
        else:
            results.append(f"❌ {stk['name']}({stk['stock']}) - {source}")

    print(f"\n\n{'='*60}")
    print(f"测试结果: {ok_count}/5 成功")
    print(f"{'='*60}")
    for r in results:
        print(r)
