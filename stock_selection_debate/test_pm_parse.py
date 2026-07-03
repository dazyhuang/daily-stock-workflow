#!/usr/bin/env python3
"""
测试 volcengine PM thinking 模式的解析
"""
import re
import os
import sys

if os.environ.get("RUN_LIVE_LLM_TESTS") != "1":
    print("skipped: set RUN_LIVE_LLM_TESTS=1 to run live PM parse probe")
    raise SystemExit(0)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from debate_engine import PORTFOLIO_MANAGER_SECONDARY_MODEL

def _parse_portfolio_manager_text(text: str):
    if not text:
        return None
    cleaned = re.sub(r'PortfolioManagerOutput\s*\([^)]*\)', '', text)
    m = re.search(r"(?:signal|最终信号)\s*[:：]\s*(\w+)", cleaned, re.IGNORECASE)
    signal = m.group(1).strip().upper() if m else None
    if signal not in {"BUY", "WATCH", "AVOID"}:
        return None
    m = re.search(r"(?:confidence|置信度)\s*[:：]\s*([0-9]+)", cleaned, re.IGNORECASE)
    confidence = int(m.group(1)) if m else 0
    m = re.search(r"(?:新开仓仓位上限|position_ratio)\s*[:：]\s*([0-9.]+)", cleaned, re.IGNORECASE)
    position_ratio = float(m.group(1)) if m else 0.0
    if position_ratio > 1.0:
        position_ratio = position_ratio / 100.0
    m = re.search(r"(?:reason|核心理由)\s*[:：]\s*(.+?)(?:\n|$)", cleaned, re.IGNORECASE)
    reason = m.group(1).strip() if m else ""
    if re.search(r"PortfolioManagerOutput|\b\w+\s*\(.*\)", reason):
        reason = re.sub(r"PortfolioManagerOutput[^,\n]*", "", reason).strip() or ""
    return {
        "signal": signal,
        "confidence": max(0, min(100, confidence)),
        "position_ratio": round(position_ratio, 4),
        "reason": reason,
    }

MODEL = PORTFOLIO_MANAGER_SECONDARY_MODEL
PROMPT = """你是一位严谨的价值投资基金经理。基于以下数据，对沪电股份（002463.SZ）进行仓位决策。

数据包：
- RSI(14): 45.2（中性）
- MA5: 112.5, MA20: 108.3, MA60: 105.7（多头排列）
- 5日涨跌: +9.41%
- 成交量: 放量配合
- 净利润增速: +35%（近季度）
- ROE: 18.5%

请严格按以下 JSON 格式输出裁决，不要输出任何解释：

{"signal": "WATCH", "confidence": 75, "position_ratio": 0.25, "reason": "均线多头排列且RSI未超买，业绩增速支持，但5日涨幅已大需等待回踩"}"""

print(f"模型: {MODEL}")
print(f"测试解析...\n")

from openai import OpenAI
import instructor

client = instructor.from_openai(
    OpenAI(
        api_key=os.environ.get("OPENAI_API_KEY"),
        base_url="https://ark.cn-beijing.volces.com/api/v3",
    )
)

def call_structured(prompt, output_cls, **kwargs):
    return client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        response_model=output_cls,
        **kwargs
    )

class PMOut:
    def __init__(self, signal="WATCH", confidence=0, position_ratio=0.0, reason=""):
        self.signal = signal
        self.confidence = confidence
        self.position_ratio = position_ratio
        self.reason = reason

# Test 1: Direct structured output
print("=== Test 1: instructor 结构化输出 ===")
try:
    result = call_structured(PROMPT, PMOut, thinking_budget=0, max_tokens=1000)
    print(f"  结构化成功: signal={result.signal} conf={result.confidence}")
except Exception as e:
    print(f"  结构化失败: {e}")

# Test 2: Raw text + regex parse
print("\n=== Test 2: 纯文本 thinking 模式 ===")
try:
    from openai import OpenAI as OAI2
    client2 = OAI2(
        api_key=os.environ.get("OPENAI_API_KEY"),
        base_url="https://ark.cn-beijing.volces.com/api/v3",
    )
    resp = client2.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": PROMPT}],
        thinking={
            "type": "thinking",
            "budget_tokens": 1000,
        },
        max_tokens=500,
    )
    raw_text = resp.choices[0].message.content
    print(f"  原始输出 (前500字):\n{raw_text[:500]}")
    parsed = _parse_portfolio_manager_text(raw_text)
    print(f"  解析结果: {parsed}")
except Exception as e:
    print(f"  thinking 模式失败: {e}")
    import traceback
    traceback.print_exc()
