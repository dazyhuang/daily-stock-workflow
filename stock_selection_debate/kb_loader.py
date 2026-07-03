"""
知识库加载模块
================
辩论前加载技术分析知识库，作为判断规则注入 Prompt
"""

import re
from pathlib import Path
from typing import Dict

KB_DIR = Path(__file__).parent.parent.parent / "knowledge-base"


def load_kb_file(rel_path: str) -> str:
    """加载知识库文件内容"""
    full = KB_DIR / rel_path
    if full.exists():
        return full.read_text(encoding="utf-8")
    return ""


def extract_key_rules(text: str, max_chars: int = 3000) -> str:
    """从知识库文本中提取核心规则（截断到 max_chars）"""
    # 去除注释行、版权行
    lines = text.split("\n")
    filtered = []
    skip_patterns = ["#", "<!--", "^--", "===>", "---", "*来源*", "*版权*", "PDF", "epub"]
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if any(p in line for p in skip_patterns):
            continue
        if len(line) < 10:  # 太短的行跳过
            continue
        filtered.append(line)
    joined = "\n".join(filtered)
    return joined[:max_chars]


def load_technical_context() -> str:
    """
    加载并结构化技术分析知识库
    返回：注入辩论 Prompt 的技术规则文本
    """
    candlestick = load_kb_file("candlestick-charting/核心概念.md")
    stock_trend = load_kb_file("stock-trend-technical-analysis/核心概念.md")
    murphy = load_kb_file("technical-analysis-murphy/核心概念.md")
    volume_price = load_kb_file("volume-price-analysis/核心概念.md")
    turtle = load_kb_file("turtle-trading/core-rules.md")
    trend_covel = load_kb_file("trend-following-covel/README.md")

    return f"""
============================================
【蜡烛图形态识别规则】（candlestick-charting）
============================================
{extract_key_rules(candlestick, 3000)}
============================================

============================================
【趋势技术分析规则】（stock-trend-technical-analysis）
============================================
{extract_key_rules(stock_trend, 3000)}
============================================

============================================
【量价分析规则】（volume-price-analysis）
============================================
{extract_key_rules(volume_price, 3000)}
============================================

============================================
【海龟交易规则】（turtle-trading）
============================================
{extract_key_rules(turtle, 2000)}
============================================

============================================
【趋势跟踪原则】（trend-following-covel）
============================================
{extract_key_rules(trend_covel, 2000)}
============================================

============================================
【技术指标规则】（technical-analysis-murphy）
============================================
{extract_key_rules(murphy, 2000)}
============================================
"""


def load_wave_and_gann_context() -> str:
    """
    加载艾略特波浪和江恩理论规则（给高级分析师用）
    """
    elliott_rules = load_kb_file("elliott-wave-prechter/rules.md")
    gann_principles = load_kb_file("gann-wall-street/核心概念.md")

    return f"""
============================================
【艾略特波浪规则】（elliott-wave-prechter）
============================================
{extract_key_rules(elliott_rules, 3000)}
============================================

============================================
【江恩理论】（gann-wall-street）
============================================
{extract_key_rules(gann_principles, 2000)}
============================================
"""


def load_volume_context() -> str:
    """
    加载量价分析专门规则
    """
    vp_core = load_kb_file("volume-price-analysis/核心概念.md")
    return f"""
============================================
【量价分析核心规则】（volume-price-analysis）
============================================
{extract_key_rules(vp_core, 4000)}
============================================
"""


def load_all_kb_for_analyst(analyst_type: str) -> str:
    """
    按角色返回相关的知识库子集
    analyst_type: "candlestick" | "trend" | "murphy" | "volume" | "turtle" | "elliott" | "gann" | "all"
    """
    if analyst_type == "volume":
        return load_volume_context()
    elif analyst_type == "turtle":
        return extract_key_rules(load_kb_file("turtle-trading/core-rules.md"), 3000)
    elif analyst_type == "elliott":
        return extract_key_rules(load_kb_file("elliott-wave-prechter/rules.md"), 3000)
    elif analyst_type == "gann":
        return extract_key_rules(load_kb_file("gann-wall-street/核心概念.md"), 2000)
    elif analyst_type == "all":
        return load_technical_context()
    elif analyst_type == "candlestick":
        return extract_key_rules(load_kb_file("candlestick-charting/核心概念.md"), 3000)
    elif analyst_type == "trend":
        return extract_key_rules(load_kb_file("stock-trend-technical-analysis/核心概念.md"), 3000)
    elif analyst_type == "murphy":
        return extract_key_rules(load_kb_file("technical-analysis-murphy/核心概念.md"), 3000)
    return ""
