"""
TradingAgents 多智能体辩论系统
================================
论文架构: 分析师 → Bull/Bear研究员(辩论) → 三方风控(辩论) → 基金经理(决策)

角色清单:
1. 分析师团队 (AnalystTeam) — 4维度分析
2. Bull Researcher — 看多辩论
3. Bear Researcher — 看空辩论  
4. Research Manager — 裁决辩论结果
5. Aggressive Risk — 激进风控
6. Conservative Risk — 保守风控
7. Neutral Risk — 中性风控
8. Fund Manager / DecisionAgent — 最终决策 (5档评级)
"""

import os
import time
import json
import logging
import requests
from typing import Dict, Any, List, Optional

logger = logging.getLogger("agents")

# ── LLM 配置 ──────────────────────────────────────────────
import sys
from pathlib import Path

MX_API_KEY = os.environ.get("MX_DIRECT_KEY", "")
VOLCAN_MODEL = "ark-code-latest"
MINIMAX_MODEL = "MiniMax-M3"
VOLCAN_BASE_URL = "https://ark.cn-beijing.volces.com/api/coding/v3/chat/completions"
MINIMAX_BASE_URL = "https://api.minimaxi.com/anthropic/v1"

# ── API Key 读取（与 debate_engine.py 保持一致）────────────
_PROVIDER_MAP: dict = {}

def _load_models_config():
    global _PROVIDER_MAP
    try:
        cfg_path = Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth-profiles.json"
        with open(cfg_path) as f:
            profiles = json.load(f)
        for key_name in ("volcengine-plan:default", "volcengine:default"):
            entry = profiles.get("profiles", {}).get(key_name, {})
            api_key = entry.get("key") or entry.get("access", "")
            if api_key and len(api_key) > 20:
                _PROVIDER_MAP["volcengine"] = api_key
                break
    except Exception:
        pass

def _get_volcan_key():
    if not _PROVIDER_MAP:
        _load_models_config()
    return _PROVIDER_MAP.get("volcengine", os.environ.get("VOLCAN_API_KEY", ""))


def call_llm(prompt: str, model: str = "ignore", timeout: int = 300,
             retry: int = 3, think: str = "high", max_tokens: int = 4096) -> str:
    """火山引擎 Coding Plan(thinking=high) → MiniMax M3 兜底"""
    headers_vc = {
        "Authorization": f"Bearer {_get_volcan_key()}",
        "Content-Type": "application/json",
    }
    payload_vc = {
        "model": VOLCAN_MODEL,
        "max_tokens": max_tokens,
        "temperature": 0.3,
        "messages": [{"role": "user", "content": prompt}],
        "thinking": {"budget_tokens": 16384},
    }
    headers_mx = {
        "Authorization": f"Bearer {MX_API_KEY}",
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01",
    }
    payload_mx = {
        "model": MINIMAX_MODEL, "max_tokens": max_tokens,
        "thinking": {"type": "enabled", "budget_tokens": 50000} if think == "high" else {"type": "disabled"},
        "messages": [{"role": "user", "content": prompt}],
    }
    for attempt in range(retry):
        try:
            resp = requests.post(VOLCAN_BASE_URL, headers=headers_vc, json=payload_vc, timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                msg = data.get("choices", [{}])[0].get("message", {})
                content = msg.get("content", "") or ""
                if content: return content
                think_blob = msg.get("thinking", "") or ""
                if think_blob: return think_blob
            elif resp.status_code == 429:
                time.sleep(30 * (2 ** attempt))
        except Exception:
            pass
        try:
            resp = requests.post(f"{MINIMAX_BASE_URL}/messages", headers=headers_mx, json=payload_mx, timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                for block in data.get("content", []):
                    if block.get("type") == "text":
                        return block.get("text", "")
            elif resp.status_code == 429:
                time.sleep(30 * (2 ** attempt))
        except Exception:
            pass
        if attempt < retry - 1: time.sleep(10)
    return ""


def _extract_json(text: str, default: dict = None) -> dict:
    """从LLM文本中提取JSON块"""
    try:
        start, end = text.find("{"), text.rfind("}") + 1
        return json.loads(text[start:end]) if start >= 0 else (default or {})
    except Exception:
        return default or {}


# ── 财务/技术数据格式化 ───────────────────────────────────
def _build_fin_tech_text(stock: Dict) -> str:
    lines = []
    for k, label, fmt in [
        ("roe", "ROE(年报)", "{:.1f}%"), ("revenue_growth", "营收增速", "{:.1f}%"),
        ("profit_growth", "净利润增速", "{:.1f}%"), ("debt_ratio", "负债率", "{:.1f}%"),
        ("pe", "PE(TTM)", "{:.2f}倍"), ("pb", "PB", "{:.2f}倍"),
    ]:
        v = stock.get(k)
        if v is not None: lines.append(f"  {label}: {fmt.format(v)}")
    for k, label in [("consecutive_profitable", "连续三年盈利"), ("ma_trend", "均线趋势"),
                      ("macd_golden_cross", "MACD金叉")]:
        v = stock.get(k)
        if v is not None: lines.append(f"  {label}: {'是' if v else '否' if isinstance(v, bool) else v}")
    for k, label, fmt in [("rsi", "RSI(14)", "{:.1f}"), ("vol_ratio", "量比", "{:.1f}x"),
                            ("macd", "MACD柱", "{:.4f}"), ("ma5", "MA5", "{:.2f}"),
                            ("ma10", "MA10", "{:.2f}"), ("ma20", "MA20", "{:.2f}"),
                            ("yesterday_chg", "昨涨幅", "{:.2f}%")]:
        v = stock.get(k)
        if v is not None: lines.append(f"  {label}: {fmt.format(v)}")
    # 板块信息（来自外部注入）
    sec_info = stock.get("sector_info", "")
    if sec_info and sec_info != "暂无板块数据":
        lines.append(f"  板块: {sec_info[:60]}")
    return "\n".join(lines) if lines else "暂无详细数据"


def _stock_header(stock: Dict) -> str:
    return f"""代码: {stock.get('code','?')}  {stock.get('name','?')}
持仓: {stock.get('quantity',0)}股  成本: {stock.get('cost',0):.2f}元  现价: {stock.get('current_price',0):.2f}元  盈亏: {stock.get('pnl_pct',0)*100:+.1f}%"""


# ════════════════════════════════════════════════════════════
# 1. 分析师团队
# ════════════════════════════════════════════════════════════

class AnalystTeam:
    """4维度并行分析: 基本面 / 技术面 / 资金面 / 消息面"""
    NAME = "分析师团队"

    def __init__(self, model: str = VOLCAN_MODEL):
        self.model = model

    def analyze(self, stock: Dict, market_data: Dict) -> str:
        kline_ctx = stock.get('_kline_ctx', '')
        kline_section = f"""\n
【K线形态与蜡烛图知识库】
{kline_ctx[:1800]}
""" if kline_ctx else ""

        prompt = f"""你是A股分析师团队（基本面+技术面+资金面+消息面四维分析）。

【目标股票】
{_stock_header(stock)}{kline_section}

【财务技术指标】
{_build_fin_tech_text(stock)}

【大盘】
上证 {market_data.get('sh_index',0):.2f} ({market_data.get('sh_change',0)*100:+.2f}%) | 
深证 {market_data.get('sz_index',0):.2f} ({market_data.get('sz_change',0)*100:+.2f}%) | 
创业板 {market_data.get('cy_index',0):.2f} ({market_data.get('cy_change',0)*100:+.2f}%)

【市场情绪】
涨停{market_data.get('limit_up','?')}家 跌停{market_data.get('limit_down','?')}家 
炸板率{market_data.get('breakout_rate',0):.1f}% 连板{market_data.get('continuous_height','?')}板

【板块强弱】（外部注入）
{market_data.get('_sector_info', stock.get('sector_info', '暂无'))}

【新闻】
{stock.get('news','暂无')[:500]}

请从4维度给出分析（每维度3-5条具体论据，引用上述财务技术数据）：
1. 基本面（PE/PB估值、ROE、增速、负债率、连续盈利）
2. 技术面（均线多空、MACD金叉/死叉、RSI超买超卖、量比；如K线出现特定形态需特别关注）
3. 资金面（主力流向、北向、量价关系）
4. 消息面（板块热度、公告、政策）

最后给出综合评分（1-10分）+ 一句话核心逻辑。"""
        return call_llm(prompt, model=self.model, max_tokens=4096)


# ════════════════════════════════════════════════════════════
# 2. Bull/Bear 研究员 + Research Manager（辩论）
# ════════════════════════════════════════════════════════════

class BullResearcher:
    """看多研究员：强调增长潜力、估值优势、催化剂"""
    NAME = "Bull研究员"
    def __init__(self, model: str = VOLCAN_MODEL):
        self.model = model

    def argue(self, stock: Dict, analyst_report: str, bear_arg: str = "",
              round_num: int = 1) -> str:
        context = ""
        if bear_arg:
            context = f"\n【空方观点（第{round_num}轮）】\n{bear_arg[:800]}\n请逐条反驳上述空方观点。"

        kline_ctx = stock.get('_kline_ctx', '')
        kline_section = f"\n\n【K线形态与蜡烛图知识库】（请结合K线形态知识评估）\n{kline_ctx[:1500]}\n" if kline_ctx else ""

        prompt = f"""你是A股看多研究员（Bull Researcher），负责为以下股票辩护。

{_stock_header(stock)}{kline_section}
财务技术: {_build_fin_tech_text(stock)[:400]}
分析师报告: {analyst_report[:600]}
{context}

请从看多角度论述（3-5条核心逻辑）：
- 引用具体的PE/PB/ROE/增速数据说明估值合理或低估
- 指出技术面的积极信号（均线、MACD、量价；如K线出现锤子线/放量阳线等形态，结合蜡烛图知识评估看涨信号强度）
- 说明持仓盈亏状态对你判断的影响（{stock.get('pnl_pct',0)*100:+.1f}%盈亏如何影响你的建议）
- 给出目标价位和1-10分看多信心评分

输出JSON: {{"bull_thesis": "核心看多逻辑", "target_price": 数字, "confidence": 1-10, "key_risks": ["风险1"]}}"""
        return call_llm(prompt, model=self.model, timeout=100, max_tokens=2048)


class BearResearcher:
    """看空研究员：强调风险、高估、下行空间"""
    NAME = "Bear研究员"
    def __init__(self, model: str = VOLCAN_MODEL):
        self.model = model

    def argue(self, stock: Dict, analyst_report: str, bull_arg: str = "",
              round_num: int = 1) -> str:
        context = ""
        if bull_arg:
            context = f"\n【多方观点（第{round_num}轮）】\n{bull_arg[:800]}\n请逐条反驳上述多方观点。"

        kline_ctx = stock.get('_kline_ctx', '')
        kline_section = f"\n\n【K线形态与蜡烛图知识库】（请结合K线形态知识识别风险）\n{kline_ctx[:1500]}\n" if kline_ctx else ""

        prompt = f"""你是A股看空研究员（Bear Researcher），负责对以下股票提出风险警告。

{_stock_header(stock)}{kline_section}
财务技术: {_build_fin_tech_text(stock)[:400]}
分析师报告: {analyst_report[:600]}
{context}

请从看空角度论述（3-5条核心风险）：
- 引用PE/PB/负债率数据说明是否高估
- 指出技术面的危险信号（均线死叉、RSI超买、缩量；如K线出现流星线/乌云盖顶/顶背离等形态，结合蜡烛图知识评估看跌信号强度）
- 持仓盈亏对决策的影响（{stock.get('pnl_pct',0)*100:+.1f}%盈亏下，是否存在"死拿亏损"或"过早止盈"的行为偏差）
- 给出最坏情况跌幅预估

输出JSON: {{"bear_thesis": "核心看空逻辑", "worst_case_price": 数字, "confidence": 1-10, "key_concerns": ["担忧1"]}}"""
        return call_llm(prompt, model=self.model, timeout=100, max_tokens=2048)


class ResearchManager:
    """裁决 Bull/Bear 辩论，输出统一研判"""
    NAME = "研究总监"
    def __init__(self, model: str = VOLCAN_MODEL):
        self.model = model

    def judge(self, stock: Dict, analyst_report: str, bull_arg: str, bear_arg: str,
              rounds: int = 1) -> Dict:
        prompt = f"""你是A股研究总监（Research Manager），裁决以下Bull/Bear{rounds}轮辩论。

{_stock_header(stock)}
分析师报告: {analyst_report[:400]}
多空辩论:
【Bull】{bull_arg[:600]}
【Bear】{bear_arg[:600]}

请裁决：
1. 哪方论点更有说服力？为什么？
2. 是否存在双方都同意的共识点？
3. 最终研判方向（偏多/偏空/中性）

输出JSON: {{"winner": "bull|bear|tie", "consensus": "双方共识", "direction": "偏多|偏空|中性", "confidence": 1-10, "reason": "裁决理由"}}"""
        result = call_llm(prompt, model=self.model, timeout=100, max_tokens=1200)
        return _extract_json(result, {"winner": "tie", "direction": "中性", "confidence": 5})


# ════════════════════════════════════════════════════════════
# 3. 三方风控团队（辩论）
# ════════════════════════════════════════════════════════════

class AggressiveRisk:
    """激进风控：追求收益最大化，容忍波动"""
    NAME = "激进风控"
    def __init__(self, model: str = VOLCAN_MODEL):
        self.model = model

    def assess(self, stock: Dict, analyst_report: str, bull_arg: str, bear_arg: str,
               prev_aggressive: str = "", prev_conservative: str = "", prev_neutral: str = "",
               round_num: int = 1) -> str:
        context = ""
        if prev_conservative or prev_neutral:
            context = f"\n【保守观点】{prev_conservative[:300]}\n【中性观点】{prev_neutral[:300]}"
        prompt = f"""你是激进风控分析师，追求高收益，容忍较高波动。

{_stock_header(stock)}
财务技术: {_build_fin_tech_text(stock)[:300]}
多空辩论: Bull={bull_arg[:300]} | Bear={bear_arg[:300]}
{context}

请从激进角度评估（侧重上行空间）:
- 当前盈亏{stock.get('pnl_pct',0)*100:+.1f}%，是否应该乘胜追击/逢低加仓？
- 最大可接受回撤是多少？
- 建议操作（ADD/HOLD/REDUCE）+ 目标仓位比例

输出JSON: {{"action": "ADD|HOLD|REDUCE", "target_ratio": 0.0-1.0, "max_drawdown": "描述", "reason": "理由"}}"""
        return call_llm(prompt, model=self.model, timeout=100, max_tokens=1200)


class ConservativeRisk:
    """保守风控：资产保护优先，最小化回撤"""
    NAME = "保守风控"
    def __init__(self, model: str = VOLCAN_MODEL):
        self.model = model

    def assess(self, stock: Dict, analyst_report: str, bull_arg: str, bear_arg: str,
               prev_aggressive: str = "", prev_conservative: str = "", prev_neutral: str = "",
               round_num: int = 1) -> str:
        context = ""
        if prev_aggressive or prev_neutral:
            context = f"\n【激进观点】{prev_aggressive[:300]}\n【中性观点】{prev_neutral[:300]}"
        prompt = f"""你是保守风控分析师，资产保护优先，风险最小化。

{_stock_header(stock)}
财务技术: {_build_fin_tech_text(stock)[:300]}
多空辩论: Bull={bull_arg[:300]} | Bear={bear_arg[:300]}
{context}

请从保守角度评估（侧重下行风险）:
- 当前盈亏{stock.get('pnl_pct',0)*100:+.1f}%，是否已达止盈/止损线？
- 止损价位建议（参考均线支撑、MACD信号）
- 建议操作（HOLD/REDUCE/CLEAR）+ 理由

输出JSON: {{"action": "HOLD|REDUCE|CLEAR", "stop_loss": 数字, "max_drawdown_risk": "描述", "reason": "理由"}}"""
        return call_llm(prompt, model=self.model, timeout=100, max_tokens=1200)


class NeutralRisk:
    """中性风控：平衡收益与风险"""
    NAME = "中性风控"
    def __init__(self, model: str = VOLCAN_MODEL):
        self.model = model

    def assess(self, stock: Dict, analyst_report: str, bull_arg: str, bear_arg: str,
               prev_aggressive: str = "", prev_conservative: str = "", prev_neutral: str = "",
               round_num: int = 1) -> str:
        context = ""
        if prev_aggressive or prev_conservative:
            context = f"\n【激进观点】{prev_aggressive[:300]}\n【保守观点】{prev_conservative[:300]}"
        prompt = f"""你是中性风控分析师，平衡收益与风险。

{_stock_header(stock)}
财务技术: {_build_fin_tech_text(stock)[:300]}
多空辩论: Bull={bull_arg[:300]} | Bear={bear_arg[:300]}
{context}

请从中性角度综合评估:
- 激进和保守两方谁的逻辑更站得住脚？
- 当前最合理的操作是什么？
- 给出综合风险评级（低/中/高）

输出JSON: {{"action": "ADD|HOLD|REDUCE|CLEAR", "risk_level": "低|中|高", "preferred_view": "激进|保守", "reason": "理由"}}"""
        return call_llm(prompt, model=self.model, timeout=100, max_tokens=1200)


# ════════════════════════════════════════════════════════════
# 4. 基金经理（最终决策）— 5档评级
# ════════════════════════════════════════════════════════════

class FundManager:
    """最终决策：综合所有辩论结果，输出5档评级"""
    NAME = "基金经理"

    def __init__(self, model: str = VOLCAN_MODEL):
        self.model = model

    def decide(self, stock: Dict, analyst_report: str,
               bull_arg: str, bear_arg: str, judge_result: Dict,
               risk_aggressive: str, risk_conservative: str, risk_neutral: str,
               all_stocks: List[Dict]) -> Dict:
        others = "\n".join([f"  {s.get('code')} {s.get('name')} "
                            f"({s.get('pnl_pct',0)*100:+.1f}%) {s.get('quantity',0)}股"
                            for s in all_stocks if s.get('code') != stock.get('code')])

        prompt = f"""你是A股基金经理，综合所有团队意见做出最终决策。

【目标股票】
{_stock_header(stock)}
财务技术: {_build_fin_tech_text(stock)[:400]}

【分析师报告】{analyst_report[:500]}

【多空辩论裁决】
方向: {judge_result.get('direction','?')}  置信度: {judge_result.get('confidence','?')}/10
理由: {judge_result.get('reason','?')}

【三方风控】
激进: {risk_aggressive[:300]}
保守: {risk_conservative[:300]}
中性: {risk_neutral[:300]}

【组合其他持仓】（用于判断集中度）
{others}

【决策要求】
输出5档评级之一:
- STRONG_BUY: 强烈买入/加仓（高置信度看多，风控一致同意）
- BUY: 买入/加仓（偏多，但风控有保留）
- HOLD: 持有不动（方向不明确，或已达到合理仓位）
- REDUCE: 减仓（偏空，或仓位过重需降集中度）
- CLEAR: 清仓（明确看空，或触发止损线）

必须考虑持仓盈亏心理偏差：盈利{stock.get('pnl_pct',0)*100:+.1f}%时是否过度自信？亏损时是否死拿？

输出JSON:
{{"action": "STRONG_BUY|BUY|HOLD|REDUCE|CLEAR", "confidence": "高|中|低",
  "target_ratio": 0.0-1.0, "reason": "决策理由80字内", "urgency": "立即|本周期|观察",
  "price_target": 数字或null, "stop_loss": 数字或null, "warning": "风险提示"}}"""
        result = call_llm(prompt, model=self.model, timeout=120, max_tokens=800)
        default = {"action": "HOLD", "confidence": "低", "target_ratio": 0.0,
                   "reason": "决策解析失败", "urgency": "观察"}
        return _extract_json(result, default)


# ── 旧接口兼容 ────────────────────────────────────────────
class AnalystAgent(AnalystTeam): pass
class ResearcherAgent:
    """旧接口兼容（现在用 BullResearcher + BearResearcher + ResearchManager）"""
    NAME = "研究员(旧)"
    def __init__(self, model=VOLCAN_MODEL): self.model = model
    def research(self, stock, analyst_output): return ""
class RiskAgent:
    """旧接口兼容（现在用三方风控）"""
    NAME = "风控(旧)"
    def __init__(self, model=VOLCAN_MODEL): self.model = model
    def assess_risk(self, stock, analyst, researcher): return {}
    def assess_portfolio_risk(self, all_stocks): return ""
class DecisionAgent(FundManager): pass


# ── 全量持仓约束（辩论后裁剪） ────────────────────────────
def apply_portfolio_constraint(stocks: List[Dict], max_hold: int = 10) -> List[Dict]:
    """如果 ADD+HOLD+REDUCE 超过 max_hold 只，将 conviction 最低的 CLEAR"""
    non_clear = [s for s in stocks if s.get("decision", {}).get("action") not in ("CLEAR",)]
    if len(non_clear) <= max_hold:
        return stocks
    sorted_non_clear = sorted(non_clear, key=lambda s: s.get("decision", {}).get("confidence", "低"))
    to_clear = sorted_non_clear[:(len(non_clear) - max_hold)]
    for s in to_clear:
        s["decision"]["action"] = "CLEAR"
        s["decision"]["reason"] = (s["decision"].get("reason", "") + " [仓位约束CLEAR]")[:80]
        s["decision"]["constrained"] = True
    return stocks
