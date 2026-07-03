"""
持仓辩论状态定义
对齐 TradingAgents AgentState 结构：

嵌套状态：
- InvestDebateState — 投资辩论（Bull/Bear + Research Manager）
- RiskDebateState — 风险辩论（三风控 + Portfolio Manager）
- StockDebateState — 主状态（包含以上两个嵌套状态）
"""

from typing import Annotated
from typing_extensions import TypedDict


# ── 投资辩论状态（Bull/Bear 辩论 + Research Manager 裁决）────────────

class InvestDebateState(TypedDict):
    """投资辩论状态"""
    bull_history: Annotated[str, "多头辩论历史"]
    bear_history: Annotated[str, "空头辩论历史"]
    history: Annotated[str, "完整辩论历史"]
    current_response: Annotated[str, "最新响应"]
    judge_decision: Annotated[str, "Research Manager 裁决结果"]
    count: Annotated[int, "辩论轮次计数"]


# ── 风险辩论状态（三风控分析师 + Portfolio Manager）────────────

class RiskDebateState(TypedDict):
    """风险辩论状态"""
    aggressive_history: Annotated[str, "激进风控历史"]
    conservative_history: Annotated[str, "保守风控历史"]
    neutral_history: Annotated[str, "中性风控历史"]
    history: Annotated[str, "完整辩论历史"]
    latest_speaker: Annotated[str, "最后发言者"]
    current_aggressive_response: Annotated[str, "激进分析师最新响应"]
    current_conservative_response: Annotated[str, "保守分析师最新响应"]
    current_neutral_response: Annotated[str, "中性分析师最新响应"]
    judge_decision: Annotated[str, "Portfolio Manager 裁决结果"]
    count: Annotated[int, "辩论轮次计数"]


# ── 持仓辩论主状态（单股票完整流程）────────────────────────────

class StockDebateState(TypedDict):
    """
    持仓辩论主状态
    
    完整流程：
    研究团队（市场/舆情/新闻/基本面并行）→ Bull/Bear 辩论 → Research Manager
    → Trader → 三风控辩论 → Portfolio Manager 最终决策
    """
    
    # 基本信息
    stock_code: Annotated[str, "股票代码"]
    stock_name: Annotated[str, "股票名称"]
    trade_date: Annotated[str, "交易日期"]
    
    # 研究团队报告（可扩展：实时从数据源获取）
    market_report: Annotated[str, "市场分析报告"]
    sentiment_report: Annotated[str, "舆情报告"]
    news_report: Annotated[str, "新闻报告"]
    fundamentals_report: Annotated[str, "基本面报告"]
    
    # 投资辩论状态
    investment_debate_state: Annotated[InvestDebateState, "投资辩论状态"]
    
    # Research Manager 裁决结果
    investment_plan: Annotated[str, "投资计划（Research Manager 裁决）"]
    
    # Trader 提案
    trader_investment_plan: Annotated[str, "交易员提案"]
    
    # 风险辩论状态
    risk_debate_state: Annotated[RiskDebateState, "风险辩论状态"]
    
    # 最终决策
    final_trade_decision: Annotated[str, "最终交易决策"]
    past_context: Annotated[str, "历史决策上下文（基金经理参考）"]
    
    # 持仓特有字段
    buy_price: Annotated[float, "买入价"]
    current_price: Annotated[float, "当前价"]
    pnl_pct: Annotated[float, "浮盈亏（%）"]
    action: Annotated[str, "建议操作：BUY/HOLD/SELL"]