"""
周复盘辩论 LangGraph State 定义
===============================
对齐 TradingAgents agent_states.py 设计：

两种状态：
1. WeeklyReviewState — 主辩论状态（5角色顺序执行）
2. StockDebateState — 持仓辩论状态（每持仓股票走完流程）

持仓辩论 = TradingAgents 单股票分析流程的周报版：
- research_team（分析师并行 → Bull/Bear辩论 → Research Manager裁决）
- risk_team（Trader → 三风控分析师辩论 → Portfolio Manager决策）
"""

from typing import Annotated
from typing_extensions import TypedDict


# ── 投资辩论状态（Bull/Bear 辩论 + Research Manager）────────────

class InvestDebateState(TypedDict):
    """投资辩论状态，对应 TradingAgents InvestDebateState"""
    bull_history: Annotated[str, "多头辩论历史"]
    bear_history: Annotated[str, "空头辩论历史"]
    history: Annotated[str, "完整辩论历史"]
    current_response: Annotated[str, "最新响应"]
    judge_decision: Annotated[str, "裁决结果"]
    count: Annotated[int, "辩论轮次计数"]


# ── 风险辩论状态（三风控分析师 + Portfolio Manager）────────────

class RiskDebateState(TypedDict):
    """风险辩论状态，对应 TradingAgents RiskDebateState"""
    aggressive_history: Annotated[str, "激进风控历史"]
    conservative_history: Annotated[str, "保守风控历史"]
    neutral_history: Annotated[str, "中性风控历史"]
    history: Annotated[str, "完整辩论历史"]
    latest_speaker: Annotated[str, "最后发言者"]
    current_aggressive_response: Annotated[str, "激进分析师最新响应"]
    current_conservative_response: Annotated[str, "保守分析师最新响应"]
    current_neutral_response: Annotated[str, "中性分析师最新响应"]
    judge_decision: Annotated[str, "裁决结果"]
    count: Annotated[int, "辩论轮次计数"]


# ── 主辩论状态 ──────────────────────────────────────────────

class WeeklyReviewState(TypedDict):
    """周复盘辩论主状态，5角色顺序执行"""
    
    # 输入数据
    week_data: Annotated[dict, "本周量化数据（trades/stats/benchmark/current_params）"]
    week_str: Annotated[str, "周范围字符串 'YYYY-MM-DD ~ YYYY-MM-DD'"]
    
    # 分析师节点输出
    analyst_output: Annotated[str, "数据分析师输出（markdown渲染后）"]
    
    # 策略师节点输出
    strategist_output: Annotated[str, "策略师输出（markdown渲染后）"]
    
    # 风控官节点输出
    risk_output: Annotated[str, "风控官输出（markdown渲染后）"]
    
    # 基金经理节点输出
    final_decision: Annotated[str, "基金经理最终决策（markdown渲染后）"]
    final_decision_obj: Annotated[dict, "基金经理决策Pydantic对象（原始）"]
    
    # 辩论历史（用于追踪）
    debate_history: Annotated[str, "辩论过程记录"]
    
    # 执行状态
    sender: Annotated[str, "当前执行节点名"]
    current_step: Annotated[str, "当前步骤：analyst/strategist/risk/fund_manager/complete"]
    
    # 持仓辩论状态（每持仓股票独立）
    stock_debates: Annotated[list, "每持仓股票的辩论结果列表"]
    current_stock_index: Annotated[int, "当前处理到第几只持仓"]


# ── 持仓辩论状态（单股票）────────────────────────────────────

class StockDebateState(TypedDict):
    """
    持仓辩论状态，对应 TradingAgents AgentState
    
    持仓辩论走完完整流程：
    Analyst并行 → Bull/Bear辩论 → Research Manager裁决
    → Trader提案 → 三风控辩论 → Portfolio Manager最终决策
    """
    
    # 基本信息
    stock_code: Annotated[str, "股票代码"]
    stock_name: Annotated[str, "股票名称"]
    trade_date: Annotated[str, "交易日期"]
    
    # 研究团队
    market_report: Annotated[str, "市场分析报告"]
    sentiment_report: Annotated[str, "舆情报告"]
    news_report: Annotated[str, "新闻报告"]
    fundamentals_report: Annotated[str, "基本面报告"]
    
    # 投资辩论状态
    investment_debate_state: Annotated[InvestDebateState, "投资辩论状态"]
    investment_plan: Annotated[str, "研究计划（Research Manager裁决）"]
    
    # 交易员提案
    trader_investment_plan: Annotated[str, "交易员提案"]
    
    # 风险辩论状态
    risk_debate_state: Annotated[RiskDebateState, "风险辩论状态"]
    
    # 最终决策
    final_trade_decision: Annotated[str, "最终交易决策"]
    past_context: Annotated[str, "历史决策上下文"]
    
    # 持仓特有
    buy_price: Annotated[float, "买入价"]
    current_price: Annotated[float, "当前价"]
    pnl_pct: Annotated[float, "浮盈亏（%）"]
    action: Annotated[str, "建议操作：BUY/HOLD/SELL"]