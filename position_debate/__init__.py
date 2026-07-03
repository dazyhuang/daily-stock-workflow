"""
持仓辩论模块
对齐 TradingAgents 单股票分析完整流程

导出：
- run_stock_debates — 对每持仓运行完整辩论
- get_stock_debate_graph — 获取预编译图
- StockDebateState — 状态类型
"""

from .graph import get_stock_debate_graph, build_stock_debate_graph
from .state import StockDebateState, InvestDebateState, RiskDebateState
from .schemas import (
    StockDecision,
    InvestmentPlan,
    TraderProposal,
    render_stock_decision,
    render_investment_plan,
    render_trader_proposal,
)

__all__ = [
    "get_stock_debate_graph",
    "build_stock_debate_graph",
    "StockDebateState",
    "InvestDebateState",
    "RiskDebateState",
    "StockDecision",
    "InvestmentPlan",
    "TraderProposal",
    "render_stock_decision",
    "render_investment_plan",
    "render_trader_proposal",
]