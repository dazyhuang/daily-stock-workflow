"""
周复盘辩论模块
=============
对齐 TradingAgents v0.2.5 架构的多智能体辩论系统

主要组件：
- schemas.py: Pydantic 数据模型（structured output）
- prompts.py: 角色 Prompt 模板
- state.py: LangGraph State 定义
- nodes.py: 5个角色节点工厂
- graph.py: LangGraph 状态机
- run_weekly_debate.py: 主入口

用法：
  python3 -m weekly_review.run_weekly_debate --input output/weekly_review_20260515.json
"""

from .schemas import (
    AnalystOutput,
    StrategistOutput,
    RiskOutput,
    WeeklyReviewDecision,
    PortfolioRating,
    Action,
    render_analyst_output,
    render_strategist_output,
    render_risk_output,
    render_weekly_decision,
)
from .graph import get_weekly_review_graph, get_stock_debate_graph
from .state import WeeklyReviewState, StockDebateState
# prepare_debate_data is in run_weekly_debate.py, imported directly there

__all__ = [
    "AnalystOutput",
    "StrategistOutput",
    "RiskOutput",
    "WeeklyReviewDecision",
    "PortfolioRating",
    "Action",
    "render_analyst_output",
    "render_strategist_output",
    "render_risk_output",
    "render_weekly_decision",
    "get_weekly_review_graph",
    "get_stock_debate_graph",
    "WeeklyReviewState",
    "StockDebateState",
]