"""
TradingAgents 辩论核心模块 v2
=============================
论文对齐: 分析师 → Bull/Bear研究员(辩论) → 三方风控(辩论) → 基金经理(5档决策)
"""
from .debate_flow import DebateFlow
from .agents import (
    AnalystTeam, BullResearcher, BearResearcher, ResearchManager,
    AggressiveRisk, ConservativeRisk, NeutralRisk, FundManager,
    AnalystAgent, ResearcherAgent, RiskAgent, DecisionAgent,  # 旧接口兼容
    apply_portfolio_constraint, call_llm,
)

__all__ = [
    "DebateFlow",
    "AnalystTeam", "BullResearcher", "BearResearcher", "ResearchManager",
    "AggressiveRisk", "ConservativeRisk", "NeutralRisk", "FundManager",
    "AnalystAgent", "ResearcherAgent", "RiskAgent", "DecisionAgent",
    "apply_portfolio_constraint", "call_llm",
]
