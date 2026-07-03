"""
周复盘辩论 LangGraph 状态机
==========================
对齐 TradingAgents graph/setup.py 的架构：

两种辩论图：
1. weekly_review_graph — 周参数决策（5角色串行）
2. stock_debate_graph — 持仓决策（完整多智能体辩论）

节点顺序：
  START → Analyst → Strategist → Risk → Fund Manager → END

条件路由：
  - 分析节点无条件向前（串行）
  - 持仓辩论走 conditional_edges（条件循环）
"""

from langgraph.graph import END, START, StateGraph
from typing import Optional, List

from .state import WeeklyReviewState, StockDebateState, InvestDebateState, RiskDebateState
from .nodes import (
    create_analyst_node,
    create_strategist_node,
    create_risk_node,
    create_fund_manager_node,
    create_complete_node,
    # 持仓辩论节点
    create_bull_researcher_node,
    create_bear_researcher_node,
    create_research_manager_node,
    create_trader_node,
    create_aggressive_risk_node,
    create_conservative_risk_node,
    create_neutral_risk_node,
    create_portfolio_manager_stock_node,
)


# ── 周复盘辩论图（主流程）────────────────────────────────────

def build_weekly_review_graph():
    """
    周复盘辩论图：5角色串行顺序执行
    
    对齐 TradingAgents setup_graph 模式：
    - 节点定义 → add_node
    - 边定义 → add_edge / add_conditional_edges
    - 编译 → workflow.compile()
    """
    workflow = StateGraph(WeeklyReviewState)
    
    # 添加节点
    workflow.add_node("Analyst", create_analyst_node())
    workflow.add_node("Strategist", create_strategist_node())
    workflow.add_node("Risk Officer", create_risk_node())
    workflow.add_node("Fund Manager", create_fund_manager_node())
    workflow.add_node("Complete", create_complete_node())
    
    # 定义边
    workflow.add_edge(START, "Analyst")
    workflow.add_edge("Analyst", "Strategist")
    workflow.add_edge("Strategist", "Risk Officer")
    workflow.add_edge("Risk Officer", "Fund Manager")
    workflow.add_edge("Fund Manager", "Complete")
    workflow.add_edge("Complete", END)
    
    return workflow.compile()


# ── 持仓辩论图（完整多智能体流程）────────────────────────────

def build_stock_debate_graph(max_debate_rounds: int = 1, max_risk_rounds: int = 1):
    """
    持仓辩论图：对齐 TradingAgents 完整流程
    
    流程：
    分析师并行（市场/舆情/新闻/基本面）
    → Bull/Bear 辩论（Research Manager 裁决）
    → Trader 提案
    → 三风控辩论（Portfolio Manager 决策）
    
    条件路由（来自 conditional_logic.py）：
    - Bull/Bear: 轮次 < 2*max_debate_rounds → 继续辩论，否则 → Research Manager
    - 三风控: 轮次 < 3*max_risk_rounds → 继续轮转，否则 → Portfolio Manager
    """
    workflow = StateGraph(StockDebateState)
    
    # 研究团队
    workflow.add_node("Bull Researcher", create_bull_researcher_node())
    workflow.add_node("Bear Researcher", create_bear_researcher_node())
    workflow.add_node("Research Manager", create_research_manager_node())
    
    # 交易员
    workflow.add_node("Trader", create_trader_node())
    
    # 风控团队
    workflow.add_node("Aggressive Analyst", create_aggressive_risk_node())
    workflow.add_node("Conservative Analyst", create_conservative_risk_node())
    workflow.add_node("Neutral Analyst", create_neutral_risk_node())
    workflow.add_node("Portfolio Manager", create_portfolio_manager_stock_node())
    
    # ── 条件路由函数 ──────────────────────────────────────
    
    def should_continue_debate(state: StockDebateState) -> str:
        """投资辩论：Bull/Bear 轮转"""
        count = state.get("investment_debate_state", {}).get("count", 0)
        current_response = state.get("investment_debate_state", {}).get("current_response", "")
        
        if count >= 2 * max_debate_rounds:
            return "Research Manager"
        if current_response.startswith("Bull"):
            return "Bear Researcher"
        return "Bull Researcher"
    
    def should_continue_risk(state: StockDebateState) -> str:
        """风险辩论：三风控分析师轮转"""
        risk_state = state.get("risk_debate_state", {})
        count = risk_state.get("count", 0)
        latest = risk_state.get("latest_speaker", "")
        
        if count >= 3 * max_risk_rounds:
            return "Portfolio Manager"
        if latest == "Aggressive Analyst":
            return "Conservative Analyst"
        if latest == "Conservative Analyst":
            return "Neutral Analyst"
        return "Aggressive Analyst"
    
    # ── 边定义 ─────────────────────────────────────────────
    
    # 入口点
    workflow.add_edge(START, "Bull Researcher")
    
    # 投资辩论条件边
    workflow.add_conditional_edges(
        "Bull Researcher",
        should_continue_debate,
        {"Bear Researcher": "Bear Researcher", "Research Manager": "Research Manager"},
    )
    workflow.add_conditional_edges(
        "Bear Researcher",
        should_continue_debate,
        {"Bull Researcher": "Bull Researcher", "Research Manager": "Research Manager"},
    )
    
    # Research Manager → Trader（固定边）
    workflow.add_edge("Research Manager", "Trader")
    
    # 风险辩论条件边
    workflow.add_conditional_edges(
        "Aggressive Analyst",
        should_continue_risk,
        {"Conservative Analyst": "Conservative Analyst", "Portfolio Manager": "Portfolio Manager"},
    )
    workflow.add_conditional_edges(
        "Conservative Analyst",
        should_continue_risk,
        {"Neutral Analyst": "Neutral Analyst", "Portfolio Manager": "Portfolio Manager"},
    )
    workflow.add_conditional_edges(
        "Neutral Analyst",
        should_continue_risk,
        {"Aggressive Analyst": "Aggressive Analyst", "Portfolio Manager": "Portfolio Manager"},
    )
    
    workflow.add_edge("Portfolio Manager", END)
    
    return workflow.compile()


# ── 预编译图实例（单例）────────────────────────────────────

_weekly_review_graph = None
_stock_debate_graph = None


def get_weekly_review_graph():
    """获取预编译的周复盘辩论图（单例）"""
    global _weekly_review_graph
    if _weekly_review_graph is None:
        _weekly_review_graph = build_weekly_review_graph()
    return _weekly_review_graph


def get_stock_debate_graph(max_debate_rounds: int = 1, max_risk_rounds: int = 1):
    """获取预编译的持仓辩论图"""
    global _stock_debate_graph
    if _stock_debate_graph is None:
        _stock_debate_graph = build_stock_debate_graph(max_debate_rounds, max_risk_rounds)
    return _stock_debate_graph