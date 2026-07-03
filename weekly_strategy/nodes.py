"""
周复盘辩论节点定义
==================
对齐 TradingAgents agents/ 目录的节点工厂模式：

周复盘节点（5个）：
1. analyst_node — 数据分析师
2. strategist_node — 策略师
3. risk_node — 风控官
4. fund_manager_node — 基金经理（structured output）
5. complete_node — 完成节点

持仓辩论节点（参考 TradingAgents）：
- market_analyst_node / sentiment_analyst_node / news_analyst_node / fundamentals_analyst_node
- bull_researcher_node / bear_researcher_node
- research_manager_node（structured output）
- trader_node（structured output）
- aggressive_analyst_node / neutral_analyst_node / conservative_analyst_node
- portfolio_manager_node（structured output）
"""

from __future__ import annotations

import functools
import logging
from typing import Any, Dict, Optional, Type, TypeVar

from langchain_core.messages import AIMessage

from .schemas import (
    AnalystOutput,
    StrategistOutput,
    RiskOutput,
    WeeklyReviewDecision,
    TraderProposal,
    render_analyst_output,
    render_strategist_output,
    render_risk_output,
    render_weekly_decision,
    render_trader_proposal,
)
from .prompts import (
    build_analyst_prompt,
    build_strategist_prompt,
    build_risk_prompt,
    build_fund_manager_prompt,
)

logger = logging.getLogger("weekly_review.nodes")

T = TypeVar("T", bound=object)

# ── 统一 LLM 调用（复用选股工作流的 providers）────────────

def _get_llm_clients():
    """延迟加载 LLM 客户端（避免循环导入）"""
    import sys
    from pathlib import Path
    
    # 复用选股工作流的 providers
    workflowProviders = Path(__file__).parent.parent / "stock_selection_debate" / "providers.py"
    if str(workflowProviders) not in sys.path:
        sys.path.insert(0, str(Path(__file__).parent.parent))
    
    from stock_selection_debate.providers import call_llm, call_llm_structured
    return call_llm, call_llm_structured


def _structured_output(
    prompt: str,
    schema: Type[T],
    agent_name: str,
    model: str = "volcengine-plan/ark-code-latest",
    thinking_budget: Optional[int] = 20000,
) -> Optional[T]:
    """
    调用结构化输出，失败时 fallback 到自由文本解析
    
    对齐 TradingAgents agents/utils/structured.py 的 invoke_structured_or_freetext 模式
    """
    call_llm, _ = _get_llm_clients()
    
    try:
        # 使用 volcengine structured output
        result = call_llm(
            prompt=prompt,
            model=model,
            system="你是一个严谨的金融分析助手，输出格式必须是合法的JSON。",
            temperature=0.3,
            thinking_budget=thinking_budget,
        )
        
        # 尝试从响应文本解析 JSON
        import json, re
        raw = result.strip()
        
        # 去掉 markdown 代码块
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:] if len(lines) > 1 else lines)
            if raw.endswith("```"):
                raw = raw[:-3].strip()
        
        # 提取 JSON 对象
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > start:
            data = json.loads(raw[start:end])
            return schema(**data)
        
        logger.warning(f"[{agent_name}] 无法从响应中提取 JSON，尝试自由文本解析")
        return None
        
    except Exception as e:
        logger.warning(f"[{agent_name}] 结构化输出失败: {e}，fallback 到自由文本")
        return None


def _call_with_fallback(
    prompt: str,
    agent_name: str,
    render_fn: callable,
    schema: Optional[Type[T]] = None,
    model: str = "volcengine-plan/ark-code-latest",
) -> str:
    """
    调用 LLM，优先结构化输出，失败时自由文本
    
    对齐 TradingAgents invoke_structured_or_freetext 模式
    """
    call_llm, _ = _get_llm_clients()
    
    if schema is not None:
        result = _structured_output(prompt, schema, agent_name, model=model)
        if result is not None:
            return render_fn(result)
    
    # Fallback 到自由文本
    response = call_llm(
        prompt=prompt,
        model=model,
        system="你是一个严谨的金融分析助手。请按要求输出JSON格式。",
        temperature=0.3,
        thinking_budget=20000,
    )
    
    # 如果返回的是 markdown JSON，尝试渲染
    if schema is not None:
        try:
            import json, re
            raw = response.strip()
            if raw.startswith("```"):
                lines = raw.split("\n")
                raw = "\n".join(lines[1:] if len(lines) > 1 else lines)
                if raw.endswith("```"):
                    raw = raw[:-3].strip()
            
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(raw[start:end])
                return render_fn(schema(**data))
        except Exception as e:
            logger.warning(f"[{agent_name}] 自由文本 JSON 解析失败: {e}")
    
    return response


# ── 周复盘节点（5个角色）────────────────────────────────────

def create_analyst_node(llm=None):
    """
    数据分析师节点
    
    对齐 TradingAgents Bull Researcher 的数据驱动风格
    输入：week_data
    输出：AnalystOutput（structured）
    """
    def analyst_node(state: dict) -> dict:
        week_data = state["week_data"]
        analyst_raw = state.get("analyst_output", "")
        
        prompt = build_analyst_prompt(week_data, analyst_raw)
        output = _call_with_fallback(
            prompt=prompt,
            agent_name="Analyst",
            render_fn=render_analyst_output,
            schema=AnalystOutput,
        )
        
        logger.info(f"[Analyst] 输出: {output[:200]}...")
        
        new_state = {
            "analyst_output": output,
            "current_step": "analyst",
            "sender": "Analyst",
            "debate_history": state.get("debate_history", "") + f"\n\n=== Analyst ===\n{output}",
        }
        return new_state
    
    return analyst_node


def create_strategist_node(llm=None):
    """
    策略师节点
    
    对齐 TradingAgents Bear Researcher 的挑战风格
    输入：week_data + analyst_output
    输出：StrategistOutput（structured）
    """
    def strategist_node(state: dict) -> dict:
        week_data = state["week_data"]
        analyst_output = state.get("analyst_output", "")
        
        prompt = build_strategist_prompt(week_data, analyst_output)
        output = _call_with_fallback(
            prompt=prompt,
            agent_name="Strategist",
            render_fn=render_strategist_output,
            schema=StrategistOutput,
        )
        
        logger.info(f"[Strategist] 输出: {output[:200]}...")
        
        new_state = {
            "strategist_output": output,
            "current_step": "strategist",
            "sender": "Strategist",
            "debate_history": state.get("debate_history", "") + f"\n\n=== Strategist ===\n{output}",
        }
        return new_state
    
    return strategist_node


def create_risk_node(llm=None):
    """
    风控官节点
    
    对齐 TradingAgents 三风控分析师风格
    输入：week_data + analyst_output + strategist_output
    输出：RiskOutput（structured）
    """
    def risk_node(state: dict) -> dict:
        week_data = state["week_data"]
        analyst_output = state.get("analyst_output", "")
        strategist_output = state.get("strategist_output", "")
        
        prompt = build_risk_prompt(week_data, analyst_output, strategist_output)
        output = _call_with_fallback(
            prompt=prompt,
            agent_name="Risk Officer",
            render_fn=render_risk_output,
            schema=RiskOutput,
        )
        
        logger.info(f"[Risk Officer] 输出: {output[:200]}...")
        
        new_state = {
            "risk_output": output,
            "current_step": "risk",
            "sender": "Risk Officer",
            "debate_history": state.get("debate_history", "") + f"\n\n=== Risk Officer ===\n{output}",
        }
        return new_state
    
    return risk_node


def create_fund_manager_node(llm=None):
    """
    基金经理节点（最终决策）
    
    对齐 TradingAgents Portfolio Manager 的 structured output 模式
    输入：week_data + analyst + strategist + risk outputs
    输出：WeeklyReviewDecision（structured）
    """
    def fund_manager_node(state: dict) -> dict:
        week_data = state["week_data"]
        analyst_output = state.get("analyst_output", "")
        strategist_output = state.get("strategist_output", "")
        risk_output = state.get("risk_output", "")
        
        prompt = build_fund_manager_prompt(week_data, analyst_output, strategist_output, risk_output)
        
        call_llm, _ = _get_llm_clients()
        
        # 尝试结构化输出
        response = call_llm(
            prompt=prompt,
            model="volcengine-plan/ark-code-latest",
            system="你是一个严谨的基金经理。输出必须是合法的JSON格式，包含所有必填字段。",
            temperature=0.3,
            thinking_budget=20000,
        )
        
        # 解析 JSON
        import json
        raw = response.strip()
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:] if len(lines) > 1 else lines)
            if raw.endswith("```"):
                raw = raw[:-3].strip()
        
        start = raw.find("{")
        end = raw.rfind("}") + 1
        
        decision_obj = None
        if start >= 0 and end > start:
            try:
                data = json.loads(raw[start:end])
                decision_obj = WeeklyReviewDecision(**data)
                output = render_weekly_decision(decision_obj)
            except Exception as e:
                logger.warning(f"[Fund Manager] JSON解析失败: {e}，使用原始文本")
                output = raw
                decision_obj = {"raw": raw, "error": str(e)}
        else:
            output = raw
            decision_obj = {"raw": raw}
        
        logger.info(f"[Fund Manager] 决策: {output[:300]}...")
        
        new_state = {
            "final_decision": output,
            "final_decision_obj": decision_obj if isinstance(decision_obj, dict) else decision_obj.model_dump() if hasattr(decision_obj, 'model_dump') else {"rating": str(decision_obj)},
            "current_step": "complete",
            "sender": "Fund Manager",
            "debate_history": state.get("debate_history", "") + f"\n\n=== Fund Manager ===\n{output}",
        }
        return new_state
    
    return fund_manager_node


def create_complete_node():
    """完成节点"""
    def complete_node(state: dict) -> dict:
        logger.info("周复盘辩论完成")
        return {"current_step": "complete"}
    return complete_node


# ── 持仓辩论节点（参考 TradingAgents 单股票流程）────────────

def create_bull_researcher_node(llm=None):
    """
    多头研究员，对齐 TradingAgents Bull Researcher
    """
    def bull_node(state: dict) -> dict:
        investment_debate_state = state.get("investment_debate_state", {})
        history = investment_debate_state.get("history", "")
        bull_history = investment_debate_state.get("bull_history", "")
        current_response = investment_debate_state.get("current_response", "")
        
        market_report = state.get("market_report", "")
        sentiment_report = state.get("sentiment_report", "")
        news_report = state.get("news_report", "")
        fundamentals_report = state.get("fundamentals_report", "")
        
        prompt = f"""你是多头分析师，为持仓股票建立强有力的看多论点。

重点：
- 增长潜力：强调市场机会、收入预测、可扩展性
- 竞争优势：独特产品、强品牌、市场主导地位
- 积极指标：财务健康、行业趋势、最新正面新闻
- 反驳空头：用具体数据和合理推理反驳空头担忧

可用资源：
市场报告：{market_report}
舆情报告：{sentiment_report}
新闻报告：{news_report}
基本面报告：{fundamentals_report}
辩论历史：{history}
空方最新论点：{current_response}

用中文输出你的多头论点（2-4句），不要有其他格式。"""
        
        call_llm, _ = _get_llm_clients()
        response = call_llm(
            prompt=prompt,
            model="volcengine-plan/ark-code-latest",
            system="你是多头分析师，用中文输出。",
            temperature=0.3,
            thinking_budget=20000,
        )
        
        argument = f"Bull Analyst: {response}"
        
        new_investment_debate_state = {
            "history": history + "\n" + argument,
            "bull_history": bull_history + "\n" + argument,
            "bear_history": investment_debate_state.get("bear_history", ""),
            "current_response": argument,
            "count": investment_debate_state.get("count", 0) + 1,
        }
        
        return {"investment_debate_state": new_investment_debate_state}
    
    return bull_node


def create_bear_researcher_node(llm=None):
    """
    空头研究员，对齐 TradingAgents Bear Researcher
    """
    def bear_node(state: dict) -> dict:
        investment_debate_state = state.get("investment_debate_state", {})
        history = investment_debate_state.get("history", "")
        bear_history = investment_debate_state.get("bear_history", "")
        current_response = investment_debate_state.get("current_response", "")
        
        market_report = state.get("market_report", "")
        sentiment_report = state.get("sentiment_report", "")
        news_report = state.get("news_report", "")
        fundamentals_report = state.get("fundamentals_report", "")
        
        prompt = f"""你是空头分析师，为持仓股票建立强有力的看空论点。

重点：
- 风险因素：市场逆风、竞争威胁、估值过高
- 负面指标：财务问题、行业下行、技术破位
- 反驳多头：用具体数据挑战多头的乐观预期

可用资源：
市场报告：{market_report}
舆情报告：{sentiment_report}
新闻报告：{news_report}
基本面报告：{fundamentals_report}
辩论历史：{history}
多方最新论点：{current_response}

用中文输出你的空头论点（2-4句），不要有其他格式。"""
        
        call_llm, _ = _get_llm_clients()
        response = call_llm(
            prompt=prompt,
            model="volcengine-plan/ark-code-latest",
            system="你是空头分析师，用中文输出。",
            temperature=0.3,
            thinking_budget=20000,
        )
        
        argument = f"Bear Analyst: {response}"
        
        new_investment_debate_state = {
            "history": history + "\n" + argument,
            "bull_history": investment_debate_state.get("bull_history", ""),
            "bear_history": bear_history + "\n" + argument,
            "current_response": argument,
            "count": investment_debate_state.get("count", 0) + 1,
        }
        
        return {"investment_debate_state": new_investment_debate_state}
    
    return bear_node


def create_research_manager_node(llm=None):
    """
    研究总监，对齐 TradingAgents Research Manager（structured output）
    """
    def research_manager_node(state: dict) -> dict:
        investment_debate_state = state.get("investment_debate_state", {})
        history = investment_debate_state.get("history", "")
        
        prompt = f"""你是研究总监，评估多空辩论后给出投资建议。

辩论历史：
{history}

Rating Scale：
- Buy(加仓): 强烈看多
- Overweight(维持偏多): 看好
- Hold(维持): 中性
- Underweight(降仓): 谨慎
- Sell(清仓): 强烈看空

输出JSON格式：
{{"rating": "Buy/Overweight/Hold/Underweight/Sell", "rationale": "理由2-3句", "strategic_actions": "具体操作建议"}}

用中文输出合法JSON，不要有其他文字。"""
        
        call_llm, _ = _get_llm_clients()
        response = call_llm(
            prompt=prompt,
            model="volcengine-plan/ark-code-latest",
            system="你是研究总监，用中文输出合法JSON。",
            temperature=0.3,
            thinking_budget=20000,
        )
        
        # 解析 JSON
        import json
        raw = response.strip()
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:] if len(lines) > 1 else lines)
            if raw.endswith("```"):
                raw = raw[:-3].strip()
        
        start = raw.find("{")
        end = raw.rfind("}") + 1
        
        investment_plan = raw
        if start >= 0 and end > start:
            try:
                data = json.loads(raw[start:end])
                investment_plan = f"**Rating**: {data.get('rating', 'Hold')}\n\n**Rationale**: {data.get('rationale', '')}\n\n**Strategic Actions**: {data.get('strategic_actions', '')}"
            except:
                pass
        
        new_investment_debate_state = {
            "history": history,
            "bull_history": investment_debate_state.get("bull_history", ""),
            "bear_history": investment_debate_state.get("bear_history", ""),
            "current_response": investment_plan,
            "judge_decision": investment_plan,
            "count": investment_debate_state.get("count", 0),
        }
        
        return {
            "investment_debate_state": new_investment_debate_state,
            "investment_plan": investment_plan,
        }
    
    return research_manager_node


def create_trader_node(llm=None):
    """
    交易员，对齐 TradingAgents Trader（structured output）
    """
    def trader_node(state: dict) -> dict:
        stock_code = state.get("stock_code", "")
        stock_name = state.get("stock_name", "")
        investment_plan = state.get("investment_plan", "")
        
        buy_price = state.get("buy_price", 0)
        current_price = state.get("current_price", 0)
        pnl_pct = state.get("pnl_pct", 0) * 100
        
        prompt = f"""你是交易员，基于研究计划对持仓做出交易决策。

持仓信息：
股票：{stock_code} {stock_name}
买入价：{buy_price}
当前价：{current_price if current_price else '?'}
浮盈亏：{pnl_pct:+.1f}%

研究计划：
{investment_plan}

输出JSON格式：
{{"action": "BUY/HOLD/SELL", "reasoning": "理由2-4句", "stop_loss": 可选止损价, "position_sizing": 可选仓位}}

用中文输出合法JSON，不要有其他文字。"""
        
        call_llm, _ = _get_llm_clients()
        response = call_llm(
            prompt=prompt,
            model="volcengine-plan/ark-code-latest",
            system="你是交易员，用中文输出合法JSON。",
            temperature=0.3,
            thinking_budget=20000,
        )
        
        # 解析
        import json
        raw = response.strip()
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:] if len(lines) > 1 else lines)
            if raw.endswith("```"):
                raw = raw[:-3].strip()
        
        start = raw.find("{")
        end = raw.rfind("}") + 1
        
        trader_plan = raw
        if start >= 0 and end > start:
            try:
                data = json.loads(raw[start:end])
                action = data.get("action", "HOLD")
                reasoning = data.get("reasoning", "")
                trader_plan = f"**Action**: {action}\n\n**Reasoning**: {reasoning}\n\nFINAL TRANSACTION PROPOSAL: **{action.upper()}**"
                if data.get("stop_loss"):
                    trader_plan += f"\n\n**Stop Loss**: {data['stop_loss']}"
                if data.get("position_sizing"):
                    trader_plan += f"\n\n**Position Sizing**: {data['position_sizing']}"
            except:
                pass
        
        return {"trader_investment_plan": trader_plan}
    
    return trader_node


def create_aggressive_risk_node(llm=None):
    """激进风控分析师，对齐 TradingAgents Aggressive Debator"""
    def node(state: dict) -> dict:
        risk_debate_state = state.get("risk_debate_state", {})
        history = risk_debate_state.get("history", "")
        aggressive_history = risk_debate_state.get("aggressive_history", "")
        
        trader_plan = state.get("trader_investment_plan", "")
        pnl_pct = state.get("pnl_pct", 0) * 100
        
        prompt = f"""你是激进风控分析师，从激进角度评估交易建议。

持仓浮盈亏：{pnl_pct:+.1f}%
交易员建议：{trader_plan}
辩论历史：{history}

用中文输出你的激进风险评估（2-3句）。"""
        
        call_llm, _ = _get_llm_clients()
        response = call_llm(
            prompt=prompt,
            model="volcengine-plan/ark-code-latest",
            system="你是激进风控分析师，用中文输出。",
            temperature=0.3,
            thinking_budget=20000,
        )
        
        argument = f"Aggressive Analyst: {response}"
        
        new_risk_debate_state = {
            "aggressive_history": aggressive_history + "\n" + argument,
            "conservative_history": risk_debate_state.get("conservative_history", ""),
            "neutral_history": risk_debate_state.get("neutral_history", ""),
            "history": history + "\n" + argument,
            "latest_speaker": "Aggressive Analyst",
            "current_aggressive_response": argument,
            "count": risk_debate_state.get("count", 0) + 1,
        }
        
        return {"risk_debate_state": new_risk_debate_state}
    
    return node


def create_conservative_risk_node(llm=None):
    """保守风控分析师，对齐 TradingAgents Conservative Debator"""
    def node(state: dict) -> dict:
        risk_debate_state = state.get("risk_debate_state", {})
        history = risk_debate_state.get("history", "")
        conservative_history = risk_debate_state.get("conservative_history", "")
        
        trader_plan = state.get("trader_investment_plan", "")
        pnl_pct = state.get("pnl_pct", 0) * 100
        
        prompt = f"""你是保守风控分析师，从保守角度评估交易建议。

持仓浮盈亏：{pnl_pct:+.1f}%
交易员建议：{trader_plan}
辩论历史：{history}

用中文输出你的保守风险评估（2-3句）。"""
        
        call_llm, _ = _get_llm_clients()
        response = call_llm(
            prompt=prompt,
            model="volcengine-plan/ark-code-latest",
            system="你是保守风控分析师，用中文输出。",
            temperature=0.3,
            thinking_budget=20000,
        )
        
        argument = f"Conservative Analyst: {response}"
        
        new_risk_debate_state = {
            "aggressive_history": risk_debate_state.get("aggressive_history", ""),
            "conservative_history": conservative_history + "\n" + argument,
            "neutral_history": risk_debate_state.get("neutral_history", ""),
            "history": history + "\n" + argument,
            "latest_speaker": "Conservative Analyst",
            "current_conservative_response": argument,
            "count": risk_debate_state.get("count", 0) + 1,
        }
        
        return {"risk_debate_state": new_risk_debate_state}
    
    return node


def create_neutral_risk_node(llm=None):
    """中性风控分析师，对齐 TradingAgents Neutral Debator"""
    def node(state: dict) -> dict:
        risk_debate_state = state.get("risk_debate_state", {})
        history = risk_debate_state.get("history", "")
        neutral_history = risk_debate_state.get("neutral_history", "")
        
        trader_plan = state.get("trader_investment_plan", "")
        pnl_pct = state.get("pnl_pct", 0) * 100
        
        prompt = f"""你是中性风控分析师，从中立角度评估交易建议。

持仓浮盈亏：{pnl_pct:+.1f}%
交易员建议：{trader_plan}
辩论历史：{history}

用中文输出你的中性风险评估（2-3句）。"""
        
        call_llm, _ = _get_llm_clients()
        response = call_llm(
            prompt=prompt,
            model="volcengine-plan/ark-code-latest",
            system="你是中性风控分析师，用中文输出。",
            temperature=0.3,
            thinking_budget=20000,
        )
        
        argument = f"Neutral Analyst: {response}"
        
        new_risk_debate_state = {
            "aggressive_history": risk_debate_state.get("aggressive_history", ""),
            "conservative_history": risk_debate_state.get("conservative_history", ""),
            "neutral_history": neutral_history + "\n" + argument,
            "history": history + "\n" + argument,
            "latest_speaker": "Neutral Analyst",
            "current_neutral_response": argument,
            "count": risk_debate_state.get("count", 0) + 1,
        }
        
        return {"risk_debate_state": new_risk_debate_state}
    
    return node


def create_portfolio_manager_stock_node(llm=None):
    """
    持仓组合经理，对齐 TradingAgents Portfolio Manager（structured output）
    """
    def node(state: dict) -> dict:
        risk_debate_state = state.get("risk_debate_state", {})
        history = risk_debate_state.get("history", "")
        
        investment_plan = state.get("investment_plan", "")
        trader_plan = state.get("trader_investment_plan", "")
        
        stock_code = state.get("stock_code", "")
        stock_name = state.get("stock_name", "")
        
        prompt = f"""你是组合经理，综合风险辩论后给出最终持仓决策。

持仓：{stock_code} {stock_name}
研究计划：{investment_plan}
交易员建议：{trader_plan}
风险辩论历史：{history}

Rating Scale：Buy(加仓)/Hold(维持)/Sell(清仓)

输出JSON格式：
{{"rating": "Buy/Hold/Sell", "executive_summary": "执行摘要2-3句", "investment_thesis": "投资论点", "confidence": "高/中/低"}}

用中文输出合法JSON，不要有其他文字。"""
        
        call_llm, _ = _get_llm_clients()
        response = call_llm(
            prompt=prompt,
            model="volcengine-plan/ark-code-latest",
            system="你是组合经理，用中文输出合法JSON。",
            temperature=0.3,
            thinking_budget=20000,
        )
        
        # 解析
        import json
        raw = response.strip()
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:] if len(lines) > 1 else lines)
            if raw.endswith("```"):
                raw = raw[:-3].strip()
        
        start = raw.find("{")
        end = raw.rfind("}") + 1
        
        final_decision = raw
        if start >= 0 and end > start:
            try:
                data = json.loads(raw[start:end])
                final_decision = f"**Rating**: {data.get('rating', 'Hold')}\n\n**Executive Summary**: {data.get('executive_summary', '')}\n\n**Investment Thesis**: {data.get('investment_thesis', '')}\n\n**Confidence**: {data.get('confidence', '中')}"
            except:
                pass
        
        new_risk_debate_state = {
            "aggressive_history": risk_debate_state.get("aggressive_history", ""),
            "conservative_history": risk_debate_state.get("conservative_history", ""),
            "neutral_history": risk_debate_state.get("neutral_history", ""),
            "history": history,
            "latest_speaker": "Portfolio Manager",
            "judge_decision": final_decision,
            "count": risk_debate_state.get("count", 0),
        }
        
        return {
            "risk_debate_state": new_risk_debate_state,
            "final_trade_decision": final_decision,
        }
    
    return node