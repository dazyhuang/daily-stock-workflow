"""
选股辩论模块
================
完全版辩论流程，替代原有的 LLM 打分

文件结构：
├── __init__.py
├── kb_loader.py      # 知识库加载
├── data_fetcher.py   # xqshare K线获取
├── debate_engine.py  # 辩论流程（5角色×4步）
├── judge.py          # 裁判判决
└── feishu_card.py   # 飞书卡片
"""

from .debate_engine import StockDebateEngine, run_debate

__all__ = ["StockDebateEngine", "run_debate"]
