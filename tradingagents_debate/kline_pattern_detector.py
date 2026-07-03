"""
K线形态检测 + 知识库动态检索
=================================
检测候选股的K线形态，从知识库中检索相关技术分析段落，
注入到 Bull/Bear 辩论 prompt 中。
"""

import os
import re
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger("kline_pattern_detector")

# ── 知识库根目录 ──────────────────────────────────────────
KB_ROOT = Path(__file__).parent.parent.parent / "knowledge-base"
CANDLESTICK_KB = KB_ROOT / "candlestick-charting"
TECH_KB = KB_ROOT / "stock-trend-technical-analysis"

# 形态 → 知识库文件映射（按优先级）
PATTERN_FILE_MAP = {
    # 单根蜡烛形态
    "锤子线":    (CANDLESTICK_KB / "core-concepts.md",  "看涨（底部）"),
    "吊颈线":    (CANDLESTICK_KB / "core-concepts.md",  "看跌（顶部）"),
    "流星线":    (CANDLESTICK_KB / "core-concepts.md",  "看跌（顶部）"),
    "十字星":    (CANDLESTICK_KB / "core-concepts.md",  "犹豫/反转信号"),
    "纺锤线":    (CANDLESTICK_KB / "core-concepts.md",  "多空争夺"),
    "高浪线":    (CANDLESTICK_KB / "core-concepts.md",  "趋势即将改变"),
    # 组合形态
    "吞没形态":  (CANDLESTICK_KB / "practical-applications.md", "反转信号"),
    "乌云盖顶":  (CANDLESTICK_KB / "practical-applications.md", "看跌"),
    "刺穿形态":  (CANDLESTICK_KB / "practical-applications.md", "看涨"),
    "启明星":    (CANDLESTICK_KB / "practical-applications.md", "看涨底部"),
    "黄昏星":    (CANDLESTICK_KB / "practical-applications.md", "看跌顶部"),
    # 反转形态
    "头肩顶":    (TECH_KB / "反转形态.md",              "顶部反转"),
    "头肩底":    (TECH_KB / "反转形态.md",              "底部反转"),
    "双顶":      (TECH_KB / "反转形态.md",              "顶部反转"),
    "双底":      (TECH_KB / "反转形态.md",              "底部反转"),
    "圆顶":      (TECH_KB / "反转形态.md",              "顶部反转"),
    "圆底":      (TECH_KB / "反转形态.md",              "底部反转"),
    "V形反转":   (TECH_KB / "反转形态.md",              "剧烈反转"),
    "扩散形态":   (TECH_KB / "反转形态.md",              "顶部反转"),
    # 整理形态
    "旗形":      (TECH_KB / "整理形态.md",              "持续形态"),
    "三角旗形":   (TECH_KB / "整理形态.md",              "持续形态"),
    "楔形":      (TECH_KB / "整理形态.md",              "整理/反转"),
    "矩形":      (TECH_KB / "整理形态.md",              "持续形态"),
    "对称三角形": (TECH_KB / "整理形态.md",              "持续形态"),
    "上升三角形": (TECH_KB / "整理形态.md",              "向上突破"),
    "下降三角形": (TECH_KB / "整理形态.md",              "向下突破"),
    # 成交量
    "放量上涨":  (TECH_KB / "成交量分析.md",             "量价配合"),
    "缩量下跌":  (TECH_KB / "成交量分析.md",             "趋势持续"),
    "放量滞涨":  (TECH_KB / "成交量分析.md",             "顶部信号"),
    "底背离":    (TECH_KB / "核心概念.md",              "反转信号"),
    "顶背离":    (TECH_KB / "核心概念.md",              "反转信号"),
}


def detect_tech_indicator_patterns(tech_data: Dict) -> List[Dict[str, Any]]:
    """
    从技术指标数据（RSI/MA/MACD/量比/持仓数据）中检测形态

    Args:
        tech_data: {
            rsi, ma_trend, ma5, ma10, ma20, vol_ratio, macd, macd_golden_cross,
            pnl_pct, current_price, cost, ...
        }
    Returns:
        [{"pattern": "MACD金叉", "signal": "看涨信号", "confidence": "高", "detail": "..."}]
    """
    detected = []
    if not tech_data:
        return detected

    # ── RSI 超买超卖 ──────────────────────────────────
    rsi = tech_data.get("rsi")
    if rsi is not None:
        if rsi > 80:
            detected.append({
                "pattern": "RSI超买", "signal": "超买警告（>80）",
                "confidence": "高", "detail": f"RSI={rsi:.1f}，处于极强状态，可能面临回调风险",
                "type": "indicator", "bullish": False
            })
        elif 70 < rsi <= 80:
            detected.append({
                "pattern": "RSI超买", "signal": "超买区域（70-80）",
                "confidence": "中", "detail": f"RSI={rsi:.1f}，偏热，需观察能否突破",
                "type": "indicator", "bullish": False
            })
        elif rsi < 20:
            detected.append({
                "pattern": "RSI超卖", "signal": "超卖反弹（<20）",
                "confidence": "高", "detail": f"RSI={rsi:.1f}，超卖严重，可能触发反弹",
                "type": "indicator", "bullish": True
            })
        elif 20 <= rsi < 30:
            detected.append({
                "pattern": "RSI超卖", "signal": "超卖区域（20-30）",
                "confidence": "中", "detail": f"RSI={rsi:.1f}，偏低，关注企稳信号",
                "type": "indicator", "bullish": True
            })

    # ── 均线趋势 ──────────────────────────────────────
    ma_trend = tech_data.get("ma_trend")
    if ma_trend == "多头":
        ma5 = tech_data.get("ma5")
        ma10 = tech_data.get("ma10")
        ma20 = tech_data.get("ma20")
        ma10_str = f"{ma10:.2f}" if ma10 is not None else "N/A"
        detected.append({
            "pattern": "均线多头排列", "signal": "看涨趋势",
            "confidence": "高", "detail": f"MA5>MA10>MA20，多头排列（MA5={ma5}, MA10={ma10_str}, MA20={ma20}）",
            "type": "indicator", "bullish": True
        })
    elif ma_trend == "空头":
        ma5 = tech_data.get("ma5")
        ma10 = tech_data.get("ma10")
        ma20 = tech_data.get("ma20")
        ma10_str = f"{ma10:.2f}" if ma10 is not None else "N/A"
        detected.append({
            "pattern": "均线空头排列", "signal": "看跌趋势",
            "confidence": "高", "detail": f"MA5<MA10<MA20，空头排列（MA5={ma5}, MA10={ma10_str}, MA20={ma20}）",
            "type": "indicator", "bullish": False
        })

    # ── MACD 金叉死叉 ────────────────────────────────
    macd_golden_cross = tech_data.get("macd_golden_cross")
    if macd_golden_cross is True:
        macd_val = tech_data.get("macd", 0)
        detected.append({
            "pattern": "MACD金叉", "signal": "看涨信号",
            "confidence": "高", "detail": f"MACD={macd_val:.4f}，MACD线向上穿越信号线，中期转强",
            "type": "indicator", "bullish": True
        })
    elif macd_golden_cross is False:
        macd_val = tech_data.get("macd", 0)
        detected.append({
            "pattern": "MACD死叉", "signal": "看跌信号",
            "confidence": "高", "detail": f"MACD={macd_val:.4f}，MACD线向下穿越信号线，中期转弱",
            "type": "indicator", "bullish": False
        })

    # ── MACD 柱方向 ─────────────────────────────────
    macd = tech_data.get("macd")
    if macd is not None:
        if macd > 0:
            detected.append({
                "pattern": "MACD正值", "signal": "多头动能",
                "confidence": "低", "detail": f"MACD柱={macd:.4f}>0，多方主导",
                "type": "indicator", "bullish": True
            })
        elif macd < 0:
            detected.append({
                "pattern": "MACD负值", "signal": "空头动能",
                "confidence": "低", "detail": f"MACD柱={macd:.4f}<0，空方主导",
                "type": "indicator", "bullish": False
            })

    # ── 量比异常 ──────────────────────────────────────
    vol_ratio = tech_data.get("vol_ratio")
    if vol_ratio is not None:
        if vol_ratio > 3.0:
            detected.append({
                "pattern": "天量放量", "signal": "异动警示",
                "confidence": "中", "detail": f"量比={vol_ratio:.1f}x，远超5日均量，警惕短期变盘",
                "type": "volume", "bullish": None
            })
        elif vol_ratio > 1.8:
            detected.append({
                "pattern": "放量上涨", "signal": "量价配合",
                "confidence": "高", "detail": f"量比={vol_ratio:.1f}x，量能健康配合价格走势",
                "type": "volume", "bullish": True
            })
        elif vol_ratio < 0.4:
            detected.append({
                "pattern": "极度缩量", "signal": "观望等待突破",
                "confidence": "中", "detail": f"量比={vol_ratio:.1f}x，流动性极度萎缩，可能酝酿突破",
                "type": "volume", "bullish": None
            })

    return detected


def detect_candlestick_patterns(closes: List[float], opens: List[float] = None,
                                  highs: List[float] = None, lows: List[float] = None,
                                  vols: List[float] = None) -> List[Dict[str, Any]]:
    """
    从 OHLCV 数据检测蜡烛图形态
    
    Args:
        closes: 收盘价列表（最近的在最后）
        opens/highs/lows/vols: 对应列表（可选，不提供时用近似）
        vols: 成交量列表
    
    Returns:
        [{"pattern": "锤子线", "position": 2, "signal": "看涨", "confidence": "高", "detail": "..."}]
    """
    if len(closes) < 5:
        return []
    
    # 如果没提供 OHLC，用收盘价近似
    n = len(closes)
    if opens is None:
        opens = closes[:-1] + [closes[-1]]
    if highs is None:
        highs = [c * 1.005 for c in closes]  # 粗略近似
    if lows is None:
        lows = [c * 0.995 for c in closes]
    if vols is None:
        vols = [1.0] * n

    detected = []

    for i in range(2, n):
        O, H, L, C = opens[i], highs[i], lows[i], closes[i]
        prev_C = closes[i - 1]
        prev_O = opens[i - 1] if i < len(opens) else closes[i - 1]
        
        body = abs(C - O)
        upper_shadow = H - max(O, C)
        lower_shadow = min(O, C) - L
        body_size = body
        avg_vol = sum(vols[max(0,i-5):i]) / min(5, i) if i > 0 else 1.0
        vol_ratio = vols[i] / avg_vol if avg_vol > 0 else 1.0

        # ── 单根蜡烛形态 ──────────────────────────────────
        
        # 锤子线 / 吊颈线：下影线 ≥ 2倍实体，上影线 ≤ 0.5倍实体
        if lower_shadow >= 2 * body_size and upper_shadow <= 0.5 * body_size and body_size > 0:
            is_bottom = C > O  # 阳线 → 锤子线（底部）
            pattern = "锤子线" if is_bottom else "吊颈线"
            if is_bottom:
                signal = "看涨（底部反转）"
            else:
                signal = "看跌（顶部警告）"
            confidence = "高" if lower_shadow >= 3 * body_size else "中"
            detail = (f"下影线是实体{lower_shadow/body_size:.1f}倍，"
                      f"{'阳线' if is_bottom else '阴线'}，{'底部' if is_bottom else '顶部'}形态，"
                      f"量比={vol_ratio:.1f}x{'（放量）' if vol_ratio > 1.5 else '（缩量）'}")
            detected.append({
                "pattern": pattern, "position": i, "signal": signal,
                "confidence": confidence, "detail": detail,
                "type": "candlestick", "bullish": is_bottom
            })

        # 流星线：上影线很长，下影线很短，出现在上涨后
        if upper_shadow >= 2 * body_size and lower_shadow <= 0.5 * body_size and body_size > 0:
            is_top = C < O  # 阴线 → 流星线
            if is_top:
                pattern = "流星线"
                signal = "看跌（顶部反转）"
                confidence = "高" if upper_shadow >= 3 * body_size else "中"
                detail = (f"上影线是实体{upper_shadow/body_size:.1f}倍，阴线，顶部长上影线，"
                          f"量比={vol_ratio:.1f}x{'（放量）' if vol_ratio > 1.5 else '（缩量）'}")
                detected.append({
                    "pattern": pattern, "position": i, "signal": signal,
                    "confidence": confidence, "detail": detail,
                    "type": "candlestick", "bullish": False
                })

        # 十字星：开盘≈收盘，实体极小
        if body_size < 0.005 * C and (H - L) > body_size * 2:
            # 长影线十字星 = 高浪线
            if (upper_shadow > body_size * 3 and lower_shadow > body_size * 3):
                detected.append({
                    "pattern": "高浪线", "position": i, "signal": "趋势即将改变",
                    "confidence": "中", "detail": f"上下影线均很长，多空剧烈争夺，量比={vol_ratio:.1f}x",
                    "type": "candlestick", "bullish": None
                })
            else:
                detected.append({
                    "pattern": "十字星", "position": i, "signal": "犹豫/十字路口",
                    "confidence": "中", "detail": f"开盘≈收盘，实体极小，市场犹豫，量比={vol_ratio:.1f}x",
                    "type": "candlestick", "bullish": None
                })

        # 纺锤线：实体小，影线正常
        if 0.5 * (H - L) > body_size > 0 and body_size > 0.003 * C:
            detected.append({
                "pattern": "纺锤线", "position": i, "signal": "多空争夺（需确认）",
                "confidence": "低", "detail": f"实体小，上下影线正常，多空拉锯，量比={vol_ratio:.1f}x",
                "type": "candlestick", "bullish": None
            })

        # 大阳线（长白实体）
        if C > O and body_size > 0.03 * C:
            bullish = C > O
            if vol_ratio > 1.5:
                detected.append({
                    "pattern": "放量长阳", "position": i, "signal": "多头强劲",
                    "confidence": "中", "detail": f"实体{C/O:.1%}涨幅，放量{vol_ratio:.1f}x，动力强劲",
                    "type": "candlestick", "bullish": True
                })

        # 大阴线（长黑实体）
        if O > C and body_size > 0.03 * C:
            if vol_ratio > 1.5:
                detected.append({
                    "pattern": "放量长阴", "position": i, "signal": "空头强劲",
                    "confidence": "中", "detail": f"实体{O/C:.1%}跌幅，放量{vol_ratio:.1f}x，下跌动力强",
                    "type": "candlestick", "bullish": False
                })

        # ── 二根蜡烛组合形态 ────────────────────────────────
        if i >= 1:
            O1, C1 = prev_O, prev_C
            O2, C2 = O, C
            body1 = abs(C1 - O1)
            body2 = abs(C2 - O2)
            
            # 吞没形态（包孕）
            if body2 > body1:
                if C1 < O1 and C2 > O2 and C2 >= O1 and C1 >= O2:
                    detected.append({
                        "pattern": "多头吞噬", "position": i, "signal": "看涨（底部反转）",
                        "confidence": "高" if body2 > 1.5 * body1 else "中",
                        "detail": f"阴线后出现大阳线，实体完全包裹前阴，量比={vol_ratio:.1f}x",
                        "type": "candlestick", "bullish": True
                    })
                if C1 > O1 and C2 < O2 and O2 >= C1 and O1 >= C2:
                    detected.append({
                        "pattern": "空头吞噬", "position": i, "signal": "看跌（顶部反转）",
                        "confidence": "高" if body2 > 1.5 * body1 else "中",
                        "detail": f"阳线后出现大阴线，实体完全包裹前阳，量比={vol_ratio:.1f}x",
                        "type": "candlestick", "bullish": False
                    })
            
            # 乌云盖顶 / 刺穿形态
            if body1 > 0.02 * C1 and body2 > 0.02 * C2:
                mid1 = (O1 + C1) / 2
                if C1 > O1 and C2 < O2 and C2 < mid1 and O2 <= C1:
                    detected.append({
                        "pattern": "乌云盖顶", "position": i, "signal": "看跌（顶部反转）",
                        "confidence": "中",
                        "detail": f"阳线后阴线切入实体中点以下，{C2:.2f}<{mid1:.2f}，量比={vol_ratio:.1f}x",
                        "type": "candlestick", "bullish": False
                    })
                if C1 < O1 and C2 > O2 and C2 > mid1 and O2 >= C1:
                    detected.append({
                        "pattern": "刺穿形态", "position": i, "signal": "看涨（底部反转）",
                        "confidence": "中",
                        "detail": f"阴线后阳线切入实体中点以上，{C2:.2f}>{mid1:.2f}，量比={vol_ratio:.1f}x",
                        "type": "candlestick", "bullish": True
                    })

    # ── 成交量形态 ──────────────────────────────────────
    if vols and len(vols) >= 5:
        avg_vol_5 = sum(vols[-5:]) / 5
        latest_vol = vols[-1]
        vol_ratio_global = latest_vol / avg_vol_5 if avg_vol_5 > 0 else 1.0
        price_change = (closes[-1] - closes[-2]) / closes[-2] if len(closes) >= 2 else 0
        
        if vol_ratio_global > 1.8 and price_change > 0.01:
            detected.append({
                "pattern": "放量上涨", "position": -1, "signal": "量价配合，多头健康",
                "confidence": "高", "detail": f"量比={vol_ratio_global:.1f}x，涨幅={price_change:.1%}",
                "type": "volume", "bullish": True
            })
        elif vol_ratio_global > 1.8 and price_change < -0.01:
            detected.append({
                "pattern": "放量下跌", "position": -1, "signal": "空头主导，下跌动力强",
                "confidence": "中", "detail": f"量比={vol_ratio_global:.1f}x，跌幅={price_change:.1%}",
                "type": "volume", "bullish": False
            })
        elif vol_ratio_global < 0.5 and abs(price_change) < 0.005:
            detected.append({
                "pattern": "缩量横盘", "position": -1, "signal": "观望，等待突破",
                "confidence": "中", "detail": f"量比={vol_ratio_global:.1f}x，波动极小",
                "type": "volume", "bullish": None
            })

    # ── 技术指标背离（简化版）─────────────────────────────
    if len(closes) >= 20:
        # 价格创新高但RSI没有 → 顶背离
        price_high = max(closes[:-1]) if len(closes) > 1 else closes[0]
        if closes[-1] > price_high * 0.98:  # 接近新高
            # 简化：用近期涨跌判断
            recent_return = (closes[-1] - closes[-10]) / closes[-10] if len(closes) >= 10 else 0
            if recent_return < 0.02:  # 涨幅很小
                detected.append({
                    "pattern": "顶背离", "position": -1, "signal": "看跌（RSI背离）",
                    "confidence": "中", "detail": f"价格接近{price_high:.2f}新高，但近期涨幅仅{recent_return:.1%}",
                    "type": "divergence", "bullish": False
                })
        # 价格创新低但RSI没有 → 底背离
        price_low = min(closes[:-1]) if len(closes) > 1 else closes[0]
        if closes[-1] < price_low * 1.02:  # 接近新低
            recent_return = (closes[-1] - closes[-10]) / closes[-10] if len(closes) >= 10 else 0
            if recent_return > -0.02:  # 跌幅很小
                detected.append({
                    "pattern": "底背离", "position": -1, "signal": "看涨（RSI底背离）",
                    "confidence": "中", "detail": f"价格接近{price_low:.2f}新低，但近期未创新低，跌幅{recent_return:.1%}",
                    "type": "divergence", "bullish": True
                })

    # 去重：同类型保留置信度最高的
    seen = {}
    for d in sorted(detected, key=lambda x: {"高": 0, "中": 1, "低": 2}[x["confidence"]]):
        key = d["pattern"]
        if key not in seen:
            seen[key] = d
    return list(seen.values())


def _extract_section_from_md(md_path: Path, section_hint: str = None) -> str:
    """从 markdown 文件中提取相关段落"""
    if not md_path.exists():
        return ""
    
    try:
        content = md_path.read_text(encoding="utf-8")
        
        if section_hint:
            # 找最相关的段落（按标题匹配）
            lines = content.split("\n")
            best_section = []
            in_target = False
            target_indent = 99
            
            for line in lines:
                stripped = line.strip()
                # 检测标题行
                if stripped.startswith("#"):
                    if in_target and len(best_section) > 0:
                        break  # 到了下一个章节，停止
                    # 检查是否匹配标题关键词
                    hint_words = section_hint.split("（")[0]  # "锤子线" from "看涨（底部）"
                    if any(w in stripped for w in [hint_words, section_hint]):
                        in_target = True
                        target_indent = len(line) - len(line.lstrip())
                        best_section = [line]
                    else:
                        in_target = False
                elif in_target:
                    # 同一标题下继续（缩进增加的子内容）
                    if line.strip():
                        curr_indent = len(line) - len(line.lstrip())
                        if curr_indent > target_indent:
                            best_section.append(line)
                        elif curr_indent <= target_indent and stripped:
                            break  # 新段落开始
                            
            if best_section:
                text = "\n".join(best_section)
                return text[:1500]  # 限制长度
        
        # Fallback: 读前500字
        return content[:1000]
    except Exception as e:
        logger.warning(f"读取知识库失败 {md_path}: {e}")
        return ""


def retrieve_knowledge_for_patterns(detected_patterns: List[Dict]) -> str:
    """
    根据检测到的K线形态+技术指标形态，从知识库中检索相关段落
    返回格式化的知识注入文本
    """
    if not detected_patterns:
        return ""

    sections = []

    # 合并两个映射（蜡烛图形态 + 趋势/动量/系统形态）
    for p in detected_patterns:
        pattern = p["pattern"]
        # 先查蜡烛图知识库
        kb_info = PATTERN_FILE_MAP.get(pattern)
        if kb_info:
            kb_path, signal_hint = kb_info
            section = _extract_section_from_md(kb_path, signal_hint)
            if section:
                sections.append(f"【{pattern}】{signal_hint}\n{section[:600]}")
                continue

        # 再查扩展知识库（趋势/动量/交易系统）
        kb_info2 = TREND_PATTERN_MAP.get(pattern)
        if kb_info2:
            kb_path2, signal_hint2 = kb_info2
            section2 = _extract_section_from_md(kb_path2, signal_hint2)
            if section2:
                sections.append(f"【{pattern}】{signal_hint2}\n{section2[:600]}")

    if not sections:
        return ""

    knowledge_text = "\n\n─── 技术分析知识库检索结果 ───\n"
    knowledge_text += f"（基于形态检测，共{len(sections)}条相关知识）\n"
    knowledge_text += "\n".join(sections)

    return knowledge_text[:4000]  # 最多4000字


def build_kline_context(stock_code: str, tech_data: Dict, 
                         ohlcv_data: Dict = None) -> str:
    """
    构建K线上下文，包含：
    1. 检测到的形态 + 知识库检索结果
    2. 关键技术指标
    """
    lines = [f"【K线形态分析 - {stock_code}】"]
    
    # ── 1. 近期K线描述 ──────────────────────────────────
    if ohlcv_data and ohlcv_data.get("closes"):
        closes = ohlcv_data["closes"]
        # 最近5根K线摘要
        recent = []
        for i in range(max(0, len(closes)-5), len(closes)):
            c = closes[i]
            if ohlcv_data.get("opens") and i < len(ohlcv_data["opens"]):
                o = ohlcv_data["opens"][i]
            else:
                o = c
            chg = (c - o) / o * 100 if o > 0 else 0
            recent.append(f"#{i+1}: {'阳' if chg >= 0 else '阴'}{abs(chg):.1f}%")
        
        lines.append(f"近5日K线: {' | '.join(recent)}")
        
        # ── 2. 形态检测 ────────────────────────────────────
        opens = ohlcv_data.get("opens")
        highs = ohlcv_data.get("highs")
        lows = ohlcv_data.get("lows")
        vols = ohlcv_data.get("vols")
        
        patterns = detect_candlestick_patterns(closes, opens, highs, lows, vols)

        if patterns:
            pattern_summary = []
            for p in patterns:
                pattern_summary.append(
                    f"{p['pattern']}({p['confidence']}置信度): {p['signal']} — {p['detail']}"
                )
            lines.append("检测到K线形态:")
            lines.extend([f"  • {s}" for s in pattern_summary])
        else:
            lines.append("未检测到明显K线形态。")

    # ── 3. 技术指标形态检测（RSI/MA/MACD/量比）──
    tech_patterns = detect_tech_indicator_patterns(tech_data)
    if tech_patterns:
        lines.append("检测到技术指标形态:")
        for p in tech_patterns:
            lines.append(f"  • {p['pattern']}({p['confidence']}置信度): {p['signal']} — {p['detail']}")

    # ── 4. 合并所有形态 + 知识库检索 ─────────────────────
    all_patterns = patterns + tech_patterns
    if all_patterns:
        kb_text = retrieve_knowledge_for_patterns(all_patterns)
        if kb_text:
            lines.append("")
            lines.append(kb_text)
    
    # ── 4. 关键指标 ──────────────────────────────────────
    if tech_data:
        indicators = []
        for k, label in [
            ("rsi", "RSI(14)"),
            ("ma_trend", "均线趋势"),
            ("vol_ratio", "量比"),
            ("macd", "MACD柱"),
            ("macd_golden_cross", "MACD金叉"),
        ]:
            v = tech_data.get(k)
            if v is not None:
                indicators.append(f"{label}={v}")
        if indicators:
            lines.append(f"技术指标: {', '.join(indicators)}")
    
    return "\n".join(lines)


# ── 批量预获取所有候选股的K线 ───────────────────────────
def prefetch_ohlcv_for_stocks(stocks: List[Dict], xqshare_client=None) -> Dict[str, Dict]:
    """
    预获取所有候选股的OHLCV数据（通过XQShare）
    返回 {code: {"closes": [...], "opens": [...], "highs": [...], "lows": [...], "vols": [...]}}
    """
    results = {}
    
    for stock in stocks:
        code = str(stock.get("code", "")).strip()
        if not code:
            continue
        
        ohlcv = _fetch_ohlcv_via_xqshare(code, xqshare_client)
        if ohlcv:
            results[code] = ohlcv
            stock["_ohlcv"] = ohlcv
    
    return results


def _fetch_ohlcv_via_xqshare(code: str, client=None) -> Optional[Dict]:
    """通过XQShare获取原始OHLCV数据，主数据源"""
    try:
        import pandas as pd

        if client is None:
            import xqshare
            host = os.environ.get("XQSHARE_HOST", "127.0.0.1")
            port = int(os.environ.get("XQSHARE_PORT", "18812"))
            client = xqshare.connect(host, port, auto_reconnect=True, max_retries=2)


        xtdata = client.xtdata
        suffix = _ensure_suffix(code)

        data = xtdata.get_market_data(
            stock_list=[suffix], period='1d', count=60,
            field_list=['open', 'high', 'low', 'close', 'volume'],
            dividend_type='none',
        )

        if 'close' not in data:
            return None

        # XQShare返回 shape=(1, N)，1行N列(天数)，需用 iloc[0,:] 取整行
        def to_list(series):
            if series.ndim == 2 and series.shape[0] == 1:
                return series.iloc[0, :].tolist()
            elif hasattr(series.iloc[0], '__len__'):
                return series.iloc[:, 0].tolist()
            else:
                return series.tolist()

        close_vals = to_list(data['close'])
        if len(close_vals) < 10:
            return None

        return {
            "closes": close_vals,
            "opens": to_list(data.get('open', data['close'])),
            "highs": to_list(data.get('high', data['close'])),
            "lows": to_list(data.get('low', data['close'])),
            "vols": to_list(data.get('volume', pd.Series([1]*len(close_vals)))),
        }
    except Exception as e:
        logger.warning(f"XQShare OHLCV获取失败 {code}: {e}")

    # ── 兜底1: akshare ───────────────────────────────
    try:
        import pandas as pd
        import akshare as ak
        today = pd.Timestamp.today()
        start_dt = today - pd.Timedelta(days=90)
        df = ak.stock_zh_a_hist(symbol=code, period='daily',
                                start_date=start_dt.strftime('%Y%m%d'),
                                end_date=today.strftime('%Y%m%d'),
                                adjust='qfq')
        if df is not None and len(df) >= 10:
            return {
                "closes": df['收盘'].tolist()[-60:],
                "opens": df['开盘'].tolist()[-60:],
                "highs": df['最高'].tolist()[-60:],
                "lows": df['最低'].tolist()[-60:],
                "vols": df['成交量'].tolist()[-60:],
            }
    except Exception as e:
        logger.warning(f"akshare OHLCV兜底失败 {code}: {e}")


    # ── 兜底2: mx_data（近20日收盘价→近似OHLCV）──
    try:
        import subprocess, re
        suffix = ".SZ" if code.startswith(("00", "30")) else ".SH"
        cmd = [
            sys.executable,
            str((BASE_DIR.parent / "skills" / "mx-data" / "mx_data.py")),
            f"{code}{suffix} 近20日行情 日期开盘收盘最高最低成交量"
        ]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if r.returncode == 0 and "错误" not in r.stdout:
            rows = re.findall(r'(\d{4}-\d{2}-\d{2})\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)\s+(\d+\.?\d*)', r.stdout)
            if rows:
                opens = [float(x[1]) for x in rows]
                closes = [float(x[2]) for x in rows]
                highs = [float(x[3]) for x in rows]
                lows = [float(x[4]) for x in rows]
                vols = [float(x[5]) for x in rows]
                return {"closes": closes, "opens": opens, "highs": highs, "lows": lows, "vols": vols}
    except Exception as e:
        logger.warning(f"mx_data OHLCV兜底失败 {code}: {e}")


    return None


def _ensure_suffix(code: str) -> str:
    """转换6位代码为xtquant格式"""
    code = str(code).strip()
    if code.endswith((".SH", ".SZ", ".BJ")):
        return code
    if code.startswith("688"):
        return code + ".SH"
    if code.startswith(("6", "8", "4", "9", "5")):
        return code + ".SH"
    return code + ".SZ"

# ── 扩展知识库：趋势跟踪 / 动量 / 交易系统 ──────────────────
TREND_KB = KB_ROOT / "trend-following-covel"
TURTLE_KB = KB_ROOT / "turtle-trading"
WYCKOFF_KB = KB_ROOT / "volume-price-analysis"
ELLIOTT_KB = KB_ROOT / "elliott-wave-prechter"
GANN_KB = KB_ROOT / "gann-wall-street"

# 扩展形态映射（追加到PATTERN_FILE_MAP的补充）
TREND_PATTERN_MAP = {
    # 趋势跟踪相关
    "均线多头排列":  (TECH_KB / "核心概念.md",        "看涨趋势"),
    "均线空头排列":  (TECH_KB / "核心概念.md",        "看跌趋势"),
    "MACD金叉":      (TECH_KB / "核心概念.md",        "看涨信号"),
    "MACD死叉":      (TECH_KB / "核心概念.md",        "看跌信号"),
    "RSI超买":       (TECH_KB / "核心概念.md",        "超买警告"),
    "RSI超卖":       (TECH_KB / "核心概念.md",        "超卖反弹"),
    "量价齐升":      (TECH_KB / "成交量分析.md",       "量价配合"),
    "量价背离":      (TECH_KB / "成交量分析.md",       "反转信号"),
    # 趋势跟踪策略
    "突破20日高点":  (TURTLE_KB / "core-rules.md",     "趋势跟踪入场"),
    "跌破20日低点":  (TURTLE_KB / "core-rules.md",     "趋势跟踪出场"),
    "ATR止损":       (TURTLE_KB / "risk-management.md","风险控制"),
    # 威科夫/成交量
    "供应主导":      (WYCKOFF_KB / "威科夫三定律.md",  "看跌"),
    "需求主导":      (WYCKOFF_KB / "威科夫三定律.md",  "看涨"),
    "吸筹":          (WYCKOFF_KB / "核心概念.md",      "底部构建"),
    "派发":          (WYCKOFF_KB / "核心概念.md",      "顶部构建"),
    "弹簧效应":      (WYCKOFF_KB / "实战应用.md",       "反转信号"),
    # 波浪理论
    "推动浪":        (ELLIOTT_KB / "核心概念.md",      "趋势延续"),
    "调整浪":        (ELLIOTT_KB / "核心概念.md",      "震荡整理"),
    "第5浪延伸":     (ELLIOTT_KB / "实战应用.md",       "趋势末端风险"),
    "ABC调整":       (ELLIOTT_KB / "核心概念.md",      "调整结构"),
    # 江恩
    "时间周期共振":  (GANN_KB / "时间周期.md",         "变盘时间点"),
    "江恩角度线":    (GANN_KB / "核心概念.md",          "支撑阻力"),
}
