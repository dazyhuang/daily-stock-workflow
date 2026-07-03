#!/usr/bin/env python3
"""
技术分析量化打分引擎
====================
6维度规则引擎，给候选股票打量化分（0-100）
不调LLM，直接根据数据包中的数值计算
"""

from __future__ import annotations
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger("tech_scoring")

# ─────────────────────────────────────────────────────────────────────────────
# 维度1：蜡烛图形态（0-20分）
# ─────────────────────────────────────────────────────────────────────────────

def score_candlestick(kline: List[Dict], packet: Dict) -> Dict:
    """
    蜡烛图形态打分
    买入信号: +3~+5, 卖出信号: -3~-5
    否决: RSI>75 + 流星/黄昏星 → 置0
    """
    # 先从数据包获取已检测的蜡烛图信号（由信号库计算）
    cs = packet.get("candlestick_patterns", {})
    if isinstance(cs, dict) and cs.get("verdict") in ("数据不足", "模块不可用"):
        return {"score": 10, "detail": "数据不足，默认中性的10分", "veto": False, "veto_reason": ""}
    
    buy_signals = cs.get("buy_signals", []) if isinstance(cs, dict) else []
    sell_signals = cs.get("sell_signals", []) if isinstance(cs, dict) else []
    pattern_score = cs.get("pattern_score", 0) if isinstance(cs, dict) else 0  # -10~10
    
    # RSI检查（否决用）
    rsi = _safe_float(packet.get("indicators", {}).get("rsi_14"), 50)
    
    score = 10  # 默认中性
    veto = False
    veto_reason = ""
    detail_parts = []
    
    # 买入信号加分（上限15分）
    buy_map = {
        "锤子线": 5, "蜻蜓十字": 4, "多头吞噬": 5, "启明星": 4,
        "刺穿形态": 3, "孕线十字底": 4, "Morning Star": 4, "Bullish Engulfing": 5,
        "Hammer": 5, "Dragonfly Doji": 4, "Harami Cross Bottom": 4,
    }
    for sig in buy_signals:
        for key, pts in buy_map.items():
            if key in sig:
                score += pts
                detail_parts.append(f"+{pts}({sig})")
                break
    
    # 卖出信号扣分（下限-10分）
    sell_map = {
        "流星线": -5, "墓碑十字": -4, "空头吞噬": -5, "黄昏星": -4,
        "乌云盖顶": -3, "孕线十字顶": -4, "Shooting Star": -5,
        "Grave Doji": -4, "Bearish Engulfing": -5, "Evening Star": -4,
        "Dark Cloud": -3, "Harami Cross Top": -4,
    }
    for sig in sell_signals:
        for key, pts in sell_map.items():
            if key in sig:
                score += pts
                detail_parts.append(f"{pts}({sig})")
                break
    
    # 否决项检查
    if rsi > 75:
        high_rsi_sells = [s for s in sell_signals if any(k in s for k in ["流星", "Shooting", "黄昏", "Evening", "墓碑", "Grave"])]
        if high_rsi_sells:
            veto = True
            veto_reason = f"RSI>{rsi:.0f} + 空头形态({high_rsi_sells[0]}) → 超买共振否决"
            score = 0
    
    # 限制范围
    score = max(-10, min(20, score))
    
    return {
        "score": score,
        "detail": " ".join(detail_parts) if detail_parts else f"pattern_score={pattern_score}",
        "veto": veto,
        "veto_reason": veto_reason,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 维度2：量价配合（0-15分）
# ─────────────────────────────────────────────────────────────────────────────

def score_volume_price(kline: List[Dict], packet: Dict) -> Dict:
    """
    量价配合打分
    放量上涨+5, 缩量整理后放量+4, 威科夫吸筹完成+4
    放量不涨-8（主力出货）, 价涨量缩-5, 放量下跌-4
    """
    if not kline or len(kline) < 5:
        return {"score": 7, "detail": "数据不足，默认7分", "veto": False, "veto_reason": ""}
    
    closes = [k["close"] for k in kline[-5:] if "close" in k]
    volumes = [k["volume"] for k in kline[-5:] if "volume" in k and k["volume"] > 0]
    
    # 计算量能状态
    score = 7  # 默认中性
    detail_parts = []
    veto = False
    veto_reason = ""
    
    if len(closes) >= 2 and len(volumes) >= 2:
        price_change = (closes[-1] - closes[0]) / closes[0] * 100 if closes[0] > 0 else 0
        
        # 近5日均量 vs 近20日均量（估算）
        vol_avg_short = sum(volumes) / len(volumes) if volumes else 0
        vol_avg_long = vol_avg_short * 1.0  # 数据不足时用估算
        if len(kline) >= 20:
            vol_avg_long = sum(k["volume"] for k in kline[-20:] if k.get("volume", 0) > 0) / 20
        
        vol_ratio = vol_avg_short / vol_avg_long if vol_avg_long > 0 else 1.0
        
        # 从数据包获取DDX
        mf = packet.get("money_flow", {})
        ddx = _safe_float(mf.get("ddx_5"), 0)
        ddy = _safe_float(mf.get("ddy_10"), 0)
        
        # 放量上涨
        if vol_ratio > 1.5 and price_change > 0:
            score += 5
            detail_parts.append("放量上涨+5")
        elif vol_ratio > 1.2 and price_change > 0:
            score += 3
            detail_parts.append("温和放量上涨+3")
        # 缩量整理后放量启动
        elif len(volumes) >= 3 and volumes[-1] > volumes[-3] * 1.3 and price_change > 1:
            score += 4
            detail_parts.append("缩量整理后放量+4")
        # 威科夫吸筹完成
        vp = packet.get("volume_price_divergence", {})
        if isinstance(vp, dict) and vp.get("signal") in ("吸筹完成", "吸筹信号", "Test支撑成功"):
            score += 4
            detail_parts.append(f"威科夫({vp.get('signal')})+4")
        
        # 放量不涨（主力出货警告）— 否决项
        if vol_ratio > 1.5 and abs(price_change) < 0.5 and ddx < -0.5:
            veto = True
            veto_reason = f"放量不涨(DDX={ddx:.2f}) + 价格平 → 主力出货否决"
            score = max(score, -5)  # 只扣分，不完全否决
        
        # 价涨量缩
        if price_change > 1 and vol_ratio < 0.7:
            score -= 5
            detail_parts.append(f"价涨量缩-5")
        
        # 放量下跌
        if vol_ratio > 1.5 and price_change < -1:
            score -= 4
            detail_parts.append(f"放量下跌-4")
    
    score = max(0, min(15, score))
    return {
        "score": score,
        "detail": " ".join(detail_parts) if detail_parts else "中性",
        "veto": veto,
        "veto_reason": veto_reason,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 维度3：海龟突破（0-20分）
# ─────────────────────────────────────────────────────────────────────────────

def score_turtle_breakout(kline: List[Dict], packet: Dict) -> Dict:
    """
    海龟突破打分
    20日突破+10, 55日突破+15
    假突破-10
    """
    ts = packet.get("turtle_signals", {})
    if isinstance(ts, dict) and ts.get("signal") in ("数据不足", "模块不可用"):
        return {"score": 10, "detail": "数据不足，默认10分", "veto": False, "veto_reason": ""}
    
    score = 10  # 默认中性
    detail_parts = []
    veto = False
    veto_reason = ""
    
    # 从信号库获取
    breakout_20d = ts.get("breakout_20d", False) if isinstance(ts, dict) else False
    breakout_55d = ts.get("breakout_55d", False) if isinstance(ts, dict) else False
    false_breakout = ts.get("false_breakout", False) if isinstance(ts, dict) else False
    atr_n = _safe_float(ts.get("atr_n"), 0)
    
    if breakout_55d:
        score = 15
        detail_parts.append("55日突破+15")
    elif breakout_20d:
        score = 10
        detail_parts.append("20日突破+10")
    
    # 假突破警告
    if false_breakout:
        score = 0
        veto = True
        veto_reason = "假突破后快速跌回 → 陷阱信号"
    
    # ATR止损空间评估（如果有数据）
    if atr_n > 0 and len(kline) >= 20:
        latest_close = kline[-1].get("close", 0)
        if latest_close > 0:
            atr_pct = atr_n / latest_close * 100
            if atr_pct < 2:
                score += 3
                detail_parts.append(f"ATR风险可控+3({atr_pct:.1f}%)")
            elif atr_pct > 5:
                score -= 3
                detail_parts.append(f"ATR风险过大-3({atr_pct:.1f}%)")
    
    score = max(0, min(20, score))
    return {
        "score": score,
        "detail": " ".join(detail_parts) if detail_parts else "无突破信号",
        "veto": veto,
        "veto_reason": veto_reason,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 维度4：波浪位置（0-15分）
# ─────────────────────────────────────────────────────────────────────────────

def score_elliott_wave(kline: List[Dict], packet: Dict) -> Dict:
    """
    波浪位置打分
    浪3(主升)+12, B浪后C完成+10, A浪进行中-3
    第5浪警告-8
    """
    ew = packet.get("elliott_wave", {})
    if isinstance(ew, dict) and ew.get("verdict") in ("数据不足", "难判断"):
        return {"score": 7, "detail": "数据不足，默认7分", "veto": False, "veto_reason": ""}
    
    score = 7
    detail_parts = []
    veto = False
    veto_reason = ""
    
    wave_pos = ew.get("wave_position", "") if isinstance(ew, dict) else ""
    wave5_warning = ew.get("wave5_warning", False) if isinstance(ew, dict) else False
    
    # 顺势加分
    if "浪3" in wave_pos or "3子浪" in wave_pos or "主升" in wave_pos:
        score = 12
        detail_parts.append("第3子浪(主升)+12")
    elif "B浪" in wave_pos and "C浪" in wave_pos:
        score = 10
        detail_parts.append("调整浪C完成+10")
    elif "A浪" in wave_pos:
        score = max(0, score - 3)
        detail_parts.append("调整浪A进行中-3")
    
    # 第5浪衰竭警告
    if wave5_warning or "第5浪" in wave_pos:
        score = max(0, score - 8)
        detail_parts.append("第5浪衰竭警告-8")
        veto = True
        veto_reason = "第5浪(衰竭) + 可能反转"
    
    score = max(0, min(15, score))
    return {
        "score": score,
        "detail": " ".join(detail_parts) if detail_parts else f"波浪位置:{wave_pos}",
        "veto": veto,
        "veto_reason": veto_reason,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 维度5：均线系统（0-15分）
# ─────────────────────────────────────────────────────────────────────────────

def score_ma_system(kline: List[Dict], packet: Dict) -> Dict:
    """
    均线系统打分
    多头完美+10, MA5>MA20+6, 横盘+2, 空头-8
    MACD金叉+2, RSI>75-2, RSI>80-4
    """
    if not kline or len(kline) < 20:
        return {"score": 7, "detail": "数据不足，默认7分", "veto": False, "veto_reason": ""}
    
    closes = [k["close"] for k in kline if "close" in k]
    
    def ma(n):
        if len(closes) >= n:
            return sum(closes[-n:]) / n
        return 0
    
    ma5 = ma(5)
    ma10 = ma(10)
    ma20 = ma(20)
    
    ind = packet.get("indicators", {})
    rsi = _safe_float(ind.get("rsi_14"), 50)
    macd_signal = ind.get("macd_signal", "")  # "金叉"/"死叉"/""
    
    score = 7
    detail_parts = []
    
    if ma5 > ma10 > ma20:
        score = 10
        detail_parts.append("完美多头+10")
    elif ma5 > ma20:
        score = 6
        detail_parts.append("偏多+6")
    elif ma5 < ma10 < ma20:
        score = 0
        detail_parts.append("空头排列-8(重置为0)")
    elif ma5 < ma20:
        score = 3
        detail_parts.append("偏空+3")
    else:
        detail_parts.append("均线缠绕")
    
    # 附加项
    latest_close = closes[-1] if closes else 0
    if latest_close > ma5 > 0:
        score += 2
        detail_parts.append("价格>MA5+2")
    
    if macd_signal == "金叉":
        score += 2
        detail_parts.append("MACD金叉+2")
    elif macd_signal == "死叉":
        score -= 2
        detail_parts.append("MACD死叉-2")
    
    if rsi > 80:
        score -= 4
        detail_parts.append(f"RSI>{rsi:.0f}-4")
    elif rsi > 75:
        score -= 2
        detail_parts.append(f"RSI>{rsi:.0f}-2")
    elif rsi < 30:
        score -= 2
        detail_parts.append(f"RSI<{rsi:.0f}-2")
    
    score = max(0, min(15, score))
    return {
        "score": score,
        "detail": " ".join(detail_parts) if detail_parts else "中性",
        "veto": False,
        "veto_reason": "",
    }


# ─────────────────────────────────────────────────────────────────────────────
# 维度6：财务底线（0-15分，扣分制）
# ─────────────────────────────────────────────────────────────────────────────

def score_financial_safety(packet: Dict) -> Dict:
    """
    财务底线打分（扣分制）
    ROE/PE/负债率/ST/*ST
    """
    fin = packet.get("financial", {})
    
    score = 10  # 默认及格
    detail_parts = []
    veto = False
    veto_reason = ""
    
    # 从数据包获取财务数据
    roe = _safe_float(fin.get("roe"), None)
    pe = _safe_float(fin.get("pe_ttm"), None)
    debt_ratio = _safe_float(fin.get("debt_ratio"), None)
    
    # ROE
    if roe is not None:
        if roe > 15:
            score += 5
            detail_parts.append(f"ROE>{roe:.1f}%+5")
        elif roe > 10:
            score += 3
            detail_parts.append(f"ROE>{roe:.1f}%+3")
        elif roe > 5:
            pass  # +0
            detail_parts.append(f"ROE={roe:.1f}%+0")
        elif roe > 0:
            score -= 3
            detail_parts.append(f"ROE={roe:.1f}%-3")
        else:
            score -= 5
            detail_parts.append(f"ROE<0-5")
    
    # PE
    if pe is not None and pe > 0:
        if pe < 30:
            score += 4
            detail_parts.append(f"PE={pe:.1f}+4")
        elif pe < 60:
            score += 2
            detail_parts.append(f"PE={pe:.1f}+2")
        elif pe < 100:
            score -= 2
            detail_parts.append(f"PE={pe:.1f}-2")
        elif pe >= 100 and roe is not None and roe < 0:
            score -= 8
            detail_parts.append(f"PE>{pe:.0f}+亏损-8")
            veto = True
            veto_reason = f"PE>{pe:.0f} + ROE<0 → 估值泡沫否决"
    
    # 负债率
    if debt_ratio is not None:
        if debt_ratio < 50:
            score += 3
            detail_parts.append(f"负债率={debt_ratio:.1f}%+3")
        elif debt_ratio < 65:
            score += 1
            detail_parts.append(f"负债率={debt_ratio:.1f}%+1")
        elif debt_ratio < 80:
            score -= 2
            detail_parts.append(f"负债率={debt_ratio:.1f}%-2")
        elif debt_ratio >= 80:
            score -= 5
            detail_parts.append(f"负债率>{debt_ratio:.1f}%-5")
    
    # ST/*ST 直接否决
    name = packet.get("name", "")
    if name and ("ST" in name or "*ST" in name):
        score = 0
        veto = True
        veto_reason = "ST/*ST股票 → 高风险否决"
    
    score = max(0, min(15, score))
    return {
        "score": score,
        "detail": " ".join(detail_parts) if detail_parts else "数据不足",
        "veto": veto,
        "veto_reason": veto_reason,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 辅助函数
# ─────────────────────────────────────────────────────────────────────────────

def _safe_float(val, default: float) -> float:
    """安全转换为float"""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# ─────────────────────────────────────────────────────────────────────────────
# 主函数：整合6维度打分
# ─────────────────────────────────────────────────────────────────────────────

def compute_tech_score(packet: Dict) -> Dict:
    """
    计算技术分析综合量化打分
    
    Args:
        packet: build_debate_packet 返回的数据包，包含：
            - kline_raw: List[Dict] K线原始数据
            - candlestick_patterns: dict 蜡烛图信号
            - volume_price_divergence: dict 量价背离
            - turtle_signals: dict 海龟突破
            - elliott_wave: dict 波浪位置
            - indicators: dict 技术指标(RSI/MACD)
            - financial: dict 财务数据
            - money_flow: dict 资金流
    
    Returns:
        {
            "total_score": int,          # 0-100 标准化总分
            "raw_total": int,            # 原始满分（各维度满分之和，不含否决）
            "breakdown": {
                "candlestick": {...},   # 蜡烛图
                "volume_price": {...},   # 量价配合
                "turtle_breakout": {...},# 海龟突破
                "elliott_wave": {...},  # 波浪位置
                "ma_system": {...},      # 均线系统
                "financial": {...},       # 财务底线
            },
            "veto": bool,                # 是否触发否决
            "veto_reason": str,         # 否决原因
            "signal": str,              # BUY/WATCH/AVOID
            "confidence": int,          # 置信度 0-100
        }
    """
    kline = packet.get("kline_raw", [])
    
    # 计算6个维度
    dim_candlestick = score_candlestick(kline, packet)
    dim_volume = score_volume_price(kline, packet)
    dim_turtle = score_turtle_breakout(kline, packet)
    dim_wave = score_elliott_wave(kline, packet)
    dim_ma = score_ma_system(kline, packet)
    dim_financial = score_financial_safety(packet)
    
    dims = {
        "candlestick": dim_candlestick,
        "volume_price": dim_volume,
        "turtle_breakout": dim_turtle,
        "elliott_wave": dim_wave,
        "ma_system": dim_ma,
        "financial": dim_financial,
    }
    
    # 检查否决
    overall_veto = False
    veto_reasons = []
    for name, dim in dims.items():
        if dim.get("veto"):
            overall_veto = True
            veto_reasons.append(f"[{name}] {dim['veto_reason']}")
    
    # 计算原始分（不含否决的维度之和）
    max_scores = {"candlestick": 20, "volume_price": 15, "turtle_breakout": 20,
                   "elliott_wave": 15, "ma_system": 15, "financial": 15}
    raw_total = sum(dim["score"] for dim in dims.values())
    max_total = sum(max_scores.values())  # 100
    
    # 标准化到0-100
    if overall_veto:
        normalized = 0
    else:
        normalized = int(raw_total / max_total * 100)
    
    # 决策信号
    if overall_veto:
        signal = "AVOID"
        confidence = 30
    elif normalized >= 70:
        signal = "BUY"
        confidence = min(95, normalized + 5)
    elif normalized >= 50:
        signal = "WATCH"
        confidence = normalized
    else:
        signal = "AVOID"
        confidence = normalized
    
    # 置信度微调：有强烈突破信号
    if dims["turtle_breakout"]["score"] >= 15 and not overall_veto:
        confidence = min(95, confidence + 10)
    if dims["candlestick"]["score"] >= 15 and not overall_veto:
        confidence = min(95, confidence + 5)
    
    return {
        "total_score": normalized,
        "raw_total": raw_total,
        "max_total": max_total,
        "breakdown": {
            "candlestick": {"score": dim_candlestick["score"], "max": 20, "detail": dim_candlestick["detail"]},
            "volume_price": {"score": dim_volume["score"], "max": 15, "detail": dim_volume["detail"]},
            "turtle_breakout": {"score": dim_turtle["score"], "max": 20, "detail": dim_turtle["detail"]},
            "elliott_wave": {"score": dim_wave["score"], "max": 15, "detail": dim_wave["detail"]},
            "ma_system": {"score": dim_ma["score"], "max": 15, "detail": dim_ma["detail"]},
            "financial": {"score": dim_financial["score"], "max": 15, "detail": dim_financial["detail"]},
        },
        "veto": overall_veto,
        "veto_reasons": veto_reasons,
        "signal": signal,
        "confidence": max(5, min(95, confidence)),
    }
