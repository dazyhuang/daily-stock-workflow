"""
甘氏信号库 - 威廉·甘恩角度线与回调位
用于选股辩论引擎 v2 的支撑阻力与时间窗口判断
"""

from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
import numpy as np

# ============================================================
# 关键回调位
# ============================================================
GANN_RETRACEMENT_LEVELS = [0.33, 0.5, 0.667, 0.75, 1.0]

# ============================================================
# 时间窗口规则
# ============================================================
TIME_WINDOW_RULES = {
    "斐波那契数列": {
        "数列": [1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233],
        "含义": "从重要高点/低点开始的第N个交易日可能变盘",
        "常用窗口": "5日、8日、13日、21日、34日、55日、89日",
        "应用": "配合价格支撑阻力位使用，效果更佳"
    },
    "对称周期": {
        "规则": "当前走势的时间跨度可能与之前的调整时间对称",
        "示例": "如果上涨用了21天，调整可能也需要21天",
        "应用": "用于预测调整结束的时间点"
    },
    "7的倍数": {
        "规则": "7、14、21、28等是重要的时间周期",
        "原因": "星期制导致7成为自然的时间分隔",
        "应用": "7日、14日、21日常用作短期变盘窗口"
    },
    "节气变盘": {
        "规则": "中国24节气前后市场容易变盘",
        "原因": "传统节气影响投资者心理",
        "应用": "节气前后1-2个交易日关注方向选择"
    },
    "月末季末": {
        "规则": "月末、季末是机构调仓窗口",
        "现象": "容易出现波动加大、方向选择",
        "应用": "月末、季末前后3天关注异动"
    }
}

# ============================================================
# 角度线规则
# ============================================================
GANN_ANGLE_RULES = {
    "1×1线（45度线）": {
        "斜率": "每交易日上涨/下跌1个价格单位",
        "含义": "代表多空平衡线，线上看多，线下看空",
        "应用": "价格在线上表明趋势向上；跌破1×1线趋势可能转弱"
    },
    "1×2线（26.25度）": {
        "斜率": "每交易日上涨/下跌2个价格单位",
        "含义": "代表较快上升/下降角度",
        "应用": "价格在线上表明上升趋势加速"
    },
    "2×1线（63.75度）": {
        "斜率": "每2个交易日上涨/下跌1个价格单位",
        "含义": "代表较慢上升/下降角度",
        "应用": "价格在线下表明上升趋势较弱"
    },
    "3×1线（71.25度）": {
        "斜率": "每3个交易日上涨/下跌1个价格单位",
        "含义": "代表极慢的上升角度",
        "应用": "通常作为强支撑/压力线"
    },
    "8×1线（82.5度）": {
        "斜率": "每8个交易日上涨/下跌1个价格单位",
        "含义": "代表极强支撑/压力",
        "应用": "通常作为长期趋势线"
    }
}

# ============================================================
# 支撑阻力位计算
# ============================================================

def _compute_retracement_levels(swing_high: float, swing_low: float) -> Dict[float, str]:
    """
    计算回撤支撑/压力位
    
    Returns:
        {回撤比例: (价格, 描述)}
    """
    amplitude = swing_high - swing_low
    levels = {}
    
    for pct in GANN_RETRACEMENT_LEVELS:
        level_price = swing_high - amplitude * pct
        if pct == 0.333:
            levels[pct] = (level_price, "33.3%关键支撑（回调1/3）")
        elif pct == 0.5:
            levels[pct] = (level_price, "50%黄金分割位（重要平衡点）")
        elif pct == 0.667:
            levels[pct] = (level_price, "66.7%强支撑（回调2/3）")
        elif pct == 0.75:
            levels[pct] = (level_price, "75%强支撑")
        elif pct == 1.0:
            levels[pct] = (level_price, "100%完全回撤（可能反转点）")
    
    return levels


def _find_recent_swing_high_low(df: pd.DataFrame, lookback: int = 60) -> Tuple[float, float, int, int]:
    """
    找到回溯周期内的摆动高点和低点
    
    Returns:
        (swing_high, swing_low, high_idx, low_idx)
    """
    n = len(df)
    lookback = min(lookback, n)
    recent = df.iloc[-lookback:]
    
    swing_high = recent['high'].max()
    swing_low = recent['low'].min()
    
    high_idx = recent['high'].idxmax()
    low_idx = recent['low'].idxmin()
    
    # 转换为原始索引
    high_idx = df.index.get_loc(high_idx) if isinstance(high_idx, pd.Timestamp) else high_idx
    low_idx = df.index.get_loc(low_idx) if isinstance(low_idx, pd.Timestamp) else low_idx
    
    return swing_high, swing_low, high_idx, low_idx


def _detect_near_support_resistance(current_price: float, 
                                    swing_high: float, 
                                    swing_low: float, 
                                    threshold_pct: float = 0.02) -> Dict[str, Any]:
    """
    检测当前价格是否接近支撑或阻力位
    
    Args:
        current_price: 当前价格
        swing_high: 近期高点
        swing_low: 近期低点
        threshold_pct: 距离阈值（默认2%）
    
    Returns:
        {"near_support": bool, "near_resistance": bool, "distance_to_support": float, "distance_to_resistance": float}
    """
    amplitude = swing_high - swing_low
    
    # 计算关键支撑位
    support_levels = [
        swing_low,
        swing_low + amplitude * 0.333,
        swing_low + amplitude * 0.5,
        swing_low + amplitude * 0.667,
        swing_low + amplitude * 0.75,
    ]
    
    # 计算关键阻力位
    resistance_levels = [
        swing_high,
        swing_high - amplitude * 0.333,
        swing_high - amplitude * 0.5,
        swing_high - amplitude * 0.667,
        swing_high - amplitude * 0.75,
    ]
    
    # 计算到最近支撑和阻力的距离
    distance_to_support = min(abs(current_price - s) / current_price for s in support_levels)
    distance_to_resistance = min(abs(current_price - r) / current_price for r in resistance_levels)
    
    near_support = distance_to_support < threshold_pct
    near_resistance = distance_to_resistance < threshold_pct
    
    return {
        "near_support": near_support,
        "near_resistance": near_resistance,
        "distance_to_support": f"{distance_to_support * 100:.2f}%",
        "distance_to_resistance": f"{distance_to_resistance * 100:.2f}%",
        "nearest_support": min(support_levels, key=lambda x: abs(x - current_price)),
        "nearest_resistance": min(resistance_levels, key=lambda x: abs(x - current_price))
    }


def _detect_time_window(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    检测时间窗口（斐波那契数列对应日期）
    
    Returns:
        [{"日期索引": int, "描述": str, "变盘概率": str}, ...]
    """
    n = len(df)
    if n < 30:
        return []
    
    fibonacci_windows = [5, 8, 13, 21, 34, 55, 89]
    
    time_signals = []
    current_idx = n - 1
    
    for window in fibonacci_windows:
        if current_idx - window >= 0:
            lookback_idx = current_idx - window
            lookback_date = df.iloc[lookback_idx]['date'] if 'date' in df.columns else f"第{lookback_idx}日"
            current_date = df.iloc[current_idx]['date'] if 'date' in df.columns else f"第{current_idx}日"
            
            time_signals.append({
                "窗口": f"{window}日",
                "起始日期": str(lookback_date),
                "当前日期": str(current_date),
                "描述": f"从{lookback_date}起第{window}个交易日",
                "变盘概率": "高" if window >= 21 else "中" if window >= 13 else "低"
            })
    
    return time_signals


def _compute_gann_levels(swing_high: float, swing_low: float, current_price: float) -> Dict[str, Any]:
    """
    计算甘氏角度线相关的支撑阻力
    
    Returns:
        包含各角度线对应的支撑阻力价格
    """
    amplitude = swing_high - swing_low
    mid_point = (swing_high + swing_low) / 2
    
    # 简化的角度线计算（使用百分比而非真实角度）
    # 1×1线对应50%位置
    gann_levels = {
        "1×1线（45度）": mid_point,
        "1×2线（强势）": swing_low + amplitude * 0.666,
        "2×1线（弱势）": swing_low + amplitude * 0.333,
        "3×1线（极弱支撑）": swing_low + amplitude * 0.25,
        "8×1线（极强支撑）": swing_low + amplitude * 0.111,
    }
    
    # 检测当前价格与各线的距离
    level_distances = {}
    for name, level_price in gann_levels.items():
        distance_pct = abs(current_price - level_price) / current_price * 100
        level_distances[name] = {
            "价格": round(level_price, 2),
            "距当前价": f"{distance_pct:.2f}%",
            "关系": "上方" if current_price > level_price else "下方"
        }
    
    return level_distances


# ============================================================
# 主函数
# ============================================================

def detect_gann_levels(kline_df: pd.DataFrame) -> dict:
    """
    检测甘氏信号：支撑阻力位与时间窗口
    
    Args:
        kline_df: pandas DataFrame，columns=[date, open, high, low, close, volume]
        
    Returns:
        {
            "key_retracement_levels": [...],   # 关键回调位
            "near_support": bool,             # 是否接近支撑
            "near_resistance": bool,          # 是否接近阻力
            "time_window": str,               # 时间窗口描述
            "time_signals": [...],            # 时间窗口信号列表
            "verdict": str                     # 综合判断结论
        }
    """
    n = len(kline_df)
    if n < 20:
        return {
            "key_retracement_levels": [],
            "near_support": False,
            "near_resistance": False,
            "time_window": "数据不足",
            "time_signals": [],
            "verdict": "K线数据不足，需要至少20日数据"
        }
    
    current_price = kline_df.iloc[-1]['close']
    
    # 找近期摆动高低价
    swing_high, swing_low, high_idx, low_idx = _find_recent_swing_high_low(kline_df, lookback=min(60, n))
    
    # 计算回撤位
    retracement_levels = _compute_retracement_levels(swing_high, swing_low)
    
    key_retracement_levels = []
    for pct, (price, desc) in retracement_levels.items():
        distance_pct = abs(current_price - price) / current_price * 100
        key_retracement_levels.append({
            "回撤比例": f"{pct * 100:.1f}%",
            "价格": round(price, 2),
            "描述": desc,
            "距当前价": f"{distance_pct:.2f}%",
            "关系": "支撑" if price < current_price else "阻力"
        })
    
    # 排序：按价格从低到高
    key_retracement_levels = sorted(key_retracement_levels, key=lambda x: x["价格"])
    
    # 检测是否接近支撑/阻力
    support_resistance = _detect_near_support_resistance(current_price, swing_high, swing_low)
    near_support = support_resistance["near_support"]
    near_resistance = support_resistance["near_resistance"]
    
    # 计算甘氏角度线
    gann_levels = _compute_gann_levels(swing_high, swing_low, current_price)
    
    # 检测时间窗口
    time_signals = _detect_time_window(kline_df)
    
    # 判断当前处于哪个时间窗口
    time_window_desc = "无明显时间窗口信号"
    if time_signals:
        high_prob_signals = [s for s in time_signals if s["变盘概率"] == "高"]
        if high_prob_signals:
            latest = high_prob_signals[-1]
            time_window_desc = f"今日是重要变盘窗口：{latest['窗口']}斐波那契窗口（{latest['描述']}）"
        else:
            medium_signals = [s for s in time_signals if s["变盘概率"] == "中"]
            if medium_signals:
                latest = medium_signals[-1]
                time_window_desc = f"今日是中等变盘窗口：{latest['窗口']}斐波那契窗口"
    
    # 生成综合结论
    verdict_parts = []
    
    if near_support and near_resistance:
        verdict_parts.append("价格处于支撑和阻力之间，震荡整理")
    elif near_support:
        verdict_parts.append(f"接近支撑位（{support_resistance['nearest_support']:.2f}），关注企稳信号")
    elif near_resistance:
        verdict_parts.append(f"接近阻力位（{support_resistance['nearest_resistance']:.2f}），观察能否突破")
    else:
        verdict_parts.append("价格处于支撑和阻力之间，暂无明确方向")
    
    if time_window_desc != "无明显时间窗口信号":
        verdict_parts.append(time_window_desc)
    else:
        verdict_parts.append("时间窗口无特殊信号")
    
    verdict = "；".join(verdict_parts)
    
    return {
        "key_retracement_levels": key_retracement_levels,
        "near_support": near_support,
        "near_resistance": near_resistance,
        "distance_to_support": support_resistance["distance_to_support"],
        "distance_to_resistance": support_resistance["distance_to_resistance"],
        "nearest_support": round(support_resistance["nearest_support"], 2),
        "nearest_resistance": round(support_resistance["nearest_resistance"], 2),
        "gann_levels": gann_levels,
        "time_window": time_window_desc,
        "time_signals": time_signals,
        "swing_high": round(swing_high, 2),
        "swing_low": round(swing_low, 2),
        "amplitude": round(swing_high - swing_low, 2),
        "verdict": verdict
    }
