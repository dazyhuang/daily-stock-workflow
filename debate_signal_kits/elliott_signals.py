"""
波浪信号库 - 艾略特波浪理论
用于选股辩论引擎 v2 的波浪位置与趋势判断
"""

from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
import numpy as np

# ============================================================
# 波浪判断核心规则
# ============================================================
ELLIOTT_WAVE_RULES = {
    "推动浪（Impulse Wave）": {
        "结构": "5浪结构：1-2-3-4-5",
        "特点": [
            "浪3永远不是最短的一浪",
            "浪4不能与浪1重叠（除倾斜三角形外）",
            "浪2的回撤不能超过浪1的100%",
            "浪1、3、5中通常有一个会延伸（比其它两浪更长）"
        ],
        "判断方法": "识别驱动浪的5个子浪结构"
    },
    "调整浪（Corrective Wave）": {
        "结构": "3浪结构：a-b-c",
        "类型": [
            "锯齿形（Zigzag）：5-3-5结构，深度回撤",
            "平台形（Flat）：3-3-5结构，横盘整理",
            "三角形（Triangle）：3-3-3-3-3结构，震荡收敛",
            "联合形（Combination）：双重三或三重三"
        ],
        "判断方法": "调整浪出现在推动浪之后，用于消化获利盘"
    },
    "波浪比率": {
        "回撤比例": [
            "浪2回撤浪1的50%、61.8%、78.6%",
            "浪4回撤浪3的38.2%、50%、61.8%",
            "浪5可能与浪1等长，或为浪1的61.8%、100%、123.6%",
            "如果浪3延伸，则浪5可能回撤至浪1终点附近"
        ],
        "延伸比例": [
            "如果浪1延伸：整个推动浪呈现9浪结构",
            "如果浪3延伸：通常为浪1的161.8%或261.8%",
            "如果浪5延伸：通常为浪1的100%、123.6%或161.8%"
        ]
    },
    "通道线规则": {
        "作用": "用于确定波浪的边界和目标位",
        "画法": "连接浪1、浪3终点画上轨，平行线通过浪2、浪4低点画下轨",
        "应用": "浪5通常会在上轨附近结束；突破通道线确认趋势加速"
    }
}

# ============================================================
# 各浪特征描述
# ============================================================
WAVE_POSITION_SIGNALS = {
    "浪1（启动浪）": {
        "特征": "趋势的起始阶段，通常较为缓慢，成交量略有增加",
        "识别难点": "常被误认为是反弹，幅度通常为前一调整浪的38%-61.8%",
        "信号": "BUY（试探性建仓，止损设在浪1起点下方）",
        "止损": "跌破浪1起点=趋势未启动，放弃"
    },
    "浪2（回撤浪）": {
        "特征": "回撤浪1的50%-61.8%，但不低于浪1起点",
        "关键点": "成交量萎缩，价格波动收窄",
        "信号": "BUY（回撤支撑位是再次买入机会）",
        "备注": "如果回撤超过61.8%，可能不是推动浪"
    },
    "浪3（主升浪/主跌浪）": {
        "特征": "最强、最持久的趋势行情，通常放量突破",
        "判断标准": "必须创出新高/新低，且幅度大于浪1",
        "信号": "BUY（顺势持仓，不逆势做空）",
        "止损追踪": "每上涨10%，止损位上移至最新低点下方1×N",
        "注意事项": "浪3永远不是最短的推动浪"
    },
    "浪4（震荡浪）": {
        "特征": "复杂的横盘或下降调整，通常是锯齿形或平台形",
        "关键点": "不能与浪1重叠（除倾斜三角形外）",
        "回撤幅度": "通常回撤浪3的38.2%-50%",
        "信号": "WATCH（观察调整形态，等待买入机会）",
        "备注": "调整时间通常与浪2相当或更长"
    },
    "浪5（衰竭浪）": {
        "特征": "趋势的最后阶段，速度快但量能可能萎缩",
        "判断标准": "创新高/新低，但RSI可能出现顶背离",
        "信号": "WATCH警惕（可能是最后一波，随时准备止盈/做空）",
        "警示信号": [
            "价格创新高但成交量萎缩",
            "价格创新高但RSI未能同步创新高（顶背离）",
            "出现延长后竭尽（5浪终点超出1浪终点比例过大）",
            "出现倾斜三角形（楔形）形态"
        ],
        "操作建议": "分批止盈，不追涨，警惕反转"
    },
    "调整浪A": {
        "特征": "调整的开始，通常回落幅度较大",
        "成交量": "可能放量（机构出货）",
        "信号": "WATCH（减仓信号）"
    },
    "调整浪B": {
        "特征": "对浪A的反弹，通常成交量较小",
        "反弹幅度": "通常为浪A的38.2%-61.8%",
        "陷阱": "容易被误认为是新趋势的开始",
        "信号": "WATCH（反弹高点是空头入场机会）"
    },
    "调整浪C": {
        "特征": "调整的主跌浪，通常放量，破坏力强",
        "判断标准": "通常跌破浪A低点，可能延伸",
        "信号": "BUY（调整接近尾声，关注止跌信号）",
        "备注": "C浪结束是较好的中线买入时机"
    }
}

# ============================================================
# 辅助函数
# ============================================================

def _find_swing_points(df: pd.DataFrame, lookback: int = 20) -> Tuple[list, list]:
    """
    寻找摆动高点低点（简化的波峰波谷识别）
    返回：(swing_highs, swing_lows)，每个元素是(index, price)元组列表
    """
    n = len(df)
    swing_highs = []
    swing_lows = []
    
    if n < lookback * 2:
        return swing_highs, swing_lows
    
    for i in range(lookback, n - lookback):
        # 检查是否是波峰
        is_high = True
        for j in range(max(0, i - lookback), i):
            if df.iloc[j]['high'] >= df.iloc[i]['high']:
                is_high = False
                break
        if is_high:
            for j in range(i + 1, min(n, i + lookback)):
                if df.iloc[j]['high'] > df.iloc[i]['high']:
                    is_high = False
                    break
        if is_high:
            swing_highs.append((i, df.iloc[i]['high']))
        
        # 检查是否是波谷
        is_low = True
        for j in range(max(0, i - lookback), i):
            if df.iloc[j]['low'] <= df.iloc[i]['low']:
                is_low = False
                break
        if is_low:
            for j in range(i + 1, min(n, i + lookback)):
                if df.iloc[j]['low'] < df.iloc[i]['low']:
                    is_low = False
                    break
        if is_low:
            swing_lows.append((i, df.iloc[i]['low']))
    
    return swing_highs, swing_lows


def _identify_wave_sequence(swing_highs: list, swing_lows: list, prices: pd.Series) -> str:
    """
    简易波浪序列识别
    返回波浪位置判断
    """
    if len(swing_lows) < 3 or len(swing_highs) < 2:
        return "无法确定"
    
    # 简化的波浪判断逻辑
    # 找到最近的低点序列
    recent_lows = sorted(swing_lows, key=lambda x: x[0])[-5:]
    recent_highs = sorted(swing_highs, key=lambda x: x[0])[-4:]
    
    if len(recent_lows) < 3:
        return "无法确定"
    
    # 检查趋势方向
    lowest = min(recent_lows, key=lambda x: x[1])
    highest = max(recent_highs, key=lambda x: x[1]) if recent_highs else recent_lows[-1]
    
    current_price = prices.iloc[-1]
    current_idx = len(prices) - 1
    
    # 计算最近几个低点是否在抬高
    low_values = [l[1] for l in recent_lows]
    is_uptrend = all(low_values[i] < low_values[i+1] for i in range(len(low_values)-1))
    
    # 计算最近几个高点是否在抬高
    if len(recent_highs) >= 2:
        high_values = [h[1] for h in recent_highs]
        high_increasing = all(high_values[i] < high_values[i+1] for i in range(len(high_values)-1))
    else:
        high_increasing = False
    
    return is_uptrend, high_increasing, lowest, highest, current_price


def _detect_wave5_warning(df: pd.DataFrame, swing_highs: list, swing_highs_values: list) -> bool:
    """
    检测第5浪警示信号
    """
    if len(swing_highs) < 3:
        return False
    
    recent_highs = sorted(swing_highs, key=lambda x: x[0])[-3:]
    
    # 检查是否出现延长后竭尽
    # 计算浪3和浪1的幅度
    if len(recent_highs) >= 3:
        wave1_amplitude = recent_highs[1][1] - recent_highs[0][1] if recent_highs[1][1] > recent_highs[0][1] else 0
        wave3_amplitude = recent_highs[2][1] - recent_highs[1][1] if recent_highs[2][1] > recent_highs[1][1] else 0
        
        # 如果浪3大幅延长，浪5可能衰竭
        if wave3_amplitude > wave1_amplitude * 1.618:
            return True
    
    # 检查顶背离（RSI）
    if 'volume' in df.columns:
        recent_vol = df['volume'].iloc[-5:].mean()
        earlier_vol = df['volume'].iloc[-20:-5].mean()
        if recent_vol < earlier_vol * 0.8:
            # 缩量
            return True
    
    return False


def _detect_three_consecutive(df: pd.DataFrame, n: int = 3) -> Optional[str]:
    """
    简易检测：连续N根阳线后出现流星/黄昏星
    返回信号类型或None
    """
    if len(df) < n + 1:
        return None
    
    # 检查最近N根是否连续阳线
    recent = df.iloc[-(n+1):]
    all_bullish = all(recent.iloc[i]['close'] > recent.iloc[i]['open'] for i in range(n))
    
    if not all_bullish:
        return None
    
    # 检查最后一根是否是流星或黄昏星
    last = df.iloc[-1]
    body = abs(last['close'] - last['open'])
    upper_shadow = last['high'] - max(last['open'], last['close'])
    
    if upper_shadow >= 2 * body and body > 0:
        return "可能是第5浪衰竭"
    
    return None


# ============================================================
# 主函数
# ============================================================

def detect_elliott_position(kline_df: pd.DataFrame) -> dict:
    """
    简易艾略特波浪位置判断
    
    Args:
        kline_df: pandas DataFrame，columns=[date, open, high, low, close, volume]
        
    Returns:
        {
            "wave_position": str,           # 当前波浪位置
            "impulse_or_correction": str,   # 推动浪还是调整浪
            "wave3_strength": str,          # 浪3强度描述
            "wave5_warning": bool,         # 是否有5浪警示
            "verdict": str                  # 综合判断结论
        }
    """
    n = len(kline_df)
    if n < 30:
        return {
            "wave_position": "数据不足",
            "impulse_or_correction": "无法判断",
            "wave3_strength": "无法判断",
            "wave5_warning": False,
            "verdict": "K线数据不足，需要至少30日数据"
        }
    
    # 寻找摆动点
    swing_highs, swing_lows = _find_swing_points(kline_df, lookback=min(15, n // 4))
    
    wave_position = "无法确定"
    impulse_or_correction = "无法判断"
    wave3_strength = "无法判断"
    wave5_warning = False
    wave_reasoning = ""
    
    if len(swing_lows) < 2 or len(swing_highs) < 1:
        verdict = "摆动点不足，无法进行波浪分析"
        return {
            "wave_position": "数据不足",
            "impulse_or_correction": "无法判断",
            "wave3_strength": "无法判断",
            "wave5_warning": False,
            "verdict": verdict
        }
    
    # 简化波浪判断
    recent_lows = sorted(swing_lows, key=lambda x: x[0])[-4:]
    recent_highs = sorted(swing_highs, key=lambda x: x[0])[-3:]
    
    current_price = kline_df.iloc[-1]['close']
    
    # 判断趋势
    if len(recent_lows) >= 2:
        low_slope = recent_lows[-1][1] - recent_lows[-2][1]
        low_time_span = recent_lows[-1][0] - recent_lows[-2][0]
        low_slope_per_bar = low_slope / low_time_span if low_time_span > 0 else 0
    else:
        low_slope_per_bar = 0
    
    if len(recent_highs) >= 2:
        high_slope = recent_highs[-1][1] - recent_highs[-2][1]
        high_time_span = recent_highs[-1][0] - recent_highs[-2][0]
        high_slope_per_bar = high_slope / high_time_span if high_time_span > 0 else 0
    else:
        high_slope_per_bar = 0
    
    # 上升趋势
    if low_slope_per_bar > 0 and high_slope_per_bar > 0:
        impulse_or_correction = "推动浪（上升趋势）"
        
        # 判断波浪位置
        if len(recent_lows) >= 3:
            # 检查是否是调整后的反弹
            wave1_low = recent_lows[-3]
            wave2_low = recent_lows[-2]
            wave3_low = recent_lows[-1]
            
            # 计算各浪幅度
            wave1_amp = wave2_low[1] - wave1_low[1] if wave2_low[1] > wave1_low[1] else 0
            wave2_amp = wave3_low[1] - wave2_low[1] if wave3_low[1] > wave2_low[1] else 0
            
            # 简易判断：回撤幅度
            if wave1_amp > 0:
                retracement = wave2_amp / wave1_amp
                
                if retracement > 0.6:
                    wave_position = "可能是浪2回撤中（关注支撑）"
                    wave_reasoning = f"回撤幅度{retracement*100:.0f}%超过61.8%，需观察是否跌破浪1起点"
                elif wave2_amp > wave1_amp * 1.5:
                    wave_position = "可能是浪3（顺势持仓）"
                    wave3_strength = "浪3延伸中，趋势最强阶段"
                    wave_reasoning = "浪3已延伸，幅度超过浪1的150%"
                else:
                    wave_position = "可能是浪4调整中"
                    wave_reasoning = f"调整幅度{retracement*100:.0f}%，需等待调整结束"
        
        # 检查5浪警示
        if len(recent_highs) >= 3:
            # 计算各浪幅度
            wave1_h_amp = recent_highs[1][1] - recent_highs[0][1] if len(recent_highs) > 1 else 0
            wave3_h_amp = recent_highs[2][1] - recent_highs[1][1] if len(recent_highs) > 2 else 0
            wave5_h_amp = current_price - recent_highs[-1][1] if len(recent_highs) > 0 else 0
            
            # 如果浪3延伸，浪5可能衰竭
            if wave3_h_amp > wave1_h_amp * 1.618:
                wave5_warning = True
                wave5_reasoning = "浪3大幅延伸（>161.8%浪1），浪5可能衰竭"
        
        # 检查连续阳线后出现流星/黄昏星
        consecutive_warning = _detect_three_consecutive(kline_df)
        if consecutive_warning:
            wave5_warning = True
            wave5_reasoning = consecutive_warning
        
        # 检查成交量背离
        if len(kline_df) >= 20:
            recent_vol_avg = kline_df['volume'].iloc[-5:].mean()
            earlier_vol_avg = kline_df['volume'].iloc[-20:-5].mean()
            if recent_vol_avg < earlier_vol_avg * 0.7 and current_price > kline_df['high'].iloc[-20:].max() * 0.98:
                wave5_warning = True
                wave5_reasoning = "价格创新高但成交量萎缩，顶背离"
    
    elif low_slope_per_bar < 0 and high_slope_per_bar < 0:
        impulse_or_correction = "调整浪（下降趋势）"
        wave_position = "调整浪中（观望为主）"
        wave_reasoning = "处于下降调整中，等待调整结束信号"
    else:
        impulse_or_correction = "横盘整理"
        wave_position = "震荡区间（等待突破）"
        wave_reasoning = "高低点无明显趋势方向，观望等待"
    
    # 生成结论
    if wave5_warning:
        verdict = "第5浪警示！趋势末端风险积累，密切关注顶部反转信号，建议分批止盈"
    elif wave_position and "浪3" in wave_position:
        verdict = "处于主升浪/主跌浪，趋势强劲，顺势持仓为主"
    elif wave_position and "浪2" in wave_position:
        verdict = "处于2浪回撤，等待调整结束，关注支撑位买入机会"
    elif wave_position and "浪4" in wave_position:
        verdict = "处于4浪调整，观察调整形态，等待5浪启动前的买入机会"
    elif impulse_or_correction == "横盘整理":
        verdict = "震荡整理中，等待方向突破后再操作"
    else:
        verdict = "波浪位置不明确，建议观望等待更多信号"
    
    return {
        "wave_position": wave_position,
        "impulse_or_correction": impulse_or_correction,
        "wave3_strength": wave3_strength,
        "wave5_warning": wave5_warning,
        "wave5_reasoning": wave5_reasoning if wave5_warning else "",
        "wave_reasoning": wave_reasoning,
        "verdict": verdict
    }
