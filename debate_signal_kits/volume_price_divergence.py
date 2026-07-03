"""
量价背离信号库 - 威科夫量价分析
用于选股辩论引擎 v2 的量价关系判断
"""

from typing import List, Dict, Any
import pandas as pd
import numpy as np

# ============================================================
# 量价背离规则列表
# ============================================================
VOL_PRICE_DIVERGENCE_RULES = [
    {
        "name": "放量不涨（主力出货）",
        "condition": "价格小涨或平盘，但成交量异常放大",
        "判断标准": "当日涨幅 < 1% 且 成交量 > 20日均量×1.8",
        "信号": "AVOID",
        "理由": "量增价不增，说明主力在出货，抛压沉重"
    },
    {
        "name": "缩量不跌（主力控盘）",
        "condition": "价格小跌或平盘，但成交量极度萎缩",
        "判断标准": "当日跌幅 < 1% 且 成交量 < 5日均量×0.4",
        "信号": "BUY",
        "理由": "跌无可跌，主力控盘锁仓，抛压枯竭"
    },
    {
        "name": "价涨量缩（上冲乏力）",
        "condition": "价格上涨但成交量萎缩量能不足",
        "判断标准": "涨幅 > 1.5% 且 成交量 < 5日均量×0.7",
        "信号": "WATCH警示",
        "理由": "无量上涨不可持续，可能是诱多或反弹尾声"
    },
    {
        "name": "价跌量增（恐慌抛售）",
        "condition": "价格下跌且成交量明显放大",
        "判断标准": "跌幅 > 2% 且 成交量 > 20日均量×1.5",
        "信号": "BUY（可能是底）",
        "理由": "恐慌盘出逃，可能是最后一跌，关注底部信号"
    },
    {
        "name": "底背离（量先于价见底）",
        "condition": "价格还在创新低，但成交量已经拒绝创新低",
        "判断标准": "价格创N日新低 且 成交量未创新低（较前期低点放大）",
        "信号": "BUY",
        "理由": "量先于价见底，主力已经开始吸筹"
    },
    {
        "name": "顶背离（量先于价见顶）",
        "condition": "价格还在创新高，但成交量已经拒绝创新高",
        "判断标准": "价格创N日新高 且 成交量未创新高（较前期高点萎缩）",
        "信号": "SELL",
        "理由": "量先于价见顶，上涨动能衰竭"
    },
]

# ============================================================
# 威科夫量价信号
# ============================================================
WYCKOFF_SIGNALS = {
    "吸筹 Accumulation": {
        "定义": "机构/主力在低位悄悄买入筹码的过程",
        "特征": [
            "价格下跌但成交量放大（下试支撑）",
            "价格反弹但成交量萎缩（自然反弹）",
            "低点逐步抬高，高点逐步上移",
            "最终放量突破震荡区间上沿"
        ],
        "信号": "BUY（机构建仓完毕，即将拉升）",
        "备注": "吸筹期间价格波动较小，常有震仓动作"
    },
    "派筹 Distribution": {
        "定义": "机构/主力在高位悄悄卖出筹码的过程",
        "特征": [
            "价格上涨但成交量放大（试卖）",
            "价格回调但成交量萎缩（无人接盘）",
            "高点逐步降低，低点逐步下移",
            "最终放量跌破震荡区间下沿"
        ],
        "信号": "SELL（机构出货完毕，即将打压）",
        "备注": "派筹期间价格波动较大，常有诱多动作"
    },
    "震仓 Shaking Out": {
        "定义": "主力在拉升前故意打压股价，吓出散户的洗盘行为",
        "特征": [
            "突然大幅下跌，跌破关键支撑",
            "成交量放大（恐慌盘出逃）",
            "快速拉回，在1-3日内收复失地",
            "下跌时放量，上涨时缩量"
        ],
        "信号": "WATCH（若快速拉回=买入机会）",
        "备注": "震仓是机构行为，与恐慌性抛售的区别在于能否快速拉回"
    },
    "弹簧 Spring": {
        "定义": "支撑位的假突破后快速拉回，是主力震仓的变体",
        "特征": [
            "价格短暂跌破支撑位（如前期低点）",
            "跌破后迅速拉回至支撑位以上",
            "成交量在跌破时放大，拉回时缩量",
            "通常在1-3日内完成"
        ],
        "信号": "BUY（支撑确认，看涨）",
        "备注": "Spring比Spring Test更强势，明确的机构控盘信号"
    },
    "无信号 No Signal": {
        "定义": "量价关系不明确，无法判断方向",
        "特征": [
            "成交量与价格波动无明显规律",
            "价格走势混乱，方向不明",
            "成交量忽大忽小"
        ],
        "信号": "WATCH（观望）",
        "备注": "没有信号就是最好的信号，等待明确"
    },
    "弹簧测试 Spring Test": {
        "定义": "价格测试支撑位后缩量反弹",
        "特征": [
            "价格下跌至支撑位附近",
            "成交量萎缩（卖压枯竭）",
            "价格企稳反弹",
            "反弹时无需放量即可推升价格"
        ],
        "信号": "BUY（支撑有效，看反弹）",
        "备注": "比Spring弱，但同样是支撑有效的信号"
    },
    "逆势Kickback": {
        "定义": "假突破后快速回归原有区间",
        "特征": [
            "价格短暂突破重要位置（压力或支撑）",
            "突破后迅速回归区间内",
            "突破时成交量可能放大，但回归时缩量",
            "是机构震仓/出货的常用手法"
        ],
        "信号": "WATCH（识别假突破方向）",
        "备注": "逆势Kickback后通常会有顺势方向的快速运动"
    },
}

# ============================================================
# 威科夫三定律
# ============================================================
WYCKOFF_LAWS = {
    "供求定律": {
        "内容": "价格由供求关系决定。供不应求价格上涨，供过于求价格下跌",
        "应用": "放量上涨=供不应求（顺势）；缩量下跌=供过于求（顺势）",
        "反向应用": "放量不涨=主力出货；缩量不跌=主力控盘"
    },
    "因果定律": {
        "内容": "没有无缘无故的涨跌，任何大行情都有充分准备",
        "应用": "横盘震荡（吸筹/派筹）越久，突破/下跌越剧烈",
        '应用2': '跳空缺口/大K线是"果"，之前必有"因"（积累/派发）'
    },
    "投入产出定律": {
        "内容": "价格在某个区间积累/派发多少，决定后续行情多大",
        "应用": "长期横盘后放量突破=大行情；长期横盘后缩量突破=诱多/诱空",
        "量价配合": "突破时放量=真突破；突破时缩量=假突破"
    },
}


def _compute_ma(series: pd.Series, window: int) -> pd.Series:
    """计算移动平均"""
    return series.rolling(window=window, min_periods=1).mean()


def _detect_price_change(df: pd.DataFrame, i: int, window: int = 5) -> float:
    """计算最近window日的价格变化百分比"""
    if i < window:
        return 0.0
    start_price = df.iloc[i - window]['close']
    if start_price == 0:
        return 0.0
    return (df.iloc[i]['close'] - start_price) / start_price * 100


def _detect_volume_change(df: pd.DataFrame, i: int, window: int = 5) -> float:
    """计算最近window日的量变化（与20日均量比）"""
    vol_ma20 = df['volume'].rolling(window=20, min_periods=10).mean().iloc[i]
    if vol_ma20 == 0:
        return 1.0
    return df.iloc[i]['volume'] / vol_ma20


def detect_volume_price_divergence(kline_df: pd.DataFrame) -> dict:
    """
    检测量价背离信号
    
    Args:
        kline_df: pandas DataFrame，columns=[date, open, high, low, close, volume]
        
    Returns:
        {
            "divergence_signals": [...],  # 量价背离信号列表
            "wyckoff_signal": str,         # 威科夫信号
            "wyckoff_description": str,     # 威科夫信号描述
            "volume_price_score": int,      # 量价评分 -10~10
            "verdict": str                  # 综合判断结论
        }
    """
    divergence_signals = []
    volume_price_score = 0
    
    n = len(kline_df)
    if n < 20:
        return {
            "divergence_signals": [],
            "wyckoff_signal": "无信号 No Signal",
            "wyckoff_description": "数据不足，无法判断量价关系",
            "volume_price_score": 0,
            "verdict": "K线数据不足，无法判断量价背离"
        }
    
    # 计算各种均值
    vol_ma5 = _compute_ma(kline_df['volume'], 5)
    vol_ma20 = _compute_ma(kline_df['volume'], 20)
    
    # 检测最近5日的量价关系
    recent_signals = []
    
    for i in range(max(20, n) - 5, n):
        row = kline_df.iloc[i]
        price_change = _detect_price_change(kline_df, i, 5)
        vol_ratio = _detect_volume_change(kline_df, i)
        
        # 放量不涨
        if abs(price_change) < 1.5 and vol_ratio > 1.8:
            divergence_signals.append({
                "类型": "放量不涨",
                "日期": str(row.get('date', '')),
                "涨幅": f"{price_change:.2f}%",
                "量比": f"{vol_ratio:.2f}x",
                "信号": "AVOID",
                "理由": "量增价不增，主力出货嫌疑"
            })
            volume_price_score -= 3
        
        # 缩量不跌
        elif abs(price_change) < 1.5 and vol_ratio < 0.4:
            divergence_signals.append({
                "类型": "缩量不跌",
                "日期": str(row.get('date', '')),
                "跌幅": f"{price_change:.2f}%",
                "量比": f"{vol_ratio:.2f}x",
                "信号": "BUY",
                "理由": "跌无可跌，主力控盘"
            })
            volume_price_score += 3
        
        # 价涨量缩
        elif price_change > 1.5 and vol_ratio < 0.7:
            divergence_signals.append({
                "类型": "价涨量缩",
                "日期": str(row.get('date', '')),
                "涨幅": f"{price_change:.2f}%",
                "量比": f"{vol_ratio:.2f}x",
                "信号": "WATCH警示",
                "理由": "无量上涨不可持续"
            })
            volume_price_score -= 1
        
        # 价跌量增
        elif price_change < -2 and vol_ratio > 1.5:
            divergence_signals.append({
                "类型": "价跌量增",
                "日期": str(row.get('date', '')),
                "跌幅": f"{price_change:.2f}%",
                "量比": f"{vol_ratio:.2f}x",
                "信号": "BUY（可能是底）",
                "理由": "恐慌盘出逃，可能是最后一跌"
            })
            volume_price_score += 2
        
        recent_signals.append({
            "i": i,
            "price_change": price_change,
            "vol_ratio": vol_ratio
        })
    
    # 检测底背离/顶背离（最近20日）
    # 找最近20日的价格最低点和量最低点
    recent_20 = kline_df.iloc[-20:]
    price_low_idx = recent_20['low'].idxmin()
    price_low_date = kline_df.iloc[price_low_idx]['date'] if 'date' in kline_df.columns else str(price_low_idx)
    price_low_value = recent_20['low'].min()
    
    # 成交量在价格低点前后的情况
    low_vol_before = kline_df.iloc[max(0, price_low_idx - 5):price_low_idx]['volume'].mean() if price_low_idx >= 5 else 0
    low_vol_after = kline_df.iloc[price_low_idx:min(n, price_low_idx + 5)]['volume'].mean() if price_low_idx < n - 5 else 0
    
    # 检测Spring（弹簧）
    spring_signal = None
    for i in range(max(5, n) - 10, n - 1):
        curr = kline_df.iloc[i]
        prev_low = kline_df.iloc[max(0, i - 20):i]['low'].min()
        prev_low_idx = kline_df.iloc[max(0, i - 20):i]['low'].idxmin()
        
        # 价格短暂跌破前期支撑后快速拉回
        if curr['low'] < prev_low * 0.995:  # 跌破前期低点
            # 检查3日内是否拉回
            for j in range(i + 1, min(i + 4, n)):
                if kline_df.iloc[j]['close'] > prev_low:
                    # 确认为Spring
                    spring_signal = {
                        "类型": "弹簧 Spring",
                        "跌破日期": str(curr.get('date', '')),
                        "拉回日期": str(kline_df.iloc[j].get('date', '')),
                        "跌破幅度": f"{(curr['low'] / prev_low - 1) * 100:.2f}%",
                        "信号": "BUY",
                        "理由": "假突破支撑后快速拉回，主力震仓"
                    }
                    divergence_signals.append(spring_signal)
                    volume_price_score += 4
                    break
    
    # 检测Spring Test
    spring_test_signal = None
    for i in range(max(5, n) - 5, n):
        row = kline_df.iloc[i]
        prev_low = kline_df.iloc[max(0, i - 20):i]['low'].min()
        
        # 价格接近但不跌破前期支撑，缩量反弹
        if row['low'] < prev_low * 1.02 and row['low'] > prev_low * 0.98:
            vol_ma5_i = vol_ma5.iloc[i] if i < len(vol_ma5) else row['volume']
            vol_ma20_i = vol_ma20.iloc[i] if i < len(vol_ma20) else row['volume']
            if vol_ma5_i < vol_ma20_i * 0.7:  # 缩量
                spring_test_signal = {
                    "类型": "弹簧测试 Spring Test",
                    "日期": str(row.get('date', '')),
                    "支撑位": f"{prev_low:.2f}",
                    "量比": f"{(row['volume'] / vol_ma20_i):.2f}x" if vol_ma20_i > 0 else "N/A",
                    "信号": "BUY",
                    "理由": "测试支撑有效，缩量反弹"
                }
                divergence_signals.append(spring_test_signal)
                volume_price_score += 2
                break
    
    # 威科夫信号判断（综合分析）
    wyckoff_signal = "无信号 No Signal"
    wyckoff_description = ""
    
    # 统计近期量价关系
    buy_signals = sum(1 for s in divergence_signals if s['信号'] in ['BUY', 'BUY（可能是底）'])
    avoid_signals = sum(1 for s in divergence_signals if s['信号'] == 'AVOID')
    watch_signals = sum(1 for s in divergence_signals if s['信号'] == 'WATCH警示')
    
    if spring_signal:
        wyckoff_signal = "弹簧 Spring"
        wyckoff_description = "支撑假突破后快速拉回，是主力震仓行为，看涨信号"
    elif spring_test_signal:
        wyckoff_signal = "弹簧测试 Spring Test"
        wyckoff_description = "支撑测试有效，缩量反弹表明抛压枯竭"
    elif buy_signals > avoid_signals * 2 and buy_signals >= 2:
        wyckoff_signal = "吸筹 Accumulation"
        wyckoff_description = "量价关系显示机构可能在低位吸筹"
    elif avoid_signals > buy_signals * 2 and avoid_signals >= 2:
        wyckoff_signal = "派筹 Distribution"
        wyckoff_description = "量价关系显示机构可能在高位派筹"
    elif watch_signals >= 2:
        wyckoff_signal = "震仓 Shaking Out"
        wyckoff_description = "频繁出现WATCH警示，可能是主力洗盘"
    
    # 生成结论
    volume_price_score = max(-10, min(10, volume_price_score))
    
    if volume_price_score >= 6:
        verdict = "量价配合良好，机构控盘迹象明显，中线偏多"
    elif volume_price_score >= 3:
        verdict = "量价偏多，低位吸筹迹象，轻仓关注"
    elif volume_price_score <= -6:
        verdict = "量价背离严重，主力出货迹象，建议回避"
    elif volume_price_score <= -3:
        verdict = "量价偏空，警惕高位风险"
    else:
        verdict = "量价关系中性，观望为主"
    
    return {
        "divergence_signals": divergence_signals,
        "wyckoff_signal": wyckoff_signal,
        "wyckoff_description": wyckoff_description,
        "volume_price_score": volume_price_score,
        "verdict": verdict
    }
