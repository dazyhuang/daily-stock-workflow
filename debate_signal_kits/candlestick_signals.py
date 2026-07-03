"""
K线形态信号库 - 蜡烛图技术分析信号
用于选股辩论引擎 v2 的短期买卖信号判断
"""

from typing import List, Dict, Any
import pandas as pd
import numpy as np

# ============================================================
# 买入形态列表（5日K线中可能出现）
# ============================================================
SHORT_TERM_BUY_SIGNALS = [
    {
        "name": "锤子线 Hammer",
        "condition": "下影线长度 ≥ 2倍实体长度，上影线极短或无，出现在下跌趋势末端",
        "判断标准": "实体位于价格区间上端；下影线 ≥ 2×实体；上影线 < 实体10%",
        "信号强度": 2,
        "备注": "需配合缩量确认底部"
    },
    {
        "name": "蜻蜓十字 Dragonfly Doji",
        "condition": "开盘=收盘，且下影线极长，类似T字形",
        "判断标准": "|open-close| < (high-low)×5%；下影线 ≥ (high-low)×60%；上影线几乎无",
        "信号强度": 2,
        "备注": "出现在支撑位附近是强烈买入信号"
    },
    {
        "name": "多头吞噬 Bullish Engulfing",
        "condition": "第1根K线为阴线（下跌），第2根K线为阳线且实体完全吞没第1根",
        "判断标准": "第1根 close < open；第2根 close > open；第2根 high > 第1根 high；第2根 low < 第1根 low",
        "信号强度": 3,
        "备注": "需出现在明显下跌后，成交量放大增强信号"
    },
    {
        "name": "启明星 Morning Star",
        "condition": "三根K线：第1根大阴线（续跌），第2根星线（实体小、跳空低开），第3根大阳线（反弹）",
        "判断标准": "第1根：中阴线，跌 ≥ 1%；第2根：星线，实体小，与第1根跳空；第3根：阳线，close > 第1根实体中点",
        "信号强度": 3,
        "备注": "止跌企稳的经典反转信号"
    },
    {
        "name": "刺穿形态 Piercing",
        "condition": "第1根大阴线，第2根跳空低开但收盘涨回至第1根实体50%以上",
        "判断标准": "第1根：中大阴线；第2根：open < 第1根 low，close > 第1根 open+(close-open)×50%",
        "信号强度": 2,
        "备注": "弱于多头吞噬，但表示短期卖压衰竭"
    },
    {
        "name": "孕线十字底 Harami Cross（底部）",
        "condition": "第1根大实体K线（阴线），第2根为十字星且完全落在第1根实体范围内",
        "判断标准": "第1根：|close-open| 大；第2根：十字星；第2根 high < 第1根 high；第2根 low > 第1根 low",
        "信号强度": 2,
        "备注": "十字星出现在大阴线后预示变盘"
    },
    {
        "name": "下降三法 Falling Three Methods",
        "condition": "下跌途中出现三根小阳线整理，最后一根大阴线跌破新低（诱空后真突破）",
        "判断标准": "整体形态在下降趋势中；3根小K线（阳线或十字）高点依次降低；最后1根大阴线收盘创出新低",
        "信号强度": 2,
        "备注": "主力借三法整理洗盘后通常快速拉升"
    },
]

# ============================================================
# 卖出形态列表（5日K线中可能出现）
# ============================================================
SHORT_TERM_SELL_SIGNALS = [
    {
        "name": "流星线 Shooting Star",
        "condition": "上影线极长（≥ 2倍实体），实体位于下端，开盘=最高价附近",
        "判断标准": "实体小且位于下端；上影线 ≥ 2×实体长度；下影线极短",
        "信号强度": 2,
        "备注": "出现在上升末期是强烈卖出警示"
    },
    {
        "name": "墓碑十字 Grave Doji",
        "condition": "开盘=收盘，且上影线极长，类似倒T字形",
        "判断标准": "|open-close| < (high-low)×5%；上影线 ≥ (high-low)×60%；下影线几乎无",
        "信号强度": 2,
        "备注": "出现在高位是强烈见顶信号"
    },
    {
        "name": "空头吞噬 Bearish Engulfing",
        "condition": "第1根为阳线（上涨），第2根为阴线且实体完全吞没第1根",
        "判断标准": "第1根 close > open；第2根 close < open；第2根 high > 第1根 high；第2根 low < 第1根 low",
        "信号强度": 3,
        "备注": "出现在上升末期是强烈卖出信号"
    },
    {
        "name": "黄昏星 Evening Star",
        "condition": "三根K线：第1根大阳线（续涨），第2根星线（跳空高开），第3根大阴线（反转）",
        "判断标准": "第1根：中大阳线，涨 ≥ 1%；第2根：星线，实体小，与第1根跳空；第3根：阴线，close < 第1根实体中点",
        "信号强度": 3,
        "备注": "经典顶部反转形态"
    },
    {
        "name": "乌云盖顶 Dark Cloud Cover",
        "condition": "第1根大阳线，第2根跳空高开但收盘跌至第1根实体50%以下",
        "判断标准": "第1根：中大阳线；第2根：open > 第1根 high，close < 第1根 open+(close-open)×50%",
        "信号强度": 2,
        "备注": "弱于空头吞噬，但表示短期买压衰竭"
    },
    {
        "name": "孕线十字顶 Harami Cross（顶部）",
        "condition": "第1根大实体K线（阳线），第2根为十字星且完全落在第1根实体范围内",
        "判断标准": "第1根：|close-open| 大；第2根：十字星；第2根 high < 第1根 high；第2根 low > 第1根 low",
        "信号强度": 2,
        "备注": "十字星出现在大阳线后预示变盘"
    },
    {
        "name": "上升三法 Rising Three Methods",
        "condition": "上涨途中出现三根小阴线整理，最后一根大阳线创出新高（诱多后真突破）",
        "判断标准": "整体形态在上升趋势中；3根小K线（阴线或十字）低点依次抬高；最后1根大阳线收盘创出新高",
        "信号强度": 2,
        "备注": "主力借三法整理出货后通常快速下跌"
    },
]

# ============================================================
# 形态否决项（满足则降级或AVOID）
# ============================================================
PATTERN_VETO_RULES = [
    {
        "规则": "RSI超买+流星线双重确认",
        "条件": "RSI(14) > 75 且 出现流星线",
        "结果": "AVOID（立即回避）",
        "理由": "超买区间出现流星线，价格即将回落概率极高"
    },
    {
        "规则": "黄昏星+放量确认顶部反转",
        "条件": "出现黄昏星形态 且 当日成交量 > 20日均量×1.5",
        "结果": "AVOID（强烈看空）",
        "理由": "量价齐跌确认顶部反转确立"
    },
    {
        "规则": "连续三窗口缺口=趋势极致必反",
        "条件": "5日内出现连续3个跳空缺口（窗口）",
        "结果": "AVOID（趋势末期）",
        "理由": "连续缺口表示趋势极度透支，随时可能反转"
    },
    {
        "规则": "孕线十字+高成交量=变盘确认",
        "条件": "出现孕线十字 且 成交量 > 20日均量×2",
        "结果": "WATCH（高度警惕）",
        "理由": "高量孕线是变盘信号，应等待确认"
    },
]

# ============================================================
# 蜡烛图核心原则速查
# ============================================================
KEY_PRINCIPLES = """
【蜡烛图核心原则】
1. 实体大小反映多空力量：实体越大，趋势越强
2. 影线长短反映博弈剧烈程度：长影线表示当日博弈剧烈
3. 形态需结合位置判断：同一形态在高位和低位意义截然相反
4. 成交量是形态有效性的验证：重要形态必须放量确认
5. 趋势中的小K线多为中继整理：不要把中继误当反转
6. 缺口（窗口）是趋势的加速器：缺口不补，趋势不止
7. 十字星是警示而非指令：需要次日确认方向
"""


def _is_doji(open_price: float, close: float, high: float, low: float, threshold: float = 0.05) -> bool:
    """判断是否为十字星"""
    body = abs(close - open_price)
    total_range = high - low
    if total_range == 0:
        return False
    return body / total_range < threshold


def _is_bullish_candle(open_price: float, close: float) -> bool:
    return close > open_price


def _is_bearish_candle(open_price: float, close: float) -> bool:
    return close < open_price


def _detect_hammer(df: pd.DataFrame, i: int) -> bool:
    """检测锤子线"""
    open_price = df.iloc[i]['open']
    high = df.iloc[i]['high']
    low = df.iloc[i]['low']
    close = df.iloc[i]['close']
    
    body = abs(close - open_price)
    upper_shadow = high - max(open_price, close)
    lower_shadow = min(open_price, close) - low
    
    if body < 0.01:  # 太小的实体不算
        return False
    
    return lower_shadow >= 2 * body and upper_shadow < body * 0.1


def _detect_dragonfly_doji(df: pd.DataFrame, i: int) -> bool:
    """检测蜻蜓十字"""
    open_price = df.iloc[i]['open']
    high = df.iloc[i]['high']
    low = df.iloc[i]['low']
    close = df.iloc[i]['close']
    
    total_range = high - low
    if total_range == 0:
        return False
    
    body = abs(close - open_price)
    lower_shadow = min(open_price, close) - low
    
    return body / total_range < 0.05 and lower_shadow / total_range >= 0.6


def _detect_grave_doji(df: pd.DataFrame, i: int) -> bool:
    """检测墓碑十字"""
    open_price = df.iloc[i]['open']
    high = df.iloc[i]['high']
    low = df.iloc[i]['low']
    close = df.iloc[i]['close']
    
    total_range = high - low
    if total_range == 0:
        return False
    
    body = abs(close - open_price)
    upper_shadow = high - max(open_price, close)
    
    return body / total_range < 0.05 and upper_shadow / total_range >= 0.6


def _detect_bullish_engulfing(df: pd.DataFrame, i: int) -> bool:
    """检测多头吞噬（第2根在i位置）"""
    if i < 1:
        return False
    prev = df.iloc[i - 1]
    curr = df.iloc[i]
    
    prev_bearish = _is_bearish_candle(prev['open'], prev['close'])
    curr_bullish = _is_bullish_candle(curr['open'], curr['close'])
    
    if not (prev_bearish and curr_bullish):
        return False
    
    return (curr['high'] > prev['high'] and 
            curr['low'] < prev['low'] and
            curr['close'] > curr['open'])  # 第2根确认为阳线


def _detect_bearish_engulfing(df: pd.DataFrame, i: int) -> bool:
    """检测空头吞噬（第2根在i位置）"""
    if i < 1:
        return False
    prev = df.iloc[i - 1]
    curr = df.iloc[i]
    
    prev_bullish = _is_bullish_candle(prev['open'], prev['close'])
    curr_bearish = _is_bearish_candle(curr['open'], curr['close'])
    
    if not (prev_bullish and curr_bearish):
        return False
    
    return (curr['high'] > prev['high'] and 
            curr['low'] < prev['low'] and
            curr['close'] < curr['open'])  # 第2根确认为阴线


def _detect_morning_star(df: pd.DataFrame, i: int) -> bool:
    """检测启明星（最后一根在i位置）"""
    if i < 2:
        return False
    first = df.iloc[i - 2]
    star = df.iloc[i - 1]
    last = df.iloc[i]
    
    # 第1根：大阴线
    first_bearish = _is_bearish_candle(first['open'], first['close'])
    first_body = abs(first['close'] - first['open'])
    first_range = first['high'] - first['low']
    first_large = first_body / first_range > 0.7 if first_range > 0 else False
    
    # 第2根：星线（实体小）
    star_body = abs(star['close'] - star['open'])
    star_range = star['high'] - star['low']
    star_small = star_body / star_range < 0.3 if star_range > 0 else True
    star_gap_down = star['open'] < first['low']  # 跳空低开
    
    # 第3根：大阳线，收盘超过第1根实体中点
    last_bullish = _is_bullish_candle(last['open'], last['close'])
    last_body = abs(last['close'] - last['open'])
    last_range = last['high'] - last['low']
    last_large = last_body / last_range > 0.7 if last_range > 0 else False
    last_recover = last['close'] > (first['open'] + first['close']) / 2
    
    return (first_bearish and first_large and 
            star_small and star_gap_down and 
            last_bullish and last_large and last_recover)


def _detect_evening_star(df: pd.DataFrame, i: int) -> bool:
    """检测黄昏星（最后一根在i位置）"""
    if i < 2:
        return False
    first = df.iloc[i - 2]
    star = df.iloc[i - 1]
    last = df.iloc[i]
    
    # 第1根：大阳线
    first_bullish = _is_bullish_candle(first['open'], first['close'])
    first_body = abs(first['close'] - first['open'])
    first_range = first['high'] - first['low']
    first_large = first_body / first_range > 0.7 if first_range > 0 else False
    
    # 第2根：星线（实体小）
    star_body = abs(star['close'] - star['open'])
    star_range = star['high'] - star['low']
    star_small = star_body / star_range < 0.3 if star_range > 0 else True
    star_gap_up = star['open'] > first['high']  # 跳空高开
    
    # 第3根：大阴线，收盘跌破第1根实体中点
    last_bearish = _is_bearish_candle(last['open'], last['close'])
    last_body = abs(last['close'] - last['open'])
    last_range = last['high'] - last['low']
    last_large = last_body / last_range > 0.7 if last_range > 0 else False
    last_recover = last['close'] < (first['open'] + first['close']) / 2
    
    return (first_bullish and first_large and 
            star_small and star_gap_up and 
            last_bearish and last_large and last_recover)


def _detect_shooting_star(df: pd.DataFrame, i: int) -> bool:
    """检测流星线"""
    open_price = df.iloc[i]['open']
    high = df.iloc[i]['high']
    low = df.iloc[i]['low']
    close = df.iloc[i]['close']
    
    body = abs(close - open_price)
    upper_shadow = high - max(open_price, close)
    lower_shadow = min(open_price, close) - low
    
    if body < 0.01:
        return False
    
    # 上影线至少是实体的2倍，下影线极短
    return upper_shadow >= 2 * body and lower_shadow < body * 0.1


def _detect_piercing(df: pd.DataFrame, i: int) -> bool:
    """检测刺穿形态"""
    if i < 1:
        return False
    prev = df.iloc[i - 1]
    curr = df.iloc[i]
    
    prev_bearish = _is_bearish_candle(prev['open'], prev['close'])
    curr_bullish = _is_bullish_candle(curr['open'], curr['close'])
    
    if not (prev_bearish and curr_bullish):
        return False
    
    # 第2根跳空低开，收盘回到第1根实体50%以上
    gap_down = curr['open'] < prev['low']
    recover_half = curr['close'] > prev['open'] + (prev['close'] - prev['open']) * 0.5
    
    return gap_down and recover_half


def _detect_dark_cloud(df: pd.DataFrame, i: int) -> bool:
    """检测乌云盖顶"""
    if i < 1:
        return False
    prev = df.iloc[i - 1]
    curr = df.iloc[i]
    
    prev_bullish = _is_bullish_candle(prev['open'], prev['close'])
    curr_bearish = _is_bearish_candle(curr['open'], curr['close'])
    
    if not (prev_bullish and curr_bearish):
        return False
    
    # 第2根跳空高开，收盘跌破第1根实体50%以下
    gap_up = curr['open'] > prev['high']
    dark_half = curr['close'] < prev['open'] + (prev['close'] - prev['open']) * 0.5
    
    return gap_up and dark_half


def _detect_harami_cross(df: pd.DataFrame, i: int) -> bool:
    """检测孕线十字（返回'top'/'bottom'/None）"""
    if i < 1:
        return False
    prev = df.iloc[i - 1]
    curr = df.iloc[i]
    
    prev_body = abs(prev['close'] - prev['open'])
    prev_range = prev['high'] - prev['low']
    prev_large = prev_body / prev_range > 0.7 if prev_range > 0 else False
    
    curr_is_doji = _is_doji(curr['open'], curr['close'], curr['high'], curr['low'])
    
    if not (prev_large and curr_is_doji):
        return None
    
    # 十字星完全在第1根实体范围内
    in_range = (curr['high'] < prev['high'] and curr['low'] > prev['low'])
    
    if not in_range:
        return None
    
    # 判断顶底
    if _is_bullish_candle(prev['open'], prev['close']):
        return 'bottom'
    elif _is_bearish_candle(prev['open'], prev['close']):
        return 'top'
    return None


def detect_candlestick_signals(kline_df: pd.DataFrame) -> dict:
    """
    检测K线形态信号
    
    Args:
        kline_df: pandas DataFrame，columns=[date, open, high, low, close, volume]
        
    Returns:
        {
            "buy_signals": [...],       # 检测到的买入信号列表
            "sell_signals": [...],      # 检测到的卖出信号列表
            "pattern_score": int(-10~10),  # 综合形态评分
            "verdict": str              # 综合判断结论
        }
    """
    buy_signals = []
    sell_signals = []
    veto_applied = []
    pattern_score = 0
    
    n = len(kline_df)
    if n < 3:
        return {
            "buy_signals": [],
            "sell_signals": [],
            "pattern_score": 0,
            "verdict": "K线数据不足，无法判断形态"
        }
    
    # 计算RSI(14)用于否决规则
    def compute_rsi(prices, period=14):
        if len(prices) < period + 1:
            return None
        deltas = prices.diff()
        gain = deltas.where(deltas > 0, 0).rolling(window=period).mean()
        loss = (-deltas.where(deltas < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1]
    
    rsi = compute_rsi(kline_df['close'])
    
    # 检测各形态
    for i in range(n):
        row = kline_df.iloc[i]
        
        # === 买入信号检测 ===
        if _detect_hammer(kline_df, i):
            buy_signals.append({
                "形态": "锤子线 Hammer",
                "日期": str(row.get('date', '')),
                "信号强度": 2,
                "提醒": "需配合缩量确认底部"
            })
        
        if _detect_dragonfly_doji(kline_df, i):
            buy_signals.append({
                "形态": "蜻蜓十字 Dragonfly Doji",
                "日期": str(row.get('date', '')),
                "信号强度": 2,
                "提醒": "出现在支撑位附近是强烈买入信号"
            })
        
        if _detect_bullish_engulfing(kline_df, i):
            buy_signals.append({
                "形态": "多头吞噬 Bullish Engulfing",
                "日期": str(row.get('date', '')),
                "信号强度": 3,
                "提醒": "需出现在明显下跌后，成交量放大增强信号"
            })
        
        if _detect_morning_star(kline_df, i):
            buy_signals.append({
                "形态": "启明星 Morning Star",
                "日期": str(row.get('date', '')),
                "信号强度": 3,
                "提醒": "止跌企稳的经典反转信号"
            })
        
        if _detect_piercing(kline_df, i):
            buy_signals.append({
                "形态": "刺穿形态 Piercing",
                "日期": str(row.get('date', '')),
                "信号强度": 2,
                "提醒": "表示短期卖压衰竭"
            })
        
        harami = _detect_harami_cross(kline_df, i)
        if harami == 'bottom':
            buy_signals.append({
                "形态": "孕线十字底 Harami Cross",
                "日期": str(row.get('date', '')),
                "信号强度": 2,
                "提醒": "十字星出现在大阴线后预示变盘"
            })
        
        # === 卖出信号检测 ===
        if _detect_shooting_star(kline_df, i):
            sell_signals.append({
                "形态": "流星线 Shooting Star",
                "日期": str(row.get('date', '')),
                "信号强度": 2,
                "提醒": "出现在上升末期是强烈卖出警示"
            })
            
            # 否决规则：RSI>75 + 流星线 = AVOID
            if rsi is not None and rsi > 75:
                veto_applied.append({
                    "规则": "RSI超买+流星线",
                    "结果": "AVOID",
                    "理由": "超买区间出现流星线，价格即将回落概率极高"
                })
        
        if _detect_grave_doji(kline_df, i):
            sell_signals.append({
                "形态": "墓碑十字 Grave Doji",
                "日期": str(row.get('date', '')),
                "信号强度": 2,
                "提醒": "出现在高位是强烈见顶信号"
            })
        
        if _detect_bearish_engulfing(kline_df, i):
            sell_signals.append({
                "形态": "空头吞噬 Bearish Engulfing",
                "日期": str(row.get('date', '')),
                "信号强度": 3,
                "提醒": "出现在上升末期是强烈卖出信号"
            })
        
        if _detect_evening_star(kline_df, i):
            sell_signals.append({
                "形态": "黄昏星 Evening Star",
                "日期": str(row.get('date', '')),
                "信号强度": 3,
                "提醒": "经典顶部反转形态"
            })
            
            # 否决规则：黄昏星+放量 = 顶部反转确认
            vol_ma = kline_df['volume'].rolling(window=20).mean().iloc[i]
            if vol_ma > 0 and row['volume'] > vol_ma * 1.5:
                veto_applied.append({
                    "规则": "黄昏星+放量",
                    "结果": "AVOID（强烈看空）",
                    "理由": "量价齐跌确认顶部反转确立"
                })
        
        if _detect_dark_cloud(kline_df, i):
            sell_signals.append({
                "形态": "乌云盖顶 Dark Cloud Cover",
                "日期": str(row.get('date', '')),
                "信号强度": 2,
                "提醒": "表示短期买压衰竭"
            })
        
        if harami == 'top':
            sell_signals.append({
                "形态": "孕线十字顶 Harami Cross",
                "日期": str(row.get('date', '')),
                "信号强度": 2,
                "提醒": "十字星出现在大阳线后预示变盘"
            })
    
    # 检测连续三窗口（5日内）
    gaps = []
    for i in range(1, min(n, 5)):
        if kline_df.iloc[i]['low'] > kline_df.iloc[i-1]['high']:
            gaps.append(i)
    if len(gaps) >= 3:
        veto_applied.append({
            "规则": "连续三窗口",
            "结果": "AVOID（趋势末期）",
            "理由": "5日内出现连续3个跳空缺口，趋势极度透支，随时可能反转"
        })
    
    # 计算综合评分
    buy_score = sum(s['信号强度'] for s in buy_signals)
    sell_score = sum(s['信号强度'] for s in sell_signals)
    pattern_score = buy_score - sell_score
    
    # 判断是否有否决规则触发
    avoid_triggered = any('AVOID' in v['结果'] for v in veto_applied)
    
    # 生成结论
    if avoid_triggered:
        verdict = "形态信号触发否决规则，建议回避"
    elif pattern_score >= 5:
        verdict = "买入信号偏多，短线偏多对待"
    elif pattern_score <= -5:
        verdict = "卖出信号偏多，短线偏空对待"
    elif len(buy_signals) > len(sell_signals):
        verdict = "买入信号略多，轻仓关注"
    elif len(sell_signals) > len(buy_signals):
        verdict = "卖出信号略多，轻仓警惕"
    else:
        verdict = "多空信号均衡，观望为主"
    
    return {
        "buy_signals": buy_signals,
        "sell_signals": sell_signals,
        "veto_rules_triggered": veto_applied,
        "pattern_score": pattern_score,
        "verdict": verdict
    }
