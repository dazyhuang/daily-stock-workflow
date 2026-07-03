"""
海龟信号库 - 趋势跟踪突破系统
用于选股辩论引擎 v2 的趋势突破与止损判断
"""

from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np

# ============================================================
# 突破信号定义（20日/55日突破）
# ============================================================
BREAKOUT_SIGNALS = {
    "20日突破": {
        "定义": "价格向上突破过去20个交易日的最高点",
        "做多条件": "当日收盘价 > 20日最高价 AND 成交量放大确认",
        "做空条件": "当日收盘价 < 20日最低价 AND 成交量放大确认",
        "适用场景": "短线趋势跟踪，适合快速波动的股票",
        "止损建议": "跌破入场价 - 2×N 则止损",
        "注意事项": "假突破频繁，需结合其他指标过滤"
    },
    "55日突破": {
        "定义": "价格向上突破过去55个交易日的最高点",
        "做多条件": "当日收盘价 > 55日最高价 AND 成交量放大确认",
        "做空条件": "当日收盘价 < 55日最低价 AND 成交量放大确认",
        "适用场景": "中线趋势确认，信号更稳定但滞后",
        "止损建议": "跌破入场价 - 2×N 则止损",
        "注意事项": "比20日突破更可靠，假突破更少"
    },
    "假突破陷阱": {
        "定义": "突破后2-3日内价格重新跌回突破位",
        "识别标准": "突破20日/55日高点后，3日内收盘价跌回突破价下方",
        "结果": "AVOID（立即回避）",
        "理由": "假突破是主力陷阱，意味着趋势未真正启动",
        "应对": "等待价格重新站上突破位并再次放量确认"
    },
    "突破后回踩": {
        "定义": "突破后价格回落到突破位附近获得支撑",
        "识别标准": "突破后3-10日内回踩突破位，不跌破",
        "结果": "BUY（加仓机会）",
        "理由": "回踩确认突破有效，是较好的加仓点",
        "注意事项": "回踩不能跌破突破位，否则可能转为假突破"
    },
}

# ============================================================
# 止损规则
# ============================================================
STOP_LOSS_RULES = {
    "2%原则": {
        "定义": "单笔交易最大损失不超过账户的2%",
        "计算公式": "单笔最大损失 = 账户金额 × 2%",
        "仓位计算": "入场股数 = 单笔最大损失 ÷ (入场价 - 止损价)",
        "适用": "所有交易，必须严格遵守",
        "备注": "这是海龟交易的核心风险控制原则"
    },
    "2N止损": {
        "定义": "止损位设置在入场价减去2倍N值(ATR)的位置",
        "计算公式": "止损价 = 入场价 - 2×N",
        "N值定义": "N = True Range的20日指数移动平均（EMA）",
        "True Range": "TR = max(H-L, |H-PDC|, |L-PDC|)，其中PDC为前一日收盘价",
        "适用": "趋势跟踪持仓，动态调整止损",
        "备注": "2N止损比固定百分比更科学，能适应不同波动率的股票"
    },
    "追踪止损": {
        "定义": "随着价格上涨，上调止损位，锁定利润",
        "方式1": "价格每上涨0.5×N，止损位上移0.5×N（快速追踪）",
        "方式2": "价格每创新高后，将止损位设置在最新收盘价 - 2×N（标准追踪）",
        "适用场景": "趋势进行中，防止回吐利润",
        "备注": "追踪止损一旦设置不能降低，只能上移"
    },
    "时间止损": {
        "定义": "持仓超过一定时间未盈利则平仓",
        "标准": "10日内未创新高/新低，或5日内未达预期涨幅的50%",
        "适用": "短线交易，防止僵化持仓",
        "备注": "时间止损用于辅助价格止损"
    },
}

# ============================================================
# 辅助函数
# ============================================================

def _compute_true_range(df: pd.DataFrame) -> pd.Series:
    """计算True Range"""
    prev_close = df['close'].shift(1)
    tr1 = df['high'] - df['low']
    tr2 = abs(df['high'] - prev_close)
    tr3 = abs(df['low'] - prev_close)
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)


def _compute_n(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """计算N值（ATR的EMA）"""
    tr = _compute_true_range(df)
    # 使用EMA计算N值
    n = tr.ewm(span=period, adjust=False).mean()
    return n


def _compute_highest(df: pd.DataFrame, column: str, period: int) -> pd.Series:
    """计算过去N日最高值"""
    return df[column].rolling(window=period, min_periods=period).max()


def _compute_lowest(df: pd.DataFrame, column: str, period: int) -> pd.Series:
    """计算过去N日最低值"""
    return df[column].rolling(window=period, min_periods=period).min()


def _detect_false_breakout(df: pd.DataFrame, i: int, breakout_col: str, breakout_value: float) -> bool:
    """
    检测假突破：突破后3日内跌回突破位
    """
    if i < 2 or i >= len(df) - 1:
        return False
    
    # 检查未来3日内是否跌回
    for j in range(i + 1, min(i + 4, len(df))):
        if df.iloc[j]['close'] < breakout_value:
            return True
    return False


def _detect_breakout_retest(df: pd.DataFrame, i: int, breakout_col: str, breakout_value: float) -> bool:
    """
    检测回踩：突破后3-10日内回踩突破位但不跌破
    """
    if i < 3 or i >= len(df) - 5:
        return False
    
    # 检查未来3-10日内是否回踩
    for j in range(i + 3, min(i + 11, len(df))):
        price_low = df.iloc[j]['low']
        if price_low <= breakout_value * 1.01 and price_low >= breakout_value * 0.99:
            # 回踩后价格重新上涨
            if j < len(df) - 1 and df.iloc[j + 1]['close'] > breakout_value:
                return True
    return False


# ============================================================
# 主函数
# ============================================================

def detect_turtle_signals(kline_df: pd.DataFrame, account_size: float = 1000000) -> dict:
    """
    检测海龟交易信号
    
    Args:
        kline_df: pandas DataFrame，columns=[date, open, high, low, close, volume]
        account_size: 账户金额（用于计算仓位），默认100万
        
    Returns:
        {
            "breakout_20d": bool,           # 20日是否突破
            "breakout_55d": bool,           # 55日是否突破
            "false_breakout": bool,         # 是否为假突破
            "atr_n": float,                 # 当前N值(ATR)
            "stop_loss_pct": float,         # 建议止损百分比
            "signal": str,                  # 综合信号
            "breakout_details": {...},      # 详细突破信息
            "stop_loss_details": {...},     # 详细止损信息
        }
    """
    n = len(kline_df)
    if n < 60:
        return {
            "breakout_20d": False,
            "breakout_55d": False,
            "false_breakout": False,
            "atr_n": 0.0,
            "stop_loss_pct": 0.0,
            "signal": "数据不足，无法判断（需要至少60日数据）",
            "breakout_details": {},
            "stop_loss_details": {}
        }
    
    # 计算各种指标
    close = kline_df['close']
    high = kline_df['high']
    low = kline_df['low']
    volume = kline_df['volume']
    
    # 计算N值(ATR)
    atr = _compute_n(kline_df)
    current_n = atr.iloc[-1]
    
    # 计算20日和55日最高价/最低价
    high_20d = _compute_highest(kline_df, 'high', 20)
    low_20d = _compute_lowest(kline_df, 'low', 20)
    high_55d = _compute_highest(kline_df, 'high', 55)
    low_55d = _compute_lowest(kline_df, 'low', 55)
    
    # 当日数据
    last_close = close.iloc[-1]
    last_high = high.iloc[-1]
    last_low = low.iloc[-1]
    last_volume = volume.iloc[-1]
    
    # 20日突破判断
    prev_high_20d = high_20d.iloc[-2] if len(high_20d) > 1 else 0
    breakout_20d_long = last_close > prev_high_20d
    
    # 55日突破判断
    prev_high_55d = high_55d.iloc[-2] if len(high_55d) > 1 else 0
    breakout_55d_long = last_close > prev_high_55d
    
    # 20日/55日向下突破（做空信号）
    breakout_20d_short = last_close < low_20d.iloc[-2] if len(low_20d) > 1 else False
    breakout_55d_short = last_close < low_55d.iloc[-2] if len(low_55d) > 1 else False
    
    # 假突破检测
    false_breakout_20d = False
    false_breakout_55d = False
    
    if breakout_20d_long:
        false_breakout_20d = _detect_false_breakout(kline_df, n - 1, '20d_high', prev_high_20d)
    if breakout_55d_long:
        false_breakout_55d = _detect_false_breakout(kline_df, n - 1, '55d_high', prev_high_55d)
    
    # 回踩检测
    retest_20d = _detect_breakout_retest(kline_df, n - 1, '20d_high', prev_high_20d)
    retest_55d = _detect_breakout_retest(kline_df, n - 1, '55d_high', prev_high_55d)
    
    # 计算成交量确认
    vol_ma20 = volume.rolling(window=20, min_periods=10).mean()
    vol_confirm_20d = last_volume > vol_ma20.iloc[-1] * 1.2 if len(vol_ma20) > 0 else False
    vol_confirm_55d = last_volume > vol_ma20.iloc[-1] * 1.2 if len(vol_ma20) > 0 else False
    
    # 计算止损百分比（2N止损）
    stop_loss_pct = (2 * current_n / last_close) * 100 if last_close > 0 else 0
    
    # 计算2%原则下的建议仓位
    max_loss_per_trade = account_size * 0.02
    shares_per_unit = max_loss_per_trade / (2 * current_n) if current_n > 0 else 0
    
    # 综合判断信号
    signal = "WATCH"
    breakout_20d = breakout_20d_long
    breakout_55d = breakout_55d_long
    
    if false_breakout_20d or false_breakout_55d:
        signal = "AVOID（假突破陷阱）"
        breakout_20d = False
        breakout_55d = False
    elif breakout_55d_long and vol_confirm_55d:
        signal = "BUY（55日突破确认，强势信号）"
    elif breakout_20d_long and vol_confirm_20d:
        signal = "BUY（20日突破，需观察持续性）"
    elif breakout_20d_long and not vol_confirm_20d:
        signal = "WATCH（20日突破无量，需确认）"
    elif retest_20d or retest_55d:
        signal = "BUY（回踩确认，可加仓）"
    elif breakout_20d_short or breakout_55d_short:
        signal = "SELL（向下突破，看空）"
    
    breakout_details = {
        "20日突破": {
            "突破": breakout_20d_long,
            "突破价位": round(prev_high_20d, 2),
            "当前收盘价": round(last_close, 2),
            "放量确认": vol_confirm_20d,
            "假突破": false_breakout_20d,
            "回踩确认": retest_20d,
            "距突破涨幅": f"{((last_close / prev_high_20d - 1) * 100):.2f}%" if prev_high_20d > 0 else "N/A"
        },
        "55日突破": {
            "突破": breakout_55d_long,
            "突破价位": round(prev_high_55d, 2),
            "当前收盘价": round(last_close, 2),
            "放量确认": vol_confirm_55d,
            "假突破": false_breakout_55d,
            "回踩确认": retest_55d,
            "距突破涨幅": f"{((last_close / prev_high_55d - 1) * 100):.2f}%" if prev_high_55d > 0 else "N/A"
        }
    }
    
    stop_loss_details = {
        "atr_n": round(current_n, 3),
        "止损价（2N）": round(last_close - 2 * current_n, 2),
        "止损百分比": f"{stop_loss_pct:.2f}%",
        "2%原则建议股数": int(shares_per_unit) if shares_per_unit > 0 else 0,
        "备注": "跌破止损价应立即平仓，不侥幸"
    }
    
    return {
        "breakout_20d": breakout_20d,
        "breakout_55d": breakout_55d,
        "false_breakout": false_breakout_20d or false_breakout_55d,
        "atr_n": round(current_n, 3),
        "stop_loss_pct": round(stop_loss_pct, 2),
        "signal": signal,
        "breakout_details": breakout_details,
        "stop_loss_details": stop_loss_details
    }
