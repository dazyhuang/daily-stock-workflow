"""
Backtrader 涨跌停滑点处理
========================
A股规则：
- 普通股票：涨跌停 10%
- 科创板/创业板：涨跌停 20%
- ST股：涨跌停 5%

涨停不能买入（无法以涨停价成交）
跌停不能卖出（无法以跌停价成交）

Backtrader 的滑点通过 broker.set_slippage_perc() 或 set_filler() 配置
"""

import backtrader as bt


def set_slippage_perc(cerebro, perc: float = 0.001,
                      slip_open: bool = True,
                      slip_limit: bool = True,
                      slip_match: bool = True,
                      slip_out: bool = False):
    """
    配置百分比滑点
    
    Args:
        cerebro: Cerebro 实例
        perc: 滑点比例（0.001 = 0.1%）
        slip_open: 是否对开盘价滑点
        slip_limit: 限价单是否强制匹配
        slip_match: 滑点是否限制在 high/low 范围内
        slip_out: 是否允许超出 high/low 滑点
    """
    cerebro.broker.set_slippage_perc(
        perc=perc,
        slip_open=slip_open,
        slip_limit=slip_limit,
        slip_match=slip_match,
        slip_out=slip_out,
    )


def set_slippage_fixed(cerebro, fixed: float = 0.01,
                       slip_open: bool = True,
                       slip_limit: bool = True,
                       slip_match: bool = True,
                       slip_out: bool = False):
    """
    配置固定滑点
    
    Args:
        cerebro: Cerebro 实例
        fixed: 固定滑点金额
    """
    cerebro.broker.set_slippage_fixed(
        fixed=fixed,
        slip_open=slip_open,
        slip_limit=slip_limit,
        slip_match=slip_match,
        slip_out=slip_out,
    )


class VolumeFiller:
    """
    成交量滑点填充器
    
    大单买入会推高价格，大单卖出会压低价格
    
    用法:
        cerebro.broker.set_filler(VolumeFiller(max_volume_pct=0.3))
    """
    
    def __init__(self, max_volume_pct: float = 0.3, slip_perc: float = 0.001):
        """
        Args:
            max_volume_pct: 超过日成交量多少比例时触发额外滑点
            slip_perc: 额外滑点比例
        """
        self.max_volume_pct = max_volume_pct
        self.slip_perc = slip_perc
        self._volume_history = {}  # {stock: [volumes]}
    
    def __call__(self, order, price, data):
        """
        填充器回调函数
        
        Args:
            order: Order 实例
            price: 订单价格
            data: 数据源
        
        Returns:
            int: 实际成交数量
        """
        # 计算日均成交量
        stock = data._name
        if stock not in self._volume_history:
            self._volume_history[stock] = []
        
        volumes = self._volume_history[stock]
        
        # 添加当日成交量
        current_vol = data.volume[0]
        volumes.append(current_vol)
        
        # 保留最近 20 天
        if len(volumes) > 20:
            volumes.pop(0)
        
        avg_vol = sum(volumes) / len(volumes) if volumes else 0
        
        # 计算成交量占比
        size = abs(order.executed.remsize)
        if avg_vol > 0:
            vol_ratio = size / avg_vol
        else:
            vol_ratio = 0
        
        # 如果超过阈值，增加滑点
        if vol_ratio > self.max_volume_pct:
            extra = price * self.slip_perc * (vol_ratio / self.max_volume_pct - 1)
            if order.isbuy():
                return price + extra
            else:
                return price - extra
        
        return None  # 返回 None 表示使用默认价格


def check_limit_up_down(data, size: int, price: float, limit_pct: float = 0.10) -> bool:
    """
    辅助函数：检查订单是否涨跌停无法成交
    
    Args:
        data: Backtrader data feed
        size: 订单数量（正=买入，负=卖出）
        price: 订单价格
        limit_pct: 涨跌停比例
    
    Returns:
        bool: True=涨跌停无法成交，False=可以成交
    """
    if size == 0:
        return False
    
    try:
        prev_close = data.close[-1]  # 前一日收盘价
    except Exception:
        return False
    
    if prev_close <= 0:
        return False
    
    limit_price = prev_close * (1 + limit_pct)  # 涨停价
    floor_price = prev_close * (1 - limit_pct)  # 跌停价
    
    if size > 0 and price >= limit_price:
        # 涨停买入 → 不能成交
        return True
    
    if size < 0 and price <= floor_price:
        # 跌停卖出 → 不能成交
        return True
    
    return False


def get_limit_price(prev_close: float, direction: int, limit_pct: float = 0.10) -> float:
    """
    辅助函数：计算涨跌停价格
    
    Args:
        prev_close: 前一日收盘价
        direction: 1=涨停价, -1=跌停价
        limit_pct: 涨跌停比例
    
    Returns:
        float: 涨停或跌停价格
    """
    if direction > 0:
        return prev_close * (1 + limit_pct)
    else:
        return prev_close * (1 - limit_pct)


def is_limit_up(open_price: float, prev_close: float, limit_pct: float = 0.10) -> bool:
    """判断是否涨停"""
    if prev_close <= 0:
        return False
    return open_price >= prev_close * (1 + limit_pct)


def is_limit_down(open_price: float, prev_close: float, limit_pct: float = 0.10) -> bool:
    """判断是否跌停"""
    if prev_close <= 0:
        return False
    return open_price <= prev_close * (1 - limit_pct)
