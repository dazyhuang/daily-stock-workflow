"""
Backtrader T+1 持仓限制
======================
A股规则：当日买入的股票，当日不能卖出
"""

import datetime
import backtrader as bt


class T1PositionTracker:
    """
    T+1 持仓追踪器
    
    追踪每个股票的买入日期，只有 T+1 交易日之后才能卖出
    
    用法:
        tracker = T1PositionTracker()
        
        # 买入时记录
        tracker.record_buy('000001', datetime.date.today(), 1000)
        
        # 检查是否可以卖出
        if tracker.can_sell('000001', datetime.date.today()):
            # 可以卖出
            pass
        else:
            # T+1 未到，禁止卖出
            pass
    """
    
    def __init__(self):
        # {stock: [(buy_date, size), ...]}
        self._buys = {}  # type: dict[str, list[tuple]]
        self._next_trade_day = {}  # type: dict[str, datetime.date]
    
    def record_buy(self, stock: str, date: datetime.date, size: int):
        """记录一笔买入"""
        if stock not in self._buys:
            self._buys[stock] = []
        self._buys[stock].append((date, size))
        
        # 计算最早可卖出日期（T+1）
        # A股 T+1: 买入后下一个交易日才能卖
        next_day = self._next_trading_day(date)
        self._next_trade_day[stock] = next_day
    
    def can_sell(self, stock: str, date: datetime.date) -> bool:
        """检查是否可以卖出"""
        next_day = self._next_trade_day.get(stock)
        if next_day is None:
            # 从未买入过，可以卖出（平仓）
            return True
        return date >= next_day
    
    def get_buy_quantity(self, stock: str, date: datetime.date) -> int:
        """
        获取在指定日期可以卖出的数量
        （即 T+1 到期的那部分持仓）
        """
        if stock not in self._buys:
            return 0
        
        next_day = self._next_trade_day.get(stock)
        if next_day is None:
            return 0
        
        if date < next_day:
            return 0
        
        # T+1 已到，返回历史买入总量
        total = sum(size for buy_date, size in self._buys[stock])
        return total
    
    def _next_trading_day(self, date: datetime.date) -> datetime.date:
        """计算下一个交易日（A simple implementation）"""
        # 简化版：只跳过周末
        next_day = date + datetime.timedelta(days=1)
        while next_day.weekday() >= 5:  # 周六=5, 周日=6
            next_day += datetime.timedelta(days=1)
        return next_day
    
    def get_total_bought(self, stock: str) -> int:
        """获取某股票总买入数量"""
        if stock not in self._buys:
            return 0
        return sum(size for _, size in self._buys[stock])
    
    def get_next_trade_day(self, stock: str) -> datetime.date:
        """获取某股票下一个可交易日"""
        return self._next_trade_day.get(stock)


class T1PositionSizer(bt.Sizer):
    """
    T+1 仓位 Size 计算器
    
    在下单前检查 T+1 限制，返回实际可卖出数量
    """
    
    params = dict(
        tracker=None,  # T1PositionTracker 实例
    )
    
    def _getsizing(self, broker, data, osize, size, cash):
        """
        返回可交易的股数
        
        Args:
            broker: cerebro.broker
            data: 当前数据源
            osize: 当前持仓
            size: 信号要求的交易数量
            cash: 可用资金
        
        Returns:
            int: 实际可交易的数量（考虑 T+1）
        """
        if self.p.tracker is None:
            return size
        
        stock = data._name
        current_date = data.datetime.date(0)
        
        if size < 0:  # 卖出
            # 检查 T+1
            if not self.p.tracker.can_sell(stock, current_date):
                # T+1 未到，禁止卖出
                return 0
            
            # 允许卖出的数量 = 当前持仓
            # （简化：假设历史买入的都已T+1）
            return min(abs(size), abs(osize))
        
        # 买入：正常计算
        if size == 0:
            return 0
        
        return min(abs(size), abs(osize))


# T1OrderNotifier 已移除，请使用 BacktestEngine 或直接在 Strategy 中处理
