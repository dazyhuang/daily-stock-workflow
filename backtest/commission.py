"""
Backtrader A股佣金方案
=====================
A股交易费用构成：
- 印花税：0.1%（仅卖出收取，ETF也收）
- 过户费：0.002%（双向收取，上海收）
- 券商佣金：默认 0.03%（万一，默认最低5元）
"""

import backtrader as bt


class AShareCommission(bt.CommissionInfo):
    """
    A 股完整佣金计算
    
    默认参数：
    - 印花税: 0.1%（仅卖出）
    - 过户费: 0.002%（双向）
    - 券商佣金: 0.03%（万一）
    - 最低佣金: 5 元
    
    用法:
        cerebro.broker.setcommission(commission=0.0003)  # 万三佣金
        # 或
        cerebro.broker.addcommissioninfo(AShareCommission(
            commission=0.0003,
            stamp_tax=0.001,
            transfer_fee=0.00002,
        ))
    """
    
    params = dict(
        commission=0.0003,     # 券商佣金 0.03%（万一）
        stamp_tax=0.001,       # 印花税 0.1%（仅卖出）
        transfer_fee=0.00002,  # 过户费 0.002%（双向）
        min_commission=5.0,    # 最低佣金 5 元
        # 以下为 bt.CommissionInfo 内部参数
        mult=1.0,             # 乘数（期货用）
        margin=0.0,          # 保证金（股票为0）
        percabs=True,         # True=绝对值, False=百分比
        commtype=0,          # 0=按比例, 其他值见 bt.CommissionInfo
        stocklike=True,       # 股票模式
    )
    
    def _getcommission(self, size: float, price: float, cash: float = None, **kwargs) -> float:
        """
        计算单笔交易佣金
        
        Args:
            size: 成交数量（正=买入，负=卖出）
            price: 成交价格
            cash: 可用资金（未使用）
        
        Returns:
            float: 该笔交易的总佣金
        """
        trade_value = abs(size * price)
        
        # 基础佣金（双向收取）
        commission = max(self.p.min_commission, trade_value * self.p.commission)
        
        # 印花税（仅卖出收取）
        if size < 0:
            commission += trade_value * self.p.stamp_tax
        
        # 过户费（双向收取，深市/沪市都收）
        # 注：实际上海有过户费包含在券商佣金中的情况，这里简化处理
        commission += trade_value * self.p.transfer_fee * 2
        
        return commission


class FutureCommission(bt.CommissionInfo):
    """
    期货佣金方案（备用）
    
    - 手续费：按合约价值比例
    - 保证金：杠杆
    """
    
    params = dict(
        commission=0.00003,    # 手续费比例（万三）
        stamp_tax=0.0,        # 期货无印花税
        transfer_fee=0.0,     # 期货无过户费
        min_commission=10.0,  # 最低佣金
        mult=10.0,           # 乘数（10倍杠杆）
        margin=2000.0,       # 保证金
        percabs=True,
        commtype=0,
        stocklike=False,      # 期货模式
    )
    
    def _getcommission(self, size: float, price: float, cash: float = None, **kwargs) -> float:
        trade_value = abs(size * price) * self.p.mult
        commission = max(self.p.min_commission, trade_value * self.p.commission)
        if size < 0:
            commission += trade_value * self.p.stamp_tax
        commission += trade_value * self.p.transfer_fee * 2
        return commission


def get_commission_info(commission_rate: float = 0.0003,
                        stamp_tax: float = 0.001,
                        transfer_fee: float = 0.00002,
                        min_commission: float = 5.0) -> AShareCommission:
    """
    工厂函数：创建 A 股佣金方案
    
    Args:
        commission_rate: 券商佣金率（默认 0.0003 = 万三）
        stamp_tax: 印花税率（默认 0.001 = 千分之一）
        transfer_fee: 过户费率（默认 0.00002 = 万分之0.2）
        min_commission: 最低佣金（默认 5 元）
    
    Returns:
        AShareCommission 实例
    """
    return AShareCommission(
        commission=commission_rate,
        stamp_tax=stamp_tax,
        transfer_fee=transfer_fee,
        min_commission=min_commission,
    )
