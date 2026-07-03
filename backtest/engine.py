"""
Backtrader 回测引擎
===================
用于 Phase 3 真实回测

核心功能：
- 多股票数据加载
- A股佣金+印花税+过户费
- 涨跌停滑点处理
- T+1 持仓限制
- 事件驱动回测（非"收盘价视角"）
- 完整统计分析输出

用法:
    engine = BacktestEngine(initial_cash=100000)
    
    # 添加股票数据
    engine.add_data('000001.SZ', fromdate='2024-01-01', todate='2026-05-08')
    engine.add_data('000002.SZ', fromdate='2024-01-01', todate='2026-05-08')
    
    # 设置信号（Phase 2 输出）
    signals = {
        '000001.SZ': {'action': 'BUY', 'confidence': 75, 'date': '2026-04-20'},
        '000002.SZ': {'action': 'WATCH', 'confidence': 60},
    }
    
    # 运行回测
    result = engine.run(signals=signals, hold_days=5)
    
    print(result)
"""

import datetime
import json
import sys
import time
from typing import Dict, List, Optional, Any

import backtrader as bt

from .data_feeds import create_qmt_datafeed, fetch_qmt_kline
from .commission import AShareCommission
from .position import T1PositionTracker, T1PositionSizer


class BacktestEngine:
    """
    Backtrader 回测引擎
    
    用于 Phase 3 真实回测
    """
    
    def __init__(
        self,
        initial_cash: float = 100000.0,
        commission_rate: float = 0.0003,
        stamp_tax: float = 0.001,
        transfer_fee: float = 0.00002,
        min_commission: float = 5.0,
        slip_perc: float = 0.001,
        host: str = '127.0.0.1',
        port: int = 8080,
    ):
        """
        初始化回测引擎
        
        Args:
            initial_cash: 初始资金
            commission_rate: 券商佣金率（默认万一）
            stamp_tax: 印花税率（默认千分之一）
            transfer_fee: 过户费率（默认万分之0.2）
            min_commission: 最低佣金（默认5元）
            slip_perc: 滑点比例（默认0.1%）
            host: QMT HTTP 主机
            port: QMT HTTP 端口
        """
        self.cerebro = bt.Cerebro(stdstats=False)
        self.cerebro.broker.setcash(initial_cash)
        
        # 设置 A 股佣金（使用自定义佣金方案）
        comm_info = AShareCommission(
            commission=commission_rate,
            stamp_tax=stamp_tax,
            transfer_fee=transfer_fee,
            min_commission=min_commission,
        )
        self.cerebro.broker.addcommissioninfo(comm_info)
        
        self.initial_cash = initial_cash
        self.slip_perc = slip_perc
        self.host = host
        self.port = port
        
        # 数据源
        self.datas = {}  # {stock: data_feed}
        
        # T+1 追踪器
        self.t1_tracker = T1PositionTracker()
        
        # 结果
        self.result = {}
    
    def add_data(
        self,
        stock: str,
        fromdate: str = None,
        todate: str = None,
        period: str = '1d',
        count: int = 500,
    ) -> bool:
        """
        添加一只股票的数据源
        
        Args:
            stock: 股票代码（如 000001.SZ）
            fromdate: 开始日期
            todate: 结束日期
            period: K 线周期（1d=日线）
            count: 最多 K 线数量
        
        Returns:
            bool: 是否成功
        """
        try:
            data = create_qmt_datafeed(
                stock=stock,
                fromdate=fromdate,
                todate=todate,
                host=self.host,
                port=self.port,
                period=period,
                count=count,
            )
            if data is None:
                raise ValueError(f"无法加载 {stock} 数据")
            self.cerebro.adddata(data, name=stock)
            self.datas[stock] = data
            return True
        except Exception as e:
            print(f"添加数据失败 {stock}: {e}", flush=True)
            return False
    
    def add_strategy(self, strategy_class, *args, **kwargs):
        """添加回测策略"""
        self.cerebro.addstrategy(strategy_class, *args, **kwargs)
    
    def add_analyzers(self):
        """添加分析器"""
        self.cerebro.addanalyzer(bt.analyzers.SharpeRatio_A, _name='sharpe')
        self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        self.cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
        self.cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='annual')
        self.cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
        
        # 观察器
        self.cerebro.addobserver(bt.observers.Broker)
        self.cerebro.addobserver(bt.observers.Trades)
        self.cerebro.addobserver(bt.observers.BuySell)
    
    def run(self, signals: Dict[str, Dict], hold_days: int = 5) -> Dict[str, Any]:
        """
        运行回测
        
        Args:
            signals: Phase 2 信号
                {
                    '000001.SZ': {'action': 'BUY', 'confidence': 75},
                    '000002.SZ': {'action': 'WATCH', 'confidence': 60},
                }
            hold_days: 持仓天数
        
        Returns:
            dict: 回测结果
        """
        # 传递 T+1 追踪器给策略
        signals['_t1_tracker'] = self.t1_tracker
        
        # 添加策略
        strategy = self._create_strategy(signals, hold_days)
        self.cerebro.addstrategy(strategy)
        
        # 添加分析器
        self.add_analyzers()
        
        # 运行
        print(f"开始回测 {len(self.datas)} 只股票...", flush=True)
        tstart = time.time()
        results = self.cerebro.run()
        tend = time.time()
        
        # 提取结果
        strat = results[0]
        
        # 汇总
        final_value = self.cerebro.broker.getvalue()
        total_return = (final_value - self.initial_cash) / self.initial_cash * 100
        
        sharpe = strat.analyzers.sharpe.get_analysis()
        drawdown = strat.analyzers.drawdown.get_analysis()
        trades = strat.analyzers.trades.get_analysis()
        annual = strat.analyzers.annual.get_analysis()
        
        self.result = {
            'status': 'ok',
            'initial_cash': self.initial_cash,
            'final_value': final_value,
            'total_return': round(total_return, 2),
            'total_return_pct': f"{total_return:+.2f}%",
            'sharpe_ratio': sharpe.get('sharperatio'),
            'max_drawdown': drawdown.get('max', {}).get('drawdown', 0),
            'max_drawdown_money': drawdown.get('max', {}).get('moneydown', 0),
            'total_trades': trades.get('total', {}).get('total', 0),
            'closed_trades': trades.get('total', {}).get('closed', 0),
            'won_trades': trades.get('won', {}).get('total', 0),
            'lost_trades': trades.get('lost', {}).get('total', 0),
            'win_rate': round(
                trades.get('won', {}).get('total', 0) /
                max(trades.get('total', {}).get('closed', 1), 1) * 100, 1
            ),
            'avg_win': round(
                trades.get('won', {}).get('pnl', {}).get('average', 0), 2
            ),
            'avg_loss': round(
                trades.get('lost', {}).get('pnl', {}).get('average', 0), 2
            ),
            'annual_returns': {str(k): v for k, v in annual.items()},
            'elapsed_seconds': round(tend - tstart, 1),
            'stocks': list(self.datas.keys()),
            'per_stock': dict(strat._per_stock_trades),  # 每只股票独立交易记录
        }
        
        return self.result
    
    def _create_strategy(self, signals: Dict, hold_days: int):
        """动态创建策略类"""
        
        class SignalBacktestStrategy(bt.Strategy):
            """基于 Phase 2 信号的回测策略"""
            
            params = dict(
                signals=signals,
                hold_days=hold_days,
                verbose=True,
                t1_tracker=signals.get('_t1_tracker'),  # 引用外部 tracker
            )
            
            def __init__(self):
                self.order_refs = {}  # {stock: order_ref}
                self.hold_count = {}  # {stock: 已持仓天数}
                self.entry_price = {}  # {stock: 买入价}
                self.verbose = self.params.verbose
                
                # 引用 engine 的 T+1 追踪器
                self._t1 = self.params.t1_tracker
                
                # 构建每个 data 的信号（用 data._name 做匹配）
                self.stock_signals = {}
                for stock, signal in self.params.signals.items():
                    for data in self.datas:
                        if hasattr(data, '_name') and data._name == stock:
                            self.stock_signals[data._name] = signal
                            break
                
                # 每只股票的交易记录 {stock: {entry_date, entry_price, exit_date, exit_price, size, pnl, reason}}
                self._per_stock_trades = {}
            
            def log(self, txt, dt=None):
                if self.verbose:
                    dt = dt or self.datas[0].datetime.date(0)
                    print(f'[{dt}] {txt}', flush=True)
            
            def notify_order(self, order):
                if order.status in [order.Submitted, order.Accepted]:
                    return
                
                if order.status == order.Completed:
                    stock = order.data._name
                    if order.isbuy():
                        self.log(f'买入 {stock}: 价格={order.executed.price:.2f}, '
                                f'数量={order.executed.size}, '
                                f'手续费={order.executed.comm:.2f}')
                        # 记录 T+1
                        self._t1.record_buy(
                            stock,
                            self.datas[0].datetime.date(0),
                            order.executed.size
                        )
                        self.hold_count[stock] = 0
                        self.entry_price[stock] = order.executed.price
                        # 记录入场
                        self._per_stock_trades[stock] = {
                            'entry_date': str(self.datas[0].datetime.date(0)),
                            'entry_price': order.executed.price,
                            'size': order.executed.size,
                            'action': self.stock_signals.get(stock, {}).get('action', 'WATCH'),
                            'confidence': self.stock_signals.get(stock, {}).get('confidence', 0),
                        }
                    else:
                        self.log(f'卖出 {stock}: 价格={order.executed.price:.2f}, '
                                f'数量={order.executed.size}, '
                                f'手续费={order.executed.comm:.2f}')
                        pnl = order.executed.pnl
                        entry_price = self._per_stock_trades.get(stock, {}).get('entry_price', 0)
                        sell_size = abs(order.executed.size)
                        pnl_pct = (pnl / (entry_price * sell_size)) * 100 if entry_price > 0 and sell_size > 0 else 0
                        if pnl != 0:
                            self.log(f'  PnL: {pnl:.2f} ({pnl_pct:.2f}%)')
                        # 更新交易记录
                        if stock in self._per_stock_trades:
                            self._per_stock_trades[stock]['exit_date'] = str(self.datas[0].datetime.date(0))
                            self._per_stock_trades[stock]['exit_price'] = order.executed.price
                            self._per_stock_trades[stock]['pnl'] = round(pnl, 2)
                            self._per_stock_trades[stock]['return_pct'] = round(pnl_pct, 2)
                            self._per_stock_trades[stock]['pnl_pct'] = round(pnl_pct, 2)
                            self._per_stock_trades[stock]['exit_reason'] = 'sold'
                        # 清除持仓记录
                        if stock in self.hold_count:
                            del self.hold_count[stock]
                        if stock in self.entry_price:
                            del self.entry_price[stock]
                elif order.status in [order.Canceled, order.Margin, order.Rejected]:
                    self.log(f'订单失败: {order.Status[order.status]}')
                
                # 清除该股票的订单引用
                stock = order.data._name
                if stock in self.order_refs:
                    del self.order_refs[stock]
            
            def next(self):
                # 跳过预热期（指标未成熟）
                if len(self.datas[0]) < 5:
                    return
                
                current_date = self.datas[0].datetime.date(0)
                
                # 检查每只股票
                for data in self.datas:
                    stock = data._name
                    signal = self.stock_signals.get(stock)
                    
                    if signal is None:
                        continue
                    
                    action = signal.get('action', 'WATCH')
                    confidence = signal.get('confidence', 50)
                    simulate_buy = bool(signal.get('simulate_buy'))

                    # 当前持仓
                    pos = self.getposition(data)
                    has_position = pos.size > 0

                    if not has_position:
                        # 无持仓 → 检查买入信号
                        if simulate_buy or (action == 'BUY' and confidence >= 60):
                            # T+1 检查
                            if self._t1.can_sell(stock, current_date):
                                cash = self.broker.getcash()
                                price = data.close[0]
                                if price > 0 and cash > 0:
                                    position_ratio = signal.get('position_ratio')
                                    ratio = 0.20
                                    if simulate_buy and position_ratio not in (None, ''):
                                        try:
                                            raw_ratio = str(position_ratio).strip().rstrip('%')
                                            ratio = float(raw_ratio)
                                            if '%' in str(position_ratio) or ratio > 1:
                                                ratio = ratio / 100.0
                                            ratio = max(0.0, min(1.0, ratio))
                                        except (TypeError, ValueError):
                                            ratio = 0.20
                                    size = int(cash * ratio / price / 100) * 100  # 100股整数
                                    if size > 0:
                                        # 非 Top5 模拟模式沿用旧 BUY 置信度仓位逻辑
                                        if not simulate_buy and confidence >= 75:
                                            size = int(size * 1.5)  # 30% 仓位

                                        mode = 'Top5模拟' if simulate_buy else '买入信号'
                                        self.log(f'{mode} {stock} (action={action}, confidence={confidence}), size={size}')
                                        self.order_refs[stock] = self.buy(data=data, size=size)
                    else:
                        # 有持仓 → 检查持仓天数和止损止盈
                        hold = self.hold_count.get(stock, 0)
                        self.hold_count[stock] = hold + 1
                        
                        entry = self.entry_price.get(stock, pos.price)
                        current = data.close[0]
                        ret_pct = (current - entry) / entry * 100
                        
                        # 止损 -3%
                        if ret_pct <= -3.0:
                            self.log(f'止损 {stock}: 亏损 {ret_pct:.2f}%')
                            self.order_refs[stock] = self.close(data=data)
                        
                        # 止盈 +5%/+8%/+10%
                        elif ret_pct >= 10.0:
                            self.log(f'止盈(10%) {stock}: 盈利 {ret_pct:.2f}%')
                            self.order_refs[stock] = self.close(data=data)
                        elif ret_pct >= 8.0:
                            self.log(f'止盈(8%) {stock}: 盈利 {ret_pct:.2f}%')
                            self.order_refs[stock] = self.close(data=data)
                        elif ret_pct >= 5.0:
                            self.log(f'止盈(5%) {stock}: 盈利 {ret_pct:.2f}%')
                            self.order_refs[stock] = self.close(data=data)
                        
                        # 持仓超过 hold_days
                        elif hold >= self.params.hold_days:
                            self.log(f'到期卖出 {stock}: 持仓 {hold} 天, 收益 {ret_pct:.2f}%')
                            self.order_refs[stock] = self.close(data=data)
            
            def stop(self):
                final = self.broker.getvalue()
                self.log(f'回测结束. 最终市值: {final:.2f}')
        
        return SignalBacktestStrategy
    
    def plot(self):
        """绘图"""
        self.cerebro.plot()
    
    def save_result(self, path: str):
        """保存结果到 JSON"""
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.result, f, ensure_ascii=False, indent=2, default=str)
        print(f"结果已保存: {path}")
    
    def print_summary(self):
        """打印结果摘要"""
        if not self.result:
            print("未运行回测")
            return
        
        r = self.result
        print(f"\n{'='*50}")
        print(f"Backtrader 回测结果")
        print(f"{'='*50}")
        print(f"初始资金:   {r['initial_cash']:.2f}")
        print(f"最终市值:   {r['final_value']:.2f}")
        print(f"总收益率:   {r['total_return_pct']}")
        print(f"夏普比率:   {r['sharpe_ratio']}")
        print(f"最大回撤:   {r['max_drawdown']:.2f}%")
        print(f"总交易次数: {r['total_trades']}")
        print(f"盈利交易:  {r['won_trades']} ({r['win_rate']}%)")
        print(f"亏损交易:  {r['lost_trades']}")
        print(f"平均盈利:   {r['avg_win']:.2f}")
        print(f"平均亏损:   {r['avg_loss']:.2f}")
        print(f"回测耗时:   {r['elapsed_seconds']}s")
        per = r.get('per_stock', {})
        if per:
            print(f"\n每只股票交易记录:")
            for stock, t in per.items():
                ret = t.get('return_pct', t.get('pnl_pct', 0))
                exit_reason = t.get('exit_reason', 'unknown')
                print(f"  {stock}: 入场{t.get('entry_date')}@{t.get('entry_price')} -> "
                      f"出场{t.get('exit_date', '持仓中')}@{t.get('exit_price', 'N/A')} "
                      f"收益率{ret:+.2f}% 原因:{exit_reason}")
        print(f"{'='*50}")
