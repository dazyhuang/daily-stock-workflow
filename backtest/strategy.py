"""
Backtrader 策略封装
==================
提供与现有 Phase 2/Phase 3 工作流对接的简单接口

用法:
    from backtest.strategy import run_signal_backtest
    
    # Phase 2 输出
    phase2_result = {
        'ranked_candidates': [
            {'stock': '000001.SZ', 'confidence': 75, 'final_decision': 'BUY'},
            {'stock': '000002.SZ', 'confidence': 60, 'final_decision': 'WATCH'},
        ]
    }
    
    # 运行回测
    result = run_signal_backtest(
        phase2_result=phase2_result,
        hold_days=5,
        initial_cash=100000,
    )
"""

import datetime
import json
import os
import re
from typing import Dict, List, Optional, Any

import backtrader as bt

from .engine import BacktestEngine
from .data_feeds import QMTHTTPData_Compat as QMTHTTPData


def parse_phase2_signals(phase2_result: Dict) -> Dict[str, Dict]:
    """
    从 Phase 2 结果解析信号
    
    Args:
        phase2_result: workflow.py run_phase2() 返回的 phase2_result
    
    Returns:
        {
            '000001.SZ': {'action': 'BUY', 'confidence': 75},
            '000002.SZ': {'action': 'WATCH', 'confidence': 60},
        }
    """
    signals = {}
    
    ranked = phase2_result.get('ranked_candidates', [])
    
    def parse_action(item: Dict) -> str:
        for key in ('signal', 'action'):
            sig = str(item.get(key) or '').upper()
            if sig in ('BUY', 'WATCH', 'AVOID'):
                return sig

        final_decision = item.get('final_decision', '')
        dec = final_decision or ''
        dec_upper = dec.upper()
        # 否定语境优先
        if any(neg in dec_upper for neg in ['不给BUY', '不支撑BUY', '不推荐BUY', '不建议BUY', '不足以BUY']):
            return 'WATCH'
        if dec_upper.strip() in ('BUY', 'WATCH', 'AVOID'):
            return dec_upper.strip()
        # 兼容旧格式、新结构化格式、冒号格式
        m = re.search(r'(?:\*\*最终信号\*\*|signal)\s*[=:：]\s*(BUY|WATCH|AVOID)', dec, re.IGNORECASE)
        if m:
            return m.group(1).upper()
        # 兜底：仓位建议 0% → AVOID
        if '仓位建议' in dec and '0%' in dec:
            return 'AVOID'
        return 'WATCH'

    def parse_confidence(item: Dict) -> float:
        for key in ('confidence', 'total_score', 'score'):
            value = item.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return 50.0
    
    for item in ranked:
        stock = item.get('stock')
        if not stock:
            continue
        
        # 只取 Top 5 BUY + WATCH
        action = parse_action(item)
        if action == 'AVOID':
            continue
        
        confidence = parse_confidence(item)
        signal_payload = {
            'action': action,
            'confidence': confidence,
            'simulate_buy': bool(item.get('simulate_buy')),
            'position_ratio': item.get('position_ratio'),
        }
        
        if stock not in signals:
            signals[stock] = signal_payload
        else:
            # 取更高置信度
            if confidence > signals[stock]['confidence']:
                signals[stock] = signal_payload
    
    # 排序并取 Top 5
    sorted_signals = sorted(
        signals.items(),
        key=lambda x: -(x[1]['confidence'] if x[1]['action'] == 'BUY' else x[1]['confidence'] * 0.8)
    )[:5]
    
    return dict(sorted_signals)


def run_signal_backtest(
    phase2_result: Dict,
    hold_days: int = 5,
    initial_cash: float = 100000.0,
    lookback_days: int = 60,
    qmt_host: str = '127.0.0.1',
    qmt_port: int = 8080,
    output_path: str = None,
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    运行基于 Phase 2 信号的回测
    
    Args:
        phase2_result: Phase 2 输出
        hold_days: 持仓天数
        initial_cash: 初始资金
        lookback_days: 回看天数（数据加载范围）
        qmt_host: QMT HTTP 主机
        qmt_port: QMT HTTP 端口
        output_path: 结果保存路径
        verbose: 是否打印详情
    
    Returns:
        回测结果字典
    """
    # 解析信号
    signals = parse_phase2_signals(phase2_result)
    
    if not signals:
        return {
            'status': 'no_signals',
            'message': 'Phase 2 无 BUY/WATCH 信号',
        }
    
    if verbose:
        print(f"\n=== Backtest 回测启动 ===")
        print(f"信号数量: {len(signals)}")
        for stock, sig in signals.items():
            print(f"  {stock}: {sig['action']} (confidence={sig['confidence']})")
    
    # 创建引擎。lookback_days 表示希望加载的交易数据点数量；
    # fromdate 用更长自然日窗口兜底，避免节假日导致 120 个交易日取不满。
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    calendar_lookback_days = int(os.getenv(
        "BACKTEST_CALENDAR_LOOKBACK_DAYS",
        str(max(lookback_days, int(lookback_days * 1.8) + 10)),
    ))
    fromdate = (datetime.datetime.now() - datetime.timedelta(days=calendar_lookback_days)).strftime('%Y-%m-%d')
    
    engine = BacktestEngine(
        initial_cash=initial_cash,
        commission_rate=0.0003,
        stamp_tax=0.001,
        transfer_fee=0.00002,
        min_commission=5.0,
        slip_perc=0.001,
        host=qmt_host,
        port=qmt_port,
    )
    
    # 添加数据
    success_count = 0
    for stock in signals.keys():
        ok = engine.add_data(
            stock=stock,
            fromdate=fromdate,
            todate=today,
            period='1d',
            count=lookback_days,
        )
        if ok:
            success_count += 1
    
    if success_count == 0:
        return {
            'status': 'no_data',
            'message': '无法加载任何股票数据',
        }
    
    if verbose:
        print(f"成功加载 {success_count}/{len(signals)} 只股票数据")
    
    # 运行
    result = engine.run(signals=signals, hold_days=hold_days)
    result["fromdate"] = fromdate
    result["todate"] = today
    result["lookback_days"] = lookback_days
    result["calendar_lookback_days"] = calendar_lookback_days
    
    # 打印摘要
    if verbose:
        engine.print_summary()
    
    # 保存
    if output_path:
        engine.save_result(output_path)
    
    return result


def quick_backtest_single_stock(
    stock: str,
    fromdate: str = None,
    todate: str = None,
    initial_cash: float = 100000.0,
    commission_rate: float = 0.0003,
    lookback_days: int = 60,
) -> Dict[str, Any]:
    """
    快速回测单只股票（用于测试数据源）
    
    Args:
        stock: 股票代码
        fromdate: 开始日期
        todate: 结束日期
        initial_cash: 初始资金
        commission_rate: 佣金率
        lookback_days: 回看天数
    
    Returns:
        回测结果
    """
    if fromdate is None:
        fromdate = (datetime.datetime.now() - datetime.timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    if todate is None:
        todate = datetime.datetime.now().strftime('%Y-%m-%d')
    
    engine = BacktestEngine(
        initial_cash=initial_cash,
        commission_rate=commission_rate,
        host='127.0.0.1',
        port=8080,
    )
    
    ok = engine.add_data(
        stock=stock,
        fromdate=fromdate,
        todate=todate,
        period='1d',
        count=lookback_days,
    )
    
    if not ok:
        return {'status': 'error', 'message': f'无法加载 {stock} 数据'}
    
    # 简单买入持有策略
    class BuyHold(bt.Strategy):
        def __init__(self):
            self.bought = False
            self.bought_bar = 0  # 买入时的 bar 索引
        
        def next(self):
            bar_idx = len(self.datas[0])
            
            if not self.bought and bar_idx > 10:
                self.buy(size=100)
                self.bought = True
                self.bought_bar = bar_idx
                print(f"[{self.datas[0].datetime.date(0)}] 买入 100 股")
            
            # 30 天后卖出
            if self.bought and bar_idx > self.bought_bar + 30:
                self.close()
                self.bought = False
                print(f"[{self.datas[0].datetime.date(0)}] 卖出")
    
    engine.cerebro.addstrategy(BuyHold)
    engine.add_analyzers()
    
    results = engine.cerebro.run()
    strat = results[0]
    
    final_value = engine.cerebro.broker.getvalue()
    total_return = (final_value - initial_cash) / initial_cash * 100
    
    return {
        'status': 'ok',
        'stock': stock,
        'fromdate': fromdate,
        'todate': todate,
        'initial_cash': initial_cash,
        'final_value': final_value,
        'total_return': round(total_return, 2),
        'total_return_pct': f"{total_return:+.2f}%",
    }


# 集成到 Phase 3 的 wrapper
def phase3_backtest_wrapper(phase2_result: Dict, phase3_config: Dict = None) -> Dict[str, Any]:
    """
    Phase 3 回测 wrapper，供 workflow.py 调用
    
    Args:
        phase2_result: Phase 2 输出
        phase3_config: 配置
            {
                'hold_days': 5,
                'initial_cash': 100000,
                'lookback_days': 60,
                'output_dir': './output/',
            }
    
    Returns:
        Phase 3 回测结果（格式兼容现有 workflow.py）
    """
    config = phase3_config or {}
    
    output_dir = config.get('output_dir', './output/')
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = f"{output_dir}/backtest_result_{datetime.datetime.now().strftime('%Y%m%d')}.json"
    
    result = run_signal_backtest(
        phase2_result=phase2_result,
        hold_days=config.get('hold_days', 5),
        initial_cash=config.get('initial_cash', 100000),
        lookback_days=config.get('lookback_days', 60),
        output_path=output_path,
        verbose=True,
    )
    
    # 兼容格式
    if result.get('status') == 'ok':
        return {
            'type': 'backtest',
            'status': 'done',
            'lookback_days': config.get('hold_days', 5),
            'result': result,
            'output_file': output_path,
        }
    else:
        return {
            'type': 'backtest',
            'status': result.get('status', 'error'),
            'message': result.get('message', '未知错误'),
        }


# 需要 os 模块
import os
