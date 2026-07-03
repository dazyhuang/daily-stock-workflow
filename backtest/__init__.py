# Backtest Engine for Phase 3
from .engine import BacktestEngine
from .data_feeds import QMTHTTPData_Compat, create_qmt_datafeed, fetch_qmt_kline
from .commission import AShareCommission
from .slippage import (
    set_slippage_perc,
    set_slippage_fixed,
    VolumeFiller,
    check_limit_up_down,
    get_limit_price,
    is_limit_up,
    is_limit_down,
)
from .position import T1PositionTracker
from .strategy import (
    parse_phase2_signals,
    run_signal_backtest,
    quick_backtest_single_stock,
    phase3_backtest_wrapper,
)

__all__ = [
    # 核心引擎
    'BacktestEngine',
    
    # 数据源
    'QMTHTTPData',
    'fetch_qmt_kline',
    
    # 佣金
    'AShareCommission',
    
    # 滑点
    'set_slippage_perc',
    'set_slippage_fixed',
    'VolumeFiller',
    'check_limit_up_down',
    'get_limit_price',
    'is_limit_up',
    'is_limit_down',
    
    # T+1
    'T1PositionTracker',
    
    # 策略
    'parse_phase2_signals',
    'run_signal_backtest',
    'quick_backtest_single_stock',
    'phase3_backtest_wrapper',
]
