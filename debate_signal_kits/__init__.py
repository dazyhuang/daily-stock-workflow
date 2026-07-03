"""
辩论信号套件
提供蜡烛图、量价背离、海龟突破、艾略特波浪、江恩回调位等信号检测
"""
from .candlestick_signals import detect_candlestick_signals
from .volume_price_divergence import detect_volume_price_divergence as vp_detect
from .turtle_signals import detect_turtle_signals
from .elliott_signals import detect_elliott_position
from .gann_signals import detect_gann_levels

__all__ = [
    "detect_candlestick_signals",
    "vp_detect",
    "detect_turtle_signals",
    "detect_elliott_position",
    "detect_gann_levels",
]
