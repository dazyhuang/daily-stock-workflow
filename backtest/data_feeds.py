"""
Backtrader Data Feed: QMT HTTP API
===================================
从 QMT HTTP 服务器获取 K 线数据，转换为 Backtrader Data Feed

QMT HTTP API: http://127.0.0.1:8080/market_data
响应格式：
{
    "success": true,
    "data": {
        "open": {"20260429": {"000001.SZ": 11.43}, ...},
        "high": {...},
        "low": {...},
        "close": {...},
        "volume": {...}
    }
}
"""

import datetime
import os
from typing import Optional

import backtrader as bt
import requests


class QMTHTTPData(bt.feeds.PandasData):
    """
    从 QMT HTTP API 读取 K 线数据
    
    重写 __new__ 以在创建实例前获取数据
    """
    
    params = (
        ('host', '127.0.0.1'),
        ('port', 8080),
        ('period', '1d'),
        ('count', 500),
    )
    
    def __new__(cls, dataname=None, fromdate=None, todate=None, **kwargs):
        """
        在创建实例前先获取数据
        """
        import pandas as pd
        
        # 保存原始参数
        host = kwargs.get('host', cls.params.host)
        port = kwargs.get('port', cls.params.port)
        period = kwargs.get('period', cls.params.period)
        count = kwargs.get('count', cls.params.count)
        
        # 如果 dataname 是 DataFrame（标准 PandasData 用法），直接创建
        if isinstance(dataname, pd.DataFrame):
            return super().__new__(cls)
        
        # 否则 dataname 是股票代码，需要先获取数据
        stock = dataname
        
        # 获取数据
        df = _fetch_qmt_kline(stock, period, count, host, port)
        if df is None or df.empty:
            raise ValueError(f"无法从 QMT 获取数据: {stock}")
        
        # 过滤日期
        if fromdate:
            if isinstance(fromdate, str):
                fromdate = datetime.datetime.strptime(fromdate, '%Y-%m-%d')
            df = df[df.index >= fromdate]
        
        if todate:
            if isinstance(todate, str):
                todate = datetime.datetime.strptime(todate, '%Y-%m-%d')
            df = df[df.index <= todate]
        
        # 用 DataFrame 创建实例
        instance = super().__new__(cls)
        instance._loaded_df = df
        return instance
    
    def __init__(self, dataname=None, fromdate=None, todate=None, **kwargs):
        """
        初始化，传入已加载的 DataFrame
        """
        # 如果有预加载的 DataFrame，用它
        if hasattr(self, '_loaded_df') and self._loaded_df is not None:
            df = self._loaded_df
            # 调用父类，但先替换 params.dataname
            self.p.dataname = df
            super().__init__()
            return
        
        # 否则按标准 PandasData 方式初始化
        super().__init__()


def _fetch_qmt_kline(stock: str, period: str = '1d', count: int = 500,
                      host: str = '127.0.0.1', port: int = 8080) -> Optional[object]:
    """
    从 QMT HTTP API 获取 K 线，返回 DataFrame
    """
    import pandas as pd
    
    # 自动补后缀（QMT 要求带 .SH/.SZ，纯数字会被拒绝）
    if not any(stock.endswith(s) for s in ('.SH', '.SZ')):
        suffix = '.SH' if stock.startswith('6') else '.SZ'
        stock = stock + suffix
    
    try:
        url = f"http://{host}:{port}/market_data"
        resp = requests.get(url, params={
            'stock': stock,
            'fields': 'date,open,high,low,close,volume',
            'period': period,
            'count': count,
        }, timeout=15)
        
        resp.raise_for_status()
        raw = resp.json()
        
        if not raw.get('success'):
            print(f"QMT API 错误: {raw.get('message', 'unknown')}", flush=True)
            return None
        
        data = raw.get('data', {})
        if not data:
            return None
        
        open_d = data.get('open', {})
        high_d = data.get('high', {})
        low_d = data.get('low', {})
        close_d = data.get('close', {})
        volume_d = data.get('volume', {})
        
        all_dates = sorted(
            set(open_d.keys()) & set(high_d.keys()) & 
            set(low_d.keys()) & set(close_d.keys()) & set(volume_d.keys())
        )
        
        rows = []
        for date_str in all_dates:
            o = open_d.get(date_str, {}).get(stock)
            h = high_d.get(date_str, {}).get(stock)
            l = low_d.get(date_str, {}).get(stock)
            c = close_d.get(date_str, {}).get(stock)
            v = volume_d.get(date_str, {}).get(stock)
            
            if c is not None:
                rows.append({
                    'Open': o, 'High': h, 'Low': l,
                    'Close': c, 'Volume': v,
                    '_date_str': date_str,
                })
        
        if not rows:
            # QMT HTTP 返回空，尝试 xqshare 备源
            return _fetch_xqshare_kline(stock, period, count)
        
        df = pd.DataFrame(rows)
        df['datetime'] = pd.to_datetime(df['_date_str'], format='%Y%m%d')
        df.set_index('datetime', inplace=True)
        df.drop('_date_str', axis=1, inplace=True)
        df.sort_index(inplace=True)
        
        # 确保数值类型
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            else:
                df[col] = 0.0
        
        return df
    
    except Exception as e:
        print(f"_fetch_qmt_kline 失败 {stock}: {e}", flush=True)
        return None


def _ensure_suffix(code: str) -> str:
    """给股票代码加后缀
    - 6开头 → SH（上交所）
    - 其他 → SZ（深交所：主板/中小板/创业板）
    注意：指数（如000300）在 workflow.py 中通过 INDEX_SUFFIX 单独处理，这里不处理指数
    """
    code = code.strip().replace('.SH', '').replace('.SZ', '')
    if code.startswith('6'):
        return code + '.SH'
    return code + '.SZ'


def _fetch_xqshare_kline(stock: str, period: str = '1d', count: int = 500) -> Optional[object]:
    """
    xqshare 备源：通过 xqshare（18812端口）获取 K 线
    在 QMT HTTP API 返回空数据时调用
    """
    import pandas as pd
    
    try:
        import xqshare
        host = os.environ.get('XQSHARE_HOST', '127.0.0.1')
        port = int(os.environ.get('XQSHARE_PORT', '18812'))
        client = xqshare.connect(host, port, auto_reconnect=True, max_retries=2)
        xtdata = client.xtdata
        
        stock_suffix = _ensure_suffix(stock)
        
        data = xtdata.get_market_data(
            field_list=['open', 'high', 'low', 'close', 'volume'],
            stock_list=[stock_suffix],
            period=period,
            count=count,
            dividend_type='none',
        )
        
        xqshare.disconnect()
        
        if not isinstance(data, dict):
            return None
        
        close_df = data.get('close')
        open_df  = data.get('open')
        high_df  = data.get('high')
        low_df   = data.get('low')
        vol_df   = data.get('volume')
        
        if close_df is None or close_df.empty:
            return None
        
        # xqshare 返回: index=['300476.SZ'], columns=['20260506', '20260507', ...]
        # 需要按日期（列）迭代
        rows = []
        dates = close_df.columns.tolist()
        stock_idx = close_df.index[0]  # 如 '300476.SZ'
        
        for date_val in dates:
            try:
                c = float(close_df.loc[stock_idx, date_val])
                o = float(open_df.loc[stock_idx, date_val]) if open_df is not None else c
                h = float(high_df.loc[stock_idx, date_val]) if high_df is not None else c
                l = float(low_df.loc[stock_idx, date_val]) if low_df is not None else c
                v = float(vol_df.loc[stock_idx, date_val]) if vol_df is not None else 0
                rows.append({'Open': o, 'High': h, 'Low': l, 'Close': c, 'Volume': v, '_dt': date_val})
            except Exception:
                continue
        
        if not rows:
            return None
        
        df = pd.DataFrame(rows)
        df['datetime'] = pd.to_datetime(df['_dt'], format='%Y%m%d', errors='coerce')
        df.set_index('datetime', inplace=True)
        df.drop('_dt', axis=1, inplace=True)
        df.sort_index(inplace=True)
        
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            else:
                df[col] = 0.0
        
        return df
    
    except Exception as e:
        print(f"_fetch_xqshare_kline 失败 {stock}: {e}", flush=True)
        return None


def create_qmt_datafeed(stock: str, fromdate: str = None, todate: str = None,
                        host: str = '127.0.0.1', port: int = 8080,
                        period: str = '1d', count: int = 500) -> Optional[bt.feeds.PandasData]:
    """
    工厂函数：创建 QMT 数据源
    
    直接获取数据后创建 PandasData，避免继承问题
    
    Returns:
        bt.feeds.PandasData 实例，或 None（获取失败）
    """
    df = _fetch_qmt_kline(stock, period, count, host, port)
    if df is None:
        return None
    
    # 过滤日期
    if fromdate:
        if isinstance(fromdate, str):
            fromdate = datetime.datetime.strptime(fromdate, '%Y-%m-%d')
        df = df[df.index >= fromdate]
    
    if todate:
        if isinstance(todate, str):
            todate = datetime.datetime.strptime(todate, '%Y-%m-%d')
        df = df[df.index <= todate]
    
    data = bt.feeds.PandasData(
        dataname=df,
        datetime=None,  # 使用 index 作为 datetime
        open='Open',
        high='High',
        low='Low',
        close='Close',
        volume='Volume',
        openinterest=-1,
    )
    
    return data


# 保留旧的 QMTHTTPData 兼容接口
def QMTHTTPData_Compat(stock: str, fromdate: str = None, todate: str = None,
                        host: str = '127.0.0.1', port: int = 8080,
                        period: str = '1d', count: int = 500) -> Optional[bt.feeds.PandasData]:
    """
    兼容接口：用工厂函数替代类
    """
    return create_qmt_datafeed(stock, fromdate, todate, host, port, period, count)


# 导出便捷函数
def fetch_qmt_kline(stock: str, period: str = '1d', count: int = 500,
                    host: str = '127.0.0.1', port: int = 8080) -> Optional[dict]:
    """
    直接获取 QMT K 线数据（dict 格式）
    """
    df = _fetch_qmt_kline(stock, period, count, host, port)
    if df is None:
        return None
    
    return {
        'date': df.index.strftime('%Y%m%d').tolist(),
        'open': df['Open'].tolist(),
        'high': df['High'].tolist(),
        'low': df['Low'].tolist(),
        'close': df['Close'].tolist(),
        'volume': df['Volume'].tolist(),
    }


def is_limit_up(open_price: float, prev_close: float, limit: float = 0.1) -> bool:
    """判断是否涨停"""
    if prev_close <= 0:
        return False
    return open_price >= prev_close * (1 + limit)


def is_limit_down(open_price: float, prev_close: float, limit: float = 0.1) -> bool:
    """判断是否跌停"""
    if prev_close <= 0:
        return False
    return open_price <= prev_close * (1 - limit)
