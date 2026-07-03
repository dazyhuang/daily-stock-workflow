#!/usr/bin/env python3
"""Minimal test: does debate_engine.run() actually call checkpoint_cb?"""
import sys, time, json
import os
sys.path.insert(0, '.')
if os.environ.get("RUN_LIVE_LLM_TESTS") != "1":
    print("skipped: set RUN_LIVE_LLM_TESTS=1 to run live StockDebateEngine checkpoint test")
    raise SystemExit(0)

from stock_selection_debate import StockDebateEngine

calls = []

def my_checkpoint_cb(code, result):
    calls.append((code, result.get('signal') if result else None, time.time()))
    print(f"  >>> checkpoint_cb called: {code} signal={result.get('signal') if result else None}")

# Use 3 stocks with minimal data
test_packets = [
    {
        'stock_code': '000001',
        'name': '平安银行',
        'kline_data': [{'date': '20260515', 'close': 11.5, 'volume': 1234567, 'open': 11.3, 'high': 11.7, 'low': 11.2}],
        'news_data': [{'title': '银行板块上涨', 'content': '公司业绩良好', 'date': '20260515'}],
        'financial_data': {'pe': 8.5, 'pb': 0.9, 'roe': 10.2, 'revenue_growth': 5.2},
        'sector': '银行', 'market_cap': 2000, 'price': 11.5, 'change_pct': 1.2,
        'volume_ratio': 1.1, 'turnover_rate': 0.5, 'ddx': 0.5, 'ddy': 0.3,
    },
    {
        'stock_code': '000002',
        'name': '万科A',
        'kline_data': [{'date': '20260515', 'close': 8.5, 'volume': 2345678, 'open': 8.3, 'high': 8.7, 'low': 8.2}],
        'news_data': [{'title': '房地产政策利好', 'content': '政策支持房地产', 'date': '20260515'}],
        'financial_data': {'pe': 12.1, 'pb': 1.1, 'roe': 8.5, 'revenue_growth': 3.2},
        'sector': '房地产', 'market_cap': 1500, 'price': 8.5, 'change_pct': 2.1,
        'volume_ratio': 1.3, 'turnover_rate': 0.8, 'ddx': 0.4, 'ddy': 0.2,
    },
]

engine = StockDebateEngine(model='minimax-portal/MiniMax-M3', max_debate_rounds=1)
print(f"Starting debate on {len(test_packets)} stocks with checkpoint callback...")
start = time.time()
results = engine.run(test_packets, market_context='A股今日震荡偏强', max_parallel=1, checkpoint_cb=my_checkpoint_cb)
elapsed = time.time() - start

print(f"\nDebate done in {elapsed:.1f}s, got {len(results)} results")
print(f"checkpoint_cb was called {len(calls)} times")
for code, sig, ts in calls:
    print(f"  {code} -> {sig} at {ts-start:.1f}s")
print(f"\nResults:")
for r in results:
    print(f"  {r.get('stock_name')} signal={r.get('signal')} conf={r.get('confidence')}")
