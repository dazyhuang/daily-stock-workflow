#!/usr/bin/env python3
"""
Minimal debug: run debate on exactly 2 stocks, watch checkpoint calls.
"""
import sys, os, json, time, threading

sys.path.insert(0, '.')
os.environ['FEISHU_WEBHOOK_URL'] = ''  # suppress feishu push
if os.environ.get("RUN_LIVE_LLM_TESTS") != "1":
    print("skipped: set RUN_LIVE_LLM_TESTS=1 to run live StockDebateEngine test")
    raise SystemExit(0)

from stock_selection_debate import StockDebateEngine
from pathlib import Path

OUTPUT_DIR = Path('./output')
cp_file = OUTPUT_DIR / 'debate_checkpoint_20260518.json'

# Clear checkpoint
with open(cp_file, 'w') as f:
    json.dump({'date': '20260518', 'completed': [], 'failed': [], 'results': {}}, f)
print(f'Cleared checkpoint: {cp_file}')

_lock = threading.Lock()
_calls = []

def checkpoint_cb(code, result):
    with _lock:
        _calls.append((code, result.get('signal') if result else None, time.time()))
        cp = {'date': '20260518', 'completed': [], 'failed': [], 'results': {}}
        try:
            with open(cp_file) as f:
                cp = json.load(f)
        except: pass
        if result:
            if code not in cp['completed']:
                cp['completed'].append(code)
            cp['results'][code] = result
            cp['failed'] = [f for f in cp.get('failed', []) if f != code]
            sig = result.get('signal')
        else:
            if code not in cp.get('failed', []):
                cp['failed'].append(code)
            sig = None
        tmp = cp_file.with_name(cp_file.name + '.tmp')
        with open(tmp, 'w') as f:
            json.dump(cp, f, ensure_ascii=False)
        tmp.replace(cp_file)
        size = cp_file.stat().st_size
        print(f'  >>> checkpoint_cb[{len(_calls)}] {code} signal={sig} size={size}')

engine = StockDebateEngine(model='minimax-portal/MiniMax-M3', max_debate_rounds=1)

packets = [
    {
        'stock_code': '000001', 'name': '平安银行',
        'kline_data': [{'date': '20260515', 'close': 11.5, 'volume': 1234567, 'open': 11.3, 'high': 11.7, 'low': 11.2}],
        'news_data': [{'title': '银行板块上涨', 'content': '公司业绩良好', 'date': '20260515'}],
        'financial_data': {'pe': 8.5, 'pb': 0.9, 'roe': 10.2, 'revenue_growth': 5.2},
        'sector': '银行', 'market_cap': 2000, 'price': 11.5, 'change_pct': 1.2,
        'volume_ratio': 1.1, 'turnover_rate': 0.5, 'ddx': 0.5, 'ddy': 0.3,
    },
    {
        'stock_code': '000002', 'name': '万科A',
        'kline_data': [{'date': '20260515', 'close': 8.5, 'volume': 2345678, 'open': 8.3, 'high': 8.7, 'low': 8.2}],
        'news_data': [{'title': '房地产政策', 'content': '政策支持房地产', 'date': '20260515'}],
        'financial_data': {'pe': 12.1, 'pb': 1.1, 'roe': 8.5, 'revenue_growth': 3.2},
        'sector': '房地产', 'market_cap': 1500, 'price': 8.5, 'change_pct': 2.1,
        'volume_ratio': 1.3, 'turnover_rate': 0.8, 'ddx': 0.4, 'ddy': 0.2,
    },
]

print(f'Running debate on {len(packets)} stocks, max_parallel=1')
print(f'Checkpoint file: {cp_file}')
start = time.time()
results = engine.run(
    packets,
    market_context='A股今日震荡偏强',
    max_parallel=1,
    checkpoint_cb=checkpoint_cb,
)
elapsed = time.time() - start

print(f'\nDone in {elapsed:.1f}s')
print(f'checkpoint_cb called {len(_calls)} times')
print(f'Results: {len(results)} stocks')
for r in results:
    print(f'  {r.get("stock_name")} signal={r.get("signal")} conf={r.get("confidence")}')

with open(cp_file) as f:
    cp = json.load(f)
print(f'\nFinal checkpoint: completed={len(cp["completed"])} failed={len(cp["failed"])} results={len(cp["results"])}')
