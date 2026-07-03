#!/usr/bin/env python3
"""
直接测试辩论引擎 + checkpoint callback
验证：1. history 前导 \n 修复  2. checkpoint 写入正常
"""
import sys, os, json, time, threading
sys.path.insert(0, '.')
from pathlib import Path
if os.environ.get("RUN_LIVE_LLM_TESTS") != "1":
    print("skipped: set RUN_LIVE_LLM_TESTS=1 to run live StockDebateEngine checkpoint test")
    raise SystemExit(0)
from stock_selection_debate import StockDebateEngine

OUTPUT_DIR = Path('./output')
cp_file = OUTPUT_DIR / 'debate_checkpoint_20260518.json'

# 清空 checkpoint
with open(cp_file, 'w') as f:
    json.dump({'date': '20260518', 'completed': [], 'failed': [], 'results': {}}, f)
print(f'checkpoint cleared')

def load_checkpoint():
    with open(cp_file) as f:
        return json.load(f)

def save_checkpoint(cp):
    tmp = cp_file.with_name(cp_file.name + '.tmp')
    with open(tmp, 'w') as f:
        json.dump(cp, f, ensure_ascii=False)
        f.flush()
    tmp.replace(cp_file)

_lock = threading.Lock()
_count = [0]
_written = []

def checkpoint_cb(code, result):
    with _lock:
        _count[0] += 1
        cp = load_checkpoint()
        if result:
            if code not in cp['completed']:
                cp['completed'].append(code)
            cp['results'][code] = result
            cp['failed'] = [f for f in cp.get('failed', []) if f != code]
        else:
            if code not in cp.get('failed', []):
                cp['failed'].append(code)
        save_checkpoint(cp)
        size = cp_file.stat().st_size
        _written.append((_count[0], code, result.get('signal') if result else None, size))
        print(f'  [{_count[0]}] {code} signal={result.get("signal") if result else None} size={size}')

# 用 000001 平安银行（已有缓存数据）
engine = StockDebateEngine(model='minimax-portal/MiniMax-M3', max_debate_rounds=1)

test_packet = {
    'stock_code': '000001',
    'name': '平安银行',
    'kline_data': [{'date': '20260515', 'close': 11.5, 'volume': 1234567, 'open': 11.3, 'high': 11.7, 'low': 11.2}],
    'news_data': [{'title': '银行板块上涨', 'content': '公司业绩良好', 'date': '20260515'}],
    'financial_data': {'pe': 8.5, 'pb': 0.9, 'roe': 10.2, 'revenue_growth': 5.2},
    'sector': '银行',
    'market_cap': 2000,
    'price': 11.5,
    'change_pct': 1.2,
    'volume_ratio': 1.1,
    'turnover_rate': 0.5,
    'ddx': 0.5, 'ddy': 0.3,
}
market_ctx = 'A股今日震荡，沪指涨0.5%，银行板块小幅上涨'

print('Running debate on 1 stock with checkpoint callback...')
start = time.time()
result = engine.run(
    debate_packets=[test_packet],
    market_context=market_ctx,
    max_parallel=1,
    checkpoint_cb=checkpoint_cb,
)
elapsed = time.time() - start

print(f'\nDebate done in {elapsed:.1f}s')
print(f'Result: signal={result[0].get("signal")} conf={result[0].get("confidence")}')
print(f'Final decision: {result[0].get("final_decision", "")[:100]}')

# Verify checkpoint
with open(cp_file) as f:
    cp = json.load(f)
print(f'\nCheckpoint final: completed={len(cp["completed"])} results={len(cp["results"])} size={cp_file.stat().st_size}')
print(f'checkpoint_cb calls: {len(_written)}')
for i, code, sig, size in _written:
    print(f'  call {i}: {code} -> {sig} (file size={size})')
