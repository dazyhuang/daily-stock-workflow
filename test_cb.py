#!/usr/bin/env python3
import sys, json, time, os
sys.path.insert(0, '.')
os.environ['FEISHU_WEBHOOK_URL'] = ''
if os.environ.get("RUN_LIVE_LLM_TESTS") != "1":
    print("skipped: set RUN_LIVE_LLM_TESTS=1 to run live StockDebateEngine test")
    raise SystemExit(0)
from stock_selection_debate import StockDebateEngine
from pathlib import Path

cp_file = Path('./output/debate_checkpoint_20260518.json')
with open(cp_file, 'w') as f:
    json.dump({'date': '20260518', 'completed': [], 'failed': [], 'results': {}}, f)

calls = []
def cb(code, result):
    sig = result.get('signal') if result else None
    calls.append((code, sig))
    # Write to file immediately
    cp = json.loads(cp_file.read_text())
    if result:
        cp['completed'].append(code)
        cp['results'][code] = result
    else:
        cp['failed'].append(code)
    cp_file.write_text(json.dumps(cp, ensure_ascii=False))
    print(f'>>> cb: {code} signal={sig} size={cp_file.stat().st_size}', flush=True)

packets = [{
    'stock_code': '000001', 'name': 'test',
    'kline_data': [], 'news_data': [], 'financial_data': {},
    'sector': '银行', 'market_cap': 2000, 'price': 11.5, 'change_pct': 1.2,
    'volume_ratio': 1.1, 'turnover_rate': 0.5, 'ddx': 0.5, 'ddy': 0.3,
}]

print('Starting engine.run()...', flush=True)
engine = StockDebateEngine(model='minimax-portal/MiniMax-M3', max_debate_rounds=1)
results = engine.run(packets, market_context='test', max_parallel=1, checkpoint_cb=cb)
print(f'done: {len(results)} results, cb_called={len(calls)}', flush=True)
print(f'cp size: {cp_file.stat().st_size}', flush=True)
