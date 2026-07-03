#!/usr/bin/env python3
"""Super simple: just check if checkpoint_cb is ever called"""
import sys, json, os, signal
sys.path.insert(0, '.')
os.environ['FEISHU_WEBHOOK_URL'] = ''
if os.environ.get("RUN_LIVE_LLM_TESTS") != "1":
    print("skipped: set RUN_LIVE_LLM_TESTS=1 to run live StockDebateEngine test")
    raise SystemExit(0)
from stock_selection_debate import StockDebateEngine
from pathlib import Path

# Clear checkpoint
cp_file = Path('./output/debate_checkpoint_20260518.json')
with open(cp_file, 'w') as f:
    json.dump({'date': '20260518', 'completed': [], 'failed': [], 'results': {}}, f)

call_count = [0]
def cb(code, result):
    call_count[0] += 1
    sig = result.get('signal') if result else None
    print(f'>>> cb called #{call_count[0]}: {code} signal={sig}', flush=True)
    # Also update the file
    cp = json.loads(cp_file.read_text())
    if result:
        if code not in cp['completed']: cp['completed'].append(code)
        cp['results'][code] = result
        cp['failed'] = [f for f in cp.get('failed', []) if f != code]
    else:
        if code not in cp.get('failed', []): cp['failed'].append(code)
    with open(cp_file, 'w') as f:
        json.dump(cp, f)
    print(f'    file updated: completed={len(cp["completed"])}', flush=True)

# Single packet, simple data
packets = [{
    'stock_code': '000001', 'name': 'test',
    'kline_data': [], 'news_data': [], 'financial_data': {},
    'sector': '银行', 'market_cap': 2000, 'price': 11.5, 'change_pct': 1.2,
    'volume_ratio': 1.1, 'turnover_rate': 0.5, 'ddx': 0.5, 'ddy': 0.3,
}]

def timeout_handler(signum, frame):
    print(f'TIMEOUT after 60s! cb was called {call_count[0]} times', flush=True)
    # Print checkpoint state
    cp = json.loads(cp_file.read_text())
    print(f'  checkpoint: completed={len(cp["completed"])} failed={len(cp["failed"])} results={len(cp["results"])}', flush=True)
    raise SystemExit(1)

signal.signal(signal.SIGALRM, timeout_handler)
signal.alarm(60)

print('Starting engine.run() with checkpoint_cb...', flush=True)
sys.stdout.flush()

try:
    engine = StockDebateEngine(model='minimax-portal/MiniMax-M3', max_debate_rounds=1)
    print(f'Engine ready, calling run()...', flush=True)
    sys.stdout.flush()
    results = engine.run(packets, market_context='test', max_parallel=1, checkpoint_cb=cb)
    signal.alarm(0)
    print(f'COMPLETED: {len(results)} results, cb_called={call_count[0]} times', flush=True)
    for r in results:
        print(f'  {r.get("stock_name")} signal={r.get("signal")} conf={r.get("confidence")}', flush=True)
except SystemExit as e:
    print(f'Exited via signal handler', flush=True)
