#!/usr/bin/env python3
"""测试run()方法的执行情况"""
import sys, json, time, logging, os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
if os.environ.get("RUN_LIVE_LLM_TESTS") != "1":
    print("skipped: set RUN_LIVE_LLM_TESTS=1 to run live StockDebateEngine test")
    raise SystemExit(0)

# Force unbuffered output
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, line_buffering=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

print('TEST 1: 导入模块', flush=True)
from stock_selection_debate.data_fetcher import load_phase1_cache, build_debate_packet
from stock_selection_debate import StockDebateEngine

print('TEST 2: 加载数据', flush=True)
cache = load_phase1_cache(Path('output'))
packets = [build_debate_packet('002463', '沪电股份', cache, [])]
print(f'数据包: {len(packets)} 只', flush=True)

print('TEST 3: 创建辩论引擎', flush=True)
debate = StockDebateEngine(model='minimax-portal/MiniMax-M3', max_debate_rounds=1)

print('TEST 4: 开始run()...', flush=True)
sys.stdout.flush()
t0 = time.time()

try:
    result = debate.run(packets)
    t1 = time.time()
    print(f'✅ run() 完成，耗时: {t1-t0:.1f}s', flush=True)
    print(f'BUY: {[c["stock"] for c in result.get("buy_list", [])]}', flush=True)
    print(f'WATCH: {[c["stock"] for c in result.get("watch_list", [])]}', flush=True)
except Exception as e:
    t1 = time.time()
    print(f'❌ run() 异常: {e}, 耗时: {t1-t0:.1f}s', flush=True)
    import traceback
    traceback.print_exc()

print('DONE', flush=True)
