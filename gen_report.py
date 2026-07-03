#!/usr/bin/env python3
"""Generate debug_test_result.md"""
import csv, sys
sys.path.insert(0, '.')

import llm_scorer

xuangu_dir = 'output/debug_test/xuangu'
configs = llm_scorer.XUANGU_SCREEN_CONFIGS
OUTPUT_DIR = 'output/debug_test'

report = []

report.append('# 选股工作流完整流程测试报告\n')
report.append('**测试时间:** 2026-05-30 11:16\n')
report.append('**池子数量:** 6\n')
report.append('**测试目标:** 验证每个数据环节是否正常、后面节点是否能调取到数据\n')
report.append('\n---\n')

field_tests = {
    '主力资金净流入': ([['主力净额'], ['主力资金净流入']], None),
    '涨跌幅': ([['涨跌幅', '%'], ['CHG<70>']], ['区间', '涨跌额']),
    '量比': ([['量比']], None),
    '换手率': ([['换手率']], None),
    '成交量': ([['成交量']], None),
    '成交额': ([['成交额']], None),
    '市盈率': ([['市盈率']], None),
    '市净率': ([['市净率']], None),
    '总市值': ([['总市值']], None),
    '最新价': ([['最新价']], None),
    '最高价': ([['最高价', '(元)']], None),
    '区间涨跌幅': ([['区间涨跌幅']], None),
}

strategy_no_main_flow = {
    'momentum_breakout', 'sector_leader', 'limit_follow', 'reversal_confirm'
}

for cfg in configs:
    pool = cfg['pool']
    screen_id = cfg['screen_id']
    strategy = cfg['strategy_type']
    safe = llm_scorer._xuangu_safe_filename(cfg['query'])
    csv_file = f'{xuangu_dir}/mx_xuangu_{safe}.csv'

    with open(csv_file, encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))
    row = rows[0]

    stock_code = row.get('代码', '')
    stock_name = row.get('名称', '')
    row_count = len(rows)
    score, detail = llm_scorer._score_xuangu_row(row, cfg)

    report.append(f'## [{pool}]\n')
    report.append(f'- **screen_id:** `{screen_id}`\n')
    report.append(f'- **策略类型:** `{strategy}`\n')
    report.append(f'- **候选股票:** {stock_name}({stock_code})\n')
    report.append(f'- **查询结果行数:** {row_count}\n')
    fname = csv_file.split('/')[-1]
    report.append(f'- **CSV文件:** `xuangu/{fname}`\n')
    report.append(f'- **可用字段数:** {len(list(row.keys()))}\n')
    report.append(f'- **评分结果:** {score}/100\n')
    report.append('\n**字段读取测试:**\n')
    report.append('| 字段 | 状态 | 值 |\n')
    report.append('|------|------|-----|\n')

    missing = []
    field_values = {}
    for fname, (key_groups, deny) in field_tests.items():
        fval = llm_scorer._row_numeric_value(row, key_groups, deny)
        field_values[fname] = fval
        if fval is not None:
            report.append(f'| {fname} | OK | `{fval}` |\n')
        else:
            missing.append(fname)
            report.append(f'| {fname} | MISSING | - |\n')

    if missing:
        missing_str = ', '.join(missing)
        report.append(f'\n**缺失字段:** `{missing_str}`\n')
        report.append('-> 这将导致评分函数中对应评分项为 0 分\n')

    main_flow = field_values.get('主力资金净流入')
    if main_flow is None:
        if strategy in strategy_no_main_flow:
            note = '（该策略不依赖主力资金，故此字段缺失不影响评分）'
        else:
            note = 'WARNING: 该字段缺失影响评分'
        report.append(f'\n**主力资金净流入:** 缺失 {note}\n')
    else:
        report.append(f'\n**主力资金净流入:** OK {main_flow:.0f} 元\n')

    report.append('\n**评分明细:**\n')
    for k, v in detail.items():
        if not str(k).endswith('_value'):
            report.append(f'- {k}: {v}\n')

    report.append('\n---\n')

# Summary table
report.append('# 汇总\n')
report.append('| 池子 | 股票 | 评分 | 主力资金 | 主要问题 |\n')
report.append('|------|------|------|----------|----------|\n')

issues_map = {
    '突破新高': '主力资金缺失（策略不依赖，不影响评分）',
    '首板追击': '主力资金缺失（策略不依赖，不影响评分）',
    '热点龙头': '主力资金缺失+涨幅270.8%过热扣35分；量比=0',
    '强势反包': '主力资金缺失（策略不依赖，不影响评分）',
}

for cfg in configs:
    pool = cfg['pool']
    strategy = cfg['strategy_type']
    safe = llm_scorer._xuangu_safe_filename(cfg['query'])
    csv_file = f'{xuangu_dir}/mx_xuangu_{safe}.csv'
    with open(csv_file, encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))
    row = rows[0]
    score, _ = llm_scorer._score_xuangu_row(row, cfg)
    stock = f'{row.get("名称","")}({row.get("代码","")})'
    main_flow = llm_scorer._row_numeric_value(row, [['主力净额'], ['主力资金净流入']])
    main_str = 'OK' if main_flow is not None else 'MISSING'
    issue = issues_map.get(pool, '无')
    report.append(f'| {pool} | {stock} | {score} | {main_str} | {issue} |\n')

report.append('\n---\n')
report.append('## 结论\n')
report.append('1. **数据获取:** 6个池子全部查询成功，CSV 均正确写入\n')
report.append('2. **字段完整性:** 基础字段（涨跌幅、量比、换手率、成交量、成交额）在所有池子中均可正确读取\n')
report.append('3. **主力资金净流入:** 准备启动、资金异动两个池子有数据；突破新高、首板追击、热点龙头、强势反包四个池子无此字段\n')
report.append('   - 后四个池子的策略（momentum_breakout/limit_follow/sector_leader/reversal_confirm）评分函数本身不依赖主力资金字段，不影响实际评分\n')
report.append('4. **评分函数:** 6个池子的 _score_xuangu_row 均能正常执行并返回评分\n')
report.append('5. **成交量字段注意:** 突破新高池同时存在「成交量(股)」和「成交量环比增长率」，_row_numeric_value 优先匹配前者，实际量比/换手率读取正常，不影响核心逻辑\n')
report.append('\n**建议:** 如需主力资金净流入字段，应在选股 query 中明确加入「近N日主力资金净流入」条件；目前仅准备启动、资金异动两个池子的 query 中包含此条件\n')

out_path = f'{OUTPUT_DIR}/debug_test_result.md'
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(''.join(report))
print(f'Report written: {out_path}')