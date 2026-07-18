"""Single source for Portfolio Manager structured-output field instructions."""

PM_FIELD_LINES = [
    "- signal: BUY、WATCH 或 AVOID",
    "- buy_score: 0 到 100 的整数，表示未来1-3个交易日短线做多吸引力",
    "- confidence: 0 到 100 的整数，表示本次裁决可靠程度",
    "- position_ratio: 0.0 到 1.0 的数字；兼容字段，盘中实际仓位另算",
    "- allow_direct_buy: true/false；只有不需要盘中确认、可直接进入买入口径时才为 true",
    "- needs_intraday_confirmation: true/false；需要盘中放量突破、回踩承接、均线确认、资金确认等条件时为 true",
    "- entry_condition: 简短写清盘中买入条件，例如“开盘强势可买”“放量突破昨日高点”“回踩承接确认”",
    "- block_buy_reason: 若分数较强但不允许直接BUY，写清阻断理由；没有则空字符串",
    "- reason: 2-3句话的核心理由",
    "- evidence_refs: 非空数组，每项包含 field、value、claim；field 必须真实存在且可用，value 必须与数据包原值一致，claim 不得为空",
    "- missing_data_used: 数组，只能填写 data_contract 中实际 status!=ok 的大类：kline、money_flow、financial、sector、news；没有则 []",
    "- unsupported_claims: 数组，列出无法由数据支持的表述；正常应为 []",
]

PM_TEXT_FIELD_LINES = [
    "最终信号: BUY / WATCH / AVOID",
    "做多分: 0-100",
    "置信度: 0-100",
    "新开仓仓位上限: 0%-40%",
    "allow_direct_buy: true/false",
    "needs_intraday_confirmation: true/false",
    "entry_condition: 盘中买入条件",
    "block_buy_reason: 不能直接BUY的原因，没有则空",
    "核心理由: 2-3句话",
]


def pm_json_field_instructions() -> str:
    return "\n".join(PM_FIELD_LINES)


def pm_text_field_instructions() -> str:
    return "\n".join(PM_TEXT_FIELD_LINES)
