"""Scoring overlay metadata and report contracts.

The heavy scoring implementation still lives in stock_selection_debate.run_debate_phase
for compatibility.  This module is the stable contract layer used by reports,
checkpoint versioning, and intraday execution.
"""

SCORING_VERSION = "2026-07-17.residual-signal-v4"
PROMPT_VERSION = "2026-07-10.evidence-guard-v2"
EDGE_RULE_VERSION = "2026-07-17.bidirectional-edge-v3"
TOP5_RULE_VERSION = "2026-07-17.dynamic-diversification-v3"

VERSION_META = {
    "scoring_version": SCORING_VERSION,
    "prompt_version": PROMPT_VERSION,
    "edge_rule_version": EDGE_RULE_VERSION,
    "top5_rule_version": TOP5_RULE_VERSION,
}

REPORT_SIGNAL_FIELDS = [
    "pm_signal",
    "pm_score",
    "pm_reason",
    "raw_signal_by_score",
    "final_signal",
    "final_reason",
    "execution_gate",
    "signal_blockers",
    "top5_sort_score",
    "data_quality_score",
    "tradable_data_ok",
]
