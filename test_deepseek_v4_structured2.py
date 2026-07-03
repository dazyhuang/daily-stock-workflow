#!/usr/bin/env python3
"""测试 DeepSeek V4 Pro 的结构化输出支持情况"""

import sys, os
sys.path.insert(0, 'stock_selection_debate')
if os.environ.get("RUN_LIVE_LLM_TESTS") != "1":
    print("skipped: set RUN_LIVE_LLM_TESTS=1 to run live Volcengine structured-output probe")
    raise SystemExit(0)
from providers import _load_models_config, _get_api_key
import urllib.request, json

_load_models_config()
api_key = _get_api_key('volcengine-plan')
if not api_key:
    print("skipped: volcengine-plan API key is not configured")
    raise SystemExit(0)

models = ['deepseek-v4-pro', 'deepseek-v4-pro-250615']
schema = {
    "type": "object",
    "properties": {
        "signal": {"type": "string", "enum": ["BUY", "WATCH", "AVOID"]},
        "confidence": {"type": "integer", "minimum": 0, "maximum": 100}
    },
    "required": ["signal", "confidence"],
    "additionalProperties": False
}

for model in models:
    print(f'\n=== {model} ===')

    # Test 1: json_object
    print('json_object: ', end='')
    body1 = {
        "model": model,
        "messages": [{"role": "user", "content": "给出股票评级 JSON"}],
        "temperature": 0,
        "max_tokens": 500,
        "response_format": {"type": "json_object"},
    }
    req1 = urllib.request.Request(
        'https://ark.cn-beijing.volces.com/api/coding/v3/chat/completions',
        data=json.dumps(body1, ensure_ascii=False).encode('utf-8'),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req1, timeout=25) as resp:
            raw = resp.read().decode('utf-8')
        result = json.loads(raw)
        content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
        print(f'✅ {content[:150]}')
    except urllib.error.HTTPError as e:
        print(f'❌ HTTP {e.code}: {e.read().decode()[:150]}')
    except Exception as e:
        print(f'❌ {e}')

    # Test 2: json_schema
    print('json_schema: ', end='')
    body2 = {
        "model": model,
        "messages": [{"role": "user", "content": "给出股票评级 JSON"}],
        "temperature": 0,
        "max_tokens": 500,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "StockRating",
                "description": "股票评级输出",
                "schema": schema,
                "strict": True
            }
        }
    }
    req2 = urllib.request.Request(
        'https://ark.cn-beijing.volces.com/api/coding/v3/chat/completions',
        data=json.dumps(body2, ensure_ascii=False).encode('utf-8'),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req2, timeout=25) as resp:
            raw = resp.read().decode('utf-8')
        result = json.loads(raw)
        content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
        print(f'✅ {content[:150]}')
    except urllib.error.HTTPError as e:
        print(f'❌ HTTP {e.code}: {e.read().decode()[:150]}')
    except Exception as e:
        print(f'❌ {e}')
