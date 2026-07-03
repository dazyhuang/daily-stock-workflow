#!/usr/bin/env python3
"""测试 volcengine ark-code-latest 的 json_schema 模式"""

import urllib.request
import json
import os

if os.environ.get("RUN_LIVE_LLM_TESTS") != "1":
    print("skipped: set RUN_LIVE_LLM_TESTS=1 to run live Volcengine mode probe")
    raise SystemExit(0)

API_KEY = os.environ.get("VOLCAN_API_KEY", "")
if not API_KEY:
    print("skipped: VOLCAN_API_KEY is not set")
    raise SystemExit(0)
print(f"Using key: {API_KEY[:10]}...")

url = "https://ark.cn-beijing.volces.com/api/coding/v3/chat/completions"
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}

# 测试1: 普通 text 模式（验证 key 有效）
body_text = {
    "model": "ark-code-latest",
    "messages": [{"role": "user", "content": "回复 OK"}],
    "temperature": 0,
    "max_tokens": 50,
}

# 测试2: json_object 模式（当前代码在用的）
body_json_object = {
    "model": "ark-code-latest",
    "messages": [{"role": "user", "content": "给出股票评级 JSON：{\"signal\": \"BUY\", \"confidence\": 75}"}],
    "temperature": 0,
    "max_tokens": 500,
    "response_format": {"type": "json_object"},
}

# 测试3: json_schema 模式（官方 SDK 示例）
schema = {
    "type": "object",
    "properties": {
        "signal": {"type": "string", "enum": ["BUY", "WATCH", "AVOID"]},
        "confidence": {"type": "integer", "minimum": 0, "maximum": 100}
    },
    "required": ["signal", "confidence"],
    "additionalProperties": False
}

body_json_schema = {
    "model": "ark-code-latest",
    "messages": [{"role": "user", "content": "给出股票评级 JSON：{\"signal\": \"BUY\", \"confidence\": 75}"}],
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

for name, body in [("text", body_text), ("json_object", body_json_object), ("json_schema", body_json_schema)]:
    print(f"\n=== Test: {name} ===")
    req = urllib.request.Request(
        url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers=headers, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
        result = json.loads(raw)
        content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
        print(f"✅ SUCCESS: {content[:200]}")
    except urllib.error.HTTPError as e:
        body_resp = e.read().decode("utf-8")
        print(f"❌ HTTP {e.code}: {body_resp[:300]}")
    except Exception as e:
        print(f"❌ ERROR: {type(e).__name__}: {e}")
