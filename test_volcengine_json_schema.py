#!/usr/bin/env python3
"""测试 volcengine ark-code-latest 是否支持 json_schema mode"""

import urllib.request
import json
import os

if os.environ.get("RUN_LIVE_LLM_TESTS") != "1":
    print("skipped: set RUN_LIVE_LLM_TESTS=1 to run live Volcengine json_schema probe")
    raise SystemExit(0)

API_KEY = os.environ.get("VOLCAN_API_KEY", "")
if not API_KEY:
    print("skipped: VOLCAN_API_KEY is not set")
    raise SystemExit(0)

url = "https://ark.cn-beijing.volces.com/api/coding/v3/chat/completions"
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}

# 测试 schema
class TestOutput:
    signal: str
    confidence: int

schema = {
    "type": "object",
    "properties": {
        "signal": {"type": "string", "enum": ["BUY", "WATCH", "AVOID"]},
        "confidence": {"type": "integer", "minimum": 0, "maximum": 100}
    },
    "required": ["signal", "confidence"],
    "additionalProperties": False
}

body = {
    "model": "ark-code-latest",
    "messages": [{"role": "user", "content": "给出股票评级 JSON：{\"signal\": \"BUY\", \"confidence\": 75}"}],
    "temperature": 0,
    "max_tokens": 500,
    "response_format": {
        "type": "json_schema",
        "json_schema": {
            "name": "TestOutput",
            "description": "股票评级输出",
            "schema": schema,
            "strict": True
        }
    }
}

req = urllib.request.Request(
    url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
    headers=headers, method="POST",
)

print("Testing json_schema mode...")
try:
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
    result = json.loads(raw)
    print(f"✅ SUCCESS: {result}")
except urllib.error.HTTPError as e:
    body_resp = e.read().decode("utf-8")
    print(f"❌ HTTP {e.code}: {body_resp}")
except Exception as e:
    print(f"❌ ERROR: {type(e).__name__}: {e}")
