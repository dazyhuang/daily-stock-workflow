#!/usr/bin/env python3
"""测试 volcengine ark-code-latest API key 是否有效"""

import urllib.request
import json
import os

if os.environ.get("RUN_LIVE_LLM_TESTS") != "1":
    print("skipped: set RUN_LIVE_LLM_TESTS=1 to run live Volcengine key probe")
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

# 先测试普通 text 模式（验证 key 是否有效）
body = {
    "model": "ark-code-latest",
    "messages": [{"role": "user", "content": "回复 OK"}],
    "temperature": 0,
    "max_tokens": 50,
}

req = urllib.request.Request(
    url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
    headers=headers, method="POST",
)

print("Test 1: 普通 text 模式...")
try:
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
    result = json.loads(raw)
    print(f"✅ text mode SUCCESS: {result}")
except urllib.error.HTTPError as e:
    body_resp = e.read().decode("utf-8")
    print(f"❌ HTTP {e.code}: {body_resp}")
except Exception as e:
    print(f"❌ ERROR: {type(e).__name__}: {e}")
