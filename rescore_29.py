import sys
import json
import time
import os
import uuid
import signal
import subprocess
from pathlib import Path

BASE_DIR = Path(".")
OUTPUT_DIR = BASE_DIR / "output"

sys.path.insert(0, str(BASE_DIR))
from llm_scorer import LLMScorer  # noqa: E402

def call_via_openclaw(prompt, timeout=180):
    """通过 openclaw agent --local 调用 LLM"""
    session_id = f"rescore-{uuid.uuid4().hex[:8]}"
    cmd = [
        "openclaw", "agent",
        "--local",
        "--session-id", session_id,
        "--message", prompt,
        "--timeout", str(timeout),
    ]
    proc = None
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ},
            preexec_fn=os.setsid if hasattr(os, 'setsid') else None,
        )
        try:
            stdout, stderr = proc.communicate(timeout=timeout + 20)
        except subprocess.TimeoutExpired:
            try:
                if hasattr(os, 'killpg'):
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                else:
                    proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
            print("  [openclaw] 超时，进程已终止")
            return ""

        if proc.returncode != 0:
            print(f"  [openclaw] 失败 code={proc.returncode}: {stderr[:100]}")
            return ""

        # 提取JSON
        json_output = stdout.strip()
        start = json_output.find("{")
        end = json_output.rfind("}") + 1
        if start >= 0 and end > start:
            return json_output[start:end]
        return ""
    except Exception as e:
        print(f"  [openclaw] 异常: {e}")
        return ""

# 先加载现有得分
with open(OUTPUT_DIR / "daily_report_20260408.json") as f:
    report = json.load(f)

candidates = report["phase2"]["candidates"]
all_scores = list(report["phase2"].get("scored", []))
scored_stocks = {s["stock"]: s for s in all_scores}

to_rescore = []
for s in all_scores:
    if s.get("total_score") == 50:
        to_rescore.append(s["stock"])
for c in candidates:
    if c["stock"] not in scored_stocks:
        to_rescore.append(c["stock"])

print(f"共 {len(to_rescore)} 只需重打: {to_rescore}")

scorer = LLMScorer(timeout=180)

for i in range(0, len(to_rescore), 2):
    batch_codes = to_rescore[i:i+2]
    batch = [c for c in candidates if c["stock"] in batch_codes]
    if not batch:
        continue

    print(f"批次 {(i//2)+1}: {batch_codes}")
    t0 = time.time()
    prompt = scorer._build_prompt([], batch)
    resp = call_via_openclaw(prompt, timeout=180)
    elapsed = time.time() - t0

    if resp:
        scores = scorer._parse_response(resp, batch)
        if scores:
            for s in scores:
                for idx, ex in enumerate(all_scores):
                    if ex["stock"] == s["stock"]:
                        all_scores[idx] = s
                        break
                else:
                    all_scores.append(s)
            print(f"  成功: {[(sc['stock'], sc['total_score']) for sc in scores]} ({elapsed:.0f}s)")
        else:
            print("  解析失败")
            for c in batch:
                for idx, ex in enumerate(all_scores):
                    if ex["stock"] == c["stock"]:
                        ex["total_score"] = 50
                        ex["action"] = "WATCH"
                        break
    else:
        print("  API/openclaw失败")
        for c in batch:
            for idx, ex in enumerate(all_scores):
                if ex["stock"] == c["stock"]:
                    ex["total_score"] = 50
                    ex["action"] = "WATCH"
                    break

    # 保存进度
    with open(OUTPUT_DIR / "daily_report_20260408.json") as f:
        r2 = json.load(f)
    r2["phase2"]["scored"] = all_scores
    top5 = sorted(all_scores, key=lambda x: x.get("total_score", 0), reverse=True)[:5]
    r2["phase2"]["top_picks"] = [s["stock"] for s in top5]
    with open(OUTPUT_DIR / "daily_report_20260408.json", "w") as f:
        json.dump(r2, f, ensure_ascii=False, indent=2)

    time.sleep(2)

top5 = sorted(all_scores, key=lambda x: x.get("total_score", 0), reverse=True)[:5]
print("=" * 50)
print("Top 5:")
for i, s in enumerate(top5, 1):
    print(f"  {i}. {s['stock']} {s['name']}: {s['total_score']}分 {s['action']}")

