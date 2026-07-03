#!/usr/bin/env python3
"""辩论 checkpoint 回调本地回归测试。

旧版脚本会直接启动真实 StockDebateEngine 并调用模型，容易遇到限流后
卡在后台线程。这里只测试 checkpoint_cb 的核心语义：成功写 completed、
失败写 failed、后续成功能从 failed 移除并保留结果。
"""

import json
import tempfile
import threading
from pathlib import Path


def make_checkpoint_cb(cp_file: Path):
    lock = threading.Lock()
    call_count = {"n": 0}

    def checkpoint_cb(code, result):
        with lock:
            call_count["n"] += 1
            cp = json.loads(cp_file.read_text(encoding="utf-8"))
            if result:
                if code not in cp["completed"]:
                    cp["completed"].append(code)
                cp["results"][code] = result
                cp["failed"] = [f for f in cp.get("failed", []) if f != code]
            else:
                if code not in cp.get("failed", []):
                    cp["failed"].append(code)
            tmp = cp_file.with_name(cp_file.name + ".tmp")
            tmp.write_text(json.dumps(cp, ensure_ascii=False), encoding="utf-8")
            tmp.replace(cp_file)

    return checkpoint_cb, call_count


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        cp_file = Path(td) / "debate_checkpoint.json"
        cp_file.write_text(
            json.dumps({"date": "20260612", "completed": [], "failed": [], "results": {}}),
            encoding="utf-8",
        )
        checkpoint_cb, calls = make_checkpoint_cb(cp_file)

        checkpoint_cb("000001", {"signal": "BUY", "confidence": 80})
        cp = json.loads(cp_file.read_text(encoding="utf-8"))
        assert cp["completed"] == ["000001"], cp
        assert cp["failed"] == [], cp
        assert cp["results"]["000001"]["signal"] == "BUY", cp

        checkpoint_cb("000002", None)
        checkpoint_cb("000002", None)
        cp = json.loads(cp_file.read_text(encoding="utf-8"))
        assert cp["failed"] == ["000002"], cp

        checkpoint_cb("000002", {"signal": "WATCH", "confidence": 60})
        cp = json.loads(cp_file.read_text(encoding="utf-8"))
        assert set(cp["completed"]) == {"000001", "000002"}, cp
        assert cp["failed"] == [], cp
        assert cp["results"]["000002"]["signal"] == "WATCH", cp
        assert calls["n"] == 4, calls

    print("debate checkpoint callback tests passed")


if __name__ == "__main__":
    main()
