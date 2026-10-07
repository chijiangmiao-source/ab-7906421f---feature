"""verify 单次容器入口。

依次执行：
1. 语法/构建自检（编译全部模块）
2. 交接规则测试（成员替换、失效确认、幂等冲突、并发、中断恢复）
3. HTTP API 冒烟（真实拉起服务进程，走健康端点与完整交接流程）

任一环节失败即以非零状态码退出。
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import unittest

ROOT = os.path.dirname(os.path.abspath(__file__))


def step(title: str) -> None:
    print(f"\n=== verify: {title} ===", flush=True)


def compile_sources() -> bool:
    step("镜像构建自检（py_compile）")
    proc = subprocess.run(
        [sys.executable, "-m", "compileall", "-q", "app", "verify.py"]
    )
    return proc.returncode == 0


def run_rule_tests() -> bool:
    step("规则测试（成员替换 / 失效确认 / 中断恢复）")
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for module in ("tests.test_protocol", "tests.test_http_api"):
        suite.addTests(loader.loadTestsFromName(module))
    runner = unittest.TextTestRunner(verbosity=1)
    return runner.run(suite).wasSuccessful()


def http_request(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if body is not None else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def run_api_smoke() -> bool:
    step("API 冒烟（真实服务进程）")
    tmp = tempfile.TemporaryDirectory()
    port = int(os.environ.get("SMOKE_PORT", "18080"))
    env = {
        **os.environ,
        "HOST": "127.0.0.1",
        "PORT": str(port),
        "DB_PATH": os.path.join(tmp.name, "smoke.db"),
        "PARTITION_COUNT": "6",
        "QUIET": "1",
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.server"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    base = f"http://127.0.0.1:{port}"
    ok = False
    try:
        for _ in range(50):
            try:
                code, body = http_request("GET", f"{base}/healthz")
                if code == 200 and body.get("status") == "ok":
                    break
            except OSError:
                pass
            time.sleep(0.1)
        else:
            raise AssertionError("服务健康端点未在超时内就绪")

        code, _ = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r1", "members": ["a", "b"]},
        )
        assert code == 200, code
        code, body = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r2", "members": ["b", "c"]},
        )
        assert code == 202 and len(body["revocations"]) == 6, body

        # 失效确认：越权与多余分区必须被拒绝且不推进
        code, body = http_request(
            "POST", f"{base}/v1/confirms",
            {"request_id": "smoke-x1", "member": "c", "parts": ["0"]},
        )
        assert code == 403 and body["error"] == "not_owner", body
        code, body = http_request(
            "POST", f"{base}/v1/confirms",
            {"request_id": "smoke-x2", "member": "a", "parts": ["7"]},
        )
        assert code == 400 and body["error"] == "unexpected_partitions", body
        code, view = http_request("GET", f"{base}/v1/assignments")
        assert view["epoch"] == 0 and view["assignments"]["0"]["owner"] == "a", view

        # 幂等：同 id 不同快照冲突
        code, body = http_request(
            "POST", f"{base}/v1/snapshots",
            {"request_id": "smoke-r1", "members": ["x"]},
        )
        assert code == 409, body

        # 合法确认完成交接
        for req_id, member, parts in (
            ("smoke-c1", "a", ["0", "2", "4"]),
            ("smoke-c2", "b", ["1", "3", "5"]),
        ):
            code, body = http_request(
                "POST", f"{base}/v1/confirms",
                {"request_id": req_id, "member": member, "parts": parts},
            )
            assert code == 200, body
            replay_code, replay = http_request(
                "POST", f"{base}/v1/confirms",
                {"request_id": req_id, "member": member, "parts": parts},
            )
            assert replay_code == 200 and replay == body, "重传必须返回首次结果"

        code, view = http_request("GET", f"{base}/v1/assignments")
        owners = {p: v["owner"] for p, v in view["assignments"].items()}
        assert view["epoch"] == 2, view
        assert owners == {
            "0": "b", "1": "c", "2": "b", "3": "c", "4": "b", "5": "c"
        }, owners
        print("API 冒烟通过：健康检查、撤销隔离、失效拒绝、幂等重放、交接发布")
        ok = True
    except AssertionError as exc:
        print(f"API 冒烟失败: {exc}", flush=True)
        try:
            print(proc.stdout.read().decode(), flush=True)
        except Exception:
            pass
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        tmp.cleanup()
    return ok


def main() -> int:
    results = [
        compile_sources(),
        run_rule_tests(),
        run_api_smoke(),
    ]
    step("总结")
    labels = ("镜像构建自检", "规则测试", "API 冒烟")
    for label, ok in zip(labels, results):
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
