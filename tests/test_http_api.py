"""HTTP API 冒烟与语义测试（启动真实端口）。"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from collections import Counter

from app.server import create_server, serve_forever


def request(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


class HttpCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "api.db")
        self.httpd, self.store = create_server(
            host="127.0.0.1", port=0, db_path=self.db, partition_count=6
        )
        self.port = self.httpd.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=serve_forever, args=(self.httpd,))
        self.thread.start()

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.thread.join()
        self.httpd.server_close()
        self.store.close()
        self.tmp.cleanup()

    def test_health(self) -> None:
        code, body = request("GET", f"{self.base}/healthz")
        self.assertEqual(code, 200)
        self.assertEqual(body["status"], "ok")

    def test_full_handover_flow_over_http(self) -> None:
        code, body = request(
            "POST", f"{self.base}/v1/snapshots",
            {"request_id": "r1", "members": ["a", "b"]},
        )
        self.assertEqual(code, 200)
        self.assertEqual(body["status"], "stable")

        code, body = request(
            "POST", f"{self.base}/v1/snapshots",
            {"request_id": "r2", "members": ["b", "c"]},
        )
        self.assertEqual(code, 202)
        self.assertEqual(len(body["revocations"]), 6)

        code, body = request(
            "POST", f"{self.base}/v1/confirms",
            {"request_id": "c1", "member": "a", "parts": ["0", "2", "4"]},
        )
        self.assertEqual(code, 200)
        self.assertEqual(body["status"], "partially_released")

        # 重传返回首次结果
        code, replay = request(
            "POST", f"{self.base}/v1/confirms",
            {"request_id": "c1", "member": "a", "parts": ["0", "2", "4"]},
        )
        self.assertEqual(code, 200)
        self.assertEqual(replay, body)

        code, body = request(
            "POST", f"{self.base}/v1/confirms",
            {"request_id": "c2", "member": "b", "parts": ["1", "3", "5"]},
        )
        self.assertEqual(code, 200)
        self.assertEqual(body["status"], "completed")

        code, body = request("GET", f"{self.base}/v1/assignments")
        self.assertEqual(code, 200)
        self.assertEqual(body["epoch"], 2)
        owners = Counter(v["owner"] for v in body["assignments"].values())
        self.assertEqual(owners, Counter({"b": 3, "c": 3}))

        code, body = request("GET", f"{self.base}/v1/assignments?member=c")
        self.assertEqual(code, 200)
        self.assertEqual(set(body["assignments"]), {"1", "3", "5"})

    def test_receipts_endpoint_covers_full_settlement_lifecycle(self) -> None:
        # 不存在的标识：明确未找到
        code, body = request("GET", f"{self.base}/v1/receipts?request_id=ghost")
        self.assertEqual(code, 404)
        self.assertEqual(body["error"], "receipt_not_found")

        # 缺少参数
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(f"{self.base}/v1/receipts", timeout=5)
        self.assertEqual(ctx.exception.code, 400)

        # 直接收敛：回执立即说明发布代次与全部目标成员
        request("POST", f"{self.base}/v1/snapshots",
                {"request_id": "r1", "members": ["a", "b"]})
        code, receipt = request("GET", f"{self.base}/v1/receipts?request_id=r1")
        self.assertEqual(code, 200)
        self.assertEqual(receipt["status"], "completed")
        self.assertEqual(receipt["kind"], "stable")
        self.assertEqual(receipt["epoch"], 0)
        self.assertEqual(
            receipt["targets"],
            {"0": "a", "1": "b", "2": "a", "3": "b", "4": "a", "5": "b"},
        )

        # 进入撤销：pending 稳定区分待释放 / 已释放
        request("POST", f"{self.base}/v1/snapshots",
                {"request_id": "r2", "members": ["b", "c"]})
        code, receipt = request("GET", f"{self.base}/v1/receipts?request_id=r2")
        self.assertEqual(code, 200)
        self.assertEqual(receipt["status"], "pending")
        self.assertNotIn("final_epoch", receipt)
        self.assertEqual(
            {p["part"] for p in receipt["pending_partitions"]}, set("012345")
        )
        self.assertEqual(receipt["released"], [])

        # 被拒确认不推进回执
        request("POST", f"{self.base}/v1/confirms",
                {"request_id": "x1", "member": "c", "parts": ["0"]})
        code, receipt = request("GET", f"{self.base}/v1/receipts?request_id=r2")
        self.assertEqual(receipt["released"], [])

        # 部分确认
        request("POST", f"{self.base}/v1/confirms",
                {"request_id": "c1", "member": "a", "parts": ["0", "2", "4"]})
        code, receipt = request("GET", f"{self.base}/v1/receipts?request_id=r2")
        self.assertEqual(code, 200)
        self.assertEqual(receipt["status"], "pending")  # 不冒充完成
        self.assertEqual(receipt["released"], ["0", "2", "4"])
        self.assertEqual(
            {p["part"] for p in receipt["pending_partitions"]}, {"1", "3", "5"}
        )
        self.assertEqual(receipt["confirmed"], {"a": ["0", "2", "4"]})

        # 最后一次有效确认 -> 唯一完成回执
        request("POST", f"{self.base}/v1/confirms",
                {"request_id": "c2", "member": "b", "parts": ["1", "3", "5"]})
        code, done = request("GET", f"{self.base}/v1/receipts?request_id=r2")
        self.assertEqual(code, 200)
        self.assertEqual(done["status"], "completed")
        self.assertEqual(done["begin_epoch"], 0)
        self.assertEqual(done["final_epoch"], 2)
        self.assertEqual(
            done["confirmed"], {"a": ["0", "2", "4"], "b": ["1", "3", "5"]}
        )

        # 完成后再交接：旧标识不混入后续代次
        request("POST", f"{self.base}/v1/snapshots",
                {"request_id": "r3", "members": ["c", "d"]})
        request("POST", f"{self.base}/v1/confirms",
                {"request_id": "c3", "member": "b", "parts": ["0", "2", "4"]})
        request("POST", f"{self.base}/v1/confirms",
                {"request_id": "c4", "member": "c", "parts": ["1", "3", "5"]})
        code, old = request("GET", f"{self.base}/v1/receipts?request_id=r2")
        self.assertEqual(old, done)
        code, newer = request("GET", f"{self.base}/v1/receipts?request_id=r3")
        self.assertEqual(newer["status"], "completed")
        self.assertEqual(newer["final_epoch"], 4)

    def test_receipt_query_survives_server_restart(self) -> None:
        request("POST", f"{self.base}/v1/snapshots",
                {"request_id": "r1", "members": ["a", "b"]})
        request("POST", f"{self.base}/v1/snapshots",
                {"request_id": "r2", "members": ["b", "c"]})
        request("POST", f"{self.base}/v1/confirms",
                {"request_id": "c1", "member": "a", "parts": ["0", "2", "4"]})

        # 关闭服务进程后用同一 DB 文件重新拉起
        self.httpd.shutdown()
        self.thread.join()
        self.httpd.server_close()
        self.store.close()
        self.httpd, self.store = create_server(
            host="127.0.0.1", port=0, db_path=self.db, partition_count=6
        )
        self.port = self.httpd.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=serve_forever, args=(self.httpd,))
        self.thread.start()

        code, receipt = request("GET", f"{self.base}/v1/receipts?request_id=r2")
        self.assertEqual(code, 200)
        self.assertEqual(receipt["status"], "pending")
        self.assertEqual(receipt["released"], ["0", "2", "4"])
        self.assertEqual(
            {p["part"] for p in receipt["pending_partitions"]}, {"1", "3", "5"}
        )
        code, receipt = request("GET", f"{self.base}/v1/receipts?request_id=r1")
        self.assertEqual(code, 200)
        self.assertEqual(receipt["status"], "completed")

    def test_invalid_confirmations_are_rejected(self) -> None:
        request("POST", f"{self.base}/v1/snapshots",
                {"request_id": "r1", "members": ["a", "b"]})
        request("POST", f"{self.base}/v1/snapshots",
                {"request_id": "r2", "members": ["b", "c"]})

        code, body = request("POST", f"{self.base}/v1/confirms",
                             {"request_id": "x1", "member": "c", "parts": ["0"]})
        self.assertEqual(code, 403)
        self.assertEqual(body["error"], "not_owner")

        code, body = request("POST", f"{self.base}/v1/confirms",
                             {"request_id": "x2", "member": "a", "parts": ["9"]})
        self.assertEqual(code, 400)

        code, body = request("POST", f"{self.base}/v1/snapshots",
                             {"request_id": "r1", "members": ["a", "c"]})
        self.assertEqual(code, 409)
        self.assertEqual(body["error"], "idempotency_conflict")

    def test_bad_json_and_missing_fields(self) -> None:
        req = urllib.request.Request(
            f"{self.base}/v1/snapshots",
            data=b"{not json",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req, timeout=5)
        self.assertEqual(ctx.exception.code, 400)

        code, body = request("POST", f"{self.base}/v1/snapshots",
                             {"request_id": "", "members": []})
        self.assertEqual(code, 400)


if __name__ == "__main__":
    unittest.main()
