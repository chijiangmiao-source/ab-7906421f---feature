"""HTTP 层：仅依赖标准库。

路由
----
* ``GET  /healthz``                  健康检查
* ``GET  /v1/assignments``           当前分配读模型（``?member=`` 过滤）
* ``GET  /v1/handover``              当前交接状态
* ``POST /v1/snapshots``             提交成员快照 ``{request_id, members}``
* ``POST /v1/confirms``              旧实例确认 ``{request_id, member, parts}``
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .store import (
    BAD_REQUEST,
    CONFLICT,
    FORBIDDEN,
    OK,
    ACCEPTED,
    SERVICE_UNAVAILABLE,
    Store,
    StoreError,
)


def create_server(
    host: str | None = None,
    port: int | None = None,
    db_path: str | None = None,
    partition_count: int | None = None,
) -> tuple[ThreadingHTTPServer, Store]:
    host = host if host is not None else os.environ.get("HOST", "0.0.0.0")
    port = port if port is not None else int(os.environ.get("PORT", "8080"))
    db_path = db_path if db_path is not None else os.environ.get(
        "DB_PATH", "/data/handoff.db"
    )
    if partition_count is None:
        partition_count = int(os.environ.get("PARTITION_COUNT", "256"))

    store = Store(db_path, partition_count=partition_count)

    class Handler(BaseHTTPRequestHandler):
        server_version = "HandoffScheduler/1.0"

        def log_message(self, fmt: str, *args: object) -> None:
            if os.environ.get("QUIET") != "1":
                super().log_message(fmt, *args)

        def _send(self, code: int, body: dict) -> None:
            payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def _read_json(self) -> dict | None:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                self._send(BAD_REQUEST, {"error": "bad_request", "message": "缺少请求体"})
                return None
            raw = self.rfile.read(length)
            try:
                body = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._send(BAD_REQUEST, {"error": "bad_request", "message": "请求体不是合法 JSON"})
                return None
            if not isinstance(body, dict):
                self._send(BAD_REQUEST, {"error": "bad_request", "message": "请求体必须是 JSON 对象"})
                return None
            return body

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/healthz":
                if store.health():
                    self._send(OK, {"status": "ok"})
                else:
                    self._send(SERVICE_UNAVAILABLE, {"status": "degraded"})
                return
            if parsed.path == "/v1/assignments":
                query = parse_qs(parsed.query)
                member = query.get("member", [None])[0]
                self._send(OK, store.assignments_view(member))
                return
            if parsed.path == "/v1/handover":
                self._send(OK, store.handover_view())
                return
            self._send(404, {"error": "not_found", "message": parsed.path})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path not in ("/v1/snapshots", "/v1/confirms"):
                self._send(404, {"error": "not_found", "message": parsed.path})
                return
            body = self._read_json()
            if body is None:
                return
            try:
                if parsed.path == "/v1/snapshots":
                    code, resp = store.snapshot(
                        body.get("request_id", ""), body.get("members")
                    )
                else:
                    code, resp = store.confirm(
                        body.get("request_id", ""),
                        body.get("member"),
                        body.get("parts"),
                    )
            except StoreError as exc:
                self._send(BAD_REQUEST, {"error": "bad_request", "message": str(exc)})
                return
            except sqlite3.DatabaseError:
                self._send(SERVICE_UNAVAILABLE, {"error": "storage_unavailable"})
                return
            self._send(code, resp)

    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.daemon_threads = True
    return httpd, store


def serve_forever(httpd: ThreadingHTTPServer) -> None:
    httpd.serve_forever(poll_interval=0.2)


def main() -> None:
    httpd, _store = create_server()

    def _shutdown(signum, frame):  # type: ignore[no-untyped-def]
        # shutdown() 不能在 serve_forever 所在线程内调用，否则会死锁
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    try:
        serve_forever(httpd)
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
