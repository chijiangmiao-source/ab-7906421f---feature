"""持久化与交接协议核心。

不变量
------
* 任一代次内，一个分区至多归属一个接收实例。
* 撤销中的分区仍记在旧实例名下（读模型看到的是旧完整分配），
  直到旧实例确认；确认时“删除旧所有权 / 转授新实例 / 推进可交接集合 /
  公布新代次”发生在同一个持久化事务中，因此外部观察只可能是：
  旧完整分配，或与已确认释放一致的中间分配。
* 所有写接口以稳定请求标识幂等：相同请求重放首次结果；
  相同请求标识携带不同快照明确冲突。
* 每次快照的结算回执按快照请求标识持久化：直接收敛立即给出发布代次
  与全部目标成员；进入撤销的交接在最后一次有效确认后定格为唯一完成
  回执，固定开始时持久化的目标与每个释放方实际确认的分区，后续代次
  的交接不得改写历史回执。
"""""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any

# 业务结果码（与 HTTP 状态对齐）
OK = 200
ACCEPTED = 202
BAD_REQUEST = 400
FORBIDDEN = 403
CONFLICT = 409
SERVICE_UNAVAILABLE = 503


class StoreError(Exception):
    """调用方可直接展示的协议错误。"""


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class Store:
    """单连接、进程内加锁的持久化存储。

    数据库层使用 ``BEGIN IMMEDIATE`` 串行化写入，因此即使被多进程/多连接
    并发访问（测试与恢复场景），协议状态仍然一致。
    """

    def __init__(self, path: str, partition_count: int = 256) -> None:
        if partition_count <= 0:
            raise ValueError("partition_count must be positive")
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path,
            isolation_level=None,  # 显式管理事务
            check_same_thread=False,
            timeout=30.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._ensure_schema(partition_count)

    # ------------------------------------------------------------------ schema

    def _ensure_schema(self, partition_count: int) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS config (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            -- 分区当前所有权；owner 为空表示尚未分配。
            -- epoch 为该所有权被（最近一次）公布时的代次。
            CREATE TABLE IF NOT EXISTS assignments (
                part  TEXT PRIMARY KEY,
                owner TEXT,
                epoch INTEGER NOT NULL
            );

            -- 撤销中的分区：旧实例仍持有（assignments.owner 不动），
            -- 目标实例只有在旧实例确认后的同一事务里才能取得所有权。
            CREATE TABLE IF NOT EXISTS revocations (
                part       TEXT PRIMARY KEY REFERENCES assignments(part),
                owner      TEXT NOT NULL,
                target     TEXT NOT NULL,
                epoch      INTEGER NOT NULL,
                req_id     TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            -- 最多一个进行中的交接（id 恒为 1）。
            CREATE TABLE IF NOT EXISTS handover_state (
                id            INTEGER PRIMARY KEY CHECK (id = 1),
                req_id        TEXT NOT NULL,
                snapshot_json TEXT NOT NULL,
                target_json   TEXT NOT NULL,
                status        TEXT NOT NULL CHECK (status IN ('pending', 'completed')),
                result_json   TEXT,
                created_at    TEXT NOT NULL,
                completed_at  TEXT
            );

            -- 稳定请求标识 -> 首次结果，用于重传重放与冲突检测。
            CREATE TABLE IF NOT EXISTS requests (
                req_id        TEXT PRIMARY KEY,
                kind          TEXT NOT NULL CHECK (kind IN ('snapshot', 'confirm')),
                snapshot_json TEXT,
                code          INTEGER NOT NULL,
                response_json TEXT NOT NULL,
                created_at    TEXT NOT NULL
            );

            -- 每次快照的结算回执，按快照请求标识索引。
            -- target_json 固定交接开始时持久化的目标；released_json 记录
            -- 每个释放方实际确认的分区。回执一旦写库即与后续代次隔离：
            -- 新的成员替换只新增回执行，从不改写历史回执。
            CREATE TABLE IF NOT EXISTS receipts (
                req_id        TEXT PRIMARY KEY,
                snapshot_json TEXT NOT NULL,
                target_json   TEXT NOT NULL,
                status        TEXT NOT NULL
                              CHECK (status IN ('stable', 'revoking', 'completed')),
                base_epoch    INTEGER NOT NULL,
                final_epoch   INTEGER,
                released_json TEXT NOT NULL DEFAULT '{}',
                created_at    TEXT NOT NULL,
                completed_at  TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_revocations_owner ON revocations(owner);
            CREATE INDEX IF NOT EXISTS idx_assignments_owner ON assignments(owner);
            """
        )
        row = self._conn.execute(
            "SELECT value FROM config WHERE key = 'epoch'"
        ).fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO config(key, value) VALUES ('epoch', '0')"
            )
        row = self._conn.execute(
            "SELECT value FROM config WHERE key = 'partition_count'"
        ).fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO config(key, value) VALUES ('partition_count', ?)",
                (str(partition_count),),
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ helpers

    def _epoch(self, conn: sqlite3.Connection) -> int:
        return int(
            conn.execute("SELECT value FROM config WHERE key = 'epoch'").fetchone()[0]
        )

    def _set_epoch(self, conn: sqlite3.Connection, epoch: int) -> None:
        conn.execute(
            "UPDATE config SET value = ? WHERE key = 'epoch'", (str(epoch),)
        )

    def _partition_count(self, conn: sqlite3.Connection) -> int:
        return int(
            conn.execute(
                "SELECT value FROM config WHERE key = 'partition_count'"
            ).fetchone()[0]
        )

    @staticmethod
    def _validate_request_id(value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            raise StoreError("request_id 必须是非空字符串")
        return value.strip()

    @staticmethod
    def _validate_members(value: Any) -> list[str]:
        if not isinstance(value, list) or not value:
            raise StoreError("members 必须是非空数组")
        members: list[str] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise StoreError("成员标识必须是非空字符串")
            member = item.strip()
            if member in seen:
                raise StoreError(f"成员快照包含重复成员: {member}")
            seen.add(member)
            members.append(member)
        return members

    @staticmethod
    def _validate_parts(value: Any) -> list[str]:
        if not isinstance(value, list) or not value:
            raise StoreError("parts 必须是非空数组")
        parts: list[str] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise StoreError("分区号必须是非空字符串")
            part = item.strip()
            if part in seen:
                raise StoreError(f"确认列表包含重复分区: {part}")
            seen.add(part)
            parts.append(part)
        return parts

    @staticmethod
    def _replay(row: sqlite3.Row) -> tuple[int, dict[str, Any]]:
        return int(row["code"]), json.loads(row["response_json"])

    def _record(
        self,
        conn: sqlite3.Connection,
        req_id: str,
        kind: str,
        snapshot: list[str] | None,
        code: int,
        response: dict[str, Any],
    ) -> None:
        conn.execute(
            "INSERT INTO requests(req_id, kind, snapshot_json, code, response_json,"
            " created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                req_id,
                kind,
                _canonical_json(snapshot) if snapshot is not None else None,
                code,
                _canonical_json(response),
                _now(),
            ),
        )

    @staticmethod
    def _record_receipt(
        conn: sqlite3.Connection,
        req_id: str,
        members: list[str],
        target: dict[str, str],
        base_epoch: int,
        status: str,
        final_epoch: int | None,
    ) -> None:
        """在快照落库的同一事务里创建结算回执。

        ``target`` 原样固定本次开始时持久化的目标（撤销前沿或全量目标），
        之后任何代次推进都不会再改写这一行。
        """
        conn.execute(
            "INSERT INTO receipts(req_id, snapshot_json, target_json, status,"
            " base_epoch, final_epoch, released_json, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, '{}', ?)",
            (
                req_id,
                _canonical_json(members),
                _canonical_json(target),
                status,
                base_epoch,
                final_epoch,
                _now(),
            ),
        )

    @staticmethod
    def _advance_receipt(
        conn: sqlite3.Connection,
        handover_req_id: str,
        member: str,
        parts: list[str],
        completed: bool,
        new_epoch: int,
    ) -> None:
        """把一次有效确认并入回执；仅在实际释放分区的临界区内调用。

        重放、过期与被拒确认都在到达临界区之前返回，因此不会推进回执。
        """
        row = conn.execute(
            "SELECT released_json FROM receipts WHERE req_id = ?",
            (handover_req_id,),
        ).fetchone()
        if row is None:
            # 历史库中交接开始于回执功能之前，没有可推进的回执。
            return
        released = json.loads(row["released_json"])
        released[member] = sorted(
            set(released.get(member, ())) | set(parts), key=_part_key
        )
        if completed:
            conn.execute(
                "UPDATE receipts SET status = 'completed', released_json = ?,"
                " final_epoch = ?, completed_at = ? WHERE req_id = ?",
                (_canonical_json(released), new_epoch, _now(), handover_req_id),
            )
        else:
            conn.execute(
                "UPDATE receipts SET released_json = ? WHERE req_id = ?",
                (_canonical_json(released), handover_req_id),
            )

    def _active_handover(
        self, conn: sqlite3.Connection
    ) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM handover_state WHERE id = 1 AND status = 'pending'"
        ).fetchone()

    def _revocations_by_owner(
        self, conn: sqlite3.Connection
    ) -> dict[str, list[sqlite3.Row]]:
        grouped: dict[str, list[sqlite3.Row]] = {}
        for row in conn.execute("SELECT * FROM revocations ORDER BY part"):
            grouped.setdefault(row["owner"], []).append(row)
        return grouped

    # ------------------------------------------------------------------- reads

    def health(self) -> bool:
        with self._lock:
            try:
                self._conn.execute("SELECT 1").fetchone()
                return True
            except sqlite3.DatabaseError:
                return False

    def assignments_view(self, member: str | None = None) -> dict[str, Any]:
        """对外读模型：旧完整分配，或与已确认释放一致的中间分配。"""
        with self._lock:
            conn = self._conn
            epoch = self._epoch(conn)
            sql = (
                "SELECT a.part, a.owner, a.epoch, r.target AS revoking_target"
                " FROM assignments a LEFT JOIN revocations r ON r.part = a.part"
            )
            params: tuple[Any, ...] = ()
            if member is not None:
                sql += " WHERE a.owner = ?"
                params = (member,)
            sql += " ORDER BY CAST(a.part AS INTEGER), a.part"
            assignments: dict[str, dict[str, Any]] = {}
            for row in conn.execute(sql, params):
                if row["owner"] is None:
                    continue
                assignments[row["part"]] = {
                    "owner": row["owner"],
                    "epoch": row["epoch"],
                    "revoking": row["revoking_target"],
                }
            return {"epoch": epoch, "assignments": assignments}

    def handover_view(self) -> dict[str, Any]:
        with self._lock:
            conn = self._conn
            row = conn.execute(
                "SELECT * FROM handover_state WHERE id = 1"
            ).fetchone()
            epoch = self._epoch(conn)
            if row is None or row["status"] != "pending":
                return {"active": False, "epoch": epoch}
            target: dict[str, str] = json.loads(row["target_json"])
            remaining = {
                r["part"]: {"owner": r["owner"], "target": r["target"]}
                for r in conn.execute("SELECT * FROM revocations ORDER BY part")
            }
            return {
                "active": True,
                "epoch": epoch,
                "request_id": row["req_id"],
                "snapshot": json.loads(row["snapshot_json"]),
                "target": target,
                "revocations": [
                    {"part": p, **remaining[p]} for p in sorted(remaining)
                ],
                "released": [p for p in sorted(target) if p not in remaining],
            }

    def receipt_view(self, request_id: str) -> dict[str, Any] | None:
        """按快照请求标识查询结算回执；标识不存在时返回 None。

        回执内容完全来自持久化的回执行，不从当前分配或逐分区链路反推：
        * ``stable``    直接收敛，立即给出发布代次与全部目标成员；
        * ``revoking``  进行中，``pending`` 为仍待释放的旧成员分区，
          ``released`` 为每个释放方已实际确认的分区；
        * ``completed`` 唯一完成回执，固定开始时持久化的目标、各释放方
          确认的分区与完成代次，后续交接不得混入。
        """
        with self._lock:
            if not isinstance(request_id, str) or not request_id.strip():
                return None
            req_id = request_id.strip()
            conn = self._conn
            row = conn.execute(
                "SELECT * FROM receipts WHERE req_id = ?", (req_id,)
            ).fetchone()
            if row is None:
                return None
            status = row["status"]
            pending: list[dict[str, str]] = []
            if status == "revoking":
                # 进行中的回执必然对应当前唯一活动交接；按请求标识过滤
                # 撤销集，稳定地区分待释放与已释放，不依赖全局状态。
                pending = [
                    {"part": r["part"], "owner": r["owner"], "target": r["target"]}
                    for r in conn.execute(
                        "SELECT part, owner, target FROM revocations"
                        " WHERE req_id = ? ORDER BY part",
                        (req_id,),
                    )
                ]
            final_epoch = row["final_epoch"]
            epoch = int(final_epoch) if final_epoch is not None else self._epoch(conn)
            return {
                "request_id": req_id,
                "status": status,
                "epoch": epoch,
                "members": json.loads(row["snapshot_json"]),
                "target": json.loads(row["target_json"]),
                "released": json.loads(row["released_json"]),
                "pending": pending,
                "created_at": row["created_at"],
                "completed_at": row["completed_at"],
            }

    # ------------------------------------------------------------------ writes

    def snapshot(
        self, request_id: str, raw_members: Any
    ) -> tuple[int, dict[str, Any]]:
        """提交完整成员快照，返回 (状态码, 响应体)。"""
        with self._lock:
            req_id = self._validate_request_id(request_id)
            members = self._validate_members(raw_members)
            ordered = sorted(members)
            snapshot_key = _canonical_json(ordered)
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                prior = conn.execute(
                    "SELECT * FROM requests WHERE req_id = ?", (req_id,)
                ).fetchone()
                if prior is not None:
                    if (
                        prior["kind"] != "snapshot"
                        or prior["snapshot_json"] != snapshot_key
                    ):
                        body = {
                            "error": "idempotency_conflict",
                            "message": "请求标识已被不同的请求体使用",
                            "request_id": req_id,
                        }
                        conn.execute("ROLLBACK")
                        return CONFLICT, body
                    result = self._replay(prior)
                    conn.execute("COMMIT")
                    return result

                active = self._active_handover(conn)
                if active is not None:
                    # 交接进行中：不接受新目标，调用方须以已持久化目标重新收敛。
                    # 这是暂时性拒绝，不占用请求标识。
                    body = {
                        "error": "handover_in_progress",
                        "message": "上一轮交接尚未完成，当前请求未被接受",
                        "request_id": req_id,
                        "active_handover": {
                            "request_id": active["req_id"],
                            "target": json.loads(active["target_json"]),
                        },
                    }
                    conn.execute("ROLLBACK")
                    return CONFLICT, body

                count = self._partition_count(conn)
                epoch = self._epoch(conn)
                current_rows = conn.execute(
                    "SELECT part, owner FROM assignments"
                ).fetchall()
                current = {r["part"]: r["owner"] for r in current_rows}

                def choose(part: str) -> str:
                    # 成员标识 + 分区号确定的稳定目标
                    return ordered[int(part) % len(ordered)]

                parts = (
                    [str(i) for i in range(count)]
                    if not current
                    else list(current)
                )
                target = {part: choose(part) for part in sorted(parts, key=_part_key)}

                grants: list[str] = []
                revokes: list[dict[str, str]] = []
                for part, new_owner in target.items():
                    old_owner = current.get(part)
                    if old_owner is None:
                        grants.append(part)
                    elif old_owner not in members or old_owner != new_owner:
                        revokes.append(
                            {"part": part, "owner": old_owner, "target": new_owner}
                        )
                # 交接前沿：仅仍需旧实例释放的分区 -> 新目标
                revoke_target = {item["part"]: item["target"] for item in revokes}

                for part in grants:
                    conn.execute(
                        "INSERT INTO assignments(part, owner, epoch)"
                        " VALUES (?, ?, ?) ON CONFLICT(part) DO"
                        " UPDATE SET owner = excluded.owner, epoch = excluded.epoch",
                        (part, target[part], epoch),
                    )

                if not revokes:
                    # 没有需要旧实例释放的分区：新视图立即生效，不产生代次推进。
                    body = {
                        "status": "stable",
                        "epoch": epoch,
                        "request_id": req_id,
                        "assigned": sorted(target, key=_part_key),
                    }
                    # 直接收敛：回执立即固定发布代次与全部目标成员。
                    self._record_receipt(
                        conn, req_id, ordered, target, epoch, "stable", epoch
                    )
                    self._record(conn, req_id, "snapshot", ordered, OK, body)
                    conn.execute("COMMIT")
                    return OK, body

                for item in revokes:
                    conn.execute(
                        "INSERT INTO revocations(part, owner, target, epoch,"
                        " req_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            item["part"],
                            item["owner"],
                            item["target"],
                            epoch,
                            req_id,
                            _now(),
                        ),
                    )
                conn.execute(
                    "INSERT INTO handover_state(id, req_id, snapshot_json,"
                    " target_json, status, created_at) VALUES (1, ?, ?, ?,"
                    " 'pending', ?) ON CONFLICT(id) DO UPDATE SET"
                    " req_id = excluded.req_id,"
                    " snapshot_json = excluded.snapshot_json,"
                    " target_json = excluded.target_json,"
                    " status = 'pending',"
                    " result_json = NULL,"
                    " created_at = excluded.created_at,"
                    " completed_at = NULL",
                    (req_id, snapshot_key, _canonical_json(revoke_target), _now()),
                )
                body = {
                    "status": "revoking",
                    "epoch": epoch,
                    "request_id": req_id,
                    "assigned_now": sorted(grants, key=_part_key),
                    "revocations": sorted(revokes, key=lambda x: _part_key(x["part"])),
                }
                # 进入撤销：回执固定本次开始时持久化的目标，完成代次待确认。
                self._record_receipt(
                    conn, req_id, ordered, revoke_target, epoch, "revoking", None
                )
                self._record(conn, req_id, "snapshot", ordered, ACCEPTED, body)
                conn.execute("COMMIT")
                return ACCEPTED, body
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def confirm(
        self,
        request_id: str,
        raw_member: Any,
        raw_parts: Any,
        _crash_hook: "callable | None" = None,
    ) -> tuple[int, dict[str, Any]]:
        """旧实例确认撤销分区；所有效果单事务提交。

        ``_crash_hook`` 仅供崩溃恢复测试使用：在释放语句已执行、
        而提交尚未发生时调用（钩子内可直接 ``os._exit`` 杀死进程）。
        """
        with self._lock:
            req_id = self._validate_request_id(request_id)
            if not isinstance(raw_member, str) or not raw_member.strip():
                raise StoreError("member 必须是非空字符串")
            member = raw_member.strip()
            parts = self._validate_parts(raw_parts)
            conn = self._conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                prior = conn.execute(
                    "SELECT * FROM requests WHERE req_id = ?", (req_id,)
                ).fetchone()
                if prior is not None:
                    if prior["kind"] != "confirm":
                        body = {
                            "error": "idempotency_conflict",
                            "message": "请求标识已被不同的请求体使用",
                            "request_id": req_id,
                        }
                        conn.execute("ROLLBACK")
                        return CONFLICT, body
                    result = self._replay(prior)
                    conn.execute("COMMIT")
                    return result

                def finish(code: int, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
                    self._record(conn, req_id, "confirm", None, code, body)
                    if _crash_hook is not None and code == OK:
                        _crash_hook()
                    conn.execute("COMMIT")
                    return code, body

                active = self._active_handover(conn)
                if active is None:
                    return finish(
                        CONFLICT,
                        {
                            "error": "confirmation_expired",
                            "message": "没有进行中的交接，确认已过期",
                            "request_id": req_id,
                        },
                    )

                rows = conn.execute(
                    "SELECT * FROM revocations WHERE part IN (%s)"
                    % ",".join("?" * len(parts)),
                    parts,
                ).fetchall()
                by_part = {r["part"]: r for r in rows}

                foreign = [
                    p for p in parts if p in by_part and by_part[p]["owner"] != member
                ]
                if foreign:
                    return finish(
                        FORBIDDEN,
                        {
                            "error": "not_owner",
                            "message": "分区并非由该实例持有，禁止越权确认",
                            "request_id": req_id,
                            "partitions": sorted(foreign, key=_part_key),
                        },
                    )
                extra = [p for p in parts if p not in by_part]
                if extra:
                    return finish(
                        BAD_REQUEST,
                        {
                            "error": "unexpected_partitions",
                            "message": "确认包含不属于本次撤销的多余分区",
                            "request_id": req_id,
                            "partitions": sorted(extra, key=_part_key),
                        },
                    )

                # ---- 临界区：释放旧所有权、转授、推进、公布代次，一次提交 ----
                new_epoch = self._epoch(conn) + 1
                for part in parts:
                    rec = by_part[part]
                    conn.execute(
                        "UPDATE assignments SET owner = ?, epoch = ? WHERE part = ?",
                        (rec["target"], new_epoch, part),
                    )
                    conn.execute("DELETE FROM revocations WHERE part = ?", (part,))

                # 已持久化目标保持不变：released 由“目标 - 仍在撤销表”推导，
                # 交接完成后仍可据此审计与重新收敛。
                remaining = conn.execute(
                    "SELECT COUNT(*) FROM revocations"
                ).fetchone()[0]
                self._set_epoch(conn, new_epoch)

                if remaining == 0:
                    conn.execute(
                        "UPDATE handover_state SET status = 'completed',"
                        " completed_at = ?, result_json = ? WHERE id = 1",
                        (
                            _now(),
                            _canonical_json(
                                {
                                    "status": "completed",
                                    "epoch": new_epoch,
                                    "request_id": active["req_id"],
                                }
                            ),
                        ),
                    )
                    status = "completed"
                else:
                    status = "partially_released"

                # 回执与交接状态在同一事务内推进：有效确认按释放方归集，
                # 最后一次有效确认把回执定格为唯一完成回执。
                self._advance_receipt(
                    conn, active["req_id"], member, parts,
                    remaining == 0, new_epoch,
                )

                body = {
                    "status": status,
                    "epoch": new_epoch,
                    "request_id": req_id,
                    "confirmed": sorted(parts, key=_part_key),
                    "remaining": [
                        r["part"]
                        for r in conn.execute(
                            "SELECT part FROM revocations ORDER BY part"
                        )
                    ],
                }
                return finish(OK, body)
            except Exception:
                conn.execute("ROLLBACK")
                raise


def _part_key(part: str) -> tuple[int, int, str]:
    """分区排序：数字分区号按数值，其余退化为字典序。"""
    try:
        return (0, int(part), part)
    except ValueError:
        return (1, 0, part)
