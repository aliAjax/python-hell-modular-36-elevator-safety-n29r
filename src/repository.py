import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS ledger_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    stable_id TEXT NOT NULL UNIQUE,
                    batch_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    equipment_id TEXT NOT NULL,
                    ref_id TEXT,
                    event_time TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    recorded_by TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_ledger_events_equipment
                    ON ledger_events(equipment_id, event_time, seq);
                CREATE INDEX IF NOT EXISTS idx_ledger_events_batch
                    ON ledger_events(batch_id, seq);
                CREATE TABLE IF NOT EXISTS backfill_batches (
                    batch_id TEXT PRIMARY KEY,
                    batch_key TEXT UNIQUE,
                    submitted_by TEXT NOT NULL,
                    status TEXT NOT NULL,
                    total INTEGER NOT NULL,
                    recorded INTEGER NOT NULL,
                    duplicates INTEGER NOT NULL,
                    report TEXT NOT NULL,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS conclusions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    equipment_id TEXT NOT NULL,
                    permit_id TEXT,
                    as_of TEXT NOT NULL,
                    basis TEXT NOT NULL,
                    result TEXT NOT NULL,
                    signature TEXT NOT NULL,
                    status TEXT NOT NULL,
                    supersedes INTEGER,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_conclusions_equipment
                    ON conclusions(equipment_id, status, as_of);
                CREATE TABLE IF NOT EXISTS permit_reviews (
                    permit_id TEXT PRIMARY KEY,
                    equipment_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
            """)

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        entities = self.list_entities(kind=kind)
        if field == "*":
            return entities
        return [
            entity
            for entity in entities
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True

    @contextmanager
    def transaction(self):
        """One BEGIN IMMEDIATE transaction; writers serialize, first commit wins."""
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _event_from_row(row):
        return {
            "seq": int(row["seq"]),
            "stable_id": row["stable_id"],
            "batch_id": row["batch_id"],
            "kind": row["kind"],
            "event_type": row["event_type"],
            "equipment_id": row["equipment_id"],
            "ref_id": row["ref_id"],
            "event_time": row["event_time"],
            "payload": json.loads(row["payload"]),
            "content_hash": row["content_hash"],
            "recorded_by": row["recorded_by"],
            "recorded_at": row["recorded_at"],
        }

    @staticmethod
    def _conclusion_from_row(row):
        return {
            "id": int(row["id"]),
            "equipment_id": row["equipment_id"],
            "permit_id": row["permit_id"],
            "as_of": row["as_of"],
            "basis": json.loads(row["basis"]),
            "result": json.loads(row["result"]),
            "signature": json.loads(row["signature"]),
            "status": row["status"],
            "supersedes": row["supersedes"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _review_from_row(row):
        return {
            "permit_id": row["permit_id"],
            "equipment_id": row["equipment_id"],
            "batch_id": row["batch_id"],
            "reason": row["reason"],
            "status": row["status"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _batch_summary(row):
        return {
            "batch_id": row["batch_id"],
            "batch_key": row["batch_key"],
            "submitted_by": row["submitted_by"],
            "status": row["status"],
            "total": int(row["total"]),
            "recorded": int(row["recorded"]),
            "duplicates": int(row["duplicates"]),
            "error": row["error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def conn_get_entity(self, connection, entity_id):
        row = connection.execute(
            "SELECT * FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        return self._entity_from_row(row) if row else None

    def conn_events_for_batch(self, connection, batch_id):
        rows = connection.execute(
            "SELECT * FROM ledger_events WHERE batch_id = ? ORDER BY seq", (batch_id,)
        ).fetchall()
        return [self._event_from_row(row) for row in rows]

    def conn_events_for_equipment(self, connection, equipment_id):
        rows = connection.execute(
            "SELECT * FROM ledger_events WHERE equipment_id = ? ORDER BY event_time, seq",
            (equipment_id,),
        ).fetchall()
        return [self._event_from_row(row) for row in rows]

    def conn_active_conclusions(self, connection, equipment_id, as_of_from):
        rows = connection.execute(
            "SELECT * FROM conclusions WHERE equipment_id = ? AND status = 'active' AND as_of >= ? "
            "ORDER BY as_of, id",
            (equipment_id, as_of_from),
        ).fetchall()
        return [self._conclusion_from_row(row) for row in rows]

    def conn_get_review(self, connection, permit_id):
        row = connection.execute(
            "SELECT * FROM permit_reviews WHERE permit_id = ?", (permit_id,)
        ).fetchone()
        return self._review_from_row(row) if row else None

    def conn_update_entity_status(self, connection, entity_id, status, data):
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection.execute(
            "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? WHERE id = ?",
            (status, payload, utcnow(), entity_id),
        )

    def conn_append_audit(self, connection, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        connection.execute(
            "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entity_id,
                actor_id,
                actor_role,
                action,
                from_status,
                to_status,
                json.dumps(detail, ensure_ascii=False, sort_keys=True),
                utcnow(),
            ),
        )

    def get_batch(self, batch_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT report FROM backfill_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        if not row:
            raise NotFoundError("batch not found: " + batch_id)
        return json.loads(row["report"])

    def get_batch_by_key(self, batch_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT report FROM backfill_batches WHERE batch_key = ?", (batch_key,)
            ).fetchone()
        return json.loads(row["report"]) if row else None

    def list_batches(self):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM backfill_batches ORDER BY created_at, batch_id"
            ).fetchall()
        return [self._batch_summary(row) for row in rows]

    def mark_batch_replay_failed(self, batch_id, error, actor):
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT report, status FROM backfill_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("batch not found: " + batch_id)
            report = json.loads(row["report"])
            report["status"] = "replay_failed"
            report["error"] = error
            connection.execute(
                "UPDATE backfill_batches SET status = 'replay_failed', error = ?, report = ?, updated_at = ? "
                "WHERE batch_id = ?",
                (error, json.dumps(report, ensure_ascii=False, sort_keys=True), utcnow(), batch_id),
            )
            self.conn_append_audit(
                connection, batch_id, actor.user_id, actor.role,
                "backfill_replay_failed", row["status"], "replay_failed", {"error": error},
            )
        return self.get_batch(batch_id)

    def list_ledger_events(self, equipment_id=None, kind=None):
        clauses = []
        params = []
        if equipment_id:
            clauses.append("equipment_id = ?")
            params.append(equipment_id)
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM ledger_events" + where + " ORDER BY event_time, seq", params
            ).fetchall()
        return [self._event_from_row(row) for row in rows]

    def list_conclusions(self, equipment_id=None):
        clauses = []
        params = []
        if equipment_id:
            clauses.append("equipment_id = ?")
            params.append(equipment_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM conclusions" + where + " ORDER BY id", params
            ).fetchall()
        return [self._conclusion_from_row(row) for row in rows]

    def list_permit_reviews(self):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM permit_reviews ORDER BY created_at, permit_id"
            ).fetchall()
        return [self._review_from_row(row) for row in rows]
