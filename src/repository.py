import json
import sqlite3
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

    def connect(self):
        """Open a connection whose transaction the caller manages."""
        return self._connect()

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
                CREATE TABLE IF NOT EXISTS backfill_batches (
                    batch_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    records TEXT NOT NULL,
                    report TEXT NOT NULL,
                    record_count INTEGER NOT NULL,
                    error TEXT,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    committed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS backfill_records (
                    source_id TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(source_id, record_id)
                );
                CREATE INDEX IF NOT EXISTS idx_backfill_records_batch
                    ON backfill_records(batch_id);
                CREATE TABLE IF NOT EXISTS conclusions (
                    equipment_id TEXT PRIMARY KEY,
                    conclusion TEXT NOT NULL,
                    basis TEXT NOT NULL,
                    as_of TEXT NOT NULL,
                    computed_at TEXT NOT NULL
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

    @staticmethod
    def _batch_from_row(row):
        return {
            "batch_id": row["batch_id"],
            "source_id": row["source_id"],
            "status": row["status"],
            "records": json.loads(row["records"]),
            "report": json.loads(row["report"]) if row["report"] else None,
            "record_count": int(row["record_count"]),
            "error": row["error"],
            "attempt": int(row["attempt"]),
            "created_at": row["created_at"],
            "committed_at": row["committed_at"],
        }

    @staticmethod
    def _conclusion_from_row(row):
        return {
            "equipment_id": row["equipment_id"],
            "conclusion": row["conclusion"],
            "basis": json.loads(row["basis"]),
            "as_of": row["as_of"],
            "computed_at": row["computed_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id, connection=None, created_at=None):
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        now = created_at or utcnow()
        sql = (
            "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
            "VALUES (?, ?, ?, 1, ?, ?, ?, ?)"
        )
        params = (entity_id, kind, status, payload, actor_id, now, now)
        if connection is not None:
            connection.execute(sql, params)
            row = connection.execute("SELECT * FROM entities WHERE id = ?", (entity_id,)).fetchone()
            return self._entity_from_row(row)
        with self._connect() as conn:
            conn.execute(sql, params)
            row = conn.execute("SELECT * FROM entities WHERE id = ?", (entity_id,)).fetchone()
        return self._entity_from_row(row)

    def get_entity(self, entity_id, connection=None):
        sql = "SELECT * FROM entities WHERE id = ?"
        if connection is not None:
            row = connection.execute(sql, (entity_id,)).fetchone()
            return self._entity_from_row(row) if row else None
        with self._connect() as conn:
            row = conn.execute(sql, (entity_id,)).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None, connection=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = "SELECT * FROM entities" + where + " ORDER BY created_at, id"
        if connection is not None:
            rows = connection.execute(sql, params).fetchall()
            return [self._entity_from_row(row) for row in rows]
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
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

    def update_entity(self, entity_id, expected_version, status, data, connection=None):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        sql = (
            "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
            "WHERE id = ? AND version = ?"
        )
        params = (status, payload, now, entity_id, expected_version)
        if connection is not None:
            connection.execute(sql, params)
            if connection.execute("SELECT changes()").fetchone()[0] == 0:
                row = connection.execute(
                    "SELECT version FROM entities WHERE id = ?", (entity_id,)
                ).fetchone()
                if not row:
                    raise NotFoundError("entity not found: " + entity_id)
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, int(row["version"]))
                )
            row = connection.execute("SELECT * FROM entities WHERE id = ?", (entity_id,)).fetchone()
            return self._entity_from_row(row)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
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
            conn.execute(sql, (status, payload, now, entity_id, current_version))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail, connection=None):
        sql = (
            "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        params = (
            entity_id,
            actor_id,
            actor_role,
            action,
            from_status,
            to_status,
            json.dumps(detail, ensure_ascii=False, sort_keys=True),
            utcnow(),
        )
        if connection is not None:
            connection.execute(sql, params)
            return
        with self._connect() as conn:
            conn.execute(sql, params)

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

    # --- backfill batches -------------------------------------------------

    def insert_batch(self, connection, batch_id, source_id, records, report):
        connection.execute(
            "INSERT INTO backfill_batches(batch_id, source_id, status, records, report, record_count, error, attempt, created_at, committed_at) "
            "VALUES (?, ?, 'pending', ?, ?, ?, NULL, 0, ?, NULL)",
            (
                batch_id,
                source_id,
                json.dumps(records, ensure_ascii=False, sort_keys=True),
                json.dumps(report, ensure_ascii=False, sort_keys=True),
                len(records),
                utcnow(),
            ),
        )

    def get_batch(self, batch_id, connection=None):
        sql = "SELECT * FROM backfill_batches WHERE batch_id = ?"
        if connection is not None:
            row = connection.execute(sql, (batch_id,)).fetchone()
            return self._batch_from_row(row) if row else None
        with self._connect() as conn:
            row = conn.execute(sql, (batch_id,)).fetchone()
        return self._batch_from_row(row) if row else None

    def list_batches(self):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM backfill_batches ORDER BY created_at, batch_id"
            ).fetchall()
        return [self._batch_from_row(row) for row in rows]

    def mark_batch_committed(self, connection, batch_id, report):
        connection.execute(
            "UPDATE backfill_batches SET status = 'committed', report = ?, committed_at = ? "
            "WHERE batch_id = ?",
            (json.dumps(report, ensure_ascii=False, sort_keys=True), utcnow(), batch_id),
        )

    def mark_batch_failed(self, connection, batch_id, report, error):
        connection.execute(
            "UPDATE backfill_batches SET status = 'failed', report = ?, error = ?, attempt = attempt + 1 "
            "WHERE batch_id = ?",
            (json.dumps(report, ensure_ascii=False, sort_keys=True), error, batch_id),
        )

    # --- backfill records (stable identity ledger) ------------------------

    def save_backfill_record(self, connection, source_id, record_id, batch_id, kind, event_time, entity_id, payload):
        connection.execute(
            "INSERT INTO backfill_records(source_id, record_id, batch_id, kind, event_time, entity_id, payload, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                source_id,
                record_id,
                batch_id,
                kind,
                event_time,
                entity_id,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                utcnow(),
            ),
        )

    def get_backfill_record(self, source_id, record_id, connection=None):
        sql = "SELECT * FROM backfill_records WHERE source_id = ? AND record_id = ?"
        if connection is not None:
            row = connection.execute(sql, (source_id, record_id)).fetchone()
            return row if row else None
        with self._connect() as conn:
            row = conn.execute(sql, (source_id, record_id)).fetchone()
        return row if row else None

    def list_backfill_records(self, batch_id=None):
        with self._connect() as connection:
            if batch_id:
                rows = connection.execute(
                    "SELECT * FROM backfill_records WHERE batch_id = ? ORDER BY event_time, rowid",
                    (batch_id,),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM backfill_records ORDER BY event_time, rowid"
                ).fetchall()
        return [dict(row) for row in rows]

    # --- conclusions (derived fitness) ------------------------------------

    def upsert_conclusion(self, connection, equipment_id, conclusion, basis, as_of):
        connection.execute(
            "INSERT INTO conclusions(equipment_id, conclusion, basis, as_of, computed_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(equipment_id) DO UPDATE SET conclusion = excluded.conclusion, "
            "basis = excluded.basis, as_of = excluded.as_of, computed_at = excluded.computed_at",
            (
                equipment_id,
                conclusion,
                json.dumps(basis, ensure_ascii=False, sort_keys=True),
                as_of,
                utcnow(),
            ),
        )

    def get_conclusion(self, equipment_id, connection=None):
        sql = "SELECT * FROM conclusions WHERE equipment_id = ?"
        if connection is not None:
            row = connection.execute(sql, (equipment_id,)).fetchone()
            return self._conclusion_from_row(row) if row else None
        with self._connect() as conn:
            row = conn.execute(sql, (equipment_id,)).fetchone()
        return self._conclusion_from_row(row) if row else None

    def list_conclusions(self):
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM conclusions ORDER BY equipment_id").fetchall()
        return [self._conclusion_from_row(row) for row in rows]

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
