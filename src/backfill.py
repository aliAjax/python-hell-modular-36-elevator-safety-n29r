"""Offline backfill replay ledger.

Records produced at a disconnected site are replayed in event-time order once
the network is back. Each record carries a stable (source_id, record_id) identity
so re-submission is idempotent. A batch is replayed atomically: if replay fails
the batch row (with its original records) is retained and can be retried.

After replay, derived fitness conclusions are recomputed for affected equipment.
Permits that were granted but no longer hold at their grant time are moved to
pending_review and the equipment is listed.
"""

import hashlib
import json
from datetime import datetime

from .domain import ConflictError, ValidationError
from .repository import utcnow


def _stable_id(source_id, record_id):
    digest = hashlib.sha256((source_id + "\0" + record_id).encode("utf-8")).hexdigest()[:32]
    return "bf-" + digest


def _valid_iso(value):
    try:
        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return False
    return True


class BackfillService:
    def __init__(self, repository, rules, audit):
        self.repository = repository
        self.rules = rules
        self.audit = audit

    # --- public API -------------------------------------------------------

    def submit(self, actor, records, batch_id=None, source_id=None):
        records = self._validate_records(records)
        if batch_id:
            batch_id = str(batch_id)
        else:
            digest = hashlib.sha256(
                json.dumps(records, sort_keys=True, ensure_ascii=False).encode("utf-8")
            ).hexdigest()[:16]
            batch_id = "batch-" + digest

        connection = self.repository.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = self.repository.get_batch(batch_id, connection)
            if existing and existing["status"] == "committed":
                connection.rollback()
                original = existing["report"]
                report = self._build_report(
                    batch_id,
                    "committed",
                    [],
                    [str(item["record_id"]) for item in records],
                    original.get("conclusions", []),
                    original.get("invalidated_conclusions", []),
                    original.get("review_required", []),
                )
                return report
            if existing and existing["status"] == "pending":
                connection.rollback()
                raise ConflictError("batch is already being replayed: " + batch_id)

            is_retry = existing is not None and existing["status"] == "failed"
            if is_retry:
                records = existing["records"]
            else:
                self.repository.insert_batch(
                    connection,
                    batch_id,
                    source_id or (records[0].get("source_id") if records else ""),
                    records,
                    self._empty_report(batch_id),
                )

            connection.execute("SAVEPOINT replay")
            try:
                applied, deduped = self._apply_records(connection, actor, batch_id, records)
                conclusions, invalidated, review_required = self._recompute(connection, actor, records)
            except Exception as exc:
                connection.execute("ROLLBACK TO SAVEPOINT replay")
                report = self._build_report(
                    batch_id, "failed", [], [], [], [], [], error=str(exc)
                )
                self.repository.mark_batch_failed(connection, batch_id, report, str(exc))
                connection.commit()
                return report

            connection.execute("RELEASE SAVEPOINT replay")
            report = self._build_report(
                batch_id, "committed", applied, deduped, conclusions, invalidated, review_required
            )
            self.repository.mark_batch_committed(connection, batch_id, report)
            connection.commit()
            return report
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def retry(self, actor, batch_id):
        batch_id = str(batch_id)
        connection = self.repository.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = self.repository.get_batch(batch_id, connection)
            if not existing:
                connection.rollback()
                raise ValidationError("batch not found: " + batch_id)
            if existing["status"] == "pending":
                connection.rollback()
                raise ConflictError("batch is already being replayed: " + batch_id)
            if existing["status"] == "committed":
                connection.rollback()
                return existing["report"]

            records = existing["records"]
            connection.execute("SAVEPOINT replay")
            try:
                applied, deduped = self._apply_records(connection, actor, batch_id, records)
                conclusions, invalidated, review_required = self._recompute(connection, actor, records)
            except Exception as exc:
                connection.execute("ROLLBACK TO SAVEPOINT replay")
                report = self._build_report(
                    batch_id, "failed", [], [], [], [], [], error=str(exc)
                )
                self.repository.mark_batch_failed(connection, batch_id, report, str(exc))
                connection.commit()
                return report

            connection.execute("RELEASE SAVEPOINT replay")
            report = self._build_report(
                batch_id, "committed", applied, deduped, conclusions, invalidated, review_required
            )
            self.repository.mark_batch_committed(connection, batch_id, report)
            connection.commit()
            return report
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def get(self, batch_id):
        batch = self.repository.get_batch(str(batch_id))
        if not batch:
            raise ValidationError("batch not found: " + str(batch_id))
        return batch["report"]

    def list(self):
        return [
            {
                "batch_id": batch["batch_id"],
                "source_id": batch["source_id"],
                "status": batch["status"],
                "record_count": batch["record_count"],
                "attempt": batch["attempt"],
                "error": batch["error"],
                "created_at": batch["created_at"],
                "committed_at": batch["committed_at"],
            }
            for batch in self.repository.list_batches()
        ]

    # --- replay internals -------------------------------------------------

    def _validate_records(self, records):
        if not isinstance(records, list):
            raise ValidationError("records must be a list")
        for raw in records:
            if not isinstance(raw, dict):
                raise ValidationError("each backfill record must be an object")
            if not str(raw.get("source_id", "")).strip() or not str(raw.get("record_id", "")).strip():
                raise ValidationError("source_id and record_id are required")
            if not str(raw.get("kind", "")).strip():
                raise ValidationError("kind is required")
            if not str(raw.get("event_time", "")).strip():
                raise ValidationError("event_time is required")
            if not _valid_iso(raw.get("event_time")):
                raise ValidationError("event_time must be ISO-8601")
        return records

    def _apply_records(self, connection, actor, batch_id, records):
        applied = []
        deduped = []
        for rec in sorted(records, key=lambda item: str(item["event_time"])):
            was_deduped = self._apply_record(connection, actor, batch_id, rec)
            if was_deduped:
                deduped.append(str(rec["record_id"]))
            else:
                applied.append(str(rec["record_id"]))
        return applied, deduped

    def _apply_record(self, connection, actor, batch_id, rec):
        source_id = str(rec["source_id"]).strip()
        record_id = str(rec["record_id"]).strip()
        kind = self.rules.normalize_kind(str(rec["kind"]).strip())
        event_time = str(rec["event_time"]).strip()
        data = dict(rec.get("data") or {})
        data["event_time"] = event_time

        if kind not in self.rules.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + kind)
        self.rules.validate_create(actor, kind, data, self._lookup)

        status = rec.get("status")
        if status is None:
            status = self.rules.initial_status(kind, data)
        else:
            status = str(status)
            if status not in self.rules.known_statuses(kind):
                raise ValidationError("invalid status %s for %s" % (status, kind))

        if self.repository.get_backfill_record(source_id, record_id, connection):
            return True

        entity_id = _stable_id(source_id, record_id)
        if self.repository.get_entity(entity_id, connection):
            return True

        self.repository.create_entity(
            entity_id, kind, status, data, actor.user_id,
            connection=connection, created_at=event_time,
        )
        self.repository.save_backfill_record(
            connection, source_id, record_id, batch_id, kind, event_time, entity_id, rec
        )
        self.repository.append_audit(
            entity_id, actor.user_id, actor.role, "backfill", None, status,
            {"source_id": source_id, "record_id": record_id, "event_time": event_time},
            connection=connection,
        )
        return False

    def _recompute(self, connection, actor, records):
        affected = set()
        for rec in records:
            data = rec.get("data") or {}
            equipment_id = data.get("equipment_id")
            if equipment_id:
                affected.add(equipment_id)
        affected.discard(None)

        conclusions = []
        invalidated = []
        review_required = []
        for equipment_id in sorted(affected):
            equipment = self.repository.get_entity(equipment_id, connection)
            if not equipment:
                continue
            batch_times = [
                str(r["event_time"])
                for r in records
                if (r.get("data") or {}).get("equipment_id") == equipment_id
            ]
            as_of = max(batch_times) if batch_times else utcnow()
            fit, basis = self._fitness(connection, equipment_id, as_of)
            new_conclusion = "fit" if fit else "unfit"

            previous = self.repository.get_conclusion(equipment_id, connection)
            if not previous or previous["conclusion"] != new_conclusion:
                invalidated.append(equipment_id)
            self.repository.upsert_conclusion(connection, equipment_id, new_conclusion, basis, as_of)
            conclusions.append({
                "equipment_id": equipment_id,
                "conclusion": new_conclusion,
                "as_of": as_of,
                "basis": basis,
            })

            for permit in self.repository.list_entities(kind="permit", connection=connection):
                if permit["data"].get("equipment_id") != equipment_id:
                    continue
                if permit["status"] != "granted":
                    continue
                grant_time = permit["data"].get("granted_at") or permit["created_at"]
                fit_at_grant, grant_basis = self._fitness(connection, equipment_id, grant_time)
                if fit_at_grant:
                    continue
                self.repository.update_entity(
                    permit["id"], permit["version"], "pending_review", permit["data"],
                    connection=connection,
                )
                self.repository.append_audit(
                    permit["id"], actor.user_id, actor.role, "permit_review",
                    "granted", "pending_review",
                    {"reason": "permit basis no longer holds at grant time", "basis": grant_basis},
                    connection=connection,
                )
                review_required.append({
                    "permit_id": permit["id"],
                    "equipment_id": equipment_id,
                    "reason": "permit basis no longer holds at grant time",
                    "basis": grant_basis,
                })
        return conclusions, invalidated, review_required

    def _fitness(self, connection, equipment_id, at_time):
        def event_time(entity):
            return entity["data"].get("event_time") or entity["created_at"]

        inspections = [
            item for item in self.repository.list_entities(kind="inspection", connection=connection)
            if item["data"].get("equipment_id") == equipment_id and event_time(item) <= at_time
        ]
        inspections.sort(key=event_time)
        latest = inspections[-1] if inspections else None

        open_remediations = [
            item for item in self.repository.list_entities(kind="remediation", connection=connection)
            if item["data"].get("equipment_id") == equipment_id
            and item["status"] != "closed"
            and event_time(item) <= at_time
        ]

        equipment_events = [
            item for item in self.repository.list_entities(kind="equipment", connection=connection)
            if item["id"] == equipment_id and event_time(item) <= at_time
        ]
        equipment_events.sort(key=event_time)
        if equipment_events:
            equipment_status = equipment_events[-1]["status"]
        else:
            equipment = self.repository.get_entity(equipment_id, connection)
            equipment_status = equipment["status"] if equipment else None

        serviceable = equipment_status in ("in_service", "suspended")
        fit = serviceable and bool(latest and latest["status"] == "passed") and not open_remediations
        basis = {
            "equipment_status": equipment_status,
            "inspection": (
                {"id": latest["id"], "status": latest["status"], "event_time": event_time(latest)}
                if latest else None
            ),
            "open_remediations": [
                {"id": item["id"], "issue": item["data"].get("issue"), "event_time": event_time(item)}
                for item in open_remediations
            ],
        }
        return fit, basis

    # --- helpers ----------------------------------------------------------

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def _empty_report(self, batch_id):
        return self._build_report(batch_id, "pending", [], [], [], [], [])

    @staticmethod
    def _build_report(batch_id, status, applied, deduped, conclusions, invalidated, review_required, error=None):
        return {
            "batch_id": batch_id,
            "status": status,
            "applied": applied,
            "deduped": deduped,
            "conclusions": conclusions,
            "invalidated_conclusions": invalidated,
            "review_required": review_required,
            "error": error,
        }
