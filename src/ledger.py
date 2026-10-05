import hashlib
import json
from datetime import datetime, timezone
from uuid import uuid4

from .domain import NotFoundError, PermissionDenied, ValidationError
from .repository import utcnow

EVENT_TYPES = {
    "inspection": ("result_recorded",),
    "alarm": ("raised", "closed"),
    "remediation": ("opened", "closed"),
    "permit": ("granted", "revoked"),
}
REF_REQUIRED = {
    ("alarm", "raised"),
    ("alarm", "closed"),
    ("remediation", "opened"),
    ("remediation", "closed"),
    ("permit", "granted"),
    ("permit", "revoked"),
}
WRITE_ROLES = ("admin", "inspector")

# Live entity activity mirrored into the ledger so backfills and online
# records replay from one book.
LIVE_EVENTS = {
    ("inspection", "pass"): ("inspection", "result_recorded"),
    ("inspection", "fail"): ("inspection", "result_recorded"),
    ("alarm", "create"): ("alarm", "raised"),
    ("alarm", "close"): ("alarm", "closed"),
    ("alarm", "mark_false"): ("alarm", "closed"),
    ("remediation", "create"): ("remediation", "opened"),
    ("remediation", "close"): ("remediation", "closed"),
    ("permit", "grant"): ("permit", "granted"),
    ("permit", "revoke"): ("permit", "revoked"),
    ("permit", "expire"): ("permit", "revoked"),
}


class _DuplicateBatch(Exception):
    """Another writer committed the same batch_key first."""

    def __init__(self, batch_id):
        super().__init__(batch_id)
        self.batch_id = batch_id


def parse_event_time(value, field="event_time"):
    try:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise ValidationError(field + " must be ISO-8601")
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).isoformat(timespec="seconds")


def _up_to(events, as_of):
    return [event for event in events if event["event_time"] <= as_of]


def compute_standing(events):
    """Fold events (ordered by event_time, seq) into equipment standing."""
    inspection = None
    open_alarms = {}
    open_remediations = {}
    permits = {}
    for event in events:
        kind = event["kind"]
        event_type = event["event_type"]
        ref_id = event["ref_id"]
        payload = event["payload"]
        if kind == "inspection":
            inspection = {
                "result": payload.get("result"),
                "findings": payload.get("findings"),
                "event_time": event["event_time"],
                "seq": event["seq"],
            }
        elif kind == "alarm":
            if event_type == "raised":
                open_alarms[ref_id] = {
                    "seq": event["seq"],
                    "code": payload.get("code"),
                    "since": event["event_time"],
                }
            else:
                open_alarms.pop(ref_id, None)
        elif kind == "remediation":
            if event_type == "opened":
                open_remediations[ref_id] = {
                    "seq": event["seq"],
                    "issue": payload.get("issue"),
                    "since": event["event_time"],
                }
            else:
                open_remediations.pop(ref_id, None)
        elif kind == "permit":
            permit = permits.setdefault(ref_id, {})
            if event_type == "granted":
                permit.update(
                    {
                        "state": "granted",
                        "grant_time": event["event_time"],
                        "grant_seq": event["seq"],
                    }
                )
            elif event_type == "revoked":
                permit["state"] = "revoked"
    reasons = []
    if inspection is None:
        reasons.append("no inspection recorded")
    elif inspection["result"] != "passed":
        reasons.append("latest inspection result is " + str(inspection["result"]))
    for remediation_id in sorted(open_remediations):
        reasons.append("open remediation " + remediation_id)
    eligible = (
        inspection is not None
        and inspection["result"] == "passed"
        and not open_remediations
    )
    return {
        "inspection": inspection,
        "open_alarms": [
            dict(value, ref_id=key) for key, value in sorted(open_alarms.items())
        ],
        "open_remediations": [
            dict(value, ref_id=key) for key, value in sorted(open_remediations.items())
        ],
        "permits": permits,
        "eligible": eligible,
        "reasons": reasons,
        "basis": {
            "inspection_seq": inspection["seq"] if inspection else None,
            "open_remediation_seqs": sorted(
                item["seq"] for item in open_remediations.values()
            ),
            "open_alarm_seqs": sorted(item["seq"] for item in open_alarms.values()),
        },
    }


def _signature(standing):
    """The permit-relevant part of a standing; alarms are context only."""
    basis = standing["basis"]
    return {
        "eligible": standing["eligible"],
        "inspection_seq": basis["inspection_seq"],
        "open_remediation_seqs": list(basis["open_remediation_seqs"]),
    }


def _content_hash(record):
    canonical = {
        "kind": record["kind"],
        "event_type": record["event_type"],
        "equipment_id": record["equipment_id"],
        "ref_id": record["ref_id"],
        "event_time": record["event_time"],
        "payload": record["payload"],
    }
    return hashlib.sha256(
        json.dumps(canonical, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


class LedgerService:
    """Append-only event ledger with time-point replay for backfilled records."""

    def __init__(self, repository):
        self.repository = repository

    def _normalize_record(self, raw, index):
        label = "record #%d" % index
        if not isinstance(raw, dict):
            raise ValidationError(label + " must be an object")
        stable_id = str(raw.get("stable_id") or "").strip()
        if not stable_id:
            raise ValidationError(label + ": stable_id is required")
        kind = str(raw.get("kind") or "").strip()
        event_type = str(raw.get("event_type") or "").strip()
        if kind not in EVENT_TYPES or event_type not in EVENT_TYPES[kind]:
            raise ValidationError(
                label + ": unknown event type %s/%s" % (kind, event_type)
            )
        equipment_id = str(raw.get("equipment_id") or "").strip()
        if not equipment_id:
            raise ValidationError(label + ": equipment_id is required")
        ref_id = str(raw.get("ref_id") or "").strip() or None
        if (kind, event_type) in REF_REQUIRED and not ref_id:
            raise ValidationError(
                label + ": ref_id is required for %s %s" % (kind, event_type)
            )
        event_time = parse_event_time(raw.get("event_time"), label + ": event_time")
        payload = raw.get("payload") or {}
        if not isinstance(payload, dict):
            raise ValidationError(label + ": payload must be an object")
        if kind == "inspection" and payload.get("result") not in ("passed", "failed"):
            raise ValidationError(
                label + ": inspection payload.result must be passed or failed"
            )
        record = {
            "stable_id": stable_id,
            "kind": kind,
            "event_type": event_type,
            "equipment_id": equipment_id,
            "ref_id": ref_id,
            "event_time": event_time,
            "payload": payload,
        }
        record["content_hash"] = _content_hash(record)
        return record

    def submit_batch(self, actor, payload):
        if actor.role not in WRITE_ROLES:
            raise PermissionDenied(
                "role %s cannot submit backfill batches" % actor.role
            )
        if not isinstance(payload, dict):
            raise ValidationError("batch must be a JSON object")
        records = payload.get("records")
        if not isinstance(records, list) or not records:
            raise ValidationError("records must be a non-empty list")
        batch_key = str(payload.get("batch_key") or "").strip() or None
        if batch_key:
            existing = self.repository.get_batch_by_key(batch_key)
            if existing:
                existing["duplicate_submission"] = True
                return existing
        normalized = [
            self._normalize_record(raw, index + 1) for index, raw in enumerate(records)
        ]
        batch_id = uuid4().hex
        now = utcnow()
        outcomes = []
        try:
            with self.repository.transaction() as connection:
                if batch_key:
                    row = connection.execute(
                        "SELECT batch_id FROM backfill_batches WHERE batch_key = ?",
                        (batch_key,),
                    ).fetchone()
                    if row:
                        raise _DuplicateBatch(row["batch_id"])
                recorded = 0
                for record in normalized:
                    row = connection.execute(
                        "SELECT seq, content_hash FROM ledger_events WHERE stable_id = ?",
                        (record["stable_id"],),
                    ).fetchone()
                    if row:
                        # First write wins; a resubmitted record never overwrites.
                        outcomes.append(
                            {
                                "stable_id": record["stable_id"],
                                "outcome": "duplicate",
                                "seq": int(row["seq"]),
                                "content_mismatch": row["content_hash"]
                                != record["content_hash"],
                            }
                        )
                        continue
                    cursor = connection.execute(
                        "INSERT INTO ledger_events(stable_id, batch_id, kind, event_type, "
                        "equipment_id, ref_id, event_time, payload, content_hash, "
                        "recorded_by, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            record["stable_id"],
                            batch_id,
                            record["kind"],
                            record["event_type"],
                            record["equipment_id"],
                            record["ref_id"],
                            record["event_time"],
                            json.dumps(
                                record["payload"], ensure_ascii=False, sort_keys=True
                            ),
                            record["content_hash"],
                            actor.user_id,
                            now,
                        ),
                    )
                    recorded += 1
                    outcomes.append(
                        {
                            "stable_id": record["stable_id"],
                            "outcome": "recorded",
                            "seq": int(cursor.lastrowid),
                            "content_mismatch": False,
                        }
                    )
                report = {
                    "batch_id": batch_id,
                    "batch_key": batch_key,
                    "status": "received",
                    "submitted_by": actor.user_id,
                    "created_at": now,
                    "total": len(normalized),
                    "recorded": recorded,
                    "duplicates": len(normalized) - recorded,
                    "records": outcomes,
                    "affected_equipment": [],
                    "conclusions_created": [],
                    "conclusions_invalidated": [],
                    "permits_pending_review": [],
                    "error": None,
                }
                connection.execute(
                    "INSERT INTO backfill_batches(batch_id, batch_key, submitted_by, status, "
                    "total, recorded, duplicates, report, error, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)",
                    (
                        batch_id,
                        batch_key,
                        actor.user_id,
                        "received",
                        len(normalized),
                        recorded,
                        len(normalized) - recorded,
                        json.dumps(report, ensure_ascii=False, sort_keys=True),
                        now,
                        now,
                    ),
                )
                self.repository.conn_append_audit(
                    connection,
                    batch_id,
                    actor.user_id,
                    actor.role,
                    "backfill_submit",
                    None,
                    "received",
                    {"total": len(normalized), "recorded": recorded},
                )
        except _DuplicateBatch as duplicate:
            existing = self.repository.get_batch(duplicate.batch_id)
            existing["duplicate_submission"] = True
            return existing
        try:
            return self._replay_batch(batch_id, actor)
        except Exception as exc:
            # The batch and its events stay recorded; only derived writes roll
            # back, so the original batch can be retried as-is.
            return self.repository.mark_batch_replay_failed(batch_id, str(exc), actor)

    def retry_batch(self, actor, batch_id):
        if actor.role not in WRITE_ROLES:
            raise PermissionDenied("role %s cannot replay batches" % actor.role)
        batch = self.repository.get_batch(batch_id)
        if batch["status"] == "applied":
            return batch
        try:
            return self._replay_batch(batch_id, actor)
        except Exception as exc:
            return self.repository.mark_batch_replay_failed(batch_id, str(exc), actor)

    def _replay_batch(self, batch_id, actor):
        with self.repository.transaction() as connection:
            row = connection.execute(
                "SELECT report, status FROM backfill_batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            if not row:
                raise NotFoundError("batch not found: " + batch_id)
            report = json.loads(row["report"])
            if row["status"] == "applied":
                return report
            events = self.repository.conn_events_for_batch(connection, batch_id)
            affected = {}
            for event in events:
                equipment = self.repository.conn_get_entity(
                    connection, event["equipment_id"]
                )
                if not equipment or equipment["kind"] != "equipment":
                    raise ValidationError(
                        "equipment not found for ledger event " + event["stable_id"]
                    )
                affected.setdefault(event["equipment_id"], []).append(event)
            created = []
            invalidated = []
            flagged = []
            for equipment_id in sorted(affected):
                new_events = affected[equipment_id]
                all_events = self.repository.conn_events_for_equipment(
                    connection, equipment_id
                )
                earliest = min(event["event_time"] for event in new_events)
                for event in new_events:
                    if event["kind"] == "permit" and event["event_type"] == "granted":
                        conclusion = self._store_conclusion(
                            connection,
                            equipment_id,
                            event["ref_id"],
                            event["event_time"],
                            all_events,
                        )
                        created.append(
                            {
                                "conclusion_id": conclusion["id"],
                                "permit_id": event["ref_id"],
                                "equipment_id": equipment_id,
                                "as_of": conclusion["as_of"],
                                "eligible": conclusion["result"]["eligible"],
                            }
                        )
                        if not conclusion["result"]["eligible"]:
                            review = self._flag_permit_review(
                                connection,
                                event["ref_id"],
                                equipment_id,
                                conclusion,
                                batch_id,
                                actor,
                                all_events,
                            )
                            if review:
                                flagged.append(review)
                # Only conclusions at or after the earliest backfilled event can
                # change; later events only move current state.
                for conclusion in self.repository.conn_active_conclusions(
                    connection, equipment_id, earliest
                ):
                    standing = compute_standing(
                        _up_to(all_events, conclusion["as_of"])
                    )
                    if _signature(standing) == conclusion["signature"]:
                        continue
                    connection.execute(
                        "UPDATE conclusions SET status = 'invalidated' WHERE id = ?",
                        (conclusion["id"],),
                    )
                    replacement = self._store_conclusion(
                        connection,
                        equipment_id,
                        conclusion["permit_id"],
                        conclusion["as_of"],
                        all_events,
                        supersedes=conclusion["id"],
                    )
                    invalidated.append(
                        {
                            "conclusion_id": conclusion["id"],
                            "new_conclusion_id": replacement["id"],
                            "permit_id": conclusion["permit_id"],
                            "equipment_id": equipment_id,
                            "as_of": conclusion["as_of"],
                            "was_eligible": conclusion["result"]["eligible"],
                            "now_eligible": replacement["result"]["eligible"],
                        }
                    )
                    if conclusion["permit_id"] and not replacement["result"]["eligible"]:
                        review = self._flag_permit_review(
                            connection,
                            conclusion["permit_id"],
                            equipment_id,
                            replacement,
                            batch_id,
                            actor,
                            all_events,
                        )
                        if review:
                            flagged.append(review)
            report.update(
                {
                    "status": "applied",
                    "error": None,
                    "affected_equipment": sorted(affected),
                    "conclusions_created": created,
                    "conclusions_invalidated": invalidated,
                    "permits_pending_review": flagged,
                }
            )
            connection.execute(
                "UPDATE backfill_batches SET status = 'applied', report = ?, error = NULL, "
                "updated_at = ? WHERE batch_id = ?",
                (
                    json.dumps(report, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                    batch_id,
                ),
            )
            self.repository.conn_append_audit(
                connection,
                batch_id,
                actor.user_id,
                actor.role,
                "backfill_replay",
                row["status"],
                "applied",
                {
                    "affected_equipment": sorted(affected),
                    "permits_pending_review": len(flagged),
                },
            )
            return report

    def _store_conclusion(self, connection, equipment_id, permit_id, as_of, all_events, supersedes=None):
        standing = compute_standing(_up_to(all_events, as_of))
        result = {"eligible": standing["eligible"], "reasons": standing["reasons"]}
        signature = _signature(standing)
        cursor = connection.execute(
            "INSERT INTO conclusions(equipment_id, permit_id, as_of, basis, result, "
            "signature, status, supersedes, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)",
            (
                equipment_id,
                permit_id,
                as_of,
                json.dumps(standing["basis"], sort_keys=True),
                json.dumps(result, ensure_ascii=False, sort_keys=True),
                json.dumps(signature, sort_keys=True),
                supersedes,
                utcnow(),
            ),
        )
        return {
            "id": int(cursor.lastrowid),
            "permit_id": permit_id,
            "as_of": as_of,
            "basis": standing["basis"],
            "result": result,
            "signature": signature,
        }

    def _flag_permit_review(self, connection, permit_id, equipment_id, conclusion, batch_id, actor, all_events):
        if not permit_id or self.repository.conn_get_review(connection, permit_id):
            return None
        permit = compute_standing(all_events)["permits"].get(permit_id)
        if not permit or permit.get("state") != "granted":
            return None
        reasons = "; ".join(conclusion["result"]["reasons"]) or "basis changed"
        reason = "permit basis no longer holds as of %s: %s" % (
            conclusion["as_of"],
            reasons,
        )
        now = utcnow()
        connection.execute(
            "INSERT INTO permit_reviews(permit_id, equipment_id, batch_id, reason, status, "
            "created_at) VALUES (?, ?, ?, ?, 'pending_review', ?)",
            (permit_id, equipment_id, batch_id, reason, now),
        )
        entity = self.repository.conn_get_entity(connection, permit_id)
        if entity and entity["kind"] == "permit" and entity["status"] == "granted":
            data = dict(entity["data"])
            data.update(
                {
                    "review_reason": reason,
                    "review_batch_id": batch_id,
                    "review_flagged_at": now,
                }
            )
            self.repository.conn_update_entity_status(
                connection, permit_id, "pending_review", data
            )
            self.repository.conn_append_audit(
                connection,
                permit_id,
                actor.user_id,
                actor.role,
                "backfill_flag_review",
                "granted",
                "pending_review",
                {"batch_id": batch_id, "reason": reason},
            )
        return {
            "permit_id": permit_id,
            "equipment_id": equipment_id,
            "reason": reason,
        }

    def record_live_event(self, entity, action, actor):
        """Mirror an online entity change into the ledger (best stable id)."""
        mapping = LIVE_EVENTS.get((entity["kind"], action))
        if not mapping:
            return
        kind, event_type = mapping
        data = entity["data"]
        equipment_id = data.get("equipment_id")
        if not equipment_id:
            return
        now = utcnow()
        if kind == "inspection":
            payload = {
                "result": "passed" if action == "pass" else "failed",
                "findings": data.get("findings"),
            }
            event_time = now
        elif kind == "alarm":
            payload = {"code": data.get("code")}
            if action == "create":
                try:
                    event_time = parse_event_time(data.get("occurred_at"))
                except ValidationError:
                    event_time = now
            else:
                event_time = now
        elif kind == "remediation":
            payload = {"issue": data.get("issue")}
            event_time = now
        else:
            payload = {"purpose": data.get("purpose")}
            if action in ("revoke", "expire"):
                payload["cause"] = action
            event_time = now
        stable_id = "live:%s:%s:%s" % (entity["id"], action, entity["version"])
        record = {
            "stable_id": stable_id,
            "kind": kind,
            "event_type": event_type,
            "equipment_id": equipment_id,
            "ref_id": entity["id"],
            "event_time": event_time,
            "payload": payload,
        }
        with self.repository.transaction() as connection:
            row = connection.execute(
                "SELECT seq FROM ledger_events WHERE stable_id = ?", (stable_id,)
            ).fetchone()
            if row:
                return
            connection.execute(
                "INSERT INTO ledger_events(stable_id, batch_id, kind, event_type, "
                "equipment_id, ref_id, event_time, payload, content_hash, recorded_by, "
                "recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    stable_id,
                    "live",
                    kind,
                    event_type,
                    equipment_id,
                    entity["id"],
                    event_time,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    _content_hash(record),
                    actor.user_id,
                    now,
                ),
            )
            if (kind, event_type) == ("permit", "granted"):
                all_events = self.repository.conn_events_for_equipment(
                    connection, equipment_id
                )
                conclusion = self._store_conclusion(
                    connection, equipment_id, entity["id"], event_time, all_events
                )
                if not conclusion["result"]["eligible"]:
                    self._flag_permit_review(
                        connection,
                        entity["id"],
                        equipment_id,
                        conclusion,
                        "live",
                        actor,
                        all_events,
                    )

    def get_batch(self, batch_id):
        return self.repository.get_batch(batch_id)

    def list_batches(self):
        return self.repository.list_batches()

    def list_events(self, equipment_id=None, kind=None):
        return self.repository.list_ledger_events(equipment_id=equipment_id, kind=kind)

    def list_conclusions(self, equipment_id=None):
        return self.repository.list_conclusions(equipment_id=equipment_id)

    def list_reviews(self):
        return self.repository.list_permit_reviews()

    def replay(self, equipment_id, as_of=None):
        equipment = self.repository.get_entity(equipment_id)
        if not equipment or equipment["kind"] != "equipment":
            raise NotFoundError("equipment not found: " + equipment_id)
        moment = parse_event_time(as_of, "as_of") if as_of else None
        events = self.repository.list_ledger_events(equipment_id=equipment_id)
        applied = [
            event for event in events if moment is None or event["event_time"] <= moment
        ]
        return {
            "equipment_id": equipment_id,
            "as_of": moment,
            "standing": compute_standing(applied),
            "events": applied,
        }
