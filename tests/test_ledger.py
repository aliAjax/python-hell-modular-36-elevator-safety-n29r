import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import Actor, PermissionDenied, ValidationError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class LedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.admin = Actor("admin", "admin")
        self.inspector = Actor("inspector-1", "inspector")
        self.inspector2 = Actor("inspector-2", "inspector")

    def tearDown(self):
        self.tmp.cleanup()

    def equipment(self, asset_no="E-1", entity_id=None):
        data = {"asset_no": asset_no, "equipment_type": "elevator", "location": "A", "inspection_interval_days": 365}
        if entity_id:
            data["id"] = entity_id
        return self.service.create(self.admin, "equipment", data)

    def grant_permit(self, equipment):
        inspection = self.service.create(self.admin, "inspection", {
            "equipment_id": equipment["id"], "scheduled_at": "2026-09-27T09:00:00Z", "cycle_days": 365})
        self.service.transition(self.admin, inspection["id"], "pass", {"findings": "ok"})
        permit = self.service.create(self.admin, "permit", {
            "equipment_id": equipment["id"], "purpose": "return_to_service", "requested_by": "ops"})
        self.service.transition(self.admin, permit["id"], "request_review", {})
        return self.service.transition(self.admin, permit["id"], "grant", {})

    def remediation_record(self, equipment, stable_id, ref_id, event_type, event_time):
        return {
            "stable_id": stable_id, "kind": "remediation", "event_type": event_type,
            "equipment_id": equipment["id"], "ref_id": ref_id,
            "event_time": event_time, "payload": {"issue": "door alignment"},
        }

    def submit(self, records, batch_key=None, actor=None):
        payload = {"records": records}
        if batch_key:
            payload["batch_key"] = batch_key
        return self.service.submit_backfill(actor or self.inspector, payload)

    def test_dedup_by_stable_id(self):
        equipment = self.equipment()
        records = [self.remediation_record(equipment, "s-1", "rem-1", "opened", "2026-09-01T08:00:00Z")]
        first = self.submit(records, batch_key="batch-a")
        self.assertEqual(first["status"], "applied")
        self.assertEqual(first["recorded"], 1)

        second = self.submit(records, batch_key="batch-b", actor=self.inspector2)
        self.assertEqual(second["recorded"], 0)
        self.assertEqual(second["duplicates"], 1)
        self.assertEqual(second["records"][0]["outcome"], "duplicate")
        self.assertFalse(second["records"][0]["content_mismatch"])

        events = self.service.ledger_events(equipment_id=equipment["id"])
        self.assertEqual(len(events), 1)

    def test_duplicate_with_different_content_keeps_first_write(self):
        equipment = self.equipment()
        first = self.remediation_record(equipment, "s-1", "rem-1", "opened", "2026-09-01T08:00:00Z")
        self.submit([first])
        changed = dict(first)
        changed["payload"] = {"issue": "changed elsewhere"}
        report = self.submit([changed])
        self.assertTrue(report["records"][0]["content_mismatch"])
        events = self.service.ledger_events(equipment_id=equipment["id"])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["payload"]["issue"], "door alignment")

    def test_batch_key_is_idempotent(self):
        equipment = self.equipment()
        records = [self.remediation_record(equipment, "s-1", "rem-1", "opened", "2026-09-01T08:00:00Z")]
        first = self.submit(records, batch_key="same-key")
        second = self.submit(records, batch_key="same-key", actor=self.inspector2)
        self.assertEqual(first["batch_id"], second["batch_id"])
        self.assertTrue(second["duplicate_submission"])
        self.assertEqual(len(self.service.backfill_batches()), 1)

    def test_concurrent_same_batch_first_write_wins(self):
        equipment = self.equipment()
        records = [
            {
                "stable_id": "sync-%d" % index, "kind": "inspection",
                "event_type": "result_recorded", "equipment_id": equipment["id"],
                "ref_id": "insp-%d" % index,
                "event_time": "2026-09-01T09:0%d:00Z" % index,
                "payload": {"result": "passed"},
            }
            for index in range(6)
        ]
        results = []
        errors = []

        def submit(actor, key):
            try:
                results.append(self.service.submit_backfill(actor, {"batch_key": key, "records": records}))
            except Exception as exc:  # pragma: no cover - failure surface
                errors.append(exc)

        threads = [
            threading.Thread(target=submit, args=(self.inspector, "key-a")),
            threading.Thread(target=submit, args=(self.inspector2, "key-b")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertTrue(all(report["status"] == "applied" for report in results))
        self.assertEqual(sum(report["recorded"] for report in results), 6)
        self.assertEqual(sum(report["duplicates"] for report in results), 6)
        self.assertEqual(len(self.service.ledger_events(equipment_id=equipment["id"])), 6)

    def test_late_backfill_invalidates_conclusion_and_flags_permit(self):
        equipment = self.equipment()
        permit = self.grant_permit(equipment)
        self.assertEqual(permit["status"], "granted")

        report = self.submit([
            self.remediation_record(equipment, "s-open", "rem-1", "opened", "2026-09-01T08:00:00Z")
        ])
        self.assertEqual(report["status"], "applied")
        self.assertEqual(report["affected_equipment"], [equipment["id"]])
        self.assertEqual(len(report["conclusions_invalidated"]), 1)
        invalidated = report["conclusions_invalidated"][0]
        self.assertTrue(invalidated["was_eligible"])
        self.assertFalse(invalidated["now_eligible"])
        self.assertEqual(len(report["permits_pending_review"]), 1)
        flagged = report["permits_pending_review"][0]
        self.assertEqual(flagged["permit_id"], permit["id"])
        self.assertEqual(flagged["equipment_id"], equipment["id"])

        updated = self.service.get(permit["id"])
        self.assertEqual(updated["status"], "pending_review")
        self.assertEqual(updated["data"]["review_batch_id"], report["batch_id"])

        conclusions = self.service.ledger_conclusions(equipment_id=equipment["id"])
        self.assertEqual(len(conclusions), 2)
        self.assertEqual(conclusions[0]["status"], "invalidated")
        self.assertEqual(conclusions[1]["status"], "active")
        self.assertFalse(conclusions[1]["result"]["eligible"])
        self.assertEqual(conclusions[1]["supersedes"], conclusions[0]["id"])

        reviews = self.service.ledger_reviews()
        self.assertEqual(len(reviews), 1)
        self.assertEqual(reviews[0]["permit_id"], permit["id"])
        self.assertEqual(reviews[0]["equipment_id"], equipment["id"])
        self.assertEqual(reviews[0]["status"], "pending_review")

    def test_backfill_after_grant_only_changes_current_state(self):
        equipment = self.equipment()
        permit = self.grant_permit(equipment)

        report = self.submit([
            self.remediation_record(equipment, "s-future", "rem-9", "opened", "2099-01-01T00:00:00Z")
        ])
        self.assertEqual(report["status"], "applied")
        self.assertEqual(report["conclusions_invalidated"], [])
        self.assertEqual(report["permits_pending_review"], [])

        self.assertEqual(self.service.get(permit["id"])["status"], "granted")
        conclusions = self.service.ledger_conclusions(equipment_id=equipment["id"])
        self.assertEqual(len(conclusions), 1)
        self.assertEqual(conclusions[0]["status"], "active")

        current = self.service.ledger_replay(equipment["id"])
        self.assertFalse(current["standing"]["eligible"])
        self.assertEqual(current["standing"]["open_remediations"][0]["ref_id"], "rem-9")

    def test_open_then_closed_before_grant_keeps_permit(self):
        equipment = self.equipment()
        permit = self.grant_permit(equipment)

        report = self.submit([
            self.remediation_record(equipment, "s-open", "rem-1", "opened", "2026-09-01T08:00:00Z"),
            self.remediation_record(equipment, "s-close", "rem-1", "closed", "2026-09-01T09:00:00Z"),
        ])
        self.assertEqual(report["conclusions_invalidated"], [])
        self.assertEqual(report["permits_pending_review"], [])
        self.assertEqual(self.service.get(permit["id"])["status"], "granted")

    def test_backfilled_failed_inspection_invalidates_permit(self):
        equipment = self.equipment()
        first = self.submit([
            {"stable_id": "s-pass", "kind": "inspection", "event_type": "result_recorded",
             "equipment_id": equipment["id"], "ref_id": "insp-1",
             "event_time": "2026-09-01T09:00:00Z", "payload": {"result": "passed"}},
            {"stable_id": "s-grant", "kind": "permit", "event_type": "granted",
             "equipment_id": equipment["id"], "ref_id": "permit-x",
             "event_time": "2026-09-01T10:00:00Z", "payload": {"purpose": "return_to_service"}},
        ])
        self.assertEqual(first["conclusions_created"][0]["eligible"], True)
        self.assertEqual(first["permits_pending_review"], [])

        second = self.submit([
            {"stable_id": "s-fail", "kind": "inspection", "event_type": "result_recorded",
             "equipment_id": equipment["id"], "ref_id": "insp-2",
             "event_time": "2026-09-01T09:30:00Z", "payload": {"result": "failed", "findings": "brake wear"}},
        ])
        self.assertEqual(len(second["conclusions_invalidated"]), 1)
        self.assertEqual(len(second["permits_pending_review"]), 1)
        flagged = second["permits_pending_review"][0]
        self.assertEqual(flagged["permit_id"], "permit-x")
        self.assertEqual(flagged["equipment_id"], equipment["id"])
        self.assertIn("failed", flagged["reason"])

        reviews = self.service.ledger_reviews()
        self.assertEqual([review["permit_id"] for review in reviews], ["permit-x"])

    def test_replay_failure_keeps_batch_for_retry(self):
        record = {
            "stable_id": "s-1", "kind": "inspection", "event_type": "result_recorded",
            "equipment_id": "eq-late", "ref_id": "insp-1",
            "event_time": "2026-09-01T09:00:00Z", "payload": {"result": "passed"},
        }
        report = self.submit([record], batch_key="late-equipment")
        self.assertEqual(report["status"], "replay_failed")
        self.assertIn("equipment not found", report["error"])

        # The original batch and its events are retained, nothing derived.
        self.assertEqual(len(self.service.ledger_events(equipment_id="eq-late")), 1)
        self.assertEqual(self.service.ledger_conclusions(), [])
        batches = self.service.backfill_batches()
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]["status"], "replay_failed")

        self.equipment(asset_no="E-late", entity_id="eq-late")
        retried = self.service.retry_backfill(self.inspector, report["batch_id"])
        self.assertEqual(retried["status"], "applied")
        self.assertIsNone(retried["error"])
        again = self.service.retry_backfill(self.inspector, report["batch_id"])
        self.assertEqual(again["status"], "applied")

    def test_time_point_replay_explains_basis(self):
        equipment = self.equipment()
        self.submit([
            {"stable_id": "s-pass", "kind": "inspection", "event_type": "result_recorded",
             "equipment_id": equipment["id"], "ref_id": "insp-1",
             "event_time": "2026-09-01T09:00:00Z", "payload": {"result": "passed"}},
            self.remediation_record(equipment, "s-open", "rem-1", "opened", "2026-09-01T10:00:00Z"),
            self.remediation_record(equipment, "s-close", "rem-1", "closed", "2026-09-01T11:00:00Z"),
        ])

        early = self.service.ledger_replay(equipment["id"], as_of="2026-09-01T09:30:00Z")
        self.assertTrue(early["standing"]["eligible"])
        self.assertEqual(len(early["events"]), 1)

        middle = self.service.ledger_replay(equipment["id"], as_of="2026-09-01T10:30:00Z")
        self.assertFalse(middle["standing"]["eligible"])
        self.assertEqual(middle["standing"]["open_remediations"][0]["ref_id"], "rem-1")

        late = self.service.ledger_replay(equipment["id"], as_of="2026-09-01T11:30:00Z")
        self.assertTrue(late["standing"]["eligible"])
        self.assertEqual(len(late["events"]), 3)

    def test_live_activity_is_mirrored_into_ledger(self):
        equipment = self.equipment()
        alarm = self.service.create(self.admin, "alarm", {
            "equipment_id": equipment["id"], "code": "DOOR-JAM",
            "occurred_at": "2026-09-27T10:00:00Z"})
        self.service.transition(self.admin, alarm["id"], "mark_false", {})

        events = self.service.ledger_events(equipment_id=equipment["id"], kind="alarm")
        self.assertEqual([event["event_type"] for event in events], ["raised", "closed"])
        self.assertEqual(events[0]["event_time"], "2026-09-27T10:00:00+00:00")

        replay = self.service.ledger_replay(equipment["id"])
        self.assertEqual(replay["standing"]["open_alarms"], [])

    def test_submit_requires_inspector_role(self):
        equipment = self.equipment()
        records = [self.remediation_record(equipment, "s-1", "rem-1", "opened", "2026-09-01T08:00:00Z")]
        with self.assertRaises(PermissionDenied):
            self.service.submit_backfill(Actor("guest", "viewer"), {"records": records})

    def test_record_shape_validation(self):
        equipment = self.equipment()
        with self.assertRaises(ValidationError):
            self.submit([{"kind": "remediation"}])
        with self.assertRaises(ValidationError):
            self.submit([self.remediation_record(equipment, "s-1", "rem-1", "opened", "not-a-time")])
        with self.assertRaises(ValidationError):
            self.submit([])


if __name__ == "__main__":
    unittest.main()
