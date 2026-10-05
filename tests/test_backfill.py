import hashlib
import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import Actor
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


def stable_id(source_id, record_id):
    digest = hashlib.sha256((source_id + "\0" + record_id).encode("utf-8")).hexdigest()[:32]
    return "bf-" + digest


class BackfillTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.actor = Actor("insp-1", "inspector")
        self.equipment = self.service.create(
            self.actor,
            "equipment",
            {"asset_no": "E-1", "equipment_type": "elevator", "location": "A", "inspection_interval_days": 365},
        )

    def tearDown(self):
        self.tmp.cleanup()

    def rec(self, source_id, record_id, kind, event_time, status=None, **data):
        payload = {
            "source_id": source_id,
            "record_id": record_id,
            "kind": kind,
            "event_time": event_time,
            "data": data,
        }
        if status:
            payload["status"] = status
        return payload

    def inspection(self, record_id, event_time, status, source_id="tab1", **extra):
        data = {
            "equipment_id": self.equipment["id"],
            "scheduled_at": event_time,
            "cycle_days": 365,
            "findings": "routine",
        }
        data.update(extra)
        return self.rec(source_id, record_id, "inspection", event_time, status, **data)

    def permit(self, record_id, event_time, status="granted", source_id="tab1", **extra):
        data = {
            "equipment_id": self.equipment["id"],
            "purpose": "return_to_service",
            "requested_by": "ops",
            "granted_by": "insp-1",
            "granted_at": event_time,
        }
        data.update(extra)
        return self.rec(source_id, record_id, "permit", event_time, status, **data)

    def remediation(self, record_id, event_time, status="open", source_id="tab1", **extra):
        data = {
            "equipment_id": self.equipment["id"],
            "issue": "door alignment",
            "owner": "Maint",
            "due_at": "2026-10-01",
        }
        data.update(extra)
        return self.rec(source_id, record_id, "remediation", event_time, status, **data)

    # --- replay application ------------------------------------------------

    def test_backfill_applies_records_in_event_time_order(self):
        records = [
            self.inspection("insp-2", "2026-09-21T09:00:00Z", "passed"),
            self.inspection("insp-1", "2026-09-20T09:00:00Z", "passed"),
        ]
        report = self.service.merge_offline(self.actor, records, batch_id="b1")
        self.assertEqual(report["status"], "committed")
        self.assertEqual(set(report["applied"]), {"insp-1", "insp-2"})
        # entities carry their event time, not the ingest time
        first = self.service.get(stable_id("tab1", "insp-1"))
        self.assertEqual(first["data"]["event_time"], "2026-09-20T09:00:00Z")
        self.assertEqual(first["status"], "passed")

    def test_dedup_by_stable_id_across_batches(self):
        records = [self.inspection("insp-1", "2026-09-20T09:00:00Z", "passed")]
        self.service.merge_offline(self.actor, records, batch_id="b1")
        # same record re-submitted under a different batch id -> deduped, not duplicated
        report = self.service.merge_offline(self.actor, records, batch_id="b2")
        self.assertEqual(report["status"], "committed")
        self.assertEqual(report["deduped"], ["insp-1"])
        self.assertEqual(report["applied"], [])
        # only one inspection entity exists for this equipment
        inspections = self.service.list("inspection")
        self.assertEqual(len(inspections), 1)

    def test_idempotent_resubmit_same_batch(self):
        records = [self.inspection("insp-1", "2026-09-20T09:00:00Z", "passed")]
        self.service.merge_offline(self.actor, records, batch_id="b1")
        report = self.service.merge_offline(self.actor, records, batch_id="b1")
        self.assertEqual(report["status"], "committed")
        self.assertEqual(report["deduped"], ["insp-1"])
        self.assertEqual(report["applied"], [])

    # --- conclusions and permits ------------------------------------------

    def test_late_backfill_invalidates_permit_and_lists_equipment(self):
        # original batch: passed inspection then granted permit
        original = [
            self.inspection("insp-1", "2026-09-20T09:00:00Z", "passed"),
            self.permit("permit-1", "2026-09-21T10:00:00Z"),
        ]
        self.service.merge_offline(self.actor, original, batch_id="b1")
        # late backfill: a failed inspection between the passed one and the grant
        late = [self.inspection("insp-2", "2026-09-20T14:00:00Z", "failed")]
        report = self.service.merge_offline(self.actor, late, batch_id="b2")
        self.assertEqual(report["status"], "committed")
        # conclusion recomputed and invalidated
        self.assertIn(self.equipment["id"], report["invalidated_conclusions"])
        conclusion = report["conclusions"][0]
        self.assertEqual(conclusion["conclusion"], "unfit")
        self.assertEqual(conclusion["basis"]["inspection"]["status"], "failed")
        # permit can no longer stand -> pending review, equipment listed
        self.assertEqual(len(report["review_required"]), 1)
        review = report["review_required"][0]
        self.assertEqual(review["equipment_id"], self.equipment["id"])
        permit = self.service.get(review["permit_id"])
        self.assertEqual(permit["status"], "pending_review")

    def test_late_backfill_keeps_past_permit_valid(self):
        # a late record that only changes the current state must not disturb
        # permits that were validly granted in the past
        original = [
            self.inspection("insp-1", "2026-09-20T09:00:00Z", "passed"),
            self.permit("permit-1", "2026-09-21T10:00:00Z"),
        ]
        self.service.merge_offline(self.actor, original, batch_id="b1")
        late = [self.inspection("insp-2", "2026-09-22T09:00:00Z", "passed")]
        report = self.service.merge_offline(self.actor, late, batch_id="b2")
        self.assertEqual(report["status"], "committed")
        self.assertEqual(report["review_required"], [])
        permit = self.service.get(stable_id("tab1", "permit-1"))
        self.assertEqual(permit["status"], "granted")

    def test_open_remediation_invalidates_permit(self):
        original = [
            self.inspection("insp-1", "2026-09-20T09:00:00Z", "passed"),
            self.permit("permit-1", "2026-09-21T10:00:00Z"),
        ]
        self.service.merge_offline(self.actor, original, batch_id="b1")
        late = [self.remediation("rem-1", "2026-09-20T15:00:00Z")]
        report = self.service.merge_offline(self.actor, late, batch_id="b2")
        self.assertEqual(report["review_required"][0]["equipment_id"], self.equipment["id"])
        permit = self.service.get(stable_id("tab1", "permit-1"))
        self.assertEqual(permit["status"], "pending_review")

    # --- batch atomicity and retry ----------------------------------------

    def test_failed_batch_is_retained_with_original_records(self):
        bad = [self.inspection("insp-bad", "2026-09-20T09:00:00Z", "passed", equipment_id="missing")]
        report = self.service.merge_offline(self.actor, bad, batch_id="b-bad")
        self.assertEqual(report["status"], "failed")
        self.assertIn("inspection requires equipment", report["error"])
        # the batch is retained and can be read back
        batch = self.service.backfill.get("b-bad")
        self.assertEqual(batch["status"], "failed")
        # nothing was applied
        self.assertIsNone(self.service.repository.get_entity(stable_id("tab1", "insp-bad")))

    def test_whole_batch_failure_applies_nothing(self):
        good = self.inspection("insp-good", "2026-09-20T09:00:00Z", "passed")
        bad = self.inspection("insp-bad", "2026-09-20T10:00:00Z", "passed", equipment_id="missing")
        report = self.service.merge_offline(self.actor, [good, bad], batch_id="b-mix")
        self.assertEqual(report["status"], "failed")
        self.assertIsNone(self.service.repository.get_entity(stable_id("tab1", "insp-good")))
        self.assertIsNone(self.service.repository.get_entity(stable_id("tab1", "insp-bad")))

    def test_retry_after_transient_failure(self):
        records = [self.inspection("insp-1", "2026-09-20T09:00:00Z", "passed")]
        original_save = self.service.repository.save_backfill_record
        calls = {"count": 0}

        def flaky(*args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("transient lock")
            return original_save(*args, **kwargs)

        self.service.repository.save_backfill_record = flaky
        first = self.service.merge_offline(self.actor, records, batch_id="b1")
        self.assertEqual(first["status"], "failed")
        self.service.repository.save_backfill_record = original_save
        second = self.service.retry_backfill(self.actor, "b1")
        self.assertEqual(second["status"], "committed")
        self.assertEqual(second["applied"], ["insp-1"])

    # --- concurrency ------------------------------------------------------

    def test_concurrent_same_batch_first_writer_wins(self):
        records = [self.inspection("insp-1", "2026-09-20T09:00:00Z", "passed")]
        results = []
        barrier = threading.Barrier(2)

        def submit():
            barrier.wait()
            results.append(self.service.merge_offline(self.actor, records, batch_id="b1"))

        threads = [threading.Thread(target=submit) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        # both submissions resolve to the same committed batch
        self.assertEqual(len(results), 2)
        for result in results:
            self.assertEqual(result["status"], "committed")
        # exactly one inspection was created
        self.assertEqual(len(self.service.list("inspection")), 1)
        self.assertEqual(len(self.service.repository.list_backfill_records()), 1)

    # --- listing ----------------------------------------------------------

    def test_list_and_get_batches(self):
        records = [self.inspection("insp-1", "2026-09-20T09:00:00Z", "passed")]
        self.service.merge_offline(self.actor, records, batch_id="b1")
        listing = self.service.backfill.list()
        self.assertEqual(len(listing), 1)
        self.assertEqual(listing[0]["batch_id"], "b1")
        self.assertEqual(listing[0]["status"], "committed")
        fetched = self.service.backfill.get("b1")
        self.assertEqual(fetched["batch_id"], "b1")


if __name__ == "__main__":
    unittest.main()
