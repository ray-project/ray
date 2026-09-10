"""Executable application-state contract, not a checkpoint storage implementation.

Run with Python alone. Each method models one atomic storage transition, and
claim() assumes lease admission has succeeded. The tests interleave attempts;
they do not start workers, write durable artifacts, or exercise a broker.
"""

import unittest

# __records_begin__
from dataclasses import dataclass, replace
from typing import Dict, Optional, Tuple


@dataclass(frozen=True)
class Delivery:
    job_id: str
    fingerprint: str
    total_units: int
    task_id: str  # Transport identity, not the key for progress or results.


@dataclass(frozen=True)
class Attempt:
    job_id: str
    attempt_id: str
    fence: int


@dataclass(frozen=True)
class JobRecord:
    fingerprint: str
    total_units: int
    fence: int = 0
    attempt_id: Optional[str] = None
    next_unit: int = 0
    checkpoint_ref: Optional[str] = None  # Restores the prefix [0, next_unit).
    result_ref: Optional[str] = None


# __records_end__
class StaleAttempt(RuntimeError):
    """A newer attempt owns the job."""


class ApplicationStateModel:
    """Serial model of atomic, conditional writes to an application job record."""

    def __init__(self):
        self.records: Dict[str, JobRecord] = {}

    def claim(
        self, delivery: Delivery, attempt_id: str
    ) -> Tuple[Optional[Attempt], JobRecord]:
        """Model an admitted lease acquisition or takeover, not lease expiry."""
        if delivery.total_units <= 0:
            raise ValueError("Expected at least one work unit")
        record = self.records.get(
            delivery.job_id, JobRecord(delivery.fingerprint, delivery.total_units)
        )
        if (
            record.fingerprint != delivery.fingerprint
            or record.total_units != delivery.total_units
        ):
            raise ValueError("job_id already belongs to a different request")
        if record.result_ref is not None:
            return None, record
        if not attempt_id or attempt_id == record.attempt_id:
            raise ValueError("Use a fresh attempt_id for each execution")
        record = replace(record, attempt_id=attempt_id, fence=record.fence + 1)
        self.records[delivery.job_id] = record
        return Attempt(delivery.job_id, attempt_id, record.fence), record

    def _owned_record(self, attempt: Attempt) -> JobRecord:
        record = self.records[attempt.job_id]
        if record.attempt_id != attempt.attempt_id or record.fence != attempt.fence:
            raise StaleAttempt("Checkpoint and result writes require the current token")
        return record

    def checkpoint(
        self, attempt: Attempt, next_unit: int, checkpoint_ref: str
    ) -> JobRecord:
        """Publish a reference to an already durable, immutable checkpoint."""
        record = self._owned_record(attempt)
        if record.result_ref is not None:
            raise ValueError("A completed job cannot accept checkpoints")
        if not checkpoint_ref:
            raise ValueError("Expected a checkpoint reference")
        if not record.next_unit <= next_unit <= record.total_units:
            raise ValueError(
                "Committed progress must not move backwards or exceed work"
            )
        if next_unit == record.next_unit:
            if checkpoint_ref != record.checkpoint_ref:
                raise ValueError("Cannot replace an already committed checkpoint")
            return record
        record = replace(record, next_unit=next_unit, checkpoint_ref=checkpoint_ref)
        self.records[attempt.job_id] = record
        return record

    def finish(self, attempt: Attempt, result_ref: str) -> JobRecord:
        """Publish an already durable result before the handler returns."""
        record = self._owned_record(attempt)
        if not result_ref:
            raise ValueError("Expected a result reference")
        if record.next_unit != record.total_units:
            raise ValueError("Complete the work before publishing a result")
        if record.result_ref is not None and record.result_ref != result_ref:
            raise ValueError("A committed result cannot be replaced")
        record = replace(record, result_ref=result_ref)
        self.records[attempt.job_id] = record
        return record


class FailureContractTests(unittest.TestCase):
    def setUp(self):
        self.store = ApplicationStateModel()
        self.delivery = Delivery(
            job_id="job-1",
            fingerprint="input-v1/model-v1/pipeline-v1",
            total_units=3,
            task_id="task-1",
        )

    def test_checkpoint_then_worker_loss(self):
        first, _ = self.store.claim(self.delivery, "attempt-a")
        saved = self.store.checkpoint(first, 1, "checkpoints/prefix-1")

        # Replace the lost worker after its lease expires.
        second, resumed = self.store.claim(self.delivery, "attempt-b")
        self.assertGreater(second.fence, first.fence)
        self.assertEqual(resumed.checkpoint_ref, saved.checkpoint_ref)
        remaining_work = list(range(resumed.next_unit, resumed.total_units))
        self.assertEqual(remaining_work, [1, 2])  # Unit 0 is already committed.
        for unit in remaining_work:
            self.store.checkpoint(second, unit + 1, f"checkpoints/prefix-{unit + 1}")
        self.assertEqual(self.store.finish(second, "results/final").next_unit, 3)

    def test_result_commit_then_redelivery_before_acknowledgement(self):
        first, _ = self.store.claim(self.delivery, "attempt-a")
        self.store.checkpoint(first, 3, "checkpoints/prefix-3")
        committed = self.store.finish(first, "results/final")

        # Redeliver after publication, before the broker sees an acknowledgement.
        for task_id in ("task-1", "task-reenqueued"):
            with self.subTest(task_id=task_id):
                attempt, result = self.store.claim(
                    replace(self.delivery, task_id=task_id), "attempt-redelivered"
                )
                self.assertIsNone(attempt)  # Return the result without inference.
                self.assertEqual(result, committed)

    def test_overlapping_attempts_fence_checkpoints_and_results(self):
        old, _ = self.store.claim(self.delivery, "attempt-a")
        self.store.checkpoint(old, 1, "checkpoints/prefix-1")
        # Admit B after A's lease expires, while A may still compute.
        current, claimed = self.store.claim(self.delivery, "attempt-b")
        with self.assertRaises(StaleAttempt):
            self.store.checkpoint(old, 3, "checkpoints/late-a")
        self.assertEqual(self.store.records[self.delivery.job_id], claimed)

        self.store.checkpoint(current, 3, "checkpoints/prefix-3")
        with self.assertRaises(StaleAttempt):
            self.store.finish(old, "results/late-a")
        result = self.store.finish(current, "results/current-b")
        self.assertEqual(result.result_ref, "results/current-b")

    def test_request_identity_is_checked_before_returning_a_result(self):
        attempt, _ = self.store.claim(self.delivery, "attempt-a")
        self.store.checkpoint(attempt, 3, "checkpoints/prefix-3")
        committed = self.store.finish(attempt, "results/final")
        for field, value in (("fingerprint", "input-v2"), ("total_units", 4)):
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "different request"):
                    self.store.claim(
                        replace(self.delivery, **{field: value}), "attempt-b"
                    )
                self.assertEqual(self.store.records[self.delivery.job_id], committed)

    def test_checkpoint_commits_are_monotonic_and_idempotent(self):
        attempt, _ = self.store.claim(self.delivery, "attempt-a")
        saved = self.store.checkpoint(attempt, 1, "checkpoints/prefix-1")
        self.assertEqual(
            self.store.checkpoint(attempt, 1, "checkpoints/prefix-1"), saved
        )
        for next_unit, ref in (
            (0, "older"),
            (4, "beyond-end"),
            (1, "conflict"),
            (2, ""),
        ):
            with self.subTest(next_unit=next_unit, checkpoint_ref=ref):
                with self.assertRaises(ValueError):
                    self.store.checkpoint(attempt, next_unit, ref)
                self.assertEqual(self.store.records[self.delivery.job_id], saved)

    def test_completed_job_is_immutable(self):
        attempt, _ = self.store.claim(self.delivery, "attempt-a")
        self.store.checkpoint(attempt, 3, "checkpoints/prefix-3")
        committed = self.store.finish(attempt, "results/final")
        self.assertEqual(self.store.finish(attempt, "results/final"), committed)
        with self.assertRaisesRegex(ValueError, "cannot be replaced"):
            self.store.finish(attempt, "results/conflicting")
        with self.assertRaisesRegex(ValueError, "completed job"):
            self.store.checkpoint(attempt, 3, "checkpoints/after-result")
        self.assertEqual(self.store.records[self.delivery.job_id], committed)


if __name__ == "__main__":
    unittest.main()
