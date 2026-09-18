from collections import deque
from typing import Collection, Deque, List, Optional, Set

from ray.train.v2._internal.execution.callback import (
    ReplicaGroupCallback,
    ReportCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.training_report import _TrainingReport
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroup,
    WorkerGroupPollStatus,
)
from ray.train.v2._internal.execution.worker_group.execution_group import ReplicaGroup


class ReportCallbackHandler(ReplicaGroupCallback, WorkerGroupCallback):
    """Consolidate training results from multiple workers and call
    subscribers implementing the `ReportCallback` interface sequentially.
    """

    def __init__(self, report_callbacks: List[ReportCallback]):
        # We set the worker group after it has been started and remove it after it
        # has been shut down.
        self._worker_group: Optional[WorkerGroup] = None
        # A list of queues holding training reports from workers.
        self._training_report_queues: Optional[List[Deque[_TrainingReport]]] = None
        # Ranks whose reports gate consolidation. Default to None means barrier should be hold for all ranks.
        # Otherwise means the survivor ranks during a preemption.
        self._expected_ranks: Optional[Set[int]] = None

        self._report_callbacks = report_callbacks

    def _assert_initialized(self):
        assert (
            self._worker_group and self._training_report_queues
        ), "Need to call initialize state with `after_worker_group_start` first."

    def set_expected_ranks(self, ranks: Optional[Collection[int]]) -> None:
        """Consolidate reports from `ranks` only, instead of from every rank."""
        self._expected_ranks = set(ranks) if ranks is not None else None

    def _get_expected_indices(self) -> List[int]:
        """Worker indices whose reports gate consolidation, ascending."""
        if self._expected_ranks is None:
            return list(range(len(self._worker_group)))
        return sorted(self._expected_ranks)

    # --------------------------
    # WorkerGroupCallback
    # --------------------------

    def after_worker_group_poll_status(
        self, worker_group_status: WorkerGroupPollStatus
    ) -> None:
        """Handle training results as they roll in from worker status polls.

        Wait for all workers to report training results to collect
        a consolidated training result.
        """
        # Step 1: Assert that the worker group has been started and not shut down.
        self._assert_initialized()

        assert len(self._worker_group) == len(worker_group_status.worker_statuses), (
            f"The number of workers in the worker group has changed unexpectedly. "
            f"Expected: {len(self._worker_group)}, got: {len(worker_group_status.worker_statuses)}"
        )

        # Step 2: Update training_reports_queues with poll_results.
        for i in range(len(self._worker_group)):
            training_report = worker_group_status.worker_statuses[i].training_report
            if training_report:
                self._training_report_queues[i].append(training_report)

        # Directly return if any of the expected worker result queues are empty.
        expected_indices = self._get_expected_indices()
        if not all(self._training_report_queues[i] for i in expected_indices):
            return

        training_reports = [
            self._training_report_queues[i].popleft() for i in expected_indices
        ]
        # Drop this round from the skipped ranks' queues too, so a straggler
        # report can't be paired with a later round if the barrier goes back to
        # being strict.
        skipped = set(range(len(self._worker_group))) - set(expected_indices)
        for i in skipped:
            if self._training_report_queues[i]:
                self._training_report_queues[i].popleft()

        # Step 3: Consolidate a list of checkpoints to single checkpoint.
        # Use the first checkpoint as the consolidated checkpoint.
        checkpoint_results = [
            tr for tr in training_reports if tr.checkpoint is not None
        ]

        consolidated_checkpoint = None
        validation = False
        if checkpoint_results:
            # Double check the storage path of the checkpoints in the training results.
            unique_checkpoint_paths = {tr.checkpoint.path for tr in checkpoint_results}
            if len(unique_checkpoint_paths) > 1:
                # TODO: Support for inconsistent checkpoints path from workers
                # instead of hard raising error. Maybe drop this iteration of
                # training results and continue with the next iteration.
                raise RuntimeError(
                    "The storage path of the checkpoints in the training results "
                    "is not the same. This means the checkpoints are not consistent."
                    "Got a mix of the following checkpoint paths: "
                    f"{unique_checkpoint_paths}\n"
                    "This is unexpected -- please file a Github issue."
                )
            consolidated_checkpoint = checkpoint_results[0].checkpoint
            validation = checkpoint_results[0].validation

        # Step 4: Invoke all dependent `ReportCallback`s.
        # NOTE: when the barrier is relaxed this list holds one entry per
        # expected rank, not per world rank.
        metrics_per_worker = [
            training_report.metrics for training_report in training_reports
        ]
        for callback in self._report_callbacks:
            callback.after_report(
                training_report=_TrainingReport(
                    checkpoint=consolidated_checkpoint,
                    metrics=metrics_per_worker[0],
                    validation=validation,
                ),
                metrics=metrics_per_worker,
            )

    def after_worker_group_start(self, worker_group: WorkerGroup) -> None:
        """Handle worker group start. Initialize internal states."""
        self._worker_group = worker_group
        self._training_report_queues = [deque() for _ in range(len(self._worker_group))]
        self._expected_ranks = None

    def before_worker_group_shutdown(self, worker_group: WorkerGroup) -> None:
        """Handle worker group shutdown. Clear internal states.

        None of the partial reported results are valid at this point.
        """
        self._worker_group = None
        self._training_report_queues = None
        self._expected_ranks = None

    # --------------------------
    # ReplicaGroupCallback
    # --------------------------

    def after_replica_group_start(self, replica_group: ReplicaGroup) -> None:
        """Handle replica group start. Initialize internal states."""
        self._assert_initialized()
        # TODO: it might be possible to reuse existing queues.
        # For example, if 3/4 ddp workers reported a checkpoint, that checkpoint is usable.
        self._training_report_queues = [deque() for _ in range(len(self._worker_group))]
