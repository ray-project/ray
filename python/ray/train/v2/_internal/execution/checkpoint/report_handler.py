from collections import deque
from typing import Deque, List, Optional

from ray.train.v2._internal.execution.callback import (
    ReportCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.training_report import _TrainingReport
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroup,
    WorkerGroupPollStatus,
)


class ReportCallbackHandler(WorkerGroupCallback):
    """Consolidate training results from multiple workers and call
    subscribers implementing the `ReportCallback` interface sequentially.
    """

    def __init__(self, report_callbacks: List[ReportCallback]):
        # Number of workers in the current worker group. It is initialized
        # to be None. It is set to the number of workers when it receives the
        # worker group status for the first time.
        # When a worker group shutdown, self._num_workers is set to None,
        # waiting to be updated when a new worker group status is received again.
        self._num_workers: Optional[int] = None
        # A list of queues holding training reports from workers.
        self._training_report_queues: Optional[List[Deque[_TrainingReport]]] = None

        self._report_callbacks = report_callbacks

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
        # Step 1: If self._num_workers is None, we need to initialize the number
        # of workers and training_reports_queues from the worker group status. This
        # happens when the handler receives the worker group status for the first time.
        assert (
            self._num_workers and self._training_report_queues
        ), "Need to call initialize state with `after_worker_group_start` first."

        assert self._num_workers == len(worker_group_status.worker_statuses), (
            f"The number of workers in the worker group has changed unexpectedly. "
            f"Expected: {self._num_workers}, got: {len(worker_group_status.worker_statuses)}"
        )

        # Step 2: Update training_reports_queues with poll_results.
        for i in range(self._num_workers):
            training_report = worker_group_status.worker_statuses[i].training_report
            if training_report:
                self._training_report_queues[i].append(training_report)

        # Directly return if any of the worker result queues are empty.
        if not all(self._training_report_queues):
            return

        training_reports = [q.popleft() for q in self._training_report_queues]

        # Step 3: Consolidate a list of checkpoints to single checkpoint.
        # Use the first checkpoint as the consolidated checkpoint.
        checkpoint_results = [
            tr for tr in training_reports if tr.checkpoint is not None
        ]

        consolidated_checkpoint = None
        validation_spec = None
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
            validation_spec = checkpoint_results[0].validation_spec

        # Step 4: Invoke all dependent `ReportCallback`s.
        metrics_per_worker = [
            training_report.metrics for training_report in training_reports
        ]
        for callback in self._report_callbacks:
            callback.after_report(
                training_report=_TrainingReport(
                    checkpoint=consolidated_checkpoint,
                    metrics=metrics_per_worker[0],
                    validation_spec=validation_spec,
                ),
                metrics=metrics_per_worker,
            )

    def after_worker_group_start(self, worker_group: WorkerGroup) -> None:
        """Handle worker group start. Initialize internal states."""
        self._num_workers = len(worker_group)
        self._training_report_queues = [deque() for _ in range(self._num_workers)]

    def before_worker_group_shutdown(self, worker_group: WorkerGroup) -> None:
        """Handle worker group shutdown. Clear internal states.

        None of the partial reported results are valid at this point.
        """
        self._num_workers = None
        self._training_report_queues = None
