from collections import deque
from typing import TYPE_CHECKING, Deque, List, Optional

from ray.train.v2._internal.execution.callback import (
    ReportCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroup,
    WorkerGroupPollStatus,
)

if TYPE_CHECKING:
    from ray.train._internal.session import _TrainingResult


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
        # A list of queues holding training results from workers.
        self._training_result_queues: Optional[List[Deque[_TrainingResult]]] = None

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
        # of workers and training_results_queues from the worker group status. This
        # happens when the handler receives the worker group status for the first time.
        assert (
            self._num_workers and self._training_result_queues
        ), "Need to call initialize state with `after_worker_group_start` first."

        assert self._num_workers == len(worker_group_status.worker_statuses), (
            f"The number of workers in the worker group has changed unexpectedly. "
            f"Expected: {self._num_workers}, got: {len(worker_group_status.worker_statuses)}"
        )

        # Step 2: Update training_results_queues with poll_results.
        for i in range(self._num_workers):
            training_result = worker_group_status.worker_statuses[i].training_result
            if training_result:
                self._training_result_queues[i].append(training_result)

        # Directly return if any of the worker result queues are empty.
        if not all(self._training_result_queues):
            return

        training_results = [q.popleft() for q in self._training_result_queues]

        # Step 3: Consolidate a list of checkpoints to single checkpoint.
        # Use the first checkpoint as the consolidated checkpoint.
        checkpoint_results = [
            tr for tr in training_results if tr.checkpoint is not None
        ]

        consolidated_checkpoint = None
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

        # Step 4: Invoke all dependent `ReportCallback`s.
        metrics_per_worker = [
            training_result.metrics for training_result in training_results
        ]
        for callback in self._report_callbacks:
            callback.after_report(
                metrics=metrics_per_worker,
                checkpoint=consolidated_checkpoint,
            )

    def after_worker_group_start(self, worker_group: WorkerGroup) -> None:
        """Handle worker group start. Initialize internal states."""
        self._num_workers = len(worker_group)
        self._training_result_queues = [deque() for _ in range(self._num_workers)]

    def before_worker_group_shutdown(self, worker_group: WorkerGroup) -> None:
        """Handle worker group shutdown. Clear internal states.

        None of the partial reported results are valid at this point.
        """
        self._num_workers = None
        self._training_result_queues = None
