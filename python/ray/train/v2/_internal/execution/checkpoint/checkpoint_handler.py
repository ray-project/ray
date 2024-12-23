from collections import deque
from typing import TYPE_CHECKING, List, Optional

from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.checkpoint.checkpoint_manager import (
    CheckpointManager,
)
from ray.train.v2._internal.execution.worker_group import WorkerGroup, WorkerGroupStatus

if TYPE_CHECKING:
    from ray.train._internal.session import _TrainingResult


class CheckpointHandler(WorkerGroupCallback):
    """
    The CheckpointHandler lives on TrainController and is responsible to
    consolidate the _TrainingResults from workers and register it to the
    CheckpointManager.

    Note that the CheckpointHandler is stateful. The values are carried across runs.
    This will handle reported checkpoints for the lifetime of a worker group.
    Any partial checkpoints will be abandoned when the worker group shuts down.
    """

    def __init__(self, checkpoint_manager: CheckpointManager):
        # Number of workers in the current worker group. It is initialized
        # to be None. It is set to the number of workers when it receives the
        # worker group status for the first time.
        # When a worker group shutdown, self._num_workers is set to None,
        # waiting to be updated when a new worker group status is received again.
        self._num_workers: Optional[int] = None
        # A list of queues holding training results from workers.
        self._training_result_queues: Optional[List[deque]] = None
        # The checkpoint manager to register the consolidated checkpoint.
        self._checkpoint_manager: CheckpointManager = checkpoint_manager

    def _update_handler_training_result_queues(
        self, training_results: List[Optional["_TrainingResult"]]
    ) -> None:
        for i in range(self._num_workers):
            if training_results[i]:
                self._training_result_queues[i].append(training_results[i])

    # --------------------------
    # WorkerGroupCallback
    # --------------------------

    def after_worker_group_poll_status(
        self, worker_group_status: WorkerGroupStatus
    ) -> None:
        """Handle poll results from workers.

        Steps:
        1. Check if the checkpoint handler is correctly initialized.
        2. Update the training result queues with poll results.
            If all workers have reported a checkpoint, continue onto the
            next step of committing the checkpoint in the checkpoint manager.
        3. Consolidate a list of checkpoints to single checkpoint.
        4. Register the checkpoint to checkpoint manager.
        """
        # Step 1: If self._num_workers is None, we need to initialize the number
        # of workers and training_results_queues from the worker group status. This
        # happens when the handler receives the worker group status for the first time.
        if not self._num_workers or not self._training_result_queues:
            raise ValueError(
                "CheckpointHandler is not initialized. "
                "Please call after_worker_group_start first."
            )
        num_workers = worker_group_status.num_workers
        if self._num_workers != num_workers:
            raise ValueError(
                f"Number of workers in the worker group has changed. "
                f"Expected {self._num_workers}, got {num_workers}."
            )

        # Step 2: Update training_results_queues with poll_results.
        training_results = [
            worker_group_status.worker_statuses[i].training_result
            for i in range(self._num_workers)
        ]
        self._update_handler_training_result_queues(training_results)

        # Directly return if there are None in the list or Only metrics
        # are being reported in the whole worker group.
        if any([not q for q in self._training_result_queues]):
            return
        training_results = [q.popleft() for q in self._training_result_queues]
        valid_training_results = [
            tr for tr in training_results if tr.checkpoint is not None
        ]
        if len(valid_training_results) == 0:
            return
        # Step 3: Consolidate a list of checkpoints to single checkpoint.
        # In our current implementation, we already make sure all the valid
        # _TrainingResults contain the same storage path on the checkpoint.
        # Here, we use the first valid checkpoint as the consolidated checkpoint.

        # Before register the checkpoint to the checkpoint manager, we should
        # double check the storage path of the checkpoints in the training results.
        unique_storage_paths = {tr.checkpoint.path for tr in valid_training_results}
        if len(unique_storage_paths) > 1:
            # TODO: Support for inconsistent checkpoints path from workers
            # instead of hard raising error. Maybe drop this iteration of
            # training results and continue with the next iteration.
            raise ValueError(
                "The storage path of the checkpoints in the training results "
                "is not the same. This means the checkpoints are not consistent."
                f"Get a set of non-unique storage paths: {unique_storage_paths}"
            )
        consolidated_checkpoint_result = valid_training_results[0]
        # Step 4: Register the checkpoint to checkpoint manager.
        self._checkpoint_manager.register_checkpoint(consolidated_checkpoint_result)

    def after_worker_group_start(self, worker_group: WorkerGroup) -> None:
        """Handle worker group start. Initialize internal states."""
        self._num_workers = len(worker_group)
        self._training_result_queues = [deque() for _ in range(self._num_workers)]

    def before_worker_group_shutdown(self, worker_group: WorkerGroup) -> None:
        """Handle worker group shutdown. clear internal states."""
        self._num_workers = None
        self._training_result_queues = None
