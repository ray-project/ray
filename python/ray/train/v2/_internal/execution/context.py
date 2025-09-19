import logging
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from queue import Queue
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray
from ray.actor import ActorHandle
from ray.data import DataIterator, Dataset
from ray.train._internal import session
from ray.train._internal.session import _TrainingResult
from ray.train.v2._internal.execution.checkpoint.sync_actor import SynchronizationActor
from ray.train.v2._internal.execution.storage import StorageContext, delete_fs_path
from ray.train.v2._internal.util import (
    _copy_doc,
    construct_user_exception_with_traceback,
    invoke_context_managers,
)
from ray.train.v2.api.config import RunConfig, ScalingConfig
from ray.train.v2.api.report_config import CheckpointUploadMode

if TYPE_CHECKING:
    from ray.train import BackendConfig, Checkpoint, DataConfig
    from ray.train.v2._internal.data_integration.interfaces import (
        DatasetShardMetadata,
        DatasetShardProvider,
    )
    from ray.train.v2._internal.execution.callback import TrainContextCallback
    from ray.train.v2._internal.execution.worker_group.thread_runner import ThreadRunner
    from ray.train.v2.api.reported_checkpoint import ReportedCheckpoint


logger = logging.getLogger(__file__)


# TODO: make this value manually or automatically configurable.
MAX_CHECKPOINT_UPLOAD_THREADS = 1


@dataclass(frozen=True)
class TrainRunContext:
    """Holds the metadata and context for the current training run."""

    # The unique ID of the training run.
    run_id: str = field(init=False, default_factory=lambda: uuid.uuid4().hex)

    # The run configuration for the current training run.
    run_config: RunConfig

    # The configuration passed to the training function.
    train_loop_config: Optional[Dict[str, Any]]

    # The scaling configuration for the current training run.
    scaling_config: ScalingConfig

    # The configuration for the training backend (e.g., PyTorch, XGBoost).
    backend_config: "BackendConfig"

    # The datasets used in the current training run.
    datasets: Dict[str, Dataset]

    # The configuration for dataset ingestion and sharding.
    dataset_config: "DataConfig"

    def get_run_config(self) -> RunConfig:
        """Returns the run config of the current training run."""
        return self.run_config


@dataclass(frozen=True)
class DistributedContext:
    world_rank: int
    world_size: int
    local_rank: int
    local_world_size: int
    node_rank: int


@dataclass(frozen=True)
class ExecutionContext:
    """Holds the execution context for the current worker process.

    Every worker process has a single execution context accessed via the
    `TrainContext`, which includes the training thread that is actually
    running the user code.
    """

    # A shared synchronization actor that helps broadcast data across ranks.
    synchronization_actor: SynchronizationActor

    # A queue that receives training results from the user training code.
    # `ray.train.report` in user code populates this queue.
    result_queue: Queue

    # The thread launcher that runs the user training loop.
    training_thread_runner: "ThreadRunner"

    # The callbacks that are run in the worker train context.
    train_context_callbacks: List["TrainContextCallback"]


@dataclass
class TrainContext:
    train_run_context: TrainRunContext
    distributed_context: DistributedContext
    execution_context: ExecutionContext
    storage_context: StorageContext
    controller_actor: ActorHandle

    dataset_shard_provider: "DatasetShardProvider"

    # TODO: consolidate into CheckpointContext
    checkpoint: Optional["Checkpoint"] = None
    current_report_index: int = 0
    report_call_index: int = 0
    report_order_condition: threading.Condition = threading.Condition()
    checkpoint_upload_threadpool: ThreadPoolExecutor = ThreadPoolExecutor(
        max_workers=MAX_CHECKPOINT_UPLOAD_THREADS
    )

    @_copy_doc(session.get_experiment_name)
    def get_experiment_name(self) -> str:
        return self.train_run_context.run_config.name

    @_copy_doc(session.get_world_size)
    def get_world_size(self) -> int:
        return self.distributed_context.world_size

    @_copy_doc(session.get_world_rank)
    def get_world_rank(self) -> int:
        return self.distributed_context.world_rank

    @_copy_doc(session.get_local_rank)
    def get_local_rank(self) -> int:
        return self.distributed_context.local_rank

    @_copy_doc(session.get_local_world_size)
    def get_local_world_size(self) -> int:
        return self.distributed_context.local_world_size

    @_copy_doc(session.get_node_rank)
    def get_node_rank(self) -> int:
        return self.distributed_context.node_rank

    @_copy_doc(session.get_storage)
    def get_storage(self):
        return self.storage_context

    # TODO: Don't allow these private methods to be called from user code.
    def get_result_queue(self):
        return self.execution_context.result_queue

    def get_synchronization_actor(self):
        return self.execution_context.synchronization_actor

    def get_checkpoint(self):
        with self.report_order_condition:
            return self.checkpoint

    def get_all_reported_checkpoints(self) -> List["ReportedCheckpoint"]:
        return ray.get(
            self.controller_actor.get_all_reported_checkpoints.remote(
                self.current_report_index
            )
        )

    def get_dataset_shard(self, dataset_info: "DatasetShardMetadata") -> DataIterator:
        """Returns the :class:`ray.data.DataIterator` shard for this worker.

        Call :meth:`~ray.data.DataIterator.iter_torch_batches` or
        :meth:`~ray.data.DataIterator.to_tf` on this shard to convert it to the
        appropriate framework-specific data type.

        Args:
            dataset_info: The shard metadata, including the dataset name and worker rank.
        Returns:
            The ``DataIterator`` shard with the given name for this worker.
        Raises:
            KeyError: If the dataset shard with the given name is not found.
        """
        return self.dataset_shard_provider.get_dataset_shard(dataset_info)

    def get_context_callbacks(self) -> List["TrainContextCallback"]:
        return self.execution_context.train_context_callbacks

    def _sync_checkpoint_dir_name_across_ranks(
        self, checkpoint_dir_name: Optional[str] = None
    ) -> str:
        """Sync the checkpoint dir name across ranks.

        Args:
            checkpoint_dir_name: The checkpoint dir name to sync.

        Returns:
            The synced checkpoint dir name.
        """
        # If checkpoint_dir_name is not set, use default checkpoint_dir_name
        # created by the storage context.
        checkpoint_dir_name = (
            checkpoint_dir_name
            or self.storage_context.make_default_checkpoint_dir_name()
        )
        # Get a consensus across ranks on the remote storage path, so distributed
        # checkpoints will be stored to the same place.
        sync_actor = self.get_synchronization_actor()
        return ray.get(
            sync_actor.broadcast_from_rank_zero.remote(
                world_rank=self.distributed_context.world_rank,
                world_size=self.distributed_context.world_size,
                data=checkpoint_dir_name,
                caller_method_name="ray.train.report",
            )
        )

    def _upload_checkpoint(
        self,
        checkpoint_dir_name: str,
        metrics: Dict[str, Any],
        checkpoint: Optional["Checkpoint"] = None,
        delete_local_checkpoint_after_upload: bool = False,
    ) -> _TrainingResult:
        """Save the checkpoint to remote storage.

        Args:
            checkpoint_dir_name: The checkpoint dir to persist to.
            metrics: The metrics to report.
            checkpoint: The checkpoint to report.
            delete_local_checkpoint_after_upload: Whether to delete the checkpoint after it is uploaded.

        Returns:
            The training result object containing the persisted checkpoint.
        """

        if not checkpoint:
            return _TrainingResult(checkpoint=None, metrics=metrics)

        # Persist the checkpoint to the remote storage path.
        try:
            persisted_checkpoint = self.storage_context.persist_current_checkpoint(
                checkpoint, checkpoint_dir_name
            )
        except FileNotFoundError:
            logger.exception(
                f"Failed to find local checkpoint {checkpoint} when attempting to upload it. "
                "This could be caused by multiple workers on a node attempting to upload the "
                "same directory, and then one of the workers deletes the directory before the "
                "others finish."
            )
            raise
        # TODO: consider deleting local checkpoint as async callback instead
        if delete_local_checkpoint_after_upload:
            try:
                delete_fs_path(checkpoint.filesystem, checkpoint.path)
            except Exception:
                logger.exception(
                    f"Failed to delete the local checkpoint after a successful upload: {checkpoint}"
                )

        return _TrainingResult(checkpoint=persisted_checkpoint, metrics=metrics)

    def _wait_then_report(
        self, training_result: _TrainingResult, report_call_index: int
    ) -> None:
        """Thread waits for its turn before reporting training result to result queue.

        It does this in order to guarantee the FIFO processing of checkpoints.

        The queue size is set to 1 to avoid accumulating unprocessed results.
        If the queue is full, the put operation blocks until a result is consumed.

        TODO: Add a metric to track the blocking time waiting for the
        training result to be consumed by the controller.
        """
        with self.report_order_condition:
            self.report_order_condition.wait_for(
                lambda: self.current_report_index == report_call_index - 1
            )
            logger.info(
                f"Reporting training result {report_call_index}: {training_result}"
            )
            # Update latest checkpoint as the persisted checkpoint.
            if training_result.checkpoint:
                self.checkpoint = training_result.checkpoint
            self.get_result_queue().put(training_result)
            self.current_report_index += 1
            self.report_order_condition.notify_all()

    def report(
        self,
        metrics: Dict[str, Any],
        checkpoint: Optional["Checkpoint"] = None,
        checkpoint_dir_name: Optional[str] = None,
        checkpoint_upload_mode: CheckpointUploadMode = CheckpointUploadMode.SYNC,
        delete_local_checkpoint_after_upload: Optional[bool] = None,
    ) -> None:
        """
        Upload checkpoint to remote storage and put a training
        result on the result queue of this worker process.

        TODO: the report function should be implemented in the worker instead
        of in the train context. The train context should only keep the train
        related information and not the worker related actions. This refactor
        would also require the `TrainContextCallback` to be updated as well.
        """
        if "torch" in sys.modules:
            from ray.air._internal.torch_utils import contains_tensor

            if contains_tensor(metrics):
                raise ValueError(
                    "Passing objects containg Torch tensors as metrics "
                    "is not supported as it will throw an exception on "
                    "deserialization. You can either convert the tensors "
                    "to Python objects (ex: `.numpy()`, `.item()`, etc.) "
                    "or save tensors as part of the checkpoint files instead."
                )

        with invoke_context_managers(
            [
                callback.on_report
                for callback in self.execution_context.train_context_callbacks
            ]
        ):
            self.report_call_index += 1
            report_call_index = self.report_call_index

            # Sync the checkpoint dir name across ranks.
            checkpoint_dir_name = self._sync_checkpoint_dir_name_across_ranks(
                checkpoint_dir_name
            )

            # Upload checkpoint, wait for turn, and report.
            if checkpoint_upload_mode == CheckpointUploadMode.SYNC:
                training_result = self._upload_checkpoint(
                    checkpoint_dir_name,
                    metrics,
                    checkpoint,
                    delete_local_checkpoint_after_upload,
                )
                self._wait_then_report(training_result, report_call_index)

            elif checkpoint_upload_mode == CheckpointUploadMode.NO_UPLOAD:
                training_result = _TrainingResult(
                    checkpoint=checkpoint, metrics=metrics
                )
                self._wait_then_report(training_result, report_call_index)

            elif checkpoint_upload_mode == CheckpointUploadMode.ASYNC:

                def _upload_checkpoint_and_report(
                    checkpoint_dir_name: str,
                    metrics: Dict[str, Any],
                    checkpoint: Optional["Checkpoint"],
                    report_call_index: int,
                ) -> None:
                    try:
                        training_result = self._upload_checkpoint(
                            checkpoint_dir_name,
                            metrics,
                            checkpoint,
                            delete_local_checkpoint_after_upload,
                        )
                        self._wait_then_report(training_result, report_call_index)
                    except Exception as e:
                        logger.exception(
                            "Async checkpoint upload failed - shutting down workers"
                        )
                        self.execution_context.training_thread_runner.get_exception_queue().put(
                            construct_user_exception_with_traceback(e)
                        )

                self.checkpoint_upload_threadpool.submit(
                    _upload_checkpoint_and_report,
                    checkpoint_dir_name,
                    metrics,
                    checkpoint,
                    report_call_index,
                )
            else:
                raise ValueError(
                    f"Invalid checkpoint upload mode: {checkpoint_upload_mode}"
                )


# The global variable holding the current TrainContext
_train_context: Optional[TrainContext] = None

# Thread lock to protect the global TrainContext
_context_lock = threading.Lock()


def get_train_context() -> TrainContext:
    """Get the internal train context.

    Note:
        This should not be used directly by user-facing APIs. User-facing APIs should
        call :class:`~ray.train.v2._internal.execution.train_fn_utils.TrainFnUtils`
        or use :class:`~ray.train.v2.api.context.TrainContext` instead.

    Returns:
        The internal TrainContext for this worker.
    """
    with _context_lock:
        if _train_context is None:
            raise RuntimeError("TrainContext has not been initialized.")
        return _train_context


def set_train_context(context) -> None:
    global _train_context
    with _context_lock:
        _train_context = context
