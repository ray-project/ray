import logging
import sys
import threading
import uuid
from dataclasses import dataclass, field
from queue import Queue
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray
from ray.data import DataIterator, Dataset
from ray.train import BackendConfig, Checkpoint, DataConfig
from ray.train._internal import session
from ray.train._internal.session import _TrainingResult
from ray.train.v2._internal.execution.checkpoint.sync_actor import SynchronizationActor
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2._internal.util import _copy_doc, invoke_context_managers
from ray.train.v2.api.config import RunConfig, ScalingConfig

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.callback import TrainContextCallback
    from ray.train.v2._internal.execution.worker_group.thread_runner import ThreadRunner


logger = logging.getLogger(__file__)


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
    backend_config: BackendConfig

    # The datasets used in the current training run.
    datasets: Dict[str, Dataset]

    # The configuration for dataset ingestion and sharding.
    dataset_config: DataConfig

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
    dataset_shards: Dict[str, DataIterator]
    checkpoint: Optional[Checkpoint] = None

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
        return self.checkpoint

    def get_dataset_shard(self, dataset_name: str) -> DataIterator:
        """Returns the :class:`ray.data.DataIterator` shard for this worker.

        Call :meth:`~ray.data.DataIterator.iter_torch_batches` or
        :meth:`~ray.data.DataIterator.to_tf` on this shard to convert it to the
        appropriate framework-specific data type.

        Args:
            dataset_name: Name of the dataset shard.
        Returns:
            The ``DataIterator`` shard with the given name for this worker.
        Raises:
            KeyError: If the dataset shard with the given name is not found.
        """
        try:
            return self.dataset_shards[dataset_name]
        except KeyError:
            raise KeyError(
                f"Dataset {dataset_name} not found. Available datasets: "
                f"{list(self.dataset_shards.keys())}."
            )

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

    def _save_checkpoint(
        self,
        checkpoint_dir_name: str,
        metrics: Dict[str, Any],
        checkpoint: Optional[Checkpoint] = None,
    ) -> _TrainingResult:
        """Save the checkpoint to remote storage.

        Returns:
            The training result object containing the persisted checkpoint.
        """

        if not checkpoint:
            return _TrainingResult(checkpoint=None, metrics=metrics)

        # Persist the checkpoint to the remote storage path.
        persisted_checkpoint = self.storage_context.persist_current_checkpoint(
            checkpoint, checkpoint_dir_name
        )
        # Update latest checkpoint as the persisted checkpoint.
        self.checkpoint = persisted_checkpoint

        return _TrainingResult(checkpoint=persisted_checkpoint, metrics=metrics)

    def report(
        self,
        metrics: Dict[str, Any],
        checkpoint: Optional[Checkpoint] = None,
        checkpoint_dir_name: Optional[str] = None,
    ) -> None:
        """
        Upload checkpoint to remote storage and put a training
        result on the result queue of this worker process.

        Args:
            metrics: The metrics to report.
            checkpoint: The checkpoint to report.
            checkpoint_dir_name: The name of the checkpoint dir
                in this iteration. Note: If not set, the checkpoint will
                be stored in the default storage path. If set, make sure
                this value is unique for each iteration.

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
            # Step 1: sync the checkpoint dir name across ranks.
            checkpoint_dir_name = self._sync_checkpoint_dir_name_across_ranks(
                checkpoint_dir_name
            )
            # Step 2: save the checkpoint to remote storage.
            training_result = self._save_checkpoint(
                checkpoint_dir_name, metrics, checkpoint
            )
            # Step 3: Report the training result to the result queue.
            # The queue size is set to 1 to avoid accumulating unprocessed results.
            # If the queue is full, the put operation blocks until a result is consumed.

            # TODO (hpguo): Add a metrics to track the blocking time waiting for the
            # training result to be consumed by the controller.
            self.get_result_queue().put(training_result)


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
