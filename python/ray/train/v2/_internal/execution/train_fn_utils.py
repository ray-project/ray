import logging
import threading
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.data import DataIterator
from ray.train.v2._internal.data_integration.interfaces import DatasetShardMetadata
from ray.train.v2._internal.execution import collective_impl
from ray.train.v2._internal.execution.context import (
    get_train_context as get_internal_train_context,
)
from ray.train.v2.api.context import (
    DistributedTrainContext,
    LocalTrainContext,
    TrainContext as ExternalTrainContext,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ray.train import Checkpoint
    from ray.train.v2.api.reported_checkpoint import ReportedCheckpoint


class TrainFnUtils(ABC):
    """Utility class providing an abstraction layer between user-facing APIs
        and :class:`~ray.train.v2.api.context.TrainContext`.

    It should be set before the users' training function is called.
    This class can be patched if new user APIs behaviors is wanted.
    """

    @abstractmethod
    def report(
        self,
        metrics: Dict[str, Any],
        checkpoint: Optional["Checkpoint"] = None,
        checkpoint_dir_name: Optional[str] = None,
    ) -> None:
        """Upload checkpoint to remote storage and put a training result on the result queue.

        Args:
            metrics: The metrics to report.
            checkpoint: The checkpoint to report.
            checkpoint_dir_name: The name of the checkpoint dir
                in this iteration. Note: If not set, the checkpoint will
                be stored in the default storage path. If set, make sure
                this value is unique for each iteration.
        """
        pass

    @abstractmethod
    def get_checkpoint(self) -> Optional["Checkpoint"]:
        """Get the latest checkpoint to resume training from.

        Returns:
            The latest checkpoint if available, None otherwise.
        """
        pass

    @abstractmethod
    def get_all_reported_checkpoints(self) -> List["ReportedCheckpoint"]:
        """Get all the checkpoints reported by the workers.

        Returns:
            A list of ReportedCheckpoint objects that represent the checkpoints and
            corresponding metrics reported by the workers.
        """
        pass

    @abstractmethod
    def get_dataset_shard(self, dataset_info: DatasetShardMetadata) -> DataIterator:
        """Get the dataset shard for this training process.

        Args:
            dataset_info: The metadata of the dataset to get the shard for.

        Returns:
            The DataIterator shard for this worker.
        """
        pass

    @abstractmethod
    def get_context(self) -> ExternalTrainContext:
        """Get the TrainContext for this training process.
        The specific type of TrainContext returned depends on the implementation of TrainFnUtils.

        Returns:
            The train context for this training process.
        """
        pass

    @abstractmethod
    def is_distributed(self) -> bool:
        pass

    @abstractmethod
    def barrier(self) -> None:
        """Create a barrier across all workers.

        All workers must call this method before the training function can continue.

        This method is used by the public API function :func:`ray.train.collective.barrier`.
        Users should typically call ``ray.train.collective.barrier()`` instead of calling this method directly.
        """
        pass

    @abstractmethod
    def broadcast_from_rank_zero(self, data: Any) -> Any:
        """Broadcast data from the rank 0 worker to all other workers.

        This method is used by the public API function :func:`ray.train.collective.broadcast_from_rank_zero`.
        Users should typically call ``ray.train.collective.broadcast_from_rank_zero()`` instead of calling this method directly.
        """
        pass


class DistributedTrainFnUtils(TrainFnUtils):
    def report(
        self,
        metrics: Dict[str, Any],
        checkpoint: Optional["Checkpoint"] = None,
        checkpoint_dir_name: Optional[str] = None,
    ) -> None:
        return get_internal_train_context().report(
            metrics, checkpoint, checkpoint_dir_name
        )

    def get_checkpoint(self):
        return get_internal_train_context().get_checkpoint()

    def get_dataset_shard(self, dataset_info: DatasetShardMetadata) -> DataIterator:
        return get_internal_train_context().get_dataset_shard(dataset_info)

    def get_context(self) -> DistributedTrainContext:
        return DistributedTrainContext()

    def is_distributed(self) -> bool:
        return True

    def barrier(self) -> None:
        return collective_impl.barrier()

    def broadcast_from_rank_zero(self, data: Any) -> Any:
        return collective_impl.broadcast_from_rank_zero(data)

    def get_all_reported_checkpoints(self) -> List["ReportedCheckpoint"]:
        return get_internal_train_context().get_all_reported_checkpoints()


class LocalTrainFnUtils(TrainFnUtils):
    def __init__(
        self,
        experiment_name: str,
        dataset_shards: Optional[Dict[str, DataIterator]] = None,
    ):
        self._context = LocalTrainContext(
            experiment_name=experiment_name,
        )
        self._dataset_shards = dataset_shards
        self._last_metrics = None
        self._last_checkpoint = None

    def report(
        self,
        metrics: Dict[str, Any],
        checkpoint: Optional["Checkpoint"] = None,
        checkpoint_dir_name: Optional[str] = None,
    ) -> None:
        self._last_metrics = metrics
        self._last_checkpoint = checkpoint
        logger.info(f"Reported metrics: {metrics}")

    def get_checkpoint(self) -> Optional["Checkpoint"]:
        return self._last_checkpoint

    def get_dataset_shard(self, dataset_info: DatasetShardMetadata) -> DataIterator:
        dataset_name = dataset_info.dataset_name
        assert (
            self._dataset_shards is not None and dataset_name in self._dataset_shards
        ), f"Dataset shard {dataset_name} not found."
        return self._dataset_shards[dataset_name]

    def get_context(self) -> LocalTrainContext:
        return self._context

    def is_distributed(self) -> bool:
        return False

    def barrier(self) -> None:
        pass

    def broadcast_from_rank_zero(self, data: Any) -> Any:
        return data

    def _get_last_metrics(self) -> Optional[Dict[str, Any]]:
        """Return the last metrics reported by the training function.
        This function should only be called by LocalController
        """
        return self._last_metrics

    def get_all_reported_checkpoints(self) -> List["ReportedCheckpoint"]:
        return []


_train_fn_utils: Optional[TrainFnUtils] = None
_train_fn_utils_lock = threading.Lock()


def get_train_fn_utils() -> TrainFnUtils:
    global _train_fn_utils
    with _train_fn_utils_lock:
        if _train_fn_utils is None:
            raise RuntimeError("TrainFnUtils has not been initialized.")
        return _train_fn_utils


def set_train_fn_utils(train_fn_utils) -> None:
    global _train_fn_utils
    with _train_fn_utils_lock:
        _train_fn_utils = train_fn_utils
