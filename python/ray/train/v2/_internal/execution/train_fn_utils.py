import logging
import threading
from typing import Any, Dict, Optional

import ray
import ray.cloudpickle as pickle
from ray.data import DataIterator
from ray.train import Checkpoint
from ray.train.v2._internal.execution.context import (
    get_train_context as get_internal_train_context,
)
from ray.train.v2.api.context import TrainContext as ExternalTrainContext

logger = logging.getLogger(__name__)

# For reference, {1:1} is 19 bytes, {"1":"1"} is 21 bytes,
# and {"12345": "12345"} is 25 bytes.
_MAX_BROADCAST_SIZE_BYTES = 1000


class TrainFnUtils:
    """Utility class providing an abstraction layer between user-facing APIs
        and :class:`~ray.train.v2._internal.execution.context.TrainContext`.

    It should be set before the users' training function is called, like training workers initialization.
    This class can be patched if new user APIs behaviors is wanted.
    """

    def report(
        self,
        metrics: Dict[str, Any],
        checkpoint: Optional[Checkpoint] = None,
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
        return get_internal_train_context().report(
            metrics, checkpoint, checkpoint_dir_name
        )

    def get_checkpoint(self):
        """Get the latest checkpoint to resume training from.

        Returns:
            The latest checkpoint if available, None otherwise.
        """
        return get_internal_train_context().get_checkpoint()

    def get_dataset_shard(self, dataset_name: str) -> DataIterator:
        """Get the dataset shard for this worker.

        This method is used by the public API function :func:`ray.train.get_dataset_shard`.
        Users should typically call ``ray.train.get_dataset_shard()`` instead of calling this method directly.

        Args:
            dataset_name: The name of the dataset to get the shard for.

        Returns:
            The DataIterator shard for this worker.
        """
        from ray.train.v2._internal.callbacks.datasets import DatasetShardMetadata

        dataset_info = DatasetShardMetadata(
            dataset_name=dataset_name,
            world_rank=get_internal_train_context().get_world_rank(),
        )
        return get_internal_train_context().get_dataset_shard(dataset_info)

    def get_context(self) -> ExternalTrainContext:
        return ExternalTrainContext()

    def barrier(self) -> None:
        """Create a barrier across all workers.

        All workers must call this method before the training function can continue.

        This method is used by the public API function :func:`ray.train.collective.barrier`.
        Users should typically call ``ray.train.collective.barrier()`` instead of calling this method directly.
        """
        sync_actor = get_internal_train_context().get_synchronization_actor()
        return ray.get(
            sync_actor.broadcast_from_rank_zero.remote(
                world_rank=get_internal_train_context().get_world_rank(),
                world_size=get_internal_train_context().get_world_size(),
                data=None,
                caller_method_name="ray.train.collective.barrier",
            )
        )

    def broadcast_from_rank_zero(self, data: Any) -> Any:
        """Broadcast data from the rank 0 worker to all other workers.

        This method is used by the public API function :func:`ray.train.collective.broadcast_from_rank_zero`.
        Users should typically call ``ray.train.collective.broadcast_from_rank_zero()`` instead of calling this method directly.
        """
        # Validate data.
        if data is not None:
            data_bytes = len(pickle.dumps(data))
            if data_bytes > _MAX_BROADCAST_SIZE_BYTES:
                logger.warning(
                    f"Data size {data_bytes} bytes exceeds the maximum broadcast "
                    f"size of {_MAX_BROADCAST_SIZE_BYTES} bytes"
                )

        sync_actor = get_internal_train_context().get_synchronization_actor()
        return ray.get(
            sync_actor.broadcast_from_rank_zero.remote(
                world_rank=get_internal_train_context().get_world_rank(),
                world_size=get_internal_train_context().get_world_size(),
                data=data,
                caller_method_name="ray.train.collective.broadcast_from_rank_zero",
            )
        )


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
