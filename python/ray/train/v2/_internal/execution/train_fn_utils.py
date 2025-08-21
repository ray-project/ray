import threading
from typing import Any, Dict, Optional

from ray.data import DataIterator
from ray.train import Checkpoint
from ray.train.v2._internal.execution.context import (
    get_train_context as get_internal_train_context,
)
from ray.train.v2.api.context import TrainContext as ExternalTrainContext


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
        return get_internal_train_context().get_dataset_shard(dataset_name)

    def get_context(self) -> ExternalTrainContext:
        return ExternalTrainContext()


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
