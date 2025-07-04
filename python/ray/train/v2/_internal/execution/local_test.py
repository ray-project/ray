import os
import threading
from queue import Queue
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ray.data import DataIterator
from ray.train import Checkpoint
from ray.train._internal.session import _TrainingResult

if TYPE_CHECKING:
    from ray.data import Dataset
    from ray.train.v2._internal.execution.callback import TrainContextCallback


class LocalTestTrainContext:
    """A simplified TrainContext for local testing purposes.

    This class provides the same API as TrainContext but with simplified
    implementations suitable for local testing with a single worker
    (world_size=1, rank=0).
    """

    def __init__(
        self,
        experiment_name: str = "test_experiment",
    ):
        self.experiment_name = experiment_name
        self.datasets = {}
        self.checkpoint = None
        self._result_queue = Queue()
        self._metrics_log = []

    def get_experiment_name(self) -> str:
        return self.experiment_name

    def get_world_size(self) -> int:
        return 1

    def get_world_rank(self) -> int:
        return 0

    def get_local_rank(self) -> int:
        return 0

    def get_local_world_size(self) -> int:
        return 1

    def get_node_rank(self) -> int:
        return 0

    def get_storage(self):
        # Return a mock storage context for local testing
        return None

    def get_result_queue(self):
        return self._result_queue

    def get_synchronization_actor(self):
        raise NotImplementedError(
            "get_synchronization_actor is not implemented for local testing"
        )

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
            dataset = self.datasets[dataset_name]
            # For local testing, return the entire dataset as a single shard
            return dataset.iterator()
        except KeyError:
            raise KeyError(
                f"Dataset {dataset_name} not found. Available datasets: "
                f"{list(self.datasets.keys())}."
            )

    def get_context_callbacks(self) -> List["TrainContextCallback"]:
        # Return empty list for local testing
        return []

    def report(
        self,
        metrics: Dict[str, Any],
        checkpoint: Optional[Checkpoint] = None,
        checkpoint_dir_name: Optional[str] = None,
    ):
        """
        Report metrics and checkpoint for local testing.

        This simplified version stores the metrics in a local log and
        updates the checkpoint without remote storage operations.

        Args:
            metrics: The metrics to report.
            checkpoint: The checkpoint to report.
            checkpoint_dir_name: The name of the checkpoint dir
                in this iteration. (Ignored in local testing)
        """
        print(f"Reporting metrics: {metrics}")

    def get_metrics_log(self) -> List[Dict[str, Any]]:
        """Get the logged metrics for local testing purposes."""
        return self._metrics_log.copy()

    def _sync_checkpoint_dir_name_across_ranks(
        self, checkpoint_dir_name: Optional[str] = None
    ) -> str:
        """Sync the checkpoint dir name across ranks.

        For local testing, simply return the provided name or a default.
        """
        return checkpoint_dir_name or "checkpoint"

    def _save_checkpoint(
        self,
        checkpoint_dir_name: str,
        metrics: Dict[str, Any],
        checkpoint: Optional[Checkpoint] = None,
    ) -> _TrainingResult:
        """Save the checkpoint for local testing.

        This simplified version just returns the training result without
        actual persistence.
        """
        if checkpoint:
            self.checkpoint = checkpoint
        return _TrainingResult(checkpoint=checkpoint, metrics=metrics)


_local_train_context = LocalTestTrainContext()

ALLOW_LOCAL_TRAIN_FUNCTION_RUN = "ALLOW_LOCAL_TRAIN_FUNCTION_RUN"

# Thread lock to protect the global TrainContext
_local_test_context_lock = threading.Lock()


def populate_dataset_shards_for_local_test(datasets: Dict[str, Dataset]) -> None:
    if os.environ.get(ALLOW_LOCAL_TRAIN_FUNCTION_RUN) == "0":
        return

    with _local_test_context_lock:
        _local_train_context.datasets = datasets
