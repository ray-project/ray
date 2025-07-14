from typing import Any, Dict

from ray.data import DataIterator
from ray.train.v2.api.base_context import TrainContext
from ray.util.annotations import DeveloperAPI


# Any context that is for local testing, e.g. single device, torchrun
# should inherit from this class.
@DeveloperAPI
class LocalTestingContext(TrainContext):
    """A TrainContext implementation for local testing.

    This context provides default implementations of all TrainContext methods
    that are suitable for local testing scenarios, typically single-node,
    single-worker setups.
    """

    dataset_shards: Dict[str, DataIterator]

    def __init__(self, dataset_shards: Dict[str, DataIterator]):
        self.dataset_shards = dataset_shards

    def get_experiment_name(self) -> str:
        """Get the experiment name for testing.

        Returns:
            The experiment name.
        """
        return "test_experiment"

    def get_world_size(self) -> int:
        """Get the world size for local testing.

        Returns:
            Always returns 1 for local testing (single worker).
        """
        return 1

    def get_world_rank(self) -> int:
        """Get the world rank for local testing.

        Returns:
            Always returns 0 for local testing (single worker).
        """
        return 0

    def get_local_rank(self) -> int:
        """Get the local rank for local testing.

        Returns:
            Always returns 0 for local testing (single worker).
        """
        return 0

    def get_local_world_size(self) -> int:
        """Get the local world size for local testing.

        Returns:
            Always returns 1 for local testing (single worker).
        """
        return 1

    def get_node_rank(self) -> int:
        """Get the node rank for local testing.

        Returns:
            Always returns 0 for local testing (single node).
        """
        return 0

    def get_storage(self) -> Any:
        """Get the storage context for local testing.

        Returns:
            A mock storage context for testing.
        """
        return self._storage

    def get_dataset_shard(self, dataset_name: str) -> DataIterator:
        """Get the dataset shard for local testing.

        Returns:
            A mock dataset shard for testing.
        """
        return self.dataset_shards[dataset_name]
