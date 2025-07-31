from typing import Dict

import ray
from ray.data import DataIterator, Dataset
from ray.train import DataConfig


@ray.remote
class GlobalLocalTrainerRayDataset:
    """A Ray actor that provides shared dataset access for multiple training processes.

    This actor serves as a centralized data provider that can share the same Ray Data
    instance among several training processes (e.g., launched by torchrun) without
    requiring the full Ray Train framework. The actor handles dataset sharding and
    distribution across workers in a distributed training setup.

    Key benefits:
    - Enables Ray Data usage in non-Ray Train distributed training scenarios
    - Provides consistent data sharding across multiple training processes
    - Reduces memory overhead by sharing dataset configuration
    - Supports locality-aware data distribution

    Typical usage:
    1. Create this actor with the total number of training workers (world_size).
    2. Every worker tries to register datasets, but only the first worker will succeed.
    3. Each training process requests its data shard using its local rank
    """

    def __init__(self, world_size: int):
        self.world_size = world_size
        # Will hold the registered datasets after register_dataset() is called
        self.dataset = None

    def register_dataset(self, dataset: Dict[str, Dataset]) -> None:
        if self.dataset is not None:
            return

        self.dataset = dataset
        self.data_config = DataConfig()
        self.dataset_shards = self.data_config.configure(
            datasets=self.dataset,
            world_size=self.world_size,
        )

    def get_dataset_shard(self, local_rank: int) -> Dict[str, DataIterator]:
        """Retrieve the dataset shard assigned to a specific training worker.

        Each training process should call this method with its unique local_rank
        to get its assigned portion of the registered datasets.

        Args:
            local_rank: The rank/ID of the requesting training worker.
                       Must be in range [0, world_size).

        Returns:
            Dictionary mapping dataset names to DataIterator objects for this worker.
            Each DataIterator contains the data shard assigned to this local_rank.

        Raises:
            AssertionError: If datasets haven't been registered or local_rank is invalid.
        """
        assert (
            self.dataset is not None
        ), "Must call register_dataset() before getting shards"
        assert (
            local_rank < self.world_size
        ), f"local_rank {local_rank} must be < world_size {self.world_size}"

        return self.dataset_shards[local_rank]
