import os
import sys
from typing import Dict, List

import torch

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


def launched_by_torchrun() -> bool:
    """Return True if this process looks like it came from `torchrun`."""
    env_markers = {
        "LOCAL_RANK",
        "LOCAL_WORLD_SIZE",
        "WORLD_SIZE",
        "TORCHELASTIC_RUN_ID",
    }  # torchrun ≥1.10
    argv_markers = (
        "--local-rank",
        "--local_rank",
    )  # torchrun always passes one of these

    # Any of the env vars *or* the CLI flag counts as evidence
    return bool(
        (env_markers & os.environ.keys())
        or any(a.startswith(argv_markers) for a in sys.argv)
    )


def local_running_get_devices() -> List[torch.device]:
    """Return a list of devices to use for training."""
    if torch.cuda.is_available():
        return [torch.device(f"cuda:{i}") for i in range(torch.cuda.device_count())]
    else:
        return [torch.device("cpu")]

LOCAL_RUNNING_DATA_PROVIDER_ACTOR_NAME = "local_running_data_provider"


def start_local_running_data_provider_and_register_datasets(datasets: Dict[str, Dataset]) -> None:
    data_provider = GlobalLocalTrainerRayDataset.options(
        name=LOCAL_RUNNING_DATA_PROVIDER_ACTOR_NAME
    ).remote(world_size=torch.distributed.get_world_size())

    data_provider.register_dataset.remote(datasets)


def get_local_running_data_provider() -> GlobalLocalTrainerRayDataset:
