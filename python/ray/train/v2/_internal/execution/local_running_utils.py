import asyncio
from typing import Dict, Set

from ray.actor import ActorHandle
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
    4. Workers call mark_worker_finished when they complete their work
    5. Use wait_for_all_workers_to_finish to ensure all workers complete before cleanup
    """

    def __init__(self, world_size: int, owner_rank: int):
        self.world_size = world_size
        # Will hold the registered datasets after register_dataset() is called
        self.dataset = None
        # Track which workers have finished (by local_rank)
        self.finished_workers: Set[int] = set()
        self.owner_rank = owner_rank

    def register_dataset(self, dataset: Dict[str, Dataset]) -> None:
        if self.dataset is not None:
            print("no action")
            return

        print("action")
        self.dataset = dataset
        self.data_config = DataConfig()
        self.dataset_shards = self.data_config.configure(
            datasets=self.dataset,
            world_size=self.world_size,
            worker_handles=None,
            worker_node_ids=None,
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

    def mark_worker_finished(self, local_rank: int) -> bool:
        """Mark a specific worker as finished.
        
        Args:
            local_rank: The rank/ID of the worker that finished.
        """
        assert (
            local_rank < self.world_size
        ), f"local_rank {local_rank} must be < world_size {self.world_size}"
        
        self.finished_workers.add(local_rank)
        return self.owner_rank == local_rank

    async def wait_for_all_workers_to_finish(self, local_rank: int) -> None:
        """Wait until all workers have marked themselves as finished.
        This method is only called by the owner of the actor.
        
        This method should be called by workers or a coordinator to ensure
        all workers complete before proceeding with cleanup.
        """
        assert self.owner_rank == local_rank, "This method is only called by the owner of the actor"
        while len(self.finished_workers) < self.world_size:
            await asyncio.sleep(0.1)
        # wait for 0.5 seconds to ensure all workers have finished
        await asyncio.sleep(0.5)


LOCAL_RUNNING_DATA_PROVIDER_ACTOR_NAME = "local_running_data_provider"
LOCAL_RUNNING_DATA_PROVIDER_NAMESPACE = "local_running_data_provider_namespace"


async def maybe_start_local_running_data_provider_and_register_dataset(
    world_size: int, dataset: Dict[str, Dataset], local_rank: int
) -> ActorHandle:
    actor = GlobalLocalTrainerRayDataset.options(
        name=LOCAL_RUNNING_DATA_PROVIDER_ACTOR_NAME,
        namespace=LOCAL_RUNNING_DATA_PROVIDER_NAMESPACE,
        get_if_exists=True,
    ).remote(world_size, local_rank)
    await actor.register_dataset.remote(dataset)
    return actor


async def get_dataset_shard(provider_actor: ActorHandle, local_rank: int) -> Dict[str, DataIterator]:
    return await provider_actor.get_dataset_shard.remote(local_rank)


async def mark_worker_finished(provider_actor: ActorHandle, local_rank: int) -> bool:
    """Mark a worker as finished."""
    return await provider_actor.mark_worker_finished.remote(local_rank)


async def wait_for_all_workers_to_finish(provider_actor: ActorHandle, local_rank: int) -> None:
    """Wait for all workers to finish."""
    await provider_actor.wait_for_all_workers_to_finish.remote(local_rank)
