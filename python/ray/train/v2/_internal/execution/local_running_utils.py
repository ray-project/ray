import time
from typing import Dict, Set

import ray
from ray.actor import ActorHandle
from ray.data import DataIterator, Dataset
from ray.train import DataConfig


@ray.remote
class LocalRunningDataProvider:
    """A Ray actor that provides shared dataset access for multiple training processes.

    This actor serves as a centralized data provider that can share the same Ray Data
    instance among several training processes (e.g., launched by torchrun) without
    the ray train instance.

    Example:
        Basic usage with multiple workers:

        ```python
        import asyncio
        import ray
        from ray.train.v2._internal.execution.local_running_utils import (
            get_dataset_shard_with_provider,
            finish_worker_and_wait
        )

        async def worker_process(local_rank: int, world_size: int):
            # Create your datasets
            datasets = {
                "train": ray.data.range(1000),
                "val": ray.data.range(200)
            }

            # Get this worker's data shard (automatically handles actor creation and registration)
            shard = await get_dataset_shard_with_provider(world_size, datasets, local_rank)

            # Use the data iterators for training
            for batch in shard["train"].iter_batches():
                # Your training logic here
                pass

            # Note: For finish_worker_and_wait, you'll need the provider actor
            # Alternative approach for more control:
            # provider_actor = await maybe_start_local_running_data_provider_and_register_dataset(
            #     world_size, datasets, local_rank
            # )
            # shard = await get_dataset_shard(provider_actor, datasets, local_rank)
            # await finish_worker_and_wait(provider_actor, local_rank)

        # Launch multiple worker processes
        async def main():
            world_size = 4
            tasks = []
            for rank in range(world_size):
                task = asyncio.create_task(worker_process(rank, world_size))
                tasks.append(task)
            await asyncio.gather(*tasks)

        asyncio.run(main())
        ```
    """

    def __init__(self, world_size: int, owner_rank: int, dataset: Dict[str, Dataset]):
        self.world_size = world_size
        # Will hold the registered datasets after register_dataset() is called
        self.dataset = None
        # Track which workers have finished (by local_rank)
        self.finished_workers: Set[int] = set()
        self.owner_rank = owner_rank
        # Lock to prevent concurrent dataset registration
        self.fetched_dataset_shards = set()
        self.dataset = dataset
        self.data_config = DataConfig()
        self.dataset_shards = self.data_config.configure(
            datasets=self.dataset,
            world_size=self.world_size,
            worker_handles=None,
            worker_node_ids=None,
        )

    def get_dataset_shard(self, local_rank: int) -> Dict[str, DataIterator]:
        """Register datasets (if not already done) and retrieve the shard for a specific worker.

        This method combines dataset registration and shard retrieval into a single operation.
        The first worker to call this method will register the datasets, and subsequent calls
        will reuse the existing registration.

        Args:
            dataset: Dictionary mapping dataset names to Dataset objects to register.
                    This parameter is ignored if datasets are already registered.
            local_rank: The rank/ID of the requesting training worker.
                       Must be in range [0, world_size).

        Returns:
            Dictionary mapping dataset names to DataIterator objects for this worker.
            Each DataIterator contains the data shard assigned to this local_rank.

        Raises:
            AssertionError: If local_rank is invalid.
        """
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

    def is_all_workers_finished(self) -> bool:
        """Check if all workers have finished."""
        return len(self.finished_workers) == self.world_size


LOCAL_RUNNING_DATA_PROVIDER_ACTOR_NAME = "local_running_data_provider"
LOCAL_RUNNING_DATA_PROVIDER_NAMESPACE = "local_running_data_provider_namespace"


def maybe_start_local_running_data_provider(
    world_size: int, dataset: Dict[str, Dataset], local_rank: int
) -> ActorHandle:
    """Create or get the LocalRunningDataProvider actor.

    Note: Dataset registration is now handled automatically by get_dataset_shard().
    This function only creates/gets the actor for backward compatibility.
    """
    actor = LocalRunningDataProvider.options(
        name=LOCAL_RUNNING_DATA_PROVIDER_ACTOR_NAME,
        namespace=LOCAL_RUNNING_DATA_PROVIDER_NAMESPACE,
        get_if_exists=True,
    ).remote(world_size, local_rank, dataset)
    ray.get(actor.__ray_ready__.remote())

    return actor


def get_dataset_shard(
    provider_actor: ActorHandle, local_rank: int
) -> Dict[str, DataIterator]:
    """Get the dataset shard for a specific worker, registering datasets if needed."""
    return ray.get(provider_actor.get_dataset_shard.remote(local_rank))


def mark_worker_finished(provider_actor: ActorHandle, local_rank: int) -> bool:
    """Mark a worker as finished."""
    return ray.get(provider_actor.mark_worker_finished.remote(local_rank))


def finish_worker_and_wait(provider_actor: ActorHandle, local_rank: int) -> bool:
    """Mark a worker as finished and wait for all workers if this is the owner.

    This is a convenience function that combines mark_worker_finished and
    wait_for_all_workers_to_finish functionality.

    Args:
        provider_actor: The LocalRunningDataProvider actor handle.
        local_rank: The rank/ID of the worker that finished.

    Returns:
        bool: True if this worker was the owner and waited for all workers,
              False if this was not the owner.
    """
    is_owner = mark_worker_finished(provider_actor, local_rank)
    if is_owner:
        while not ray.get(provider_actor.is_all_workers_finished.remote()):
            time.sleep(0.1)
        # wait for 1 second to ensure all workers have finished
        time.sleep(1)
