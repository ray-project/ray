import asyncio
import os
import sys
from typing import Dict, List, Set

import torch

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
        Basic usage with multiple processes:

        ```python
        import argparse
        import asyncio
        import ray
        from ray.train.v2._internal.execution.local_running_utils import (
            maybe_start_local_running_data_provider,
            get_dataset_shard,
            finish_worker_and_wait
        )

        async def main():
            ray.init(address="auto")
            # Parse command line arguments
            parser = argparse.ArgumentParser(description="Ray Data Worker")
            parser.add_argument("--local-rank", type=int, required=True,
                              help="Local rank of this worker")
            parser.add_argument("--world-size", type=int, required=True,
                              help="Total number of workers")
            args = parser.parse_args()

            local_rank = args.local_rank
            world_size = args.world_size

            # Create your datasets
            datasets = {
                "train": ray.data.range(1000),
                "val": ray.data.range(200)
            }

            # Create or get the data provider actor
            provider_actor = await maybe_start_local_running_data_provider(
                world_size, datasets, local_rank
            )

            # Get this worker's data shard
            shard = await get_dataset_shard(provider_actor, local_rank)

            # Use the data iterators for training
            for batch in shard["train"].iter_batches():
                # Your training logic here
                pass

            # Mark worker as finished and wait for all workers if this is the owner
            await finish_worker_and_wait(provider_actor, local_rank)

        if __name__ == "__main__":
            asyncio.run(main())
        ```

        Run multiple workers with:
        ```bash
        python worker.py --local-rank 0 --world-size 4 &
        python worker.py --local-rank 1 --world-size 4 &
        python worker.py --local-rank 2 --world-size 4 &
        python worker.py --local-rank 3 --world-size 4 &
        wait
        ```
    """

    def __init__(self, world_size: int, local_rank: int, dataset: Dict[str, Dataset]):
        """Initialize the LocalRunningDataProvider.

        Args:
            world_size: Total number of workers that will request data shards.
            local_rank: The rank of the worker that owns this actor and will wait for all workers to finish at the end.
            dataset: Dictionary mapping dataset names to Dataset objects to be shared.
        """
        self.world_size = world_size
        # Will hold the registered datasets after register_dataset() is called
        self.dataset = None
        # Track which workers have finished (by local_rank)
        self.finished_workers: Set[int] = set()
        self.owner_rank = local_rank
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
        self.lock = asyncio.Lock()

    def get_dataset_shard(self, local_rank: int) -> Dict[str, DataIterator]:
        """Retrieve the dataset shard for a specific worker.

        Args:
            local_rank: The rank/ID of the requesting training worker.
                       Must be in range [0, world_size).

        Returns:
            Dictionary mapping dataset names to DataIterator objects for this worker.
            Each DataIterator contains the data shard assigned to this local_rank.

        Raises:
            AssertionError: If local_rank is invalid.
        """
        return self.dataset_shards[local_rank]

    async def mark_worker_finished(self, local_rank: int) -> None:
        """Mark a specific worker as finished. If this worker is the owner, this method will block until all workers finish.

        Args:
            local_rank: The rank/ID of the worker that finished.

        Returns:
            None. If this worker is the owner, this method will block until all workers finish.
        """
        assert (
            local_rank < self.world_size
        ), f"local_rank {local_rank} must be < world_size {self.world_size}"

        async with self.lock:
            self.finished_workers.add(local_rank)

        if local_rank == self.owner_rank:
            while True:
                async with self.lock:
                    if len(self.finished_workers) == self.world_size:
                        break
                await asyncio.sleep(0.1)
            # wait for 1 second to ensure all workers have finished
            await asyncio.sleep(1)


LOCAL_RUNNING_DATA_PROVIDER_ACTOR_NAME = "local_running_data_provider"
LOCAL_RUNNING_DATA_PROVIDER_NAMESPACE = "local_running_data_provider_namespace"


async def maybe_start_local_running_data_provider(
    world_size: int, dataset: Dict[str, Dataset], local_rank: int
) -> ActorHandle:
    """Create or get the LocalRunningDataProvider actor. This named actor is created only once."""
    actor = LocalRunningDataProvider.options(
        name=LOCAL_RUNNING_DATA_PROVIDER_ACTOR_NAME,
        namespace=LOCAL_RUNNING_DATA_PROVIDER_NAMESPACE,
        get_if_exists=True,
    ).remote(world_size, local_rank, dataset)
    await actor.__ray_ready__.remote()

    return actor


async def get_dataset_shard(
    provider_actor: ActorHandle, local_rank: int
) -> Dict[str, DataIterator]:
    return await provider_actor.get_dataset_shard.remote(local_rank)


async def finish_worker_and_wait(provider_actor: ActorHandle, local_rank: int) -> None:
    """Mark a worker as finished and wait for all workers if this is the owner.

    Args:
        provider_actor: The LocalRunningDataProvider actor handle.
        local_rank: The rank/ID of the worker that finished.
    """
    await provider_actor.mark_worker_finished.remote(local_rank)


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
