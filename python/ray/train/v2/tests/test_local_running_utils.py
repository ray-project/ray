import asyncio

import pytest

import ray
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray.train.v2._internal.execution.local_running_utils import (
    finish_worker_and_wait,
    get_dataset_shard,
    maybe_start_local_running_data_provider,
)


class TestLocalRunningDataProvider:
    @pytest.fixture(autouse=True)
    def setup_ray(self):
        ray.init(num_cpus=4)
        yield
        ray.shutdown()

    def test_single_process_basic_functionality(self):
        """Test basic functionality in single process."""

        async def run_test():
            world_size = 1
            local_rank = 0

            datasets = {
                "train": ray.data.range(30),
                "val": ray.data.range(15),
            }

            provider_actor = await maybe_start_local_running_data_provider(
                world_size, datasets, local_rank
            )

            shard = await get_dataset_shard(provider_actor, local_rank)

            assert "train" in shard
            assert "val" in shard

            train_count = len(list(shard["train"].iter_rows()))
            val_count = len(list(shard["val"].iter_rows()))

            # Single worker should get all data
            assert train_count == 30
            assert val_count == 15

            # Mark worker as finished and wait for all workers (as owner)
            await finish_worker_and_wait(provider_actor, local_rank)

        asyncio.run(run_test())

    def test_data_consistency_across_workers(self):
        """Test that data is consistently distributed across workers using multi-processes."""
        world_size = 4

        # Worker script template - each worker gets its shard and verifies data
        worker_script = """
import ray
import asyncio
from ray.train.v2._internal.execution.local_running_utils import (
    maybe_start_local_running_data_provider,
    get_dataset_shard,
    finish_worker_and_wait
)

async def run_test():
    ray.init(address="auto")

    world_size = 4
    local_rank = {rank}

    train_data = list(range(100))
    val_data = list(range(100, 120))

    datasets = {{
        "train": ray.data.from_items(
            [{{"id": i, "value": i * 2}} for i in train_data]
        ),
        "val": ray.data.from_items([{{"id": i, "value": i * 3}} for i in val_data]),
    }}

    provider_actor = await maybe_start_local_running_data_provider(
        world_size, datasets, local_rank
    )

    # Get shard for this worker
    shard = await get_dataset_shard(provider_actor, local_rank)

    # Collect data from this worker's shard
    train_rows = list(shard["train"].iter_rows())
    val_rows = list(shard["val"].iter_rows())

    train_ids = [row["id"] for row in train_rows]
    val_ids = [row["id"] for row in val_rows]

    # Print results for parent process to collect
    print("WORKER_{rank}_TRAIN_IDS:" + ",".join(map(str, train_ids)))
    print("WORKER_{rank}_VAL_IDS:" + ",".join(map(str, val_ids)))
    print("WORKER_{rank}_TRAIN_COUNT:" + str(len(train_ids)))
    print("WORKER_{rank}_VAL_COUNT:" + str(len(val_ids)))

    await finish_worker_and_wait(provider_actor, local_rank)

    ray.shutdown()

asyncio.run(run_test())
"""

        # Launch all worker processes simultaneously
        worker_processes = []
        for rank in range(world_size):
            script = worker_script.format(rank=rank)
            proc = run_string_as_driver_nonblocking(script)
            worker_processes.append((rank, proc))

        # Collect results from all processes
        all_train_ids = set()
        all_val_ids = set()
        worker_results = {}

        for rank, proc in worker_processes:
            out = proc.stdout.read().decode("ascii")
            proc.wait()

            # Parse results from this worker
            train_ids = []
            val_ids = []
            train_count = 0
            val_count = 0

            for line in out.split("\n"):
                if line.startswith(f"WORKER_{rank}_TRAIN_IDS:"):
                    ids_str = line.split(":")[1]
                    if ids_str:  # Handle empty case
                        train_ids = list(map(int, ids_str.split(",")))
                elif line.startswith(f"WORKER_{rank}_VAL_IDS:"):
                    ids_str = line.split(":")[1]
                    if ids_str:  # Handle empty case
                        val_ids = list(map(int, ids_str.split(",")))
                elif line.startswith(f"WORKER_{rank}_TRAIN_COUNT:"):
                    train_count = int(line.split(":")[1])
                elif line.startswith(f"WORKER_{rank}_VAL_COUNT:"):
                    val_count = int(line.split(":")[1])

            # Verify no overlap with data from other workers
            train_ids_set = set(train_ids)
            val_ids_set = set(val_ids)

            assert all_train_ids.isdisjoint(
                train_ids_set
            ), f"Train data overlap for worker {rank}"
            assert all_val_ids.isdisjoint(
                val_ids_set
            ), f"Val data overlap for worker {rank}"

            all_train_ids.update(train_ids_set)
            all_val_ids.update(val_ids_set)

            worker_results[rank] = {
                "train_count": train_count,
                "val_count": val_count,
                "train_ids": train_ids,
                "val_ids": val_ids,
            }

        expected_train_data = set(range(100))
        expected_val_data = set(range(100, 120))

        assert all_train_ids == expected_train_data, "Missing or extra train data"
        assert all_val_ids == expected_val_data, "Missing or extra val data"

        # Verify each worker got roughly equal amounts (25 train, 5 val each)
        for rank in range(world_size):
            assert (
                worker_results[rank]["train_count"] == 25
            ), f"Worker {rank} got {worker_results[rank]['train_count']} train items, expected 25"
            assert (
                worker_results[rank]["val_count"] == 5
            ), f"Worker {rank} got {worker_results[rank]['val_count']} val items, expected 5"


if __name__ == "__main__":
    pytest.main([__file__])
