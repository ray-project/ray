from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict

import pytest

import ray
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray.train.v2._internal.execution.local_running_utils import (
    get_dataset_shard,
    maybe_start_local_running_data_provider_and_register_dataset,
    mark_worker_finished,
    wait_for_all_workers_to_finish,
)


class TestGlobalLocalTrainerRayDataset:
    """Test suite for GlobalLocalTrainerRayDataset actor with multiple processes/workers."""

    @pytest.fixture(autouse=True)
    def setup_ray(self):
        ray.init(num_cpus=4)
        yield
        ray.shutdown()

    # def test_single_process_basic_functionality(self):
    #     """Test basic functionality in single process."""
    #     world_size = 1

    #     # Create test datasets
    #     datasets = {
    #         "train": ray.data.range(30),  # 10 items per worker
    #         "val": ray.data.range(15),  # 5 items per worker
    #     }

    #     # Register datasets
    #     provider_actor = maybe_start_local_running_data_provider_and_register_dataset(
    #         world_size, datasets
    #     )

    #     # Test getting shards for each worker
    #     for local_rank in range(world_size):
    #         shard = get_dataset_shard(provider_actor, local_rank)

    #         # Verify we get the expected datasets
    #         assert "train" in shard
    #         assert "val" in shard

    #         # Verify shard sizes (approximately equal distribution)
    #         train_count = len(list(shard["train"].iter_rows()))
    #         val_count = len(list(shard["val"].iter_rows()))

    #         # Each worker should get roughly equal portions
    #         assert train_count == 30
    #         assert val_count == 15

    # def test_multi_threading_concurrent_access(self):
    #     """Test concurrent access from multiple threads (simulates multi-worker scenario)."""
    #     world_size = 4

    #     # Create test datasets
    #     datasets = {
    #         "train": ray.data.range(40),  # 10 items per worker
    #         "eval": ray.data.range(20),  # 5 items per worker
    #     }

    #     # Register datasets
    #     provider_actor = maybe_start_local_running_data_provider_and_register_dataset(
    #         world_size, datasets
    #     )

    #     def worker_function(local_rank: int) -> Dict:
    #         """Simulate a worker requesting its data shard."""
    #         shard = get_dataset_shard(provider_actor, local_rank)

    #         # Count items in each dataset
    #         train_count = len(list(shard["train"].iter_rows()))
    #         eval_count = len(list(shard["eval"].iter_rows()))

    #         return {
    #             "local_rank": local_rank,
    #             "train_count": train_count,
    #             "eval_count": eval_count,
    #             "train_data": list(shard["train"].iter_rows()),
    #             "eval_data": list(shard["eval"].iter_rows()),
    #         }

    #     # Execute workers concurrently using threads
    #     results = []
    #     with ThreadPoolExecutor(max_workers=world_size) as executor:
    #         futures = [
    #             executor.submit(worker_function, rank) for rank in range(world_size)
    #         ]
    #         for future in as_completed(futures):
    #             results.append(future.result())

    #     # Verify results
    #     assert len(results) == world_size

    #     # Each worker should get different data
    #     all_train_data = []
    #     all_eval_data = []

    #     for result in sorted(results, key=lambda x: x["local_rank"]):
    #         # Each worker should get roughly equal amounts
    #         assert result["train_count"] == 10
    #         assert result["eval_count"] == 5

    #         # Collect all data to verify no overlap
    #         all_train_data.extend([row["id"] for row in result["train_data"]])
    #         all_eval_data.extend([row["id"] for row in result["eval_data"]])

    #     # Verify all data is distributed and no duplicates
    #     assert len(all_train_data) == 40
    #     assert len(set(all_train_data)) == 40  # No duplicates
    #     assert len(all_eval_data) == 20
    #     assert len(set(all_eval_data)) == 20  # No duplicates


    def test_multi_process_simulation(self):
        """Test with actual multiple processes using Ray's testing utilities."""

        # Worker script template - each worker creates/gets actor and tries to register
        worker_script = """
import ray
import asyncio
from ray.train.v2._internal.execution.local_running_utils import (
    maybe_start_local_running_data_provider_and_register_dataset, 
    get_dataset_shard,
    mark_worker_finished,
    wait_for_all_workers_to_finish
)

async def main():
    ray.init(address="auto")
    # Create or get the shared actor (first process creates it, others get it)
    world_size = 3

    # All workers try to register datasets (only first one succeeds)
    datasets = {{
        "train": ray.data.range(30),
        "val": ray.data.range(15)
    }}
    actor = await maybe_start_local_running_data_provider_and_register_dataset(world_size, datasets)
    print("WORKER_{rank}: Attempted dataset registration")

    # Get data shard
    local_rank = {rank}
    shard = await get_dataset_shard(actor, local_rank)
    assert shard is not None

    # Count data
    train_count = len(list(shard["train"].iter_rows()))
    val_count = len(list(shard["val"].iter_rows()))

    print("WORKER_{rank}_RESULT:" + str(train_count) + "," + str(val_count))
    
    # Mark worker as finished
    await mark_worker_finished(actor, local_rank)
    
    ray.shutdown()

asyncio.run(main())
"""

        # Launch all worker processes simultaneously
        worker_processes = []
        for rank in range(3):
            script = worker_script.format(rank=rank)
            proc = run_string_as_driver_nonblocking(script)
            worker_processes.append((rank, proc))

        # Collect results
        results = {}
        for rank, proc in worker_processes:
            out = proc.stdout.read().decode("ascii")
            proc.wait()
            assert proc.returncode == 0

            # Parse results
            for line in out.split("\n"):
                if line.startswith(f"WORKER_{rank}_RESULT:"):
                    train_count, val_count = map(int, line.split(":")[1].split(","))
                    results[rank] = {"train_count": train_count, "val_count": val_count}

        # Verify each worker got correct shard sizes
        assert len(results) == 3
        for rank in range(3):
            assert results[rank]["train_count"] == 10  # 30 / 3
            assert results[rank]["val_count"] == 5  # 15 / 3

    def test_data_consistency_across_workers(self):
        """Test that data is consistently distributed across workers using multi-processes."""
        world_size = 4

        # Worker script template - each worker gets its shard and verifies data
        worker_script = """
import ray
import asyncio
from ray.train.v2._internal.execution.local_running_utils import (
    maybe_start_local_running_data_provider_and_register_dataset,
    get_dataset_shard,
    mark_worker_finished,
    wait_for_all_workers_to_finish
)

async def main():
    ray.init(address="auto")

    world_size = 4
    local_rank = {rank}

    # Create datasets with known data
    train_data = list(range(100))  # 0-99
    val_data = list(range(100, 120))  # 100-119

    datasets = {{
        "train": ray.data.from_items(
            [{{"id": i, "value": i * 2}} for i in train_data]
        ),
        "val": ray.data.from_items([{{"id": i, "value": i * 3}} for i in val_data]),
    }}

    # Register datasets (only first worker succeeds, others get existing actor)
    provider_actor = await maybe_start_local_running_data_provider_and_register_dataset(
        world_size, datasets
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

    # Mark worker as finished
    await mark_worker_finished(provider_actor, local_rank)

    ray.shutdown()

asyncio.run(main())
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
            assert proc.returncode == 0

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

            # Add this worker's data to the global sets
            all_train_ids.update(train_ids_set)
            all_val_ids.update(val_ids_set)

            worker_results[rank] = {
                "train_count": train_count,
                "val_count": val_count,
                "train_ids": train_ids,
                "val_ids": val_ids
            }

        # Verify all data is accounted for across all workers
        expected_train_data = set(range(100))  # 0-99
        expected_val_data = set(range(100, 120))  # 100-119

        assert all_train_ids == expected_train_data, "Missing or extra train data"
        assert all_val_ids == expected_val_data, "Missing or extra val data"

        # Verify each worker got roughly equal amounts (25 train, 5 val each)
        for rank in range(world_size):
            assert worker_results[rank]["train_count"] == 25, f"Worker {rank} got {worker_results[rank]['train_count']} train items, expected 25"
            assert worker_results[rank]["val_count"] == 5, f"Worker {rank} got {worker_results[rank]['val_count']} val items, expected 5"


if __name__ == "__main__":
    pytest.main([__file__])
