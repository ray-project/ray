from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict

import pytest

import ray
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray.train.v2._internal.execution.local_running_utils import (
    get_dataset_shard,
    maybe_start_local_running_data_provider_and_register_dataset,
)


class TestGlobalLocalTrainerRayDataset:
    """Test suite for GlobalLocalTrainerRayDataset actor with multiple processes/workers."""

    @pytest.fixture(autouse=True)
    def setup_ray(self):
        ray.init(num_cpus=4)
        yield
        ray.shutdown()

    def test_single_process_basic_functionality(self):
        """Test basic functionality in single process."""
        world_size = 1

        # Create test datasets
        datasets = {
            "train": ray.data.range(30),  # 10 items per worker
            "val": ray.data.range(15),  # 5 items per worker
        }

        # Register datasets
        maybe_start_local_running_data_provider_and_register_dataset(
            world_size, datasets
        )

        # Test getting shards for each worker
        for local_rank in range(world_size):
            shard = get_dataset_shard(local_rank)

            # Verify we get the expected datasets
            assert "train" in shard
            assert "val" in shard

            # Verify shard sizes (approximately equal distribution)
            train_count = len(list(shard["train"].iter_rows()))
            val_count = len(list(shard["val"].iter_rows()))

            # Each worker should get roughly equal portions
            assert train_count == 30
            assert val_count == 15

    def test_multi_threading_concurrent_access(self):
        """Test concurrent access from multiple threads (simulates multi-worker scenario)."""
        world_size = 4

        # Create test datasets
        datasets = {
            "train": ray.data.range(40),  # 10 items per worker
            "eval": ray.data.range(20),  # 5 items per worker
        }

        # Register datasets
        maybe_start_local_running_data_provider_and_register_dataset(
            world_size, datasets
        )

        def worker_function(local_rank: int) -> Dict:
            """Simulate a worker requesting its data shard."""
            shard = get_dataset_shard(local_rank)

            # Count items in each dataset
            train_count = len(list(shard["train"].iter_rows()))
            eval_count = len(list(shard["eval"].iter_rows()))

            return {
                "local_rank": local_rank,
                "train_count": train_count,
                "eval_count": eval_count,
                "train_data": list(shard["train"].iter_rows()),
                "eval_data": list(shard["eval"].iter_rows()),
            }

        # Execute workers concurrently using threads
        results = []
        with ThreadPoolExecutor(max_workers=world_size) as executor:
            futures = [
                executor.submit(worker_function, rank) for rank in range(world_size)
            ]
            for future in as_completed(futures):
                results.append(future.result())

        # Verify results
        assert len(results) == world_size

        # Each worker should get different data
        all_train_data = []
        all_eval_data = []

        for result in sorted(results, key=lambda x: x["local_rank"]):
            # Each worker should get roughly equal amounts
            assert result["train_count"] == 10
            assert result["eval_count"] == 5

            # Collect all data to verify no overlap
            all_train_data.extend([row["id"] for row in result["train_data"]])
            all_eval_data.extend([row["id"] for row in result["eval_data"]])

        # Verify all data is distributed and no duplicates
        assert len(all_train_data) == 40
        assert len(set(all_train_data)) == 40  # No duplicates
        assert len(all_eval_data) == 20
        assert len(set(all_eval_data)) == 20  # No duplicates

    def test_multiple_registration_attempts(self):
        """Test that only the first registration succeeds (simulates multiple workers trying to register)."""
        world_size = 3

        datasets1 = {"train": ray.data.range(30)}
        datasets2 = {"train": ray.data.range(60)}  # Different dataset

        def attempt_registration(datasets: Dict, worker_id: int) -> bool:
            maybe_start_local_running_data_provider_and_register_dataset(
                world_size, datasets
            )

        # Simulate multiple workers trying to register concurrently
        results = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(attempt_registration, datasets1, 0),
                executor.submit(attempt_registration, datasets2, 1),
                executor.submit(attempt_registration, datasets1, 2),
            ]
            for future in as_completed(futures):
                results.append(future.result())

        shard = get_dataset_shard(0)
        assert "train" in shard

        # The data should be from the first dataset (range 30)
        train_data = list(shard["train"].iter_rows())
        train_count = len(train_data)
        assert train_count == 10  # 30 / 3 workers

    def test_multi_process_simulation(self):
        """Test with actual multiple processes using Ray's testing utilities."""

        # Worker script template - each worker creates/gets actor and tries to register
        worker_script = """
import ray
import time
from ray.train.v2._internal.execution.local_running_utils import maybe_start_local_running_data_provider_and_register_dataset

# Create or get the shared actor (first process creates it, others get it)
world_size = 3
actor_name = "test_data_actor"

try:
    maybe_start_local_running_data_provider_and_register_dataset(world_size, datasets)

# All workers try to register datasets (only first one succeeds)
datasets = {{
    "train": ray.data.range(30),
    "val": ray.data.range(15)
}}
maybe_start_local_running_data_provider_and_register_dataset(world_size, datasets)
print("WORKER_{rank}: Attempted dataset registration")

# Get data shard
local_rank = {rank}
shard = ray.get(actor.get_dataset_shard.remote(local_rank))

# Count data
train_count = len(list(shard["train"].iter_rows()))
val_count = len(list(shard["val"].iter_rows()))

print("WORKER_{rank}_RESULT:" + str(train_count) + "," + str(val_count))
ray.shutdown()
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
        """Test that data is consistently distributed across workers."""
        world_size = 4

        # Create datasets with known data
        train_data = list(range(100))  # 0-99
        val_data = list(range(100, 120))  # 100-119

        datasets = {
            "train": ray.data.from_items(
                [{"id": i, "value": i * 2} for i in train_data]
            ),
            "val": ray.data.from_items([{"id": i, "value": i * 3} for i in val_data]),
        }

        maybe_start_local_running_data_provider_and_register_dataset(
            world_size, datasets
        )

        # Collect all data from all workers
        all_train_ids = set()
        all_val_ids = set()

        for local_rank in range(world_size):
            shard = get_dataset_shard(local_rank)

            train_rows = list(shard["train"].iter_rows())
            val_rows = list(shard["val"].iter_rows())

            train_ids = {row["id"] for row in train_rows}
            val_ids = {row["id"] for row in val_rows}

            # Verify no overlap with previous workers
            assert all_train_ids.isdisjoint(
                train_ids
            ), f"Train data overlap for worker {local_rank}"
            assert all_val_ids.isdisjoint(
                val_ids
            ), f"Val data overlap for worker {local_rank}"

            all_train_ids.update(train_ids)
            all_val_ids.update(val_ids)

        # Verify all data is accounted for
        assert all_train_ids == set(train_data), "Missing or extra train data"
        assert all_val_ids == set(val_data), "Missing or extra val data"


if __name__ == "__main__":
    pytest.main([__file__])
