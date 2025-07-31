import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict

import pytest

import ray
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray.train.v2._internal.execution.local_running_utils import (
    GlobalLocalTrainerRayDataset,
)


class TestGlobalLocalTrainerRayDataset:
    """Test suite for GlobalLocalTrainerRayDataset actor with multiple processes/workers."""

    @pytest.fixture(autouse=True)
    def setup_ray(self):
        """Setup Ray cluster for testing."""
        if not ray.is_initialized():
            ray.init(num_cpus=4)
        yield
        # Don't shutdown Ray here to allow sharing between tests

    def test_single_process_basic_functionality(self):
        """Test basic functionality in single process."""
        world_size = 3
        actor = GlobalLocalTrainerRayDataset.remote(world_size)

        # Create test datasets
        datasets = {
            "train": ray.data.range(30),  # 10 items per worker
            "val": ray.data.range(15),  # 5 items per worker
        }

        # Register datasets
        ray.get(actor.register_dataset.remote(datasets))

        # Test getting shards for each worker
        for local_rank in range(world_size):
            shard = ray.get(actor.get_dataset_shard.remote(local_rank))

            # Verify we get the expected datasets
            assert "train" in shard
            assert "val" in shard

            # Verify shard sizes (approximately equal distribution)
            train_count = len(list(shard["train"].iter_rows()))
            val_count = len(list(shard["val"].iter_rows()))

            # Each worker should get roughly equal portions
            assert train_count == 10  # 30 / 3 workers
            assert val_count == 5  # 15 / 3 workers

    def test_invalid_operations(self):
        """Test error conditions and edge cases."""
        world_size = 2
        actor = GlobalLocalTrainerRayDataset.remote(world_size)

        # Test getting shard before registration should fail
        with pytest.raises(AssertionError, match="Must call register_dataset"):
            ray.get(actor.get_dataset_shard.remote(0))

        # Register datasets
        datasets = {"train": ray.data.range(10)}
        ray.get(actor.register_dataset.remote(datasets))

        # Test invalid local_rank
        with pytest.raises(AssertionError, match="local_rank .* must be < world_size"):
            ray.get(actor.get_dataset_shard.remote(world_size))  # >= world_size

        with pytest.raises(AssertionError, match="local_rank .* must be < world_size"):
            ray.get(actor.get_dataset_shard.remote(-1))  # negative rank

    def test_multi_threading_concurrent_access(self):
        """Test concurrent access from multiple threads (simulates multi-worker scenario)."""
        world_size = 4
        actor = GlobalLocalTrainerRayDataset.remote(world_size)

        # Create test datasets
        datasets = {
            "train": ray.data.range(40),  # 10 items per worker
            "eval": ray.data.range(20),  # 5 items per worker
        }

        # Register datasets
        ray.get(actor.register_dataset.remote(datasets))

        def worker_function(local_rank: int) -> Dict:
            """Simulate a worker requesting its data shard."""
            shard = ray.get(actor.get_dataset_shard.remote(local_rank))

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
        actor = GlobalLocalTrainerRayDataset.remote(world_size)

        datasets1 = {"train": ray.data.range(30)}
        datasets2 = {"train": ray.data.range(60)}  # Different dataset

        def attempt_registration(datasets: Dict, worker_id: int) -> bool:
            """Attempt to register datasets and return success status."""
            try:
                ray.get(actor.register_dataset.remote(datasets))
                return True
            except Exception:
                return False

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

        # With the bug fixed, only the first registration should succeed
        # All registration attempts return successfully, but only the first one actually registers
        successful_registrations = sum(results)
        assert successful_registrations == 3  # All attempts succeed (no exceptions)

        # But only the first registration actually took effect
        # Verify we can get shards successfully
        shard = ray.get(actor.get_dataset_shard.remote(0))
        assert "train" in shard

        # The data should be from the first dataset (range 30)
        train_data = list(shard["train"].iter_rows())
        train_count = len(train_data)
        assert train_count == 10  # 30 / 3 workers

    def test_multi_process_simulation(self):
        """Test with actual multiple processes using Ray's testing utilities."""
        # Create a temporary file to store the actor name for sharing between processes
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            actor_name_file = f.name

        try:
            # Driver script that creates the actor and registers datasets
            setup_script = f"""
import ray
import tempfile
from ray.train.v2._internal.execution.local_running_utils import GlobalLocalTrainerRayDataset

ray.init(address="auto")

# Create the actor with a known name
world_size = 3
actor = GlobalLocalTrainerRayDataset.options(name="test_data_actor").remote(world_size)

# Create and register test datasets
datasets = {{
    "train": ray.data.range(30),
    "val": ray.data.range(15)
}}

ray.get(actor.register_dataset.remote(datasets))

# Write completion signal
with open("{actor_name_file}", "w") as f:
    f.write("ready")

print("SETUP_COMPLETE")
ray.shutdown()
"""

            # Worker script that connects to actor and gets its shard
            worker_script_template = f"""
import ray
import time
from ray.train.v2._internal.execution.local_running_utils import GlobalLocalTrainerRayDataset

ray.init(address="auto")

# Wait for setup to complete
while True:
    try:
        with open("{actor_name_file}", "r") as f:
            if f.read().strip() == "ready":
                break
    except FileNotFoundError:
        pass
    time.sleep(0.1)

# Get the actor by name
actor = ray.get_actor("test_data_actor")

# Get shard for this worker
local_rank = {{local_rank}}
shard = ray.get(actor.get_dataset_shard.remote(local_rank))

# Verify and print results
train_count = len(list(shard["train"].iter_rows()))
val_count = len(list(shard["val"].iter_rows()))

print(f"WORKER_{{local_rank}}_RESULT:{{train_count}},{{val_count}}")
ray.shutdown()
"""

            # Run setup process
            setup_proc = run_string_as_driver_nonblocking(setup_script)
            setup_out = setup_proc.stdout.read().decode("ascii")
            setup_proc.wait()
            assert "SETUP_COMPLETE" in setup_out
            assert setup_proc.returncode == 0

            # Run worker processes
            worker_processes = []
            for rank in range(3):
                worker_script = worker_script_template.format(local_rank=rank)
                proc = run_string_as_driver_nonblocking(worker_script)
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
                        results[rank] = {
                            "train_count": train_count,
                            "val_count": val_count,
                        }

            # Verify each worker got correct shard sizes
            assert len(results) == 3
            for rank in range(3):
                assert results[rank]["train_count"] == 10  # 30 / 3
                assert results[rank]["val_count"] == 5  # 15 / 3

        finally:
            # Cleanup
            try:
                os.unlink(actor_name_file)
            except FileNotFoundError:
                pass

    def test_data_consistency_across_workers(self):
        """Test that data is consistently distributed across workers."""
        world_size = 4
        actor = GlobalLocalTrainerRayDataset.remote(world_size)

        # Create datasets with known data
        train_data = list(range(100))  # 0-99
        val_data = list(range(100, 120))  # 100-119

        datasets = {
            "train": ray.data.from_items(
                [{"id": i, "value": i * 2} for i in train_data]
            ),
            "val": ray.data.from_items([{"id": i, "value": i * 3} for i in val_data]),
        }

        ray.get(actor.register_dataset.remote(datasets))

        # Collect all data from all workers
        all_train_ids = set()
        all_val_ids = set()

        for local_rank in range(world_size):
            shard = ray.get(actor.get_dataset_shard.remote(local_rank))

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
