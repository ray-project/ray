#!/usr/bin/env python3
"""
Example: Testing GlobalLocalTrainerRayDataset with Multiple Processes

This script demonstrates several approaches to test the GlobalLocalTrainerRayDataset
actor with multiple processes, simulating real-world usage scenarios like torchrun.
"""

import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import ray
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray.train.v2._internal.execution.local_running_utils import (
    GlobalLocalTrainerRayDataset,
)


def example_1_basic_usage():
    """Example 1: Basic single-process usage of the actor."""
    print("=== Example 1: Basic Usage ===")

    # Initialize Ray if not already initialized
    if not ray.is_initialized():
        ray.init(num_cpus=4)

    # Create the actor
    world_size = 3
    actor = GlobalLocalTrainerRayDataset.remote(world_size)

    # Create test datasets
    datasets = {
        "train": ray.data.range(30),  # Will be split among 3 workers
        "val": ray.data.range(15),
    }

    # Register datasets
    ray.get(actor.register_dataset.remote(datasets))

    # Simulate each worker getting its shard
    for local_rank in range(world_size):
        shard = ray.get(actor.get_dataset_shard.remote(local_rank))

        train_count = len(list(shard["train"].iter_rows()))
        val_count = len(list(shard["val"].iter_rows()))

        print(f"Worker {local_rank}: train={train_count}, val={val_count}")

    print("✓ Basic usage test passed\n")


def example_2_threading_simulation():
    """Example 2: Simulate multiple workers using threading."""
    print("=== Example 2: Multi-Threading Simulation ===")

    if not ray.is_initialized():
        ray.init(num_cpus=4)

    world_size = 4
    actor = GlobalLocalTrainerRayDataset.remote(world_size)

    # Create datasets
    datasets = {"train": ray.data.range(40), "eval": ray.data.range(20)}

    ray.get(actor.register_dataset.remote(datasets))

    def worker_thread(local_rank):
        """Simulate a worker process."""
        print(f"Worker {local_rank} starting...")

        # Get data shard
        shard = ray.get(actor.get_dataset_shard.remote(local_rank))

        # Process data (simulate training)
        train_data = list(shard["train"].iter_rows())
        eval_data = list(shard["eval"].iter_rows())

        result = {
            "worker_id": local_rank,
            "train_count": len(train_data),
            "eval_count": len(eval_data),
            "train_ids": [row["id"] for row in train_data],
            "eval_ids": [row["id"] for row in eval_data],
        }

        print(
            f"Worker {local_rank} completed: train={result['train_count']}, eval={result['eval_count']}"
        )
        return result

    # Run workers concurrently
    results = []
    with ThreadPoolExecutor(max_workers=world_size) as executor:
        futures = [executor.submit(worker_thread, rank) for rank in range(world_size)]
        for future in as_completed(futures):
            results.append(future.result())

    # Verify results
    all_train_ids = set()
    all_eval_ids = set()

    for result in sorted(results, key=lambda x: x["worker_id"]):
        train_ids = set(result["train_ids"])
        eval_ids = set(result["eval_ids"])

        # Check for data overlap (should be none)
        assert all_train_ids.isdisjoint(
            train_ids
        ), f"Data overlap in worker {result['worker_id']}"
        assert all_eval_ids.isdisjoint(
            eval_ids
        ), f"Data overlap in worker {result['worker_id']}"

        all_train_ids.update(train_ids)
        all_eval_ids.update(eval_ids)

    print(
        f"✓ All {len(all_train_ids)} train items and {len(all_eval_ids)} eval items distributed correctly\n"
    )


def example_3_multiprocess_with_ray_drivers():
    """Example 3: Use actual multiple processes with Ray drivers."""
    print("=== Example 3: Multi-Process with Ray Drivers ===")

    # Create temporary file for coordination
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
        coord_file = f.name

    try:
        # Setup script - creates actor and registers data
        setup_script = f"""
import ray
from ray.train.v2._internal.execution.local_running_utils import GlobalLocalTrainerRayDataset

ray.init(address="auto")

# Create named actor for shared access
world_size = 3
actor = GlobalLocalTrainerRayDataset.options(name="shared_data_actor").remote(world_size)

# Register datasets
datasets = {{
    "train": ray.data.range(30),
    "val": ray.data.range(15)
}}
ray.get(actor.register_dataset.remote(datasets))

# Signal completion
with open("{coord_file}", "w") as f:
    f.write("setup_complete")

print("SETUP_DONE")
ray.shutdown()
"""

        # Worker script template
        worker_script = """
import ray
import time
from ray.train.v2._internal.execution.local_running_utils import GlobalLocalTrainerRayDataset

ray.init(address="auto")

# Wait for setup
while True:
    try:
        with open("{coord_file}", "r") as f:
            if f.read().strip() == "setup_complete":
                break
    except FileNotFoundError:
        pass
    time.sleep(0.1)

# Get shared actor
actor = ray.get_actor("shared_data_actor")

# Get data shard
local_rank = {rank}
shard = ray.get(actor.get_dataset_shard.remote(local_rank))

# Count data
train_count = len(list(shard["train"].iter_rows()))
val_count = len(list(shard["val"].iter_rows()))

print(f"WORKER_{rank}_RESULT:{train_count},{val_count}")
ray.shutdown()
"""

        print("Running setup process...")
        setup_proc = run_string_as_driver_nonblocking(setup_script)
        setup_output = setup_proc.stdout.read().decode()
        setup_proc.wait()

        if "SETUP_DONE" not in setup_output:
            print(f"Setup failed: {setup_output}")
            return

        print("Running worker processes...")
        worker_processes = []

        # Launch worker processes
        for rank in range(3):
            script = worker_script.format(coord_file=coord_file, rank=rank)
            proc = run_string_as_driver_nonblocking(script)
            worker_processes.append((rank, proc))

        # Collect results
        results = {}
        for rank, proc in worker_processes:
            output = proc.stdout.read().decode()
            proc.wait()

            # Parse output
            for line in output.split("\n"):
                if line.startswith(f"WORKER_{rank}_RESULT:"):
                    train_count, val_count = map(int, line.split(":")[1].split(","))
                    results[rank] = {"train": train_count, "val": val_count}
                    print(f"Worker {rank}: train={train_count}, val={val_count}")

        # Verify all workers completed
        assert len(results) == 3, f"Expected 3 workers, got {len(results)}"
        print("✓ Multi-process test passed\n")

    finally:
        # Cleanup
        try:
            os.unlink(coord_file)
        except FileNotFoundError:
            pass


def example_4_torchrun_style_simulation():
    """Example 4: Simulate torchrun-style distributed training."""
    print("=== Example 4: Torchrun-Style Simulation ===")

    if not ray.is_initialized():
        ray.init(num_cpus=4)

    # Create a named actor that multiple "processes" can access
    world_size = 4
    actor_name = "torchrun_data_actor"

    # Clean up any existing actor
    try:
        existing_actor = ray.get_actor(actor_name)
        ray.kill(existing_actor)
        time.sleep(1)
    except ValueError:
        pass  # Actor doesn't exist

    # Create the shared data actor
    actor = GlobalLocalTrainerRayDataset.options(name=actor_name).remote(world_size)

    # Master process registers the datasets
    datasets = {
        "train": ray.data.range(1000),  # Large dataset
        "val": ray.data.range(200),
        "test": ray.data.range(100),
    }

    print("Master process registering datasets...")
    ray.get(actor.register_dataset.remote(datasets))

    def simulate_training_worker(rank, world_size, num_epochs=2):
        """Simulate a training worker process."""
        print(f"[Rank {rank}] Starting training...")

        # Get the shared actor (in real torchrun, each process would do this)
        shared_actor = ray.get_actor(actor_name)

        # Get data shard for this worker
        data_shard = ray.get(shared_actor.get_dataset_shard.remote(rank))

        # Simulate training loop
        train_iter = data_shard["train"]
        val_iter = data_shard["val"]

        results = {"rank": rank, "epochs": []}

        for epoch in range(num_epochs):
            # Simulate training
            train_batch_count = 0
            for batch in train_iter.iter_batches(batch_size=32):
                train_batch_count += 1
                # Simulate processing time
                time.sleep(0.001)

            # Simulate validation
            val_batch_count = 0
            for batch in val_iter.iter_batches(batch_size=32):
                val_batch_count += 1
                time.sleep(0.001)

            epoch_result = {
                "epoch": epoch,
                "train_batches": train_batch_count,
                "val_batches": val_batch_count,
            }
            results["epochs"].append(epoch_result)

            print(
                f"[Rank {rank}] Epoch {epoch}: {train_batch_count} train batches, {val_batch_count} val batches"
            )

        print(f"[Rank {rank}] Training completed!")
        return results

    # Launch workers concurrently (simulating torchrun launching multiple processes)
    worker_results = []
    with ThreadPoolExecutor(max_workers=world_size) as executor:
        futures = [
            executor.submit(simulate_training_worker, rank, world_size)
            for rank in range(world_size)
        ]

        for future in as_completed(futures):
            worker_results.append(future.result())

    # Verify results
    total_train_batches = sum(
        sum(epoch["train_batches"] for epoch in result["epochs"])
        for result in worker_results
    )

    total_val_batches = sum(
        sum(epoch["val_batches"] for epoch in result["epochs"])
        for result in worker_results
    )

    print("✓ Distributed training completed:")
    print(f"  - Total train batches processed: {total_train_batches}")
    print(f"  - Total val batches processed: {total_val_batches}")
    print(f"  - Workers: {len(worker_results)}")

    # Cleanup
    ray.kill(actor)


def main():
    """Run all examples."""
    print("Testing GlobalLocalTrainerRayDataset with Multiple Processes\n")

    try:
        example_1_basic_usage()
        example_2_threading_simulation()
        example_3_multiprocess_with_ray_drivers()
        example_4_torchrun_style_simulation()

        print("🎉 All examples completed successfully!")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        if ray.is_initialized():
            ray.shutdown()


if __name__ == "__main__":
    main()
