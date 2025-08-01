#!/usr/bin/env python3
"""
Debug test for multi-process race condition in LocalRunningDataProvider.

Focus on the core issue: multiple processes racing to register datasets
leading to inconsistent data sharding.
"""

import ray
from ray._private.test_utils import run_string_as_driver_nonblocking


def debug_multiprocess_race_condition():
    """Test the exact scenario that's failing: multiple processes with datasets."""

    world_size = 4

    # Worker script that mimics the failing test
    worker_script = """
import ray
import asyncio
from ray.train.v2._internal.execution.local_running_utils import (
    maybe_start_local_running_data_provider_and_register_dataset,
    get_dataset_shard,
    finish_worker_and_wait
)

async def main():
    ray.init(address="auto")

    world_size = 4
    local_rank = {rank}

    print(f"PROCESS_{rank}: Starting...")

    # Create datasets with known data - EACH PROCESS CREATES ITS OWN DATASETS!
    # This might be the problem - different dataset objects in each process
    train_data = list(range(100))  # 0-99
    val_data = list(range(100, 120))  # 100-119

    datasets = {{
        "train": ray.data.from_items(
            [{{"id": i, "value": i * 2}} for i in train_data]
        ),
        "val": ray.data.from_items([{{"id": i, "value": i * 3}} for i in val_data]),
    }}

    print(f"PROCESS_{rank}: Created datasets")

    # Register datasets - THIS IS WHERE THE RACE CONDITION HAPPENS
    print(f"PROCESS_{rank}: Registering datasets...")
    provider_actor = await maybe_start_local_running_data_provider_and_register_dataset(
        world_size, datasets, local_rank
    )
    print(f"PROCESS_{rank}: Registration complete")

    # Get shard for this worker
    print(f"PROCESS_{rank}: Getting shard...")
    shard = await get_dataset_shard(provider_actor, local_rank)
    print(f"PROCESS_{rank}: Got shard")

    # Collect data from this worker's shard
    train_rows = list(shard["train"].iter_rows())
    val_rows = list(shard["val"].iter_rows())

    train_ids = [row["id"] for row in train_rows]
    val_ids = [row["id"] for row in val_rows]

    # Print results for parent process to collect
    print(f"WORKER_{rank}_TRAIN_IDS:" + ",".join(map(str, train_ids)))
    print(f"WORKER_{rank}_VAL_IDS:" + ",".join(map(str, val_ids)))
    print(f"WORKER_{rank}_TRAIN_COUNT:" + str(len(train_ids)))
    print(f"WORKER_{rank}_VAL_COUNT:" + str(len(val_ids)))

    print(f"PROCESS_{rank}: Data collection complete")

    # Mark worker as finished and wait for all workers (if owner)
    await finish_worker_and_wait(provider_actor, local_rank)

    print(f"PROCESS_{rank}: Finished")
    ray.shutdown()

asyncio.run(main())
"""

    print("🔍 Debug: Multi-Process Race Condition Test")
    print(f"Launching {world_size} separate processes...")

    # Launch all worker processes simultaneously
    worker_processes = []
    for rank in range(world_size):
        script = worker_script.format(rank=rank)
        print(f"Starting process {rank}...")
        proc = run_string_as_driver_nonblocking(script)
        worker_processes.append((rank, proc))

    # Collect results from all processes
    all_train_ids = set()
    all_val_ids = set()
    worker_results = {}

    print(f"\nCollecting results from {world_size} processes...")
    for rank, proc in worker_processes:
        print(f"Waiting for process {rank}...")
        out = proc.stdout.read().decode("ascii")
        proc.wait()

        print(f"Process {rank} output:")
        print("=" * 40)
        print(out)
        print("=" * 40)

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

        # Store results
        worker_results[rank] = {
            "train_count": train_count,
            "val_count": val_count,
            "train_ids": sorted(train_ids),
            "val_ids": sorted(val_ids),
        }

        print(f"Worker {rank} summary:")
        print(
            f"  Train: {len(train_ids)} items - {sorted(train_ids)[:10]}{'...' if len(train_ids) > 10 else ''}"
        )
        print(
            f"  Val: {len(val_ids)} items - {sorted(val_ids)[:10]}{'...' if len(val_ids) > 10 else ''}"
        )

        # Check for data overlaps
        train_ids_set = set(train_ids)
        val_ids_set = set(val_ids)

        train_overlap = all_train_ids.intersection(train_ids_set)
        val_overlap = all_val_ids.intersection(val_ids_set)

        if train_overlap:
            print(f"  ❌ TRAIN OVERLAP: {sorted(train_overlap)}")
        if val_overlap:
            print(f"  ❌ VAL OVERLAP: {sorted(val_overlap)}")

        all_train_ids.update(train_ids_set)
        all_val_ids.update(val_ids_set)

    # Final analysis
    expected_train_data = set(range(100))  # 0-99
    expected_val_data = set(range(100, 120))  # 100-119

    missing_train = expected_train_data - all_train_ids
    missing_val = expected_val_data - all_val_ids
    extra_train = all_train_ids - expected_train_data
    extra_val = all_val_ids - expected_val_data

    print("\n🔍 RACE CONDITION ANALYSIS:")
    print("Expected: 100 train items (0-99), 20 val items (100-119)")
    print(f"Collected: {len(all_train_ids)} train items, {len(all_val_ids)} val items")

    if missing_train:
        print(
            f"❌ Missing train IDs: {sorted(missing_train)[:20]}{'...' if len(missing_train) > 20 else ''}"
        )
    if missing_val:
        print(f"❌ Missing val IDs: {sorted(missing_val)}")
    if extra_train:
        print(
            f"❌ Extra train IDs: {sorted(extra_train)[:20]}{'...' if len(extra_train) > 20 else ''}"
        )
    if extra_val:
        print(f"❌ Extra val IDs: {sorted(extra_val)}")

    # Check for equal distribution
    print("\n📊 DISTRIBUTION ANALYSIS:")
    for rank in range(world_size):
        result = worker_results[rank]
        print(
            f"Worker {rank}: {result['train_count']} train, {result['val_count']} val"
        )

    success = (
        len(missing_train) == 0
        and len(missing_val) == 0
        and len(extra_train) == 0
        and len(extra_val) == 0
    )

    print(
        f"\n🏁 RESULT: {'✅ SUCCESS - No race condition detected' if success else '❌ RACE CONDITION CONFIRMED'}"
    )

    if not success:
        print(
            "\n💡 HYPOTHESIS: Multiple processes are creating different dataset instances"
        )
        print(
            "   and the actor registration is not properly handling concurrent registrations."
        )

    return success


if __name__ == "__main__":
    # Initialize Ray for the test coordinator
    ray.init(num_cpus=8)

    try:
        debug_multiprocess_race_condition()
    finally:
        ray.shutdown()
