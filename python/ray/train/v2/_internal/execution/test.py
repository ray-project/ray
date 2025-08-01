import ray
import time
import asyncio
import argparse
from ray.train.v2._internal.execution.local_running_utils import (
    maybe_start_local_running_data_provider_and_register_dataset, 
    get_dataset_shard,
    mark_worker_finished,
    wait_for_all_workers_to_finish
)

ray.init(address="auto")
# Parse command line arguments
parser = argparse.ArgumentParser(description='Test script for local running data provider')
parser.add_argument('rank', type=int, help='Rank of the worker')
args = parser.parse_args()


world_size = 3
local_rank = args.rank

# Create datasets with known data
train_data = list(range(100))  # 0-99
val_data = list(range(100, 120))  # 100-119

datasets = {
    "train": ray.data.from_items(
        [{"id": i, "value": i * 2} for i in train_data]
    ),
    "val": ray.data.from_items([{"id": i, "value": i * 3} for i in val_data]),
}

async def main():
    # Register datasets (only first worker succeeds, others get existing actor)
    provider_actor = await maybe_start_local_running_data_provider_and_register_dataset(
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
    print(f"WORKER_{local_rank}_TRAIN_IDS:" + ",".join(map(str, train_ids)))
    print(f"WORKER_{local_rank}_VAL_IDS:" + ",".join(map(str, val_ids)))
    print(f"WORKER_{local_rank}_TRAIN_COUNT:" + str(len(train_ids)))
    print(f"WORKER_{local_rank}_VAL_COUNT:" + str(len(val_ids)))

    # Mark this worker as finished
    is_owner = await mark_worker_finished(provider_actor, local_rank)
    if is_owner:
        # Wait for all workers to finish before shutting down
        await wait_for_all_workers_to_finish(provider_actor, local_rank)

# Run the async main function
asyncio.run(main())

ray.shutdown()