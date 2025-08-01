import argparse
import traceback

import ray
from ray.train.v2._internal.execution.local_running_utils import (
    get_dataset_shard,
    maybe_start_local_running_data_provider_and_register_dataset,
)

# Parse command line arguments
parser = argparse.ArgumentParser(description="Test worker process")
parser.add_argument("--rank", type=int, required=True, help="Worker rank")
parser.add_argument("--world-size", type=int, default=3, help="Total number of workers")
args = parser.parse_args()

rank = args.rank
world_size = args.world_size

try:
    # Initialize Ray
    ray.init(address="auto")
    print(f"WORKER_{rank}: Connected to Ray cluster")

    # Create or get the shared actor (first process creates it, others get it)
    actor_name = "shared_data_actor"

    # All workers try to register datasets (only first one succeeds)
    datasets = {"train": ray.data.range(30), "val": ray.data.range(15)}
    maybe_start_local_running_data_provider_and_register_dataset(world_size, datasets)

    # Get data shard
    local_rank = rank
    shard = get_dataset_shard(local_rank)

    train_count = len(list(shard["train"].iter_rows()))
    val_count = len(list(shard["val"].iter_rows()))

    print(f"WORKER_{rank}_RESULT:" + str(train_count) + "," + str(val_count))

except Exception as e:
    print(f"WORKER_{rank}_ERROR:" + str(e))
    traceback.print_exc()
