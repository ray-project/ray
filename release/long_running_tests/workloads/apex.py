# This workload tests running APEX

import ray
from ray.tune import run_experiments
from ray.tune.utils.release_test_util import ProgressCallback

object_store_memory = 10**9
num_nodes = 3

message = (
    "Make sure there is enough memory on this machine to run this "
    "workload. We divide the system memory by 2 to provide a buffer."
)
assert (
    num_nodes * object_store_memory < ray._private.utils.get_system_memory() / 2
), message

# Simulate a cluster on one machine.

ray.init()

# Run the workload.

run_experiments(
    {
        "apex": {
            "run": "APEX",
            "env": "ale_py:ALE/Pong-v5",
            "config": {
                "num_workers": 3,
                "num_gpus": 0,
                "replay_buffer_config": {
                    "capacity": 10000,
                },
                "num_steps_sampled_before_learning_starts": 0,
                "rollout_fragment_length": "auto",
                "train_batch_size": 1,
                "min_time_s_per_iteration": 10,
                "min_sample_timesteps_per_iteration": 10,
            },
            "storage_path": "/mnt/cluster_storage",
        }
    },
    callbacks=[ProgressCallback()],
)
