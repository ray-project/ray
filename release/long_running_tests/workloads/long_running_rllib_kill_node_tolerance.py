import pytest
import sys
from ray.tune.utils.mock import FailureInjectorCallback

# This workload tests RLlib's ability to keep iterating when facing a randomly killed
# node

import ray
from ray.tune import run_experiments
from ray.tune.utils.release_test_util import ProgressCallback

ray.init()

# Run the workload and pause training without failing while a node is failing
run_experiments(
    {
        "ppo": {
            "run": "PPO",
            "env": "CartPole-v0",
            "config": {
                "num_workers": 3,
                "ignore_worker_failures": True,
                "num_failing_workers_tolerance": 0,
                "num_gpus": 0,
            },
            "framework": "torch",
        }
    },
    callbacks=[
        FailureInjectorCallback(when_to_fail="once_on_trial_result"),
        ProgressCallback(),
    ],
)

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
