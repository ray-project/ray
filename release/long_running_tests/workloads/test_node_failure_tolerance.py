import pytest
import sys
from ray.tune.utils.mock import FailureInjectorCallback

# This workload tests RLlib's ability to recover from failing workers nodes

import ray
from ray.tune import run_experiments
from ray.tune.utils.release_test_util import ProgressCallback

ray.init()

# Run the workload.
run_experiments(
    {
        "ppo": {
            "run": "PPO",
            "env": "CartPole-v0",
            "config": {
                "num_workers": 3,
                "worker_failure_tolerance": 3,
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
