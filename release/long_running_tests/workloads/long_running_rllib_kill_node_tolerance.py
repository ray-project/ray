import pytest
import sys
from ray.tune.utils.mock import FailureInjectorCallback
from ray.tune.callback import Callback

# This workload tests RLlib's ability to keep iterating when facing a randomly killed
# node

import ray
from ray.tune import run_experiments

ray.init()


class CheckWorkersCallback(Callback):
    def on_step_end(self, iteration, trials, **kwargs):
        raise NotImplementedError
        # TODO: Implement


# Run the workload and pause training everytime a workers goes missing
run_experiments(
    {
        "ppo": {
            "run": "PPO",
            "env": "CartPole-v0",
            "config": {
                "framework": "torch",
                "num_workers": 20,
                "ignore_worker_failures": True,
                "num_failing_workers_tolerance": 20,
                "num_gpus": 0,
            },
        }
    },
    callbacks=[
        FailureInjectorCallback(when_to_fail="once_on_trial_result"),
        CheckWorkersCallback(),
    ],
)

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
