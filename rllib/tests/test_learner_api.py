"""This is a multi-gpu test."""

import unittest

import ray
from ray import air, tune

from ray.rllib.utils.test_utils import framework_iterator
from ray.rllib.algorithms.ppo import PPOConfig

REMOTE_RESOURCE_CONFIGS = {
    "remote-cpu": {"num_learner_workers": 1},
    "remote-gpu": {"num_learner_workers": 1, "num_gpus_per_learner_worker": 1},
    "multi-gpu-ddp": {
        "num_learner_workers": 2, 
        "num_gpus_per_learner_worker": 1,
    },
    "multi-cpu-ddp": {
        "num_learner_workers": 2,
        "num_cpus_per_learner_worker": 2,
    },
}

LOCAL_RESOURCE_CONFIGS = {
    "local-cpu": {},
    "local-gpu": {"num_gpus_per_learner_worker": 0.5},
}


class TestLearnerAPIFromAlgorithm(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_learner_api_remote(self):
        """Tests whether the learner API works when using an algorithm configs."""

        for resource_config in REMOTE_RESOURCE_CONFIGS.values():
            config = (
                PPOConfig()
                .training(_enable_learner_api=True)
                .rl_module(_enable_rl_module_api=True)
                .environment("CartPole-v1")
                .resources(**resource_config)
            )
            
            for fw in framework_iterator(config, frameworks=("torch", "tf2"), with_eager_tracing=True):
                print("Testing with resource config: ", resource_config)
                print("Testing with framework: ", fw)
                print("-" * 80)
                tuner = tune.Tuner(
                    "PPO",
                    param_space=config.to_dict(),
                    run_config=air.RunConfig(stop={"training_iteration": 1})
                )
                results = tuner.fit()
                if not results:
                    raise ValueError("No results returned from tune.")
            



if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
