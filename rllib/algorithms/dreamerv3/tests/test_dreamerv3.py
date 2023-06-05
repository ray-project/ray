import unittest

import gymnasium as gym
import numpy as np

import ray
from ray.rllib.algorithms.dreamerv3 import dreamerv3
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.test_utils import framework_iterator


class TestDreamerV3(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_dreamerv3_compilation(self):
        """Test whether DreamerV3 can be built with all frameworks."""

        # Build a DreamerV3Config object.
        config = (
            dreamerv3.DreamerV3Config()
            .framework(eager_tracing=True)
            .training(
                # TODO (sven): Fix having to provide this.
                #  Should be compiled as `RLModuleConfig` by AlgorithmConfig?
                model={
                    "batch_size_B": 16,
                    "batch_length_T": 64,
                    "horizon_H": 15,
                    "model_size": "nano",  # Use a tiny model for testing.
                    "gamma": 0.997,
                    "training_ratio": 512,
                    "symlog_obs": True,
                },
                _enable_learner_api=True,
            )
            .resources(
                num_learner_workers=0,
                num_cpus_per_learner_worker=1,
                num_gpus_per_learner_worker=0,
                num_gpus=0,
            )
            .debugging(log_level="info")
            .rl_module(_enable_rl_module_api=True)
        )

        num_iterations = 2

        for _ in framework_iterator(config, frameworks="tf2"):
            for env in ["FrozenLake-v1", "CartPole-v1", "ALE/MsPacman-v5"]:
                print("Env={}".format(env))
                config.environment(env)
                algo = config.build()

                for i in range(num_iterations):
                    results = algo.train()
                    print(results)

                algo.stop()

    def test_dreamerv3_dreamer_model_sizes(self):
        """Tests, whether the different model sizes match the ones reported in [1]."""

        config = (
            dreamerv3.DreamerV3Config()
            .environment(
                observation_space=gym.spaces.Box(-1.0, 0.0, (64, 64, 3), np.float32),
                action_space=gym.spaces.Discrete(6),
            )
            .framework("tf2", eager_tracing=False)
            .training(
                model={
                    "batch_size_B": 16,
                    "batch_length_T": 64,
                    "horizon_H": 15,
                    "model_size": "XS",
                    "gamma": 0.997,
                    "training_ratio": 512,
                    "symlog_obs": True,
                }
            )
        )
        # Create our RLModule to compute actions with.
        policy_dict, _ = config.get_multi_agent_setup()
        module_spec = config.get_marl_module_spec(policy_dict=policy_dict)
        rl_module = module_spec.build()[DEFAULT_POLICY_ID]
        # Count the generated RLModule's parameters and compare to the paper's reported
        # numbers.


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
