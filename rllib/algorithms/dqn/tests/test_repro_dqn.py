import unittest
import os

import ray

from ray.tune import register_env
import ray.rllib.algorithms.dqn as dqn
from ray.rllib.examples.env.deterministic_envs import create_cartpole_deterministic
from ray.rllib.utils.test_utils import check_reproducibilty


class TestReproDQN(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_reproducibility_dqn_cartpole(self):
        """Tests whether the algorithm is reproducible within 3 iterations
        on discrete env cartpole."""

        register_env("DeterministicCartPole-v0", create_cartpole_deterministic)
        config = dqn.DQNConfig().environment(
            env="DeterministicCartPole-v0", env_config={"seed": 42}
        )
        # tf-gpu is excluded for determinism
        # reason: https://github.com/tensorflow/tensorflow/issues/2732
        # https://github.com/tensorflow/tensorflow/issues/2652
        frameworks = ["torch"]
        if int(os.environ.get("RLLIB_NUM_GPUS", 0)) == 0:
            frameworks.append("tf")

        check_reproducibilty(
            algo_class=dqn.DQN,
            algo_config=config,
            fw_kwargs={"frameworks": frameworks},
            training_iteration=3,
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
