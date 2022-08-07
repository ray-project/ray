import unittest

import os
import ray
from ray.tune import register_env
import ray.rllib.algorithms.dqn as dqn
from ray.rllib.examples.env.deterministic_envs import DeterministicCartPole
from ray.rllib.utils.test_utils import check_reproducibilty


class TestReproPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_reproducibility_dqn_cartpole(self):
        """Tests whether the algorithm is reproducible within 3 iterations
        on discrete env cartpole."""

        register_env(
            "DeterministicCartPole-v0", lambda _: DeterministicCartPole(seed=42)
        )
        config = (
            dqn.DQNConfig()
            # .reporting(
            #     min_sample_timesteps_per_iteration=0,
            #     min_train_timesteps_per_iteration=0,
            #     min_time_s_per_iteration=0,
            # )
            .environment(env="DeterministicCartPole-v0")
        )
        # tf-gpu is excluded for determnism
        # reason: https://github.com/tensorflow/tensorflow/issues/2732
        # https://github.com/tensorflow/tensorflow/issues/2652
        frameworks = ["torch"]
        if int(os.environ.get("RLLIB_NUM_GPUS", 0)) > 0:
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
