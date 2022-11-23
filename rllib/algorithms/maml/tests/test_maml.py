from gym.wrappers import TimeLimit
import unittest

import ray
import ray.rllib.algorithms.maml as maml
from ray.rllib.examples.env.cartpole_mass import CartPoleMassEnv
from ray.rllib.examples.env.pendulum_mass import PendulumMassEnv
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)
from ray.tune.registry import register_env


class TestMAML(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()
        register_env(
            "cartpole",
            lambda env_cfg: TimeLimit(CartPoleMassEnv(), max_episode_steps=200),
        )
        register_env(
            "pendulum",
            lambda env_cfg: TimeLimit(PendulumMassEnv(), max_episode_steps=200),
        )

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_maml_compilation(self):
        """Test whether MAML can be built with all frameworks."""
        config = maml.MAMLConfig().rollouts(num_rollout_workers=1)

        num_iterations = 1

        # Test for tf framework (torch not implemented yet).
        for fw in framework_iterator(config, frameworks=("tf", "torch")):
            for env in ["cartpole", "pendulum"]:
                if fw == "tf" and env.startswith("cartpole"):
                    continue
                print("env={}".format(env))
                config.environment(env)
                algo = config.build()
                for i in range(num_iterations):
                    results = algo.train()
                    check_train_results(results)
                    print(results)
                check_compute_single_action(algo, include_prev_action_reward=True)
                algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
