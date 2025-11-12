import unittest

import numpy as np

import ray
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.examples.envs.classes.random_env import RandomEnv
from ray.rllib.utils.test_utils import check


class TestTimeSteps(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_timesteps(self):
        """Test whether PG can be built with both frameworks."""
        config = (
            ppo.PPOConfig()
            .api_stack(
                enable_env_runner_and_connector_v2=False,
                enable_rl_module_and_learner=False,
            )
            .experimental(_disable_preprocessor_api=True)
            .environment(RandomEnv)
            .env_runners(num_env_runners=0)
            .training(
                model={
                    "fcnet_hiddens": [1],
                    "fcnet_activation": None,
                }
            )
        )

        obs = np.array(1)
        obs_batch = np.array([1])

        algo = config.build()
        policy = algo.get_policy()

        for i in range(1, 21):
            algo.compute_single_action(obs)
            check(int(policy.global_timestep), i)
        for i in range(1, 21):
            policy.compute_actions(obs_batch)
            check(int(policy.global_timestep), i + 20)

        # Artificially set ts to 100Bio, then keep computing actions and
        # train.
        crazy_timesteps = int(1e11)
        policy.on_global_var_update({"timestep": crazy_timesteps})
        # Run for 10 more ts.
        for i in range(1, 11):
            policy.compute_actions(obs_batch)
            check(int(policy.global_timestep), i + crazy_timesteps)
        algo.train()
        algo.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
