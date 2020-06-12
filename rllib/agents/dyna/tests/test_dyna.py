import copy
import gym
import numpy as np
import unittest

import ray
import ray.rllib.agents.dyna as dyna
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_compute_action, framework_iterator

tf = try_import_tf()


class TestDYNA(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_dyna_compilation(self):
        """Test whether a DYNATrainer can be built with both frameworks."""
        config = copy.deepcopy(dyna.DEFAULT_CONFIG)
        config["num_workers"] = 1
        num_iterations = 10
        env = "CartPole-v0"
        test_env = gym.make(env)

        for _ in framework_iterator(config, frameworks="torch"):
            trainer = dyna.DYNATrainer(config=config, env=env)
            policy = trainer.get_policy()
            for i in range(num_iterations):
                trainer.train()
            check_compute_action(trainer)

            # Check, whether env dynamics were actually learnt more or less.
            obs = test_env.reset()
            for _ in range(10):
                action = trainer.compute_action(obs)
                # Make the prediction:
                predicted_next_obs_delta = policy.dynamics_model.get_next_obs(
                    obs, action)
                predicted_next_obs = obs + predicted_next_obs_delta
                next_obs, _, done, _ = test_env.step(action)
                self.assertLess(np.sum(next_obs - predicted_next_obs), 0.1)
                # Reset if done.
                if done:
                    obs = test_env.reset()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
