import numpy as np
import unittest

import ray.rllib.agents.ddpg.td3 as td3
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check, check_compute_single_action, \
    framework_iterator

tf1, tf, tfv = try_import_tf()


class TestTD3(unittest.TestCase):
    def test_td3_compilation(self):
        """Test whether a TD3Trainer can be built with both frameworks."""
        config = td3.TD3_DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.

        # Test against all frameworks.
        for _ in framework_iterator(config):
            trainer = td3.TD3Trainer(config=config, env="Pendulum-v0")
            num_iterations = 1
            for i in range(num_iterations):
                results = trainer.train()
                print(results)
            check_compute_single_action(trainer)
            trainer.stop()

    def test_td3_exploration_and_with_random_prerun(self):
        """Tests TD3's Exploration (w/ random actions for n timesteps)."""
        config = td3.TD3_DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        obs = np.array([0.0, 0.1, -0.1])

        # Test against all frameworks.
        for _ in framework_iterator(config):
            lcl_config = config.copy()
            # Default GaussianNoise setup.
            trainer = td3.TD3Trainer(config=lcl_config, env="Pendulum-v0")
            # Setting explore=False should always return the same action.
            a_ = trainer.compute_action(obs, explore=False)
            for _ in range(50):
                a = trainer.compute_action(obs, explore=False)
                check(a, a_)
            # explore=None (default: explore) should return different actions.
            actions = []
            for _ in range(50):
                actions.append(trainer.compute_action(obs))
            check(np.std(actions), 0.0, false=True)
            trainer.stop()

            # Check randomness at beginning.
            lcl_config["exploration_config"] = {
                # Act randomly at beginning ...
                "random_timesteps": 30,
                # Then act very closely to deterministic actions thereafter.
                "stddev": 0.001,
                "initial_scale": 0.001,
                "final_scale": 0.001,
            }
            trainer = td3.TD3Trainer(config=lcl_config, env="Pendulum-v0")
            # ts=1 (get a deterministic action as per explore=False).
            deterministic_action = trainer.compute_action(obs, explore=False)
            # ts=2-5 (in random window).
            random_a = []
            for _ in range(29):
                random_a.append(trainer.compute_action(obs, explore=True))
                check(random_a[-1], deterministic_action, false=True)
            self.assertTrue(np.std(random_a) > 0.5)

            # ts > 30 (a=deterministic_action + scale * N[0,1])
            for _ in range(50):
                a = trainer.compute_action(obs, explore=True)
                check(a, deterministic_action, rtol=0.1)

            # ts >> 30 (BUT: explore=False -> expect deterministic action).
            for _ in range(50):
                a = trainer.compute_action(obs, explore=False)
                check(a, deterministic_action)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
