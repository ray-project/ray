import numpy as np
import unittest

import ray.rllib.agents.ddpg as ddpg
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check

tf = try_import_tf()


class TestDDPG(unittest.TestCase):
    def test_ddpg_compilation(self):
        """Test whether a DDPGTrainer can be built with both frameworks."""
        config = ddpg.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.

        # Test against all frameworks.
        for fw in ["tf", "eager", "torch"]:
            if fw != "tf":
                continue
            config["eager"] = True if fw == "eager" else False
            config["use_pytorch"] = True if fw == "torch" else False
            trainer = ddpg.DDPGTrainer(config=config, env="Pendulum-v0")
            num_iterations = 2
            for i in range(num_iterations):
                results = trainer.train()
                print(results)

    def test_ddpg_exploration_and_with_random_prerun(self):
        """Tests DDPG's Exploration (w/ random actions for n timesteps)."""
        config = ddpg.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        obs = np.array([0.0, 0.1, -0.1])

        # Test against all frameworks.
        for fw in ["tf", "eager", "torch"]:
            if fw != "tf":
                continue
            config["eager"] = True if fw == "eager" else False
            config["use_pytorch"] = True if fw == "torch" else False

            # Default OUNoise setup.
            trainer = ddpg.DDPGTrainer(config=config, env="Pendulum-v0")
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

            # Check randomness at beginning.
            config["exploration_config"] = {
                # Act randomly at beginning ...
                "random_timesteps": 50,
                # Then act very closely to deterministic actions thereafter.
                "ou_base_scale": 0.001,
                "initial_scale": 0.001,
                "final_scale": 0.001,
            }
            trainer = ddpg.DDPGTrainer(config=config, env="Pendulum-v0")
            # ts=1 (get a deterministic action as per explore=False).
            deterministic_action = trainer.compute_action(obs, explore=False)
            # ts=2-5 (in random window).
            random_a = []
            for _ in range(49):
                random_a.append(trainer.compute_action(obs, explore=True))
                check(random_a[-1], deterministic_action, false=True)
            self.assertTrue(np.std(random_a) > 0.5)

            # ts > 50 (a=deterministic_action + scale * N[0,1])
            for _ in range(50):
                a = trainer.compute_action(obs, explore=True)
                check(a, deterministic_action, rtol=0.1)

            # ts >> 50 (BUT: explore=False -> expect deterministic action).
            for _ in range(50):
                a = trainer.compute_action(obs, explore=False)
                check(a, deterministic_action)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
