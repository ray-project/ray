import numpy as np
import unittest

import ray.rllib.agents.dqn as dqn
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check, framework_iterator

tf = try_import_tf()


class TestDQN(unittest.TestCase):

    def test_dqn_exploration_and_soft_q_config(self):
        """Tests, whether a DQN Agent outputs exploration/softmaxed actions."""
        config = dqn.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["env_config"] = {"is_slippery": False, "map_name": "4x4"}
        obs = np.array(0)

        # Test against all frameworks.
        for _ in framework_iterator(config):
            # Default EpsilonGreedy setup.
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
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

            # Low softmax temperature. Behaves like argmax
            # (but no epsilon exploration).
            config["exploration_config"] = {
                "type": "SoftQ",
                "temperature": 0.000001
            }
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
            # Due to the low temp, always expect the same action.
            actions = [trainer.compute_action(obs)]
            for _ in range(50):
                actions.append(trainer.compute_action(obs))
            check(np.std(actions), 0.0, decimals=3)

            # Higher softmax temperature.
            config["exploration_config"]["temperature"] = 1.0
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")

            # Even with the higher temperature, if we set explore=False, we
            # should expect the same actions always.
            a_ = trainer.compute_action(obs, explore=False)
            for _ in range(50):
                a = trainer.compute_action(obs, explore=False)
                check(a, a_)

            # Due to the higher temp, expect different actions avg'ing
            # around 1.5.
            actions = []
            for _ in range(300):
                actions.append(trainer.compute_action(obs))
            check(np.std(actions), 0.0, false=True)

            # With Random exploration.
            config["exploration_config"] = {"type": "Random"}
            config["explore"] = True
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
            actions = []
            for _ in range(300):
                actions.append(trainer.compute_action(obs))
            check(np.std(actions), 0.0, false=True)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
