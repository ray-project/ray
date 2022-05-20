from copy import deepcopy
import numpy as np
import unittest

import ray
import ray.rllib.algorithms.dqn as dqn
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)


class TestDQN(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_dqn_compilation(self):
        """Test whether a DQNTrainer can be built on all frameworks."""
        num_iterations = 1
        config = dqn.dqn.DQNConfig().rollouts(num_rollout_workers=2)

        for _ in framework_iterator(config, with_eager_tracing=True):
            # Double-dueling DQN.
            print("Double-dueling")
            plain_config = deepcopy(config)
            trainer = dqn.DQNTrainer(config=plain_config, env="CartPole-v0")
            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                print(results)

            check_compute_single_action(trainer)
            trainer.stop()

            # Rainbow.
            print("Rainbow")
            rainbow_config = deepcopy(config).training(
                num_atoms=10, noisy=True, double_q=True, dueling=True, n_step=5
            )
            trainer = dqn.DQNTrainer(config=rainbow_config, env="CartPole-v0")
            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                print(results)

            check_compute_single_action(trainer)

            trainer.stop()

    def test_dqn_exploration_and_soft_q_config(self):
        """Tests, whether a DQN Agent outputs exploration/softmaxed actions."""
        config = (
            dqn.dqn.DQNConfig()
            .rollouts(num_rollout_workers=0)
            .environment(env_config={"is_slippery": False, "map_name": "4x4"})
        )
        obs = np.array(0)

        # Test against all frameworks.
        for _ in framework_iterator(config):
            # Default EpsilonGreedy setup.
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v1")
            # Setting explore=False should always return the same action.
            a_ = trainer.compute_single_action(obs, explore=False)
            for _ in range(50):
                a = trainer.compute_single_action(obs, explore=False)
                check(a, a_)
            # explore=None (default: explore) should return different actions.
            actions = []
            for _ in range(50):
                actions.append(trainer.compute_single_action(obs))
            check(np.std(actions), 0.0, false=True)
            trainer.stop()

            # Low softmax temperature. Behaves like argmax
            # (but no epsilon exploration).
            config.exploration(
                exploration_config={"type": "SoftQ", "temperature": 0.000001}
            )
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v1")
            # Due to the low temp, always expect the same action.
            actions = [trainer.compute_single_action(obs)]
            for _ in range(50):
                actions.append(trainer.compute_single_action(obs))
            check(np.std(actions), 0.0, decimals=3)
            trainer.stop()

            # Higher softmax temperature.
            config.exploration_config["temperature"] = 1.0
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v1")

            # Even with the higher temperature, if we set explore=False, we
            # should expect the same actions always.
            a_ = trainer.compute_single_action(obs, explore=False)
            for _ in range(50):
                a = trainer.compute_single_action(obs, explore=False)
                check(a, a_)

            # Due to the higher temp, expect different actions avg'ing
            # around 1.5.
            actions = []
            for _ in range(300):
                actions.append(trainer.compute_single_action(obs))
            check(np.std(actions), 0.0, false=True)
            trainer.stop()

            # With Random exploration.
            config.exploration(exploration_config={"type": "Random"}, explore=True)
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v1")
            actions = []
            for _ in range(300):
                actions.append(trainer.compute_single_action(obs))
            check(np.std(actions), 0.0, false=True)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
