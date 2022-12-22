from copy import deepcopy
import numpy as np
import os
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
        """Test whether DQN can be built on all frameworks."""
        num_iterations = 1
        config = (
            dqn.dqn.DQNConfig()
            .resources(
                # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
                num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0"))
            )
            .environment("CartPole-v1")
            .rollouts(num_rollout_workers=2)
            .training(num_steps_sampled_before_learning_starts=0)
        )

        for _ in framework_iterator(config, with_eager_tracing=True):
            # Double-dueling DQN.
            print("Double-dueling")
            trainer = config.build()
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
            trainer = rainbow_config.build()
            for i in range(num_iterations):
                results = trainer.train()
                check_train_results(results)
                print(results)

            check_compute_single_action(trainer)

            trainer.stop()

    def test_dqn_compilation_integer_rewards(self):
        """Test whether DQN can be built on all frameworks.
        Unlike the previous test, this uses an environment with integer rewards
        in order to test that type conversions are working correctly."""
        num_iterations = 1
        config = (
            dqn.dqn.DQNConfig()
            .resources(
                # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
                num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0"))
            )
            .environment("Taxi-v3")
            .rollouts(num_rollout_workers=2)
            .training(num_steps_sampled_before_learning_starts=0)
        )

        for _ in framework_iterator(config, with_eager_tracing=True):
            # Double-dueling DQN.
            print("Double-dueling")
            trainer = config.build()
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
            trainer = rainbow_config.build()
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
            .resources(
                # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
                num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0"))
            )
            .environment("FrozenLake-v1")
            .rollouts(num_rollout_workers=0)
            .environment(env_config={"is_slippery": False, "map_name": "4x4"})
            .training(num_steps_sampled_before_learning_starts=0)
        )

        obs = np.array(0)

        # Test against all frameworks.
        for _ in framework_iterator(config):
            # Default EpsilonGreedy setup.
            trainer = config.build()
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
            trainer = config.build()
            # Due to the low temp, always expect the same action.
            actions = [trainer.compute_single_action(obs)]
            for _ in range(50):
                actions.append(trainer.compute_single_action(obs))
            check(np.std(actions), 0.0, decimals=3)
            trainer.stop()

            # Higher softmax temperature.
            config.exploration_config["temperature"] = 1.0
            trainer = config.build()

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
            trainer = config.build()
            actions = []
            for _ in range(300):
                actions.append(trainer.compute_single_action(obs))
            check(np.std(actions), 0.0, false=True)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
