import numpy as np
from tensorflow.python.eager.context import eager_mode
import unittest

import ray.rllib.agents.dqn as dqn
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check

tf = try_import_tf()


class TestDQN(unittest.TestCase):
    def test_dqn_compilation(self):
        """Test whether a DQNTrainer can be built with both frameworks."""
        config = dqn.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.

        # Rainbow.
        rainbow_config = config.copy()
        rainbow_config["eager"] = False
        rainbow_config["num_atoms"] = 10
        rainbow_config["noisy"] = True
        rainbow_config["double_q"] = True
        rainbow_config["dueling"] = True
        rainbow_config["n_step"] = 5
        trainer = dqn.DQNTrainer(config=rainbow_config, env="CartPole-v0")
        num_iterations = 2
        for i in range(num_iterations):
            results = trainer.train()
            print(results)

        # tf.
        tf_config = config.copy()
        tf_config["eager"] = False
        trainer = dqn.DQNTrainer(config=tf_config, env="CartPole-v0")
        num_iterations = 1
        for i in range(num_iterations):
            results = trainer.train()
            print(results)

        # Eager.
        eager_config = config.copy()
        eager_config["eager"] = True
        eager_ctx = eager_mode()
        eager_ctx.__enter__()
        trainer = dqn.DQNTrainer(config=eager_config, env="CartPole-v0")
        num_iterations = 1
        for i in range(num_iterations):
            results = trainer.train()
            print(results)
        eager_ctx.__exit__(None, None, None)

    def test_dqn_exploration_and_soft_q_config(self):
        """Tests, whether a DQN Agent outputs exploration/softmaxed actions."""
        config = dqn.DEFAULT_CONFIG.copy()
        config["num_workers"] = 0  # Run locally.
        config["env_config"] = {"is_slippery": False, "map_name": "4x4"}
        obs = np.array(0)

        # Test against all frameworks.
        for fw in ["tf", "eager", "torch"]:
            if fw == "torch":
                continue

            print("framework={}".format(fw))

            eager_mode_ctx = None
            if fw == "tf":
                assert not tf.executing_eagerly()
            else:
                eager_mode_ctx = eager_mode()
                eager_mode_ctx.__enter__()

            config["eager"] = fw == "eager"
            config["use_pytorch"] = fw == "torch"

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
                "temperature": 0.001
            }
            trainer = dqn.DQNTrainer(config=config, env="FrozenLake-v0")
            # Due to the low temp, always expect the same action.
            a_ = trainer.compute_action(obs)
            for _ in range(50):
                a = trainer.compute_action(obs)
                check(a, a_)

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
