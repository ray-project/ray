"""Tests for the TQC (Truncated Quantile Critics) algorithm."""

import unittest

import gymnasium as gym
import numpy as np
from gymnasium.spaces import Box, Dict, Discrete, Tuple

import ray
from ray import tune
from ray.rllib.algorithms import tqc
from ray.rllib.connectors.env_to_module.flatten_observations import FlattenObservations
from ray.rllib.examples.envs.classes.random_env import RandomEnv
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import check_train_results_new_api_stack

torch, _ = try_import_torch()


class SimpleEnv(gym.Env):
    """Simple continuous control environment for testing."""

    def __init__(self, config):
        self.action_space = Box(0.0, 1.0, (1,))
        self.observation_space = Box(0.0, 1.0, (1,))
        self.max_steps = config.get("max_steps", 100)
        self.state = None
        self.steps = None

    def reset(self, *, seed=None, options=None):
        self.state = self.observation_space.sample()
        self.steps = 0
        return self.state, {}

    def step(self, action):
        self.steps += 1
        # Reward is 1.0 - (max(actions) - state).
        [rew] = 1.0 - np.abs(np.max(action) - self.state)
        terminated = False
        truncated = self.steps >= self.max_steps
        self.state = self.observation_space.sample()
        return self.state, rew, terminated, truncated, {}


class TestTQC(unittest.TestCase):
    """Test cases for TQC algorithm."""

    @classmethod
    def setUpClass(cls) -> None:
        np.random.seed(42)
        torch.manual_seed(42)
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def setUp(self) -> None:
        """Set up base config for tests."""
        self.base_config = (
            tqc.TQCConfig()
            .training(
                n_step=3,
                n_quantiles=25,
                n_critics=2,
                top_quantiles_to_drop_per_net=2,
                replay_buffer_config={
                    "capacity": 40000,
                },
                num_steps_sampled_before_learning_starts=0,
                store_buffer_in_checkpoints=True,
                train_batch_size=10,
            )
            .env_runners(
                num_env_runners=0,
                rollout_fragment_length=10,
            )
        )

    def test_tqc_compilation(self):
        """Test whether TQC can be built and trained."""
        config = self.base_config.copy().env_runners(
            env_to_module_connector=(lambda env, spaces, device: FlattenObservations()),
        )
        num_iterations = 1

        image_space = Box(-1.0, 1.0, shape=(84, 84, 3))
        simple_space = Box(-1.0, 1.0, shape=(3,))

        tune.register_env(
            "random_dict_env_tqc",
            lambda _: RandomEnv(
                {
                    "observation_space": Dict(
                        {
                            "a": simple_space,
                            "b": Discrete(2),
                            "c": image_space,
                        }
                    ),
                    "action_space": Box(-1.0, 1.0, shape=(1,)),
                }
            ),
        )
        tune.register_env(
            "random_tuple_env_tqc",
            lambda _: RandomEnv(
                {
                    "observation_space": Tuple(
                        [simple_space, Discrete(2), image_space]
                    ),
                    "action_space": Box(-1.0, 1.0, shape=(1,)),
                }
            ),
        )

        # Test for different env types (dict and tuple observations).
        for env in [
            "random_dict_env_tqc",
            "random_tuple_env_tqc",
        ]:
            print("Env={}".format(env))
            config.environment(env)
            algo = config.build()
            for i in range(num_iterations):
                results = algo.train()
                check_train_results_new_api_stack(results)
                print(results)

            algo.stop()

    def test_tqc_simple_env(self):
        """Test TQC on a simple continuous control environment."""
        tune.register_env("simple_env_tqc", lambda config: SimpleEnv(config))

        config = (
            tqc.TQCConfig()
            .environment("simple_env_tqc", env_config={"max_steps": 50})
            .training(
                n_quantiles=10,
                n_critics=2,
                top_quantiles_to_drop_per_net=1,
                replay_buffer_config={
                    "capacity": 10000,
                },
                num_steps_sampled_before_learning_starts=0,
                train_batch_size=32,
            )
            .env_runners(
                num_env_runners=0,
                rollout_fragment_length=10,
            )
        )

        algo = config.build()
        for _ in range(2):
            results = algo.train()
            check_train_results_new_api_stack(results)
            print(results)

        algo.stop()

    def test_tqc_quantile_parameters(self):
        """Test TQC with different quantile configurations."""
        tune.register_env("simple_env_tqc_params", lambda config: SimpleEnv(config))

        # Test with different n_quantiles and n_critics
        for n_quantiles, n_critics, top_drop in [
            (5, 2, 1),
            (25, 3, 2),
            (50, 2, 5),
        ]:
            print(
                f"Testing n_quantiles={n_quantiles}, n_critics={n_critics}, "
                f"top_drop={top_drop}"
            )

            config = (
                tqc.TQCConfig()
                .environment("simple_env_tqc_params", env_config={"max_steps": 20})
                .training(
                    n_quantiles=n_quantiles,
                    n_critics=n_critics,
                    top_quantiles_to_drop_per_net=top_drop,
                    replay_buffer_config={
                        "capacity": 5000,
                    },
                    num_steps_sampled_before_learning_starts=0,
                    train_batch_size=16,
                )
                .env_runners(
                    num_env_runners=0,
                    rollout_fragment_length=5,
                )
            )

            algo = config.build()
            results = algo.train()
            check_train_results_new_api_stack(results)
            algo.stop()

    def test_tqc_config_validation(self):
        """Test that TQC config validation works correctly."""
        # Test invalid n_quantiles
        with self.assertRaises(ValueError):
            config = tqc.TQCConfig().training(n_quantiles=0)
            config.validate()

        # Test invalid n_critics
        with self.assertRaises(ValueError):
            config = tqc.TQCConfig().training(n_critics=0)
            config.validate()

        # Test dropping too many quantiles
        with self.assertRaises(ValueError):
            # With n_quantiles=5, n_critics=2, total=10
            # Dropping 6 per net = 12 total, which is > 10
            config = tqc.TQCConfig().training(
                n_quantiles=5,
                n_critics=2,
                top_quantiles_to_drop_per_net=6,
            )
            config.validate()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
