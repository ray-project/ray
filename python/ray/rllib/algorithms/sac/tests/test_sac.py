import unittest

import gymnasium as gym
import numpy as np
from gymnasium.spaces import Box, Dict, Discrete, Tuple

import ray
from ray import tune
from ray.rllib.algorithms import sac
from ray.rllib.connectors.env_to_module.flatten_observations import FlattenObservations
from ray.rllib.examples.envs.classes.random_env import RandomEnv
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.test_utils import check_train_results_new_api_stack

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class SimpleEnv(gym.Env):
    def __init__(self, config):
        if config.get("simplex_actions", False):
            self.action_space = Simplex((2,))
        else:
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


class TestSAC(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        np.random.seed(42)
        torch.manual_seed(42)
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_sac_compilation(self):
        """Test whether SAC can be built and trained."""
        config = (
            sac.SACConfig()
            .training(
                n_step=3,
                twin_q=True,
                replay_buffer_config={
                    "capacity": 40000,
                },
                num_steps_sampled_before_learning_starts=0,
                store_buffer_in_checkpoints=True,
                train_batch_size=10,
            )
            .env_runners(
                env_to_module_connector=(
                    lambda env, spaces, device: FlattenObservations()
                ),
                num_env_runners=0,
                rollout_fragment_length=10,
            )
        )
        num_iterations = 1

        image_space = Box(-1.0, 1.0, shape=(84, 84, 3))
        simple_space = Box(-1.0, 1.0, shape=(3,))

        tune.register_env(
            "random_dict_env",
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
            "random_tuple_env",
            lambda _: RandomEnv(
                {
                    "observation_space": Tuple(
                        [simple_space, Discrete(2), image_space]
                    ),
                    "action_space": Box(-1.0, 1.0, shape=(1,)),
                }
            ),
        )

        # Test for different env types (discrete w/ and w/o image, + cont).
        for env in [
            "random_dict_env",
            "random_tuple_env",
        ]:
            print("Env={}".format(env))
            config.environment(env)
            algo = config.build()
            for i in range(num_iterations):
                results = algo.train()
                check_train_results_new_api_stack(results)
                print(results)

            algo.stop()

    def test_sac_dict_obs_order(self):
        dict_space = Dict(
            {
                "img": Box(low=0, high=1, shape=(42, 42, 3)),
                "cont": Box(low=0, high=100, shape=(3,)),
            }
        )

        # Dict space .sample() returns an ordered dict.
        # Make sure the keys in samples are ordered differently.
        dict_samples = [dict(reversed(dict_space.sample().items())) for _ in range(10)]

        class NestedDictEnv(gym.Env):
            def __init__(self):
                self.action_space = Box(low=-1.0, high=1.0, shape=(2,))
                self.observation_space = dict_space
                self.steps = 0

            def reset(self, *, seed=None, options=None):
                self.steps = 0
                return dict_samples[0], {}

            def step(self, action):
                self.steps += 1
                terminated = False
                truncated = self.steps >= 5
                return dict_samples[self.steps], 1, terminated, truncated, {}

        tune.register_env("nested", lambda _: NestedDictEnv())
        config = (
            sac.SACConfig()
            .environment("nested")
            .training(
                replay_buffer_config={
                    "capacity": 10,
                },
                num_steps_sampled_before_learning_starts=0,
                train_batch_size=5,
            )
            .env_runners(
                num_env_runners=0,
                rollout_fragment_length=5,
                env_to_module_connector=(
                    lambda env, spaces, device: FlattenObservations()
                ),
            )
        )
        num_iterations = 1

        algo = config.build()
        for _ in range(num_iterations):
            results = algo.train()
            check_train_results_new_api_stack(results)
            print(results)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
