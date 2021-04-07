import gym
import unittest

from ray.rllib.env.wrappers.recsim_wrapper import (
    MultiDiscreteToDiscreteActionWrapper, make_recsim_env)
from ray.rllib.utils.error import UnsupportedSpaceException


class TestRecSimWrapper(unittest.TestCase):
    def test_observation_space(self):
        env = make_recsim_env(config={})
        obs = env.reset()
        self.assertTrue(
            env.observation_space.contains(obs),
            f"{env.observation_space} doesn't contain {obs}")
        new_obs, _, _, _ = env.step(env.action_space.sample())
        self.assertTrue(env.observation_space.contains(new_obs))

    def test_action_space_conversion(self):
        env = make_recsim_env(
            config={"convert_to_discrete_action_space": True})
        self.assertIsInstance(env.action_space, gym.spaces.Discrete)
        env.reset()
        action = env.action_space.sample()
        env.step(action)

    def test_double_action_space_conversion_raises_exception(self):
        env = make_recsim_env(
            config={"convert_to_discrete_action_space": True})
        with self.assertRaises(UnsupportedSpaceException):
            env = MultiDiscreteToDiscreteActionWrapper(env)


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
