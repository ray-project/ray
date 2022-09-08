import gym
import unittest

from ray.rllib.env.vector_env import VectorEnv


class Info(dict):
    pass


class MockEnvDictSubclass(gym.Env):
    def __init__(self):
        self.observation_space = gym.spaces.Discrete(1)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        return 0

    def step(self, action):
        return 0, 1, True, Info()


class TestExternalEnv(unittest.TestCase):
    def test_vector_step(self):
        env = VectorEnv.vectorize_gym_envs(
            make_env=lambda _: MockEnvDictSubclass(), num_envs=3
        )
        env.vector_step([0] * 3)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
