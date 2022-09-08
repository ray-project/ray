import gym
import unittest

import ray
from ray.rllib.algorithms.ppo import PPOConfig


class GymOld(gym.Env):
    def __init__(self, config=None):
        self.observation_space = gym.spaces.Box(-1.0, 1.0, (1,))
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        return self.observation_space.sample()

    def step(self, action):
        done = True
        return self.observation_space.sample(), 1.0, done, {}

    def seed(self, seed=None):
        pass


class GymNew(gym.Env):
    def __init__(self, config=None):
        self.observation_space = gym.spaces.Box(-1.0, 1.0, (1,))
        self.action_space = gym.spaces.Discrete(2)

    def reset(self, seed=None):
        assert seed is None or isinstance(seed, int)
        return self.observation_space.sample()

    def step(self, action):
        done = truncated = True
        return self.observation_space.sample(), 1.0, done, truncated, {}


class TestOldGymEnvAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(local_mode=True)#TODO

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_reset_wo_seed_and_step_returning_4_tuple(self):
        algo = PPOConfig().\
            environment(env=GymOld).\
            rollouts(num_envs_per_worker=2, num_rollout_workers=2).\
            build()
        algo.train()
        algo.stop()

    def test_new_api(self):
        algo = PPOConfig().\
            environment(env=GymNew).\
            rollouts(num_envs_per_worker=2, num_rollout_workers=2).\
            build()
        algo.train()
        algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
