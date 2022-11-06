import unittest

import ray
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.utils.gym import try_import_gymnasium_and_gym

gym, old_gym = try_import_gymnasium_and_gym()


class GymnasiumOldAPI(gym.Env):
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


class GymnasiumNewAPI(gym.Env):
    def __init__(self, config=None):
        self.observation_space = gym.spaces.Box(-1.0, 1.0, (1,))
        self.action_space = gym.spaces.Discrete(2)

    def reset(self, *, seed=None, options=None):
        assert seed is None or isinstance(seed, int)
        assert options is None or isinstance(options, dict)
        return self.observation_space.sample()

    def step(self, action):
        done = truncated = True
        return self.observation_space.sample(), 1.0, done, truncated, {}


class GymOldAPI(old_gym.Env):
    def __init__(self, config=None):
        self.observation_space = gym.spaces.Box(-1.0, 1.0, (1,))
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        return self.observation_space.sample()

    def step(self, action):
        done = True
        return self.observation_space.sample(), 1.0, done, truncated

    def seed(self, seed=None):
        pass


class GymNewAPI(old_gym.Env):
    def __init__(self, config=None):
        self.observation_space = gym.spaces.Box(-1.0, 1.0, (1,))
        self.action_space = gym.spaces.Discrete(2)

    def reset(self, *, seed=None, options=None):
        assert seed is None or isinstance(seed, int)
        assert options is None or isinstance(options, dict)
        return self.observation_space.sample()

    def step(self, action):
        done = truncated = True
        return self.observation_space.sample(), 1.0, done, truncated, {}


class TestGymEnvAPIs(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_gymnasium_old_api(self):
        """Tests a gymnasium Env that uses the old API."""
        algo = (
            PPOConfig()
            .environment(env=GymnasiumOldAPI)
            .rollouts(num_envs_per_worker=2, num_rollout_workers=2)
            .build()
        )
        print(algo.train())
        algo.stop()

    def test_gymnasium_new_api(self):
        """Tests a gymnasium Env that uses the new API."""
        algo = (
            PPOConfig()
            .environment(env=GymnasiumNewAPI)
            .rollouts(num_envs_per_worker=2, num_rollout_workers=2)
            .build()
        )
        print(algo.train())
        algo.stop()

    def test_gym_old_api(self):
        """Tests a gymnasium Env that uses the old API."""
        algo = (
            PPOConfig()
            .environment(env=GymOldAPI)
            .rollouts(num_envs_per_worker=2, num_rollout_workers=2)
            .build()
        )
        print(algo.train())
        algo.stop()

    def test_gym_new_api(self):
        """Tests a gymnasium Env that uses the new API."""
        algo = (
            PPOConfig()
            .environment(env=GymNewAPI)
            .rollouts(num_envs_per_worker=2, num_rollout_workers=2)
            .build()
        )
        print(algo.train())
        algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
