import unittest

import ray
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.utils.gym import try_import_gymnasium_and_gym
from ray.tune.registry import register_env

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

    def render(self, mode="human"):
        pass


class GymnasiumNewAPIButOldSpaces(gym.Env):
    render_mode = "human"

    def __init__(self, config=None):
        self.observation_space = old_gym.spaces.Box(-1.0, 1.0, (1,))
        self.action_space = old_gym.spaces.Discrete(2)

    def reset(self, *, seed=None, options=None):
        return self.observation_space.sample(), {}

    def step(self, action):
        terminated = truncated = True
        return self.observation_space.sample(), 1.0, terminated, truncated, {}

    def render(self):
        pass


class OldGymEnv(old_gym.Env):
    def __init__(self, config=None):
        self.observation_space = old_gym.spaces.Box(-1.0, 1.0, (1,))
        self.action_space = old_gym.spaces.Discrete(2)

    def reset(self):
        return self.observation_space.sample()

    def step(self, action):
        done = True
        return self.observation_space.sample(), 1.0, done, {}

    def seed(self, seed=None):
        pass

    def render(self, mode="human"):
        pass


class TestGymEnvAPIs(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_gymnasium_old_api(self):
        """Tests a gymnasium Env that uses the old API."""

        def test_():
            (
                PPOConfig()
                .environment(env=GymnasiumOldAPI)
                .rollouts(num_envs_per_worker=2, num_rollout_workers=2)
                .build()
            )

        self.assertRaisesRegex(
            ValueError,
            ".*In particular, the `reset\\(\\)` method seems to be faulty..*",
            lambda: test_(),
        )

    def test_gymnasium_new_api_but_old_spaces(self):
        """Tests a gymnasium Env that uses the new API, but has old spaces."""

        def test_():
            (
                PPOConfig()
                .environment(env=GymnasiumNewAPIButOldSpaces)
                .rollouts(num_envs_per_worker=2, num_rollout_workers=2)
                .build()
            )

        self.assertRaisesRegex(
            ValueError,
            "Observation space must be a gymnasium.Space!",
            lambda: test_(),
        )

    def test_gymnasium_old_api_but_wrapped(self):
        """Tests a gymnasium Env that uses the old API, but is correctly wrapped."""

        from gymnasium.wrappers import EnvCompatibility

        register_env(
            "test",
            lambda env_ctx: EnvCompatibility(GymnasiumOldAPI(env_ctx)),
        )

        algo = (
            PPOConfig()
            .environment(env="test")
            .rollouts(num_envs_per_worker=2, num_rollout_workers=2)
            .build()
        )
        algo.train()
        algo.stop()

    def test_old_gym_env(self):
        """Tests a old gym.Env (should fail)."""

        def test_():
            (
                PPOConfig()
                .environment(env=OldGymEnv)
                .rollouts(num_envs_per_worker=2, num_rollout_workers=2)
                .build()
            )

        self.assertRaisesRegex(
            ValueError,
            "does not abide to the new gymnasium-style API",
            lambda: test_(),
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
