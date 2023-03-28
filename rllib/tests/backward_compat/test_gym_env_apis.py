import unittest

import ray
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.wrappers.multi_agent_env_compatibility import (
    MultiAgentEnvCompatibility,
)
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


class GymnasiumNewAPIButThrowsErrorOnReset(gym.Env):
    render_mode = "human"

    def __init__(self, config=None):
        self.observation_space = gym.spaces.Box(-1.0, 1.0, (1,))
        self.action_space = gym.spaces.Discrete(2)

    def reset(self, *, seed=None, options=None):
        assert False, "kaboom!"
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


class MultiAgentGymnasiumOldAPI(MultiAgentEnv):
    def __init__(self, config=None):
        super().__init__()
        self.observation_space = gym.spaces.Dict(
            {"agent0": gym.spaces.Box(-1.0, 1.0, (1,))}
        )
        self.action_space = gym.spaces.Dict({"agent0": gym.spaces.Discrete(2)})
        self._observation_space_in_preferred_format = True
        self._action_space_in_preferred_format = True

    def reset(self):
        return {"agent0": self.observation_space.sample()}

    def step(self, action):
        done = True
        return (
            {"agent0": self.observation_space.sample()},
            {"agent0": 1.0},
            {"agent0": done, "__all__": done},
            {},
        )

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
                .environment(env=GymnasiumOldAPI, auto_wrap_old_gym_envs=False)
                .rollouts(num_envs_per_worker=2, num_rollout_workers=2)
                .build()
            )

        self.assertRaisesRegex(
            ValueError,
            ".*In particular, the `reset\\(\\)` method seems to be faulty..*",
            lambda: test_(),
        )

    def test_gymnasium_old_api_using_auto_wrap(self):
        """Tests a gymnasium Env that uses the old API, but is auto-wrapped by RLlib."""
        algo = (
            PPOConfig()
            .environment(env=GymnasiumOldAPI, auto_wrap_old_gym_envs=True)
            .rollouts(num_envs_per_worker=2, num_rollout_workers=2)
            .build()
        )
        algo.train()
        algo.stop()

    def test_gymnasium_new_api_but_old_spaces(self):
        """Tests a gymnasium Env that uses the new API, but has old spaces."""

        def test_():
            (
                PPOConfig()
                .environment(GymnasiumNewAPIButOldSpaces, auto_wrap_old_gym_envs=True)
                .rollouts(num_envs_per_worker=2, num_rollout_workers=2)
                .build()
            )

        self.assertRaisesRegex(
            ValueError,
            "Observation space must be a gymnasium.Space!",
            lambda: test_(),
        )

    def test_gymnasium_new_api_but_throws_error_on_reset(self):
        """Tests a gymnasium Env that uses the new API, but errors on reset() call."""

        def test_():
            (
                PPOConfig()
                .environment(
                    GymnasiumNewAPIButThrowsErrorOnReset,
                    auto_wrap_old_gym_envs=True,
                )
                .rollouts(num_envs_per_worker=1, num_rollout_workers=0)
                .build()
            )

        self.assertRaisesRegex(AssertionError, "kaboom!", lambda: test_())

    def test_gymnasium_old_api_but_manually_wrapped(self):
        """Tests a gymnasium Env that uses the old API, but is correctly wrapped."""

        from gymnasium.wrappers import EnvCompatibility

        register_env(
            "test",
            lambda env_ctx: EnvCompatibility(GymnasiumOldAPI(env_ctx)),
        )

        algo = (
            PPOConfig()
            .environment("test", auto_wrap_old_gym_envs=False)
            .rollouts(num_envs_per_worker=2, num_rollout_workers=2)
            .build()
        )
        algo.train()
        algo.stop()

    def test_old_gym_env(self):
        """Tests a old gym.Env (should fail, even with auto-wrapping enabled)."""

        def test_():
            (
                PPOConfig()
                .environment(env=OldGymEnv, auto_wrap_old_gym_envs=True)
                .rollouts(num_envs_per_worker=2, num_rollout_workers=2)
                .build()
            )

        self.assertRaisesRegex(
            ValueError,
            "does not abide to the new gymnasium-style API",
            lambda: test_(),
        )

    def test_multi_agent_gymnasium_old_api(self):
        """Tests a MultiAgentEnv (gymnasium.Env subclass) that uses the old API."""

        def test_():
            (
                PPOConfig()
                .environment(
                    MultiAgentGymnasiumOldAPI,
                    auto_wrap_old_gym_envs=False,
                )
                .build()
            )

        self.assertRaisesRegex(
            ValueError,
            ".*In particular, the `reset\\(\\)` method seems to be faulty..*",
            lambda: test_(),
        )

    def test_multi_agent_gymnasium_old_api_auto_wrapped(self):
        """Tests a MultiAgentEnv (gymnasium.Env subclass) that uses the old API."""

        algo = (
            PPOConfig()
            .environment(
                MultiAgentGymnasiumOldAPI,
                auto_wrap_old_gym_envs=True,
            )
            .build()
        )
        algo.train()
        algo.stop()

    def test_multi_agent_gymnasium_old_api_manually_wrapped(self):
        """Tests a MultiAgentEnv (gymnasium.Env subclass) that uses the old API."""

        register_env(
            "test",
            lambda env_ctx: MultiAgentEnvCompatibility(
                MultiAgentGymnasiumOldAPI(env_ctx)
            ),
        )

        algo = PPOConfig().environment("test", auto_wrap_old_gym_envs=False).build()
        algo.train()
        algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
