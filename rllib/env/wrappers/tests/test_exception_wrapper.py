import random
import unittest

import gymnasium as gym
from ray.rllib.env.wrappers.exception_wrapper import (
    ResetOnExceptionWrapper,
    TooManyResetAttemptsException,
)


class TestResetOnExceptionWrapper(unittest.TestCase):
    def test_unstable_env(self):
        class UnstableEnv(gym.Env):
            observation_space = gym.spaces.Discrete(2)
            action_space = gym.spaces.Discrete(2)

            def step(self, action):
                if random.choice([True, False]):
                    raise ValueError("An error from a unstable environment.")
                return self.observation_space.sample(), 0.0, False, False, {}

            def reset(self, *, seed=None, options=None):
                return self.observation_space.sample(), {}

        env = UnstableEnv()
        env = ResetOnExceptionWrapper(env)

        try:
            self._run_for_100_steps(env)
        except Exception:
            self.fail()

    def test_very_unstable_env(self):
        class VeryUnstableEnv(gym.Env):
            observation_space = gym.spaces.Discrete(2)
            action_space = gym.spaces.Discrete(2)

            def step(self, action):
                return self.observation_space.sample(), 0.0, False, False, {}

            def reset(self, *, seed=None, options=None):
                raise ValueError("An error from a very unstable environment.")

        env = VeryUnstableEnv()
        env = ResetOnExceptionWrapper(env)
        self.assertRaises(
            TooManyResetAttemptsException, lambda: self._run_for_100_steps(env)
        )

    @staticmethod
    def _run_for_100_steps(env):
        env.reset()
        for _ in range(100):
            env.step(env.action_space.sample())


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
