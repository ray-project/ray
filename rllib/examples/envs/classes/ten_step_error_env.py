import logging

import gymnasium as gym

logger = logging.getLogger(__name__)


class TenStepErrorEnv(gym.Env):
    """An environment that lets you sample 1 episode and raises an error during the next one.

    The expectation to the env runner is that it will sample one episode and recreate the env
    to sample the second one.
    """

    def __init__(self, config):
        super().__init__()
        self.step_count = 0
        self.last_eps_errored = False
        self.observation_space = gym.spaces.Box(low=0, high=1, shape=(1,))
        self.action_space = gym.spaces.Box(low=0, high=1, shape=(1,))

    def reset(self, seed=None, options=None):
        self.step_count = 0
        return self.observation_space.sample(), {
            "last_eps_errored": self.last_eps_errored
        }

    def step(self, action):
        self.step_count += 1
        if self.step_count == 10:
            if not self.last_eps_errored:
                self.last_eps_errored = True
                return (
                    self.observation_space.sample(),
                    0.0,
                    True,
                    False,
                    {"last_eps_errored": False},
                )
            else:
                raise Exception("Test error")

        return (
            self.observation_space.sample(),
            0.0,
            False,
            False,
            {"last_eps_errored": self.last_eps_errored},
        )
