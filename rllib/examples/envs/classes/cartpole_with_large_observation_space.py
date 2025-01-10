import gymnasium as gym
from gymnasium.envs.classic_control import CartPoleEnv
import numpy as np


class CartPoleWithLargeObservationSpace(CartPoleEnv):
    """CartPole gym environment that has a large dict observation space.

    However, otherwise, the information content in each observation remains the same.

    https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/envs/classic_control/cartpole.py  # noqa

    The new observation space looks as follows (a little quirky, but this is
    for testing purposes only):

    gym.spaces.Dict({
        "1": gym.spaces.Tuple((
            gym.spaces.Discrete(1000),
            gym.spaces.Box(0, 256, shape=(3000,), dtype=float32),
        )),
        "2": gym.spaces.Tuple((
            gym.spaces.Discrete(1000),
            gym.spaces.Box(0, 256, shape=(3000,), dtype=float32),
        )),
        "3": gym.spaces.Box(-inf, inf, (4,), float32),
    })
    """

    def __init__(self, config=None):
        super().__init__()

        # Fix our observation-space as described above.
        low = self.observation_space.low
        high = self.observation_space.high

        # Test as many quirks and oddities as possible: Dict, Dict inside a Dict,
        # Tuple inside a Dict, and both (1,)-shapes as well as ()-shapes for Boxes.
        # Also add a random discrete variable here.
        self.observation_space = gym.spaces.Dict(
            {
                "dead-weight": gym.spaces.Tuple((
                    gym.spaces.Discrete(1000),
                    gym.spaces.Box(0, 256, shape=(3000,), dtype=np.float32),
                )),
                "more-dead-weight": gym.spaces.Tuple((
                    gym.spaces.Discrete(1000),
                    gym.spaces.Box(0, 256, shape=(3000,), dtype=np.float32),
                )),
                "actually-useful-stuff": (
                    gym.spaces.Box(low[0], high[0], (4,), np.float32)
                ),
            }
        )

    def step(self, action):
        next_obs, reward, done, truncated, info = super().step(action)
        return self._compile_current_obs(next_obs), reward, done, truncated, info

    def reset(self, *, seed=None, options=None):
        init_obs, init_info = super().reset(seed=seed, options=options)
        return self._compile_current_obs(init_obs), init_info

    def _compile_current_obs(self, original_cartpole_obs):
        return {
            "dead-weight": self.observation_space.spaces["dead-weight"].sample(),
            "more-dead-weight": (
                self.observation_space.spaces["more-dead-weight"].sample()
            ),
            # original_cartpole_obs is [x-pos, x-veloc, angle, angle-veloc]
            "actually-useful-stuff": original_cartpole_obs,
        }
