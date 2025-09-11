import gymnasium as gym
import numpy as np
from gymnasium.envs.classic_control import CartPoleEnv


class CartPoleWithLargeObservationSpace(CartPoleEnv):
    """CartPole gym environment that has a large dict observation space.

    However, otherwise, the information content in each observation remains the same.

    https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/envs/classic_control/cartpole.py  # noqa

    The new observation space looks as follows (a little quirky, but this is
    for testing purposes only):

    gym.spaces.Dict({
        "1": gym.spaces.Tuple((
            gym.spaces.Discrete(100),
            gym.spaces.Box(0, 256, shape=(30,), dtype=float32),
        )),
        "2": gym.spaces.Tuple((
            gym.spaces.Discrete(100),
            gym.spaces.Box(0, 256, shape=(30,), dtype=float32),
        )),
        "3": ...
        "actual-obs": gym.spaces.Box(-inf, inf, (4,), float32),
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
        spaces = {
            str(i): gym.spaces.Tuple(
                (
                    gym.spaces.Discrete(100),
                    gym.spaces.Box(0, 256, shape=(30,), dtype=np.float32),
                )
            )
            for i in range(100)
        }
        spaces.update(
            {
                "actually-useful-stuff": (
                    gym.spaces.Box(low[0], high[0], (4,), np.float32)
                )
            }
        )
        self.observation_space = gym.spaces.Dict(spaces)

    def step(self, action):
        next_obs, reward, done, truncated, info = super().step(action)
        return self._compile_current_obs(next_obs), reward, done, truncated, info

    def reset(self, *, seed=None, options=None):
        init_obs, init_info = super().reset(seed=seed, options=options)
        return self._compile_current_obs(init_obs), init_info

    def _compile_current_obs(self, original_cartpole_obs):
        return {
            str(i): self.observation_space.spaces[str(i)].sample() for i in range(100)
        } | {"actually-useful-stuff": original_cartpole_obs}
