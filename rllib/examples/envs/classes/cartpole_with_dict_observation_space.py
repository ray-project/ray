import gymnasium as gym
from gymnasium.envs.classic_control import CartPoleEnv
import numpy as np


class CartPoleWithDictObservationSpace(CartPoleEnv):
    """CartPole gym environment that has a dict observation space.

    However, otherwise, the information content in each observation remains the same.

    https://github.com/Farama-Foundation/Gymnasium/blob/main/gymnasium/envs/classic_control/cartpole.py  # noqa

    The new observation space looks as follows (a little quirky, but this is
    for testing purposes only):

    gym.spaces.Dict({
        "x-pos": [x-pos],
        "angular-pos": gym.spaces.Dict({"test": [angular-pos]}),
        "velocs": gym.spaces.Tuple([x-veloc, angular-veloc]),
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
                "x-pos": gym.spaces.Box(low[0], high[0], (1,), dtype=np.float32),
                "angular-pos": gym.spaces.Dict(
                    {
                        "value": gym.spaces.Box(low[2], high[2], (), dtype=np.float32),
                        # Add some random non-essential information.
                        "some_random_stuff": gym.spaces.Discrete(3),
                    }
                ),
                "velocs": gym.spaces.Tuple(
                    [
                        # x-veloc
                        gym.spaces.Box(low[1], high[1], (1,), dtype=np.float32),
                        # angular-veloc
                        gym.spaces.Box(low[3], high[3], (), dtype=np.float32),
                    ]
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
        # original_cartpole_obs is [x-pos, x-veloc, angle, angle-veloc]
        return {
            "x-pos": np.array([original_cartpole_obs[0]], np.float32),
            "angular-pos": {
                "value": original_cartpole_obs[2],
                "some_random_stuff": np.random.randint(3),
            },
            "velocs": (
                np.array([original_cartpole_obs[1]], np.float32),
                np.array(original_cartpole_obs[3], np.float32),
            ),
        }
