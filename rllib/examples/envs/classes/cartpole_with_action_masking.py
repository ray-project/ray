import gymnasium as gym
from gymnasium.envs.classic_control import CartPoleEnv
import numpy as np

from ray.rllib.utils.numpy import one_hot


class CartPoleWithActionMasking(CartPoleEnv):
    """CartPole gym environment that requires action masking to be used.

    The size of the discrete action space is determined by config["num_actions"].
    The key under which the allowed actions (as a tuple of discrete values) are stored
    is determined by config["allowed_actions_key"].
    For example, if there are 10 actions altogether (Discrete(10)), then 0-4 translate
    to moving left and 5-9 translate to moving right. Only one of the left moving and
    one of the right moving actions per step are published as allowed.
    If config["allowed_actions_location"] == "infos", then the allowed actions key can
    be found in the infos dicts, if its value is "observations", then the allowed
    actions will be added to an observations dict. The observation space is then
    converted into a dict space with "original_obs" (stores the normal CartPole obs)
    and the allowed actions key in it.

    For more details on the actual underlying CartPole env, see here:
    https://github.com/openai/gym/blob/master/gym/envs/classic_control/
    cartpole.py
    """

    def __init__(self, config=None):
        super().__init__()

        config = config or {}
        # Fix our action- and observation spaces as described above.
        self.num_actions = config.get("num_actions", 10)
        assert self.num_actions % 2 == 0, (
            f"num_actions {self.num_actions} must be dividable by 2!"
        )
        self.action_space = gym.spaces.Discrete(self.num_actions)

        # What the key for the allowed actions information should be called.
        self.allowed_actions_key = config.get("allowed_actions_key", "allowed_actions")

        # Whether the allowed actions should be put into infos or observations.
        self.allowed_actions_location = config.get("allowed_actions_location", "infos")
        assert self.allowed_actions_location in ["infos", "observations"]
        # If allowed actions are in observations, change our observation space.
        if self.allowed_actions_location == "observations":
            self.observation_space = gym.spaces.Dict(
                {
                    # original obs tensor
                    "original_obs": self.observation_space,
                    # 1.0 = allowed action
                    self.allowed_actions_key: gym.spaces.Box(
                        0.0, 1.0, (self.num_actions,), np.float32
                    ),
                }
            )

        self._currently_allowed_actions = None

    def reset(self, *, seed=None, options=None):
        init_obs, init_info = super().reset(seed=seed, options=options)
        init_obs, init_info = self._compile_current_obs_and_infos(init_obs, init_info)
        return init_obs, init_info

    def step(self, action):
        # Check, whether received action is allowed.
        if action not in self._currently_allowed_actions:
            raise ValueError(
                f"Action {action} not allowed! Allowed actions are"
                f" {self._currently_allowed_actions}."
            )
        # Left.
        if action < self.num_actions // 2:
            actual_action = 0
        else:
            actual_action = 1
        next_obs, reward, done, truncated, info = super().step(actual_action)
        next_obs, info = self._compile_current_obs_and_infos(next_obs, info)
        return next_obs, reward, done, truncated, info

    def _compile_current_obs_and_infos(self, original_obs, original_info):
        n_half_act = self.num_actions // 2
        self._currently_allowed_actions = (
            np.random.randint(n_half_act),
            np.random.randint(n_half_act, n_half_act * 2),
        )
        if self.allowed_actions_location == "observations":
            return {
                "original_obs": original_obs,
                self.allowed_actions_key: np.concatenate([
                    one_hot(self._currently_allowed_actions[0], depth=n_half_act),
                    one_hot(
                        self._currently_allowed_actions[1] - n_half_act, depth=n_half_act
                    ),
                ]),
            }, original_info
        else:
            new_info = dict(
                original_info,
                **{self.allowed_actions_key: self._currently_allowed_actions},
            )
            return original_obs, new_info
