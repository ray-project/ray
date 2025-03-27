from typing import Dict

import numpy as np
from gymnasium import Env, spaces


class SauteBaseEnv(Env):
    def __init__(
        self,
        saute_discount_factor: float = 0.99,
        max_ep_len: int = 200,
        unsafe_reward: float = 0,
        use_reward_shaping: bool = False,  # ablation
        use_state_augmentation: bool = True,  # ablation
    ):
        assert (
            saute_discount_factor > 0 and saute_discount_factor <= 1
        ), "Please specify a discount factor in (0, 1]"
        assert max_ep_len > 0

        self.max_ep_len = max_ep_len

        self._saute_discount_factor = saute_discount_factor
        self._unsafe_reward = unsafe_reward
        self._safety_state = None
        self._safety_budget = None
        self.wrap = None
        self.num_steps = max_ep_len
        self.steps = 0
        self.use_reward_shaping = use_reward_shaping
        self.use_state_augmentation = use_state_augmentation
        if not self.use_state_augmentation:
            # passing the state without augmenting
            self._augment_state = lambda x, y: x
            self._get_state = lambda x: x

        if self.use_reward_shaping:
            self.reshape_reward = self._reshape_reward
        else:
            # passing the reward without reshaping
            self.reshape_reward = lambda x, y: x

    @property
    def safety_budget(self):
        return self._safety_budget

    @property
    def saute_discount_factor(self):
        return self._saute_discount_factor

    @property
    def unsafe_reward(self):
        return self._unsafe_reward

    def safety_step(self, cost: np.ndarray) -> np.ndarray:
        """Update the normalized safety state z' = (z - cost / d) / gamma."""
        self._safety_state -= cost / self.safety_budget
        self._safety_state /= self.saute_discount_factor
        return self._safety_state

    def step(self, action):
        """Step through the environment."""
        self.steps += 1
        next_obs, reward, done, truncated, info = self.wrap.step(action)
        next_safety_state = self.safety_step(info["cost"])
        info["true_reward"] = reward
        info["next_safety_state"] = next_safety_state
        reward = self.reshape_reward(reward, next_obs)
        augmented_state = self._augment_state(next_obs, next_safety_state)
        return augmented_state, reward, done, truncated, info

    def reset(self, seed=None, options: Dict = None) -> np.ndarray:
        """Resets the environment."""
        self.steps = 0
        state, infos = self.wrap.reset(seed=seed, options=options)
        self._safety_state = self.wrap.np_random.uniform(
            low=self.min_rel_budget, high=self.max_rel_budget
        )
        augmented_state = self._augment_state(state, self._safety_state)
        return augmented_state, infos

    def _get_state(self, state):
        """Get the true state (without augmentation)"""
        return state[:, -1].view(-1, 1)

    def _reshape_reward(
        self, reward: np.ndarray, next_safety_state: np.ndarray
    ) -> np.ndarray:
        """Reshaping the reward"""
        raise NotImplementedError

    def reshape_reward(
        self, reward: np.ndarray, next_safety_state: np.ndarray
    ) -> np.ndarray:
        """Reshaping the reward"""
        raise NotImplementedError

    def _augment_state(self, state: np.ndarray, safety_state: np.ndarray):
        """Augmenting the state with the safety state, if needed"""
        augmented_state = np.hstack([state, safety_state])
        return augmented_state


class SauteEnv(SauteBaseEnv):
    def __init__(
        self,
        env,
        safety_budget: float = 1.0,
        saute_discount_factor: float = 0.99,
        max_ep_len: int = 200,
        # minimum relative (with respect to safety_budget) budget
        min_rel_budget: float = 1.0,
        # maximum relative (with respect to safety_budget) budget
        max_rel_budget: float = 1.0,
        # test relative budget
        test_rel_budget: float = 1.0,
        unsafe_reward: float = 0,
        use_reward_shaping: bool = False,
        use_state_augmentation: bool = True,
    ):
        super().__init__(
            saute_discount_factor=saute_discount_factor,
            max_ep_len=max_ep_len,
            unsafe_reward=unsafe_reward,
            use_reward_shaping=use_reward_shaping,
            use_state_augmentation=use_state_augmentation,
        )
        # wrapping the safe environment
        self.wrap = env

        # dealing with safety budget variables
        assert safety_budget > 0, "Please specify a positive safety budget"
        self.min_rel_budget = min_rel_budget
        self.max_rel_budget = max_rel_budget
        self.test_rel_budget = test_rel_budget
        if self.saute_discount_factor < 1:
            safety_budget = (
                safety_budget
                * (1 - self.saute_discount_factor**self.max_ep_len)
                / (1 - self.saute_discount_factor)
                / float(self.max_ep_len)
            )
        self._safety_budget = np.float32(safety_budget)

        # safety state definition
        self._safety_state = 1.0

        # space definitions
        self.action_space = self.wrap.action_space
        self.obs_high = self.wrap.observation_space.high
        self.obs_low = self.wrap.observation_space.low
        if self.use_state_augmentation:
            self.obs_high = np.array(
                np.hstack([self.obs_high, np.inf]), dtype=np.float32
            )
            self.obs_low = np.array(
                np.hstack([self.obs_low, -np.inf]), dtype=np.float32
            )
        self.observation_space = spaces.Box(high=self.obs_high, low=self.obs_low)

    def _reshape_reward(
        self, reward: np.ndarray, next_safety_state: np.ndarray
    ) -> np.ndarray:
        """Reshaping the reward."""
        return reward * (next_safety_state > 0) + self.unsafe_reward * (
            next_safety_state <= 0
        )
