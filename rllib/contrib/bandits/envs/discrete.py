import copy

import gym
import numpy as np
from gym import spaces

DEFAULT_CONFIG_LINEAR = {
    "feature_dim": 8,
    "num_actions": 4,
    "reward_noise_std": 0.01
}


class LinearDiscreteEnv(gym.Env):
    """Samples data from linearly parameterized arms.

    The reward for context X and arm i is given by X^T * theta_i, for some
    latent set of parameters {theta_i : i = 1, ..., k}.
    The thetas are sampled uniformly at random, the contexts are Gaussian,
    and Gaussian noise is added to the rewards.
    """

    def __init__(self, config=None):
        self.config = copy.copy(DEFAULT_CONFIG_LINEAR)
        if config is not None and type(config) == dict:
            self.config.update(config)

        self.feature_dim = self.config["feature_dim"]
        self.num_actions = self.config["num_actions"]
        self.sigma = self.config["reward_noise_std"]

        self.action_space = spaces.Discrete(self.num_actions)
        self.observation_space = spaces.Box(
            low=-10, high=10, shape=(self.feature_dim, ))

        self.thetas = np.random.uniform(-1, 1,
                                        (self.num_actions, self.feature_dim))
        self.thetas /= np.linalg.norm(self.thetas, axis=1, keepdims=True)

        self._elapsed_steps = 0
        self._current_context = None

    def _sample_context(self):
        return np.random.normal(scale=1 / 3, size=(self.feature_dim, ))

    def reset(self):
        self._current_context = self._sample_context()
        return self._current_context

    def step(self, action):
        assert self._elapsed_steps is not None,\
            "Cannot call env.step() beforecalling reset()"
        assert action < self.num_actions, "Invalid action."

        action = int(action)
        context = self._current_context
        rewards = self.thetas.dot(context)

        opt_action = rewards.argmax()

        regret = rewards.max() - rewards[action]

        # Add Gaussian noise
        rewards += np.random.normal(scale=self.sigma, size=rewards.shape)

        reward = rewards[action]
        self._current_context = self._sample_context()
        return self._current_context, reward, True, {
            "regret": regret,
            "opt_action": opt_action
        }

    def render(self, mode="human"):
        raise NotImplementedError


DEFAULT_CONFIG_WHEEL = {
    "delta": 0.5,
    "mu_1": 1.2,
    "mu_2": 1,
    "mu_3": 50,
    "std": 0.01
}


class WheelBanditEnv(gym.Env):
    """Wheel bandit environment for 2D contexts
    (see https://arxiv.org/abs/1802.09127).
    """

    feature_dim = 2
    num_actions = 5

    def __init__(self, config=None):
        self.config = copy.copy(DEFAULT_CONFIG_WHEEL)
        if config is not None and type(config) == dict:
            self.config.update(config)

        self.delta = self.config["delta"]
        self.mu_1 = self.config["mu_1"]
        self.mu_2 = self.config["mu_2"]
        self.mu_3 = self.config["mu_3"]
        self.std = self.config["std"]

        self.action_space = spaces.Discrete(self.num_actions)
        self.observation_space = spaces.Box(
            low=-1, high=1, shape=(self.feature_dim, ))

        self.means = [self.mu_1] + 4 * [self.mu_2]
        self._elapsed_steps = 0
        self._current_context = None

    def _sample_context(self):
        while True:
            state = np.random.uniform(-1, 1, self.feature_dim)
            if np.linalg.norm(state) <= 1:
                return state

    def reset(self):
        self._current_context = self._sample_context()
        return self._current_context

    def step(self, action):
        assert self._elapsed_steps is not None,\
            "Cannot call env.step() before calling reset()"

        action = int(action)
        self._elapsed_steps += 1
        rewards = [
            np.random.normal(self.means[j], self.std)
            for j in range(self.num_actions)
        ]
        context = self._current_context
        r_big = np.random.normal(self.mu_3, self.std)

        if np.linalg.norm(context) >= self.delta:
            if context[0] > 0:
                if context[1] > 0:
                    # First quadrant
                    rewards[1] = r_big
                    opt_action = 1
                else:
                    # Fourth quadrant
                    rewards[4] = r_big
                    opt_action = 4
            else:
                if context[1] > 0:
                    # Second quadrant
                    rewards[2] = r_big
                    opt_action = 2
                else:
                    # Third quadrant
                    rewards[3] = r_big
                    opt_action = 3
        else:
            # Smaller region where action 0 is optimal
            opt_action = 0

        reward = rewards[action]

        regret = rewards[opt_action] - reward

        self._current_context = self._sample_context()
        return self._current_context, reward, True, {
            "regret": regret,
            "opt_action": opt_action
        }

    def render(self, mode="human"):
        raise NotImplementedError
