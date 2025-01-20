import gymnasium as gym
from gymnasium.spaces import Box, Discrete, Tuple
import numpy as np
from typing import Any, Dict, Optional


class AutoRegressiveActionEnv(gym.Env):
    """Environment that can only be solved through an autoregressive actions model.

    In each step, the agent observes a random number (between -1 and 1) and has
    to choose two actions, a1 (discrete, 0, 1, or 2) and a2 (cont. between -1 and 1).

    There are two reward components. The first is the negative absolute value of the
    delta between 1.0 and the sum of obs and a1. For example, if obs is -0.3 and a1
    was sampled to be 1, then the value of the first reward component is:
    r1 = -abs(1.0 - [obs+a1]) = -abs(1.0 - (-0.3 + 1)) = -abs(0.3) = -0.3

    Thus, the agent has to learn to pick a1 such that r1 gets maximized.

    The second reward component is computed

    It gets 0 reward for matching a2 to the random obs times action a1. In all
    other cases the negative deviance between the desired action a2 and its
    actual counterpart serves as reward. The reward is constructed in such a
    way that actions need to be correlated to succeed. It is not possible
    for the network to learn each action head separately.

    One way to effectively learn this is through correlated action
    distributions, e.g., in examples/rl_modules/autoregressive_action_rlm.py

    The game ends after the first step.
    """

    def __init__(self, _=None):

        # Define the action space (two continuous actions a1, a2)
        self.action_space = Tuple([Discrete(2), Discrete(2)])

        # Define the observation space (state is a single continuous value)
        self.observation_space = Box(low=-1, high=1, shape=(1,), dtype=np.float32)

        # Internal state for the environment (e.g., could represent a factor
        # influencing the relationship)
        self.obs = None

    def reset(
        self, seed: Optional[int] = None, options: Optional[Dict[str, Any]] = None
    ):
        """Reset the environment to an initial state."""
        super().reset(seed=seed, options=options)

        # Randomly initialize the state between -1 and 1
        self.obs = np.random.uniform(-1, 1, size=(1,))

        return self.obs, {}

    def step(self, action):
        """Apply the autoregressive action and return step information."""

        # Extract individual action components, a1 and a2.
        a1, a2 = action

        # The observation determines the desired relationship between a1 and a2.
        desired_a2 = self.obs[0] * a1

        # Reward is based on how close a2 is to the state-dependent autoregressive
        # relationship
        reward = -np.square(a2 - desired_a2)  # Negative absolute error as the reward

        # Optionally: add some noise or complexity to the reward function
        # reward += np.random.normal(0, 0.01)  # Small noise can be added

        # Terminate after each step (no episode length in this simple example)
        return self.obs, reward, True, False, {}
