from typing import Any, Dict, Optional

import gymnasium as gym
import numpy as np


class CorrelatedActionsEnv(gym.Env):
    """Environment that can only be solved through an autoregressive action model.

    In each step, the agent observes a random number (between -1 and 1) and has
    to choose two actions, a1 (discrete, 0, 1, or 2) and a2 (cont. between -1 and 1).

    The reward is constructed such that actions need to be correlated to succeed. It's
    impossible for the network to learn each action head separately.

    There are two reward components:
    The first is the negative absolute value of the delta between 1.0 and the sum of
    obs + a1. For example, if obs is -0.3 and a1 was sampled to be 1, then the value of
    the first reward component is:
    r1 = -abs(1.0 - [obs+a1]) = -abs(1.0 - (-0.3 + 1)) = -abs(0.3) = -0.3
    The second reward component is computed as the negative absolute value
    of `obs + a1 + a2`. For example, if obs is 0.5, a1 was sampled to be 0,
    and a2 was sampled to be -0.7, then the value of the second reward component is:
    r2 = -abs(obs + a1 + a2) = -abs(0.5 + 0 - 0.7)) = -abs(-0.2) = -0.2

    Because of this specific reward function, the agent must learn to optimally sample
    a1 based on the observation and to optimally sample a2, based on the observation
    AND the sampled value of a1.

    One way to effectively learn this is through correlated action
    distributions, e.g., in examples/actions/auto_regressive_actions.py

    The game ends after the first step.
    """

    def __init__(self, config=None):
        super().__init__()
        # Observation space (single continuous value between -1. and 1.).
        self.observation_space = gym.spaces.Box(-1.0, 1.0, shape=(1,), dtype=np.float32)

        # Action space (discrete action a1 and continuous action a2).
        self.action_space = gym.spaces.Tuple(
            [gym.spaces.Discrete(3), gym.spaces.Box(-2.0, 2.0, (1,), np.float32)]
        )

        # Internal state for the environment (e.g., could represent a factor
        # influencing the relationship)
        self.obs = None

    def reset(
        self, seed: Optional[int] = None, options: Optional[Dict[str, Any]] = None
    ):
        """Reset the environment to an initial state."""
        super().reset(seed=seed, options=options)

        # Randomly initialize the observation between -1 and 1.
        self.obs = np.random.uniform(-1, 1, size=(1,))

        return self.obs, {}

    def step(self, action):
        """Apply the autoregressive action and return step information."""

        # Extract individual action components, a1 and a2.
        a1, a2 = action
        a2 = a2[0]  # dissolve shape=(1,)

        # r1 depends on how well a1 is aligned to obs:
        r1 = -abs(1.0 - (self.obs[0] + a1))
        # r2 depends on how well a2 is aligned to both, obs and a1.
        r2 = -abs(self.obs[0] + a1 + a2)

        reward = r1 + r2

        # Optionally: add some noise or complexity to the reward function
        # reward += np.random.normal(0, 0.01)  # Small noise can be added

        # Terminate after each step (no episode length in this simple example)
        return self.obs, reward, True, False, {}
