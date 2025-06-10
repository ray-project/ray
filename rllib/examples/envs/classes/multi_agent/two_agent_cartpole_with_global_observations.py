import gymnasium as gym
import numpy as np

from ray.rllib.core import Columns
from ray.rllib.env.multi_agent_env import MultiAgentEnv


class TwoAgentCartPoleWithGlobalObservations(MultiAgentEnv):
    """TODO (sven): """

    def __init__(self, config=None):
        super().__init__()

        # Create the underlying (single-agent) CartPole env.
        self._env = gym.make("CartPole-v1")

        self.agents = self.possible_agents = ["global"]

        # Define spaces.
        self.observation_spaces = {"global": self._env.observation_space}
        self.action_spaces = {
            "global": gym.spaces.MultiDiscrete(
                [self._env.action_space.n, self._env.action_space.n]
            ),
        }

    def reset(self, *, seed=None, options=None):
        super().reset(seed=seed, options=options)
        obs, _ = self._env.reset()
        return {"global": obs}, {}

    def step(self, actions):
        move_left, move_right = actions["global"]

        # Reward both agents equally for stabilizing the pole.
        reward_agent_left = reward_agent_right = 0.5
        # Slightly reward the agent that "takes initiative" (performs the action).
        if move_left and not move_right:
            action = 0
            reward_agent_left += 0.1
        elif move_right and not move_left:
            action = 1
            reward_agent_right += 0.1
        # Penalize inconsistent behavior (both agents wanting to move or none of them).
        else:
            # Act randomly in these cases.
            action = np.random.randint(0, 2)
            #reward_agent_right += -0.1
            #reward_agent_left += -0.1

        obs, _, terminated, truncated, _ = self._env.step(action)

        return (
            # The global observation.
            {"global": obs},
            # Global reward (not used for training).
            {"global": reward_agent_left + reward_agent_right},
            # The global termination/truncation flags.
            {"__all__": terminated},
            {"__all__": truncated},
            # Individual rewards, per-agent.
            {
                "global": {
                    Columns.REWARDS + "_agent0": reward_agent_left,
                    Columns.REWARDS + "_agent1": reward_agent_right,
                },
            },
        )
