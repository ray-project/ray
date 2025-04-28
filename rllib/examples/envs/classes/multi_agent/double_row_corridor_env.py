import gymnasium as gym
import numpy as np

from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import AgentID


class DoubleRowCorridorEnv(MultiAgentEnv):
    """A MAEnv with a single, global observation space.

    The env

    TODO: describe


    """

    def __init__(self, config=None):
        super().__init__()

        config = config or {}

        self.length = config.get("length", 15)
        self.terminateds = {}
        self.collided = False

        # Provide information about agents and possible agents.
        self.agents = self.possible_agents = ["agent_0", "agent_1"]
        self.terminateds = {}

        # Observations: positions of both agents (row, col).
        # For example: [0.0, 2.0, 1.0, 4.0] means agent_0 is in position (0, 2)
        # and agent_1 is in position (1, 4), where the first number is the row index,
        # the second number is the column index.
        self._global_obs_space = gym.spaces.Box(
            0.0, self.length - 1, shape=(4,), dtype=np.int32
        )
        self._global_act_space = gym.spaces.Discrete(4)

    @override(MultiAgentEnv)
    def reset(self, *, seed=None, options=None):
        self.agent_pos = {
            "agent_0": [0, 0],
            "agent_1": [1, self.length - 1],
        }
        self.goals = {
            "agent_0": [0, self.length - 1],
            "agent_1": [1, 0],
        }
        self.terminateds = {agent_id: False for agent_id in self.agent_pos}
        self.collided = False

        return self._get_obs(), {}

    @override(MultiAgentEnv)
    def step(self, action_dict):
        rewards = {
            agent_id: -0.1
            for agent_id in self.agent_pos
            if not self.terminateds[agent_id]
        }

        for agent_id, action in action_dict.items():
            row, col = self.agent_pos[agent_id]

            # up
            if action == 0 and row > 0:
                row -= 1
            # down
            elif action == 1 and row < 1:
                row += 1
            # left
            elif action == 2 and col > 0:
                col -= 1
            # right
            elif action == 3 and col < self.length - 1:
                col += 1

            # Update positions.
            self.agent_pos[agent_id] = [row, col]

        obs = self._get_obs()

        # Check for collision (only if both agents are still active).
        if (
            not any(self.terminateds.values())
            and self.agent_pos["agent_0"] == self.agent_pos["agent_1"]
        ):
            if not self.collided:
                rewards["agent_0"] += 5
                rewards["agent_1"] += 5
                self.collided = True

        # Check goals.
        for agent_id in self.agent_pos:
            if (
                self.agent_pos[agent_id] == self.goals[agent_id]
                and not self.terminateds[agent_id]
            ):
                rewards[agent_id] += 10
                self.terminateds[agent_id] = True

        terminateds = {
            agent_id: self.terminateds[agent_id] for agent_id in self.agent_pos
        }
        terminateds["__all__"] = all(self.terminateds.values())

        return obs, rewards, terminateds, {}, {}

    @override(MultiAgentEnv)
    def get_observation_space(self, agent_id: AgentID) -> gym.Space:
        return self._global_obs_space

    @override(MultiAgentEnv)
    def get_action_space(self, agent_id: AgentID) -> gym.Space:
        return self._global_act_space

    def _get_obs(self):
        obs = {}
        pos = self.agent_pos
        for agent_id in self.agent_pos:
            if self.terminateds[agent_id]:
                continue
            obs[agent_id] = np.array(pos["agent_0"] + pos["agent_1"], dtype=np.int32)
        return obs


if __name__ == "__main__":
    env = DoubleRowCorridorEnv()
    print(env.observation_space)
