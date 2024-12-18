import gymnasium as gym
import numpy as np

from ray.rllib.env.multi_agent_env import MultiAgentEnv


class DebugCounterEnv(gym.Env):
    """Simple Env that yields a ts counter as observation (0-based).

    Actions have no effect.
    The episode length is always 15.
    Reward is always: current ts % 3.
    """

    def __init__(self, config=None):
        config = config or {}
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Box(0, 100, (1,), dtype=np.float32)
        self.start_at_t = int(config.get("start_at_t", 0))
        self.i = self.start_at_t

    def reset(self, *, seed=None, options=None):
        self.i = self.start_at_t
        return self._get_obs(), {}

    def step(self, action):
        self.i += 1
        terminated = False
        truncated = self.i >= 15 + self.start_at_t
        return self._get_obs(), float(self.i % 3), terminated, truncated, {}

    def _get_obs(self):
        return np.array([self.i], dtype=np.float32)


class MultiAgentDebugCounterEnv(MultiAgentEnv):
    def __init__(self, config):
        super().__init__()
        self.num_agents = config["num_agents"]
        self.base_episode_len = config.get("base_episode_len", 103)

        # Observation dims:
        # 0=agent ID.
        # 1=episode ID (0.0 for obs after reset).
        # 2=env ID (0.0 for obs after reset).
        # 3=ts (of the agent).
        self.observation_space = gym.spaces.Dict(
            {
                aid: gym.spaces.Box(float("-inf"), float("inf"), (4,))
                for aid in range(self.num_agents)
            }
        )

        # Actions are always:
        # (episodeID, envID) as floats.
        self.action_space = gym.spaces.Dict(
            {
                aid: gym.spaces.Box(-float("inf"), float("inf"), shape=(2,))
                for aid in range(self.num_agents)
            }
        )

        self.timesteps = [0] * self.num_agents
        self.terminateds = set()
        self.truncateds = set()

    def reset(self, *, seed=None, options=None):
        self.timesteps = [0] * self.num_agents
        self.terminateds = set()
        self.truncateds = set()
        return {
            i: np.array([i, 0.0, 0.0, 0.0], dtype=np.float32)
            for i in range(self.num_agents)
        }, {}

    def step(self, action_dict):
        obs, rew, terminated, truncated = {}, {}, {}, {}
        for i, action in action_dict.items():
            self.timesteps[i] += 1
            obs[i] = np.array([i, action[0], action[1], self.timesteps[i]])
            rew[i] = self.timesteps[i] % 3
            terminated[i] = False
            truncated[i] = (
                True if self.timesteps[i] > self.base_episode_len + i else False
            )
            if terminated[i]:
                self.terminateds.add(i)
            if truncated[i]:
                self.truncateds.add(i)
        terminated["__all__"] = len(self.terminateds) == self.num_agents
        truncated["__all__"] = len(self.truncateds) == self.num_agents
        return obs, rew, terminated, truncated, {}
