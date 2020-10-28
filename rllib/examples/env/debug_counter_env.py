import gym
import numpy as np

from ray.rllib.env.multi_agent_env import MultiAgentEnv


class DebugCounterEnv(gym.Env):
    """Simple Env that yields a ts counter as observation (0-based).

    Actions have no effect.
    The episode length is always 15.
    Reward is always: current ts % 3.
    """

    def __init__(self):
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Box(0, 100, (1, ))
        self.i = 0

    def reset(self):
        self.i = 0
        return [self.i]

    def step(self, action):
        self.i += 1
        return [self.i], self.i % 3, self.i >= 15, {}


class MultiAgentDebugCounterEnv(MultiAgentEnv):
    def __init__(self, config):
        self.num_agents = config["num_agents"]
        self.base_episode_len = config.get("base_episode_len", 103)
        # Actions are always:
        # (episodeID, envID) as floats.
        self.action_space = \
            gym.spaces.Box(-float("inf"), float("inf"), shape=(2, ))
        # Observation dims:
        # 0=agent ID.
        # 1=episode ID (0.0 for obs after reset).
        # 2=env ID (0.0 for obs after reset).
        # 3=ts (of the agent).
        self.observation_space = \
            gym.spaces.Box(float("-inf"), float("inf"), (4, ))
        self.timesteps = [0] * self.num_agents
        self.dones = set()

    def reset(self):
        self.timesteps = [0] * self.num_agents
        self.dones = set()
        return {
            i: np.array([i, 0.0, 0.0, 0.0], dtype=np.float32)
            for i in range(self.num_agents)
        }

    def step(self, action_dict):
        obs, rew, done = {}, {}, {}
        for i, action in action_dict.items():
            self.timesteps[i] += 1
            obs[i] = np.array([i, action[0], action[1], self.timesteps[i]])
            rew[i] = self.timesteps[i] % 3
            done[i] = True if self.timesteps[i] > self.base_episode_len + i \
                else False
            if done[i]:
                self.dones.add(i)
        done["__all__"] = len(self.dones) == self.num_agents
        return obs, rew, done, {}
