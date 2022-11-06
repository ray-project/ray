import gymnasium as gym
import numpy as np
import random

from ray.rllib.env.multi_agent_env import MultiAgentEnv, make_multi_agent
from ray.rllib.examples.env.mock_env import MockEnv, MockEnv2
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.deprecation import Deprecated


@Deprecated(
    old="ray.rllib.examples.env.multi_agent.make_multiagent",
    new="ray.rllib.env.multi_agent_env.make_multi_agent",
    error=True,
)
def make_multiagent(env_name_or_creator):
    return make_multi_agent(env_name_or_creator)


class BasicMultiAgent(MultiAgentEnv):
    """Env of N independent agents, each of which exits after 25 steps."""

    metadata = {
        "render.modes": ["rgb_array"],
    }
    render_mode = "rgb_array"

    def __init__(self, num):
        super().__init__()
        self.agents = [MockEnv(25) for _ in range(num)]
        self._agent_ids = set(range(num))
        self.dones = set()
        self.truncateds = set()
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)
        self.resetted = False

    def reset(self, *, seed=None, options=None):
        self.resetted = True
        self.dones = set()
        self.truncateds = set()
        reset_results = [a.reset() for a in self.agents]
        return (
            {i: oi[0] for i, oi in enumerate(reset_results)},
            {i: oi[1] for i, oi in enumerate(reset_results)},
        )

    def step(self, action_dict):
        obs, rew, done, truncated, info = {}, {}, {}, {}, {}
        for i, action in action_dict.items():
            obs[i], rew[i], done[i], truncated[i], info[i] = self.agents[i].step(action)
            if done[i]:
                self.dones.add(i)
            if truncated[i]:
                self.truncateds.add(i)
        done["__all__"] = len(self.dones) == len(self.agents)
        truncated["__all__"] = len(self.truncateds) == len(self.agents)
        return obs, rew, done, truncated, info

    def render(self):
        # Just generate a random image here for demonstration purposes.
        # Also see `gym/envs/classic_control/cartpole.py` for
        # an example on how to use a Viewer object.
        return np.random.randint(0, 256, size=(200, 300, 3), dtype=np.uint8)


class EarlyDoneMultiAgent(MultiAgentEnv):
    """Env for testing when the env terminates (after agent 0 does)."""

    def __init__(self):
        super().__init__()
        self.agents = [MockEnv(3), MockEnv(5)]
        self._agent_ids = set(range(len(self.agents)))
        self.dones = set()
        self.last_obs = {}
        self.last_rew = {}
        self.last_done = {}
        self.last_truncated = {}
        self.last_info = {}
        self.i = 0
        self.observation_space = gym.spaces.Discrete(10)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self, *, seed=None, options=None):
        self.dones = set()
        self.last_obs = {}
        self.last_rew = {}
        self.last_done = {}
        self.last_truncated = {}
        self.last_info = {}
        self.i = 0
        for i, a in enumerate(self.agents):
            self.last_obs[i], self.last_info[i] = a.reset()
            self.last_rew[i] = 0
            self.last_done[i] = False
            self.last_truncated[i] = False
        obs_dict = {self.i: self.last_obs[self.i]}
        info_dict = {self.i: self.last_info[self.i]}
        self.i = (self.i + 1) % len(self.agents)
        return obs_dict, info_dict

    def step(self, action_dict):
        assert len(self.dones) != len(self.agents)
        for i, action in action_dict.items():
            (
                self.last_obs[i],
                self.last_rew[i],
                self.last_done[i],
                self.last_truncated[i],
                self.last_info[i],
            ) = self.agents[i].step(action)
        obs = {self.i: self.last_obs[self.i]}
        rew = {self.i: self.last_rew[self.i]}
        done = {self.i: self.last_done[self.i]}
        truncated = {self.i: self.last_done[self.i]}
        info = {self.i: self.last_info[self.i]}
        if done[self.i]:
            rew[self.i] = 0
            self.dones.add(self.i)
        self.i = (self.i + 1) % len(self.agents)
        done["__all__"] = len(self.dones) == len(self.agents) - 1
        truncated["__all__"] = done["__all__"]
        return obs, rew, done, truncated, info


class FlexAgentsMultiAgent(MultiAgentEnv):
    """Env of independent agents, each of which exits after n steps."""

    def __init__(self):
        super().__init__()
        self.agents = {}
        self._agent_ids = set()
        self.agentID = 0
        self.dones = set()
        self.truncateds = set()
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)
        self.resetted = False

    def spawn(self):
        # Spawn a new agent into the current episode.
        agentID = self.agentID
        self.agents[agentID] = MockEnv(25)
        self._agent_ids.add(agentID)
        self.agentID += 1
        return agentID

    def reset(self, *, seed=None, options=None):
        self.agents = {}
        self._agent_ids = set()
        self.spawn()
        self.resetted = True
        self.dones = set()
        self.truncateds = set()
        obs = {}
        infos = {}
        for i, a in self.agents.items():
            obs[i], infos[i] = a.reset()

        return obs, infos

    def step(self, action_dict):
        obs, rew, done, truncated, info = {}, {}, {}, {}, {}
        # Apply the actions.
        for i, action in action_dict.items():
            obs[i], rew[i], done[i], truncated[i], info[i] = self.agents[i].step(action)
            if done[i]:
                self.dones.add(i)
            if truncated[i]:
                self.truncateds.add(i)

        # Sometimes, add a new agent to the episode.
        if random.random() > 0.75 and len(action_dict) > 0:
            i = self.spawn()
            obs[i], rew[i], done[i], truncated[i], info[i] = self.agents[i].step(action)
            if done[i]:
                self.dones.add(i)
            if truncated[i]:
                self.truncateds.add(i)

        # Sometimes, kill an existing agent.
        if len(self.agents) > 1 and random.random() > 0.25:
            keys = list(self.agents.keys())
            key = random.choice(keys)
            done[key] = True
            del self.agents[key]

        done["__all__"] = len(self.dones) == len(self.agents)
        truncated["__all__"] = len(self.truncateds) == len(self.agents)
        return obs, rew, done, truncated, info


class RoundRobinMultiAgent(MultiAgentEnv):
    """Env of N independent agents, each of which exits after 5 steps.

    On each step() of the env, only one agent takes an action."""

    def __init__(self, num, increment_obs=False):
        super().__init__()
        if increment_obs:
            # Observations are 0, 1, 2, 3... etc. as time advances
            self.agents = [MockEnv2(5) for _ in range(num)]
        else:
            # Observations are all zeros
            self.agents = [MockEnv(5) for _ in range(num)]
        self._agent_ids = set(range(num))
        self.dones = set()
        self.truncateds = set()

        self.last_obs = {}
        self.last_rew = {}
        self.last_done = {}
        self.last_truncated = {}
        self.last_info = {}
        self.i = 0
        self.num = num
        self.observation_space = gym.spaces.Discrete(10)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self, *, seed=None, options=None):
        self.dones = set()
        self.truncateds = set()

        self.last_obs = {}
        self.last_rew = {}
        self.last_done = {}
        self.last_truncated = {}
        self.last_info = {}
        self.i = 0
        for i, a in enumerate(self.agents):
            self.last_obs[i], self.last_info[i] = a.reset()
            self.last_rew[i] = 0
            self.last_done[i] = False
            self.last_truncated[i] = False
        obs_dict = {self.i: self.last_obs[self.i]}
        info_dict = {self.i: self.last_info[self.i]}
        self.i = (self.i + 1) % self.num
        return obs_dict, info_dict

    def step(self, action_dict):
        assert len(self.dones) != len(self.agents)
        for i, action in action_dict.items():
            (
                self.last_obs[i],
                self.last_rew[i],
                self.last_done[i],
                self.last_truncated[i],
                self.last_info[i],
            ) = self.agents[i].step(action)
        obs = {self.i: self.last_obs[self.i]}
        rew = {self.i: self.last_rew[self.i]}
        done = {self.i: self.last_done[self.i]}
        truncated = {self.i: self.last_truncated[self.i]}
        info = {self.i: self.last_info[self.i]}
        if done[self.i]:
            rew[self.i] = 0
            self.dones.add(self.i)
        if truncated[self.i]:
            self.truncateds.add(self.i)
        self.i = (self.i + 1) % self.num
        done["__all__"] = len(self.dones) == len(self.agents)
        truncated["__all__"] = len(self.truncateds) == len(self.agents)
        return obs, rew, done, truncated, info


MultiAgentCartPole = make_multi_agent("CartPole-v1")
MultiAgentMountainCar = make_multi_agent("MountainCarContinuous-v0")
MultiAgentPendulum = make_multi_agent("Pendulum-v1")
MultiAgentStatelessCartPole = make_multi_agent(lambda config: StatelessCartPole(config))
