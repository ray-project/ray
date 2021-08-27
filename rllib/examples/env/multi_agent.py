import gym
import random

from ray.rllib.env.multi_agent_env import MultiAgentEnv, make_multi_agent
from ray.rllib.examples.env.mock_env import MockEnv, MockEnv2
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.annotations import Deprecated


@Deprecated(
    old="ray.rllib.examples.env.multi_agent.make_multiagent",
    new="ray.rllib.env.multi_agent_env.make_multi_agent",
    error=False)
def make_multiagent(env_name_or_creator):
    return make_multi_agent(env_name_or_creator)


class BasicMultiAgent(MultiAgentEnv):
    """Env of N independent agents, each of which exits after 25 steps.

    All N agents return an observation at each timestep, thereby requesting
    actions for all agents to be provided in the next step.

    ag0: o0 a0 o1 a1 o2 a2 ...
    ag1: o0 a0 o1 a1 o2 a2 ...
    ..
    """

    def __init__(self, num_agents=4):
        """Initializes a BasicMultiAgent instance.

        Args:
            num_agents (int): The number of individual agents to create.
        """
        self.agents = [MockEnv(25) for _ in range(num_agents)]
        self.dones = set()
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)
        self.resetted = False

    def reset(self):
        self.resetted = True
        self.dones = set()
        return {i: a.reset() for i, a in enumerate(self.agents)}

    def step(self, action_dict):
        obs, rew, done, info = {}, {}, {}, {}
        for i, action in action_dict.items():
            obs[i], rew[i], done[i], info[i] = self.agents[i].step(action)
            if done[i]:
                self.dones.add(i)
        done["__all__"] = len(self.dones) == len(self.agents)
        return obs, rew, done, info


class EarlyDoneMultiAgent(MultiAgentEnv):
    """Env of 2 independent agents, exiting after 3 and 5 steps, respectively.

    The agents alternatingly publish their observations and expect respective
    actions in the next step, then the other agent published its observation,
    etc..

    ag0: o0 a0       o1 a1       ...
    ag1:       o0 a0       o1 a1 ...
    ..
    """

    def __init__(self):
        """Initializes a EarlyDoneMultiAgent instance."""
        self.agents = [MockEnv(3), MockEnv(5)]
        self.dones = set()
        self.last_obs = {}
        self.last_rew = {}
        self.last_done = {}
        self.last_info = {}
        self.i = 0
        self.observation_space = gym.spaces.Discrete(10)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        self.dones = set()
        self.last_obs = {}
        self.last_rew = {}
        self.last_done = {}
        self.last_info = {}
        self.i = 0
        for i, a in enumerate(self.agents):
            self.last_obs[i] = a.reset()
            self.last_rew[i] = None
            self.last_done[i] = False
            self.last_info[i] = {}
        obs_dict = {self.i: self.last_obs[self.i]}
        # Determine, which obs to publish in next step.
        self.i = (self.i + 1) % len(self.agents)
        return obs_dict

    def step(self, action_dict):
        assert len(self.dones) != len(self.agents)
        # Make sure user doesn't send an action for an agent, whose observation
        # we did not publish in the previous step.
        assert len(action_dict) == 1
        acting_agent = next(iter(action_dict.keys()))
        assert acting_agent != self.i
        (self.last_obs[acting_agent], self.last_rew[acting_agent],
         self.last_done[acting_agent],
         self.last_info[acting_agent]) = self.agents[acting_agent].step(
             action_dict[acting_agent])
        obs = {self.i: self.last_obs[self.i]}
        rew = {self.i: self.last_rew[self.i]}
        done = {self.i: self.last_done[self.i]}
        info = {self.i: self.last_info[self.i]}
        if done[self.i]:
            rew[self.i] = 0
            self.dones.add(self.i)
        # Determine, which obs/reward to publish in next step.
        self.i = (self.i + 1) % len(self.agents)
        # Everything is done, if one agent is done (this will always be
        # agent 0 as it's only doing 3 steps).
        done["__all__"] = len(self.dones) == len(self.agents) - 1
        assert len(obs) == 1
        return obs, rew, done, info


class FlexAgentsMultiAgent(MultiAgentEnv):
    """Env with dynamically added/removed agents, each exiting after M steps.

    Agents may publish their observations at any timestep, together or not with
    other agents. Actions are not expected to arrive in the very next step.
    Multiple rewards can be returned by the env in consecutive steps, even
    though no new observation was published in the meantime (RLlib will add
    those rewards).

    New agents spawned at any time:
    ag0: o0 a0 r0 o1 a1 r1 o2 a2 r2 o3 a3 r3 ...
    ag1:      [not spawned yet]     o0 a0 r0 ...

    Agents being removed at any time:
    ag2: o0 a0 r0 o1 a1 r1 [DONE]
    ag3:          o2 a2 r2 ... [will continue ->]

    Agents publishing obs, but not receiving actions right away:
    ag4: o0  wait->  a0 r0 o1 a1 r1
    ag5: o0 a0 r0 o1 a1 r1 o2 a2 r2

    Agents publishing reward(s), but no obs at the same time.
    ag6: o0 a0 r0       r1 o2 a2 r2  (r0+r1 will be the reward for taking a0)
    ag7: o0 a0 r0 o1 a1 r1 o2 a2 r2
    """

    def __init__(self, config=None):
        """Initializes a FlexAgentsMultiAgent instance."""
        config = config or {}
        self.agents = {}
        self.agentID = 0
        self.dones = set()
        self.observation_space = config.get("observation_space",
                                            gym.spaces.Discrete(2))
        self.action_space = config.get("action_space", gym.spaces.Discrete(2))
        self.max_episode_len = config.get("max_episode_len", 25)
        self.p_done = config.get("p_done", 0.0)
        self.resetted = False

    def spawn(self):
        # Spawn a new agent into the current episode.
        agentID = self.agentID
        from ray.rllib.examples.env.random_env import RandomEnv
        self.agents[agentID] = RandomEnv(
            config=dict(
                observation_space=self.observation_space,
                action_space=self.action_space,
                max_episode_len=self.max_episode_len,
                p_done=0.0,
            ))
        self.agentID += 1
        return agentID

    def reset(self):
        self.agents = {}
        self.spawn()
        self.resetted = True
        self.dones = set()

        obs = {}
        for i, a in self.agents.items():
            obs[i] = a.reset()

        return obs

    def step(self, action_dict):
        obs, rew, done, info = {}, {}, {}, {}
        # Apply the actions.
        for i, action in action_dict.items():
            obs[i], rew[i], done[i], info[i] = self.agents[i].step(action)
            if done[i]:
                self.dones.add(i)

        # Sometimes, add a new agent to the episode.
        if random.random() > 0.75:
            i = self.spawn()
            # Take the same action as the last agent in action_dict.
            # Normally, you would probably rather do something like:
            # obs[i] = self.agents[i].reset()
            # and leave reward/done out of the returns.
            obs[i], rew[i], done[i], info[i] = self.agents[i].step(action)
            if done[i]:
                self.dones.add(i)

        # Sometimes, kill an existing agent.
        if len(self.agents) > 1 and random.random() > 0.25:
            keys = list(self.agents.keys())
            key = random.choice(keys)
            done[key] = True
            del self.agents[key]

        done["__all__"] = len(self.dones) == len(self.agents)
        return obs, rew, done, info


class RoundRobinMultiAgent(MultiAgentEnv):
    """Env of N independent agents, each of which exits after 5 steps.

    On each step() of the env, only one agent takes an action."""

    def __init__(self, num_agents=4, increment_obs=False):
        """Initializes a RoundRobinMultiAgent instance.

        Args:
            num_agents (int): The number of individual agents to create.
            increment_obs (bool): If True, obs will increment by 1 each step.
                If False, obs will always be 0.
        """
        if increment_obs:
            # Observations are 0, 1, 2, 3... etc. as time advances
            self.agents = [MockEnv2(5) for _ in range(num_agents)]
        else:
            # Observations are all zeros.
            self.agents = [MockEnv(5) for _ in range(num_agents)]
        self.dones = set()
        self.last_obs = {}
        self.last_rew = {}
        self.last_done = {}
        self.last_info = {}
        self.i = 0
        self.num_agents = num_agents
        self.observation_space = gym.spaces.Discrete(10)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        self.dones = set()
        self.last_obs = {}
        self.last_rew = {}
        self.last_done = {}
        self.last_info = {}
        self.i = 0
        for i, a in enumerate(self.agents):
            self.last_obs[i] = a.reset()
            self.last_rew[i] = None
            self.last_done[i] = False
            self.last_info[i] = {}
        obs_dict = {self.i: self.last_obs[self.i]}
        self.i = (self.i + 1) % self.num_agents
        return obs_dict

    def step(self, action_dict):
        assert len(self.dones) != len(self.agents)
        for i, action in action_dict.items():
            (self.last_obs[i], self.last_rew[i], self.last_done[i],
             self.last_info[i]) = self.agents[i].step(action)
        obs = {self.i: self.last_obs[self.i]}
        rew = {self.i: self.last_rew[self.i]}
        done = {self.i: self.last_done[self.i]}
        info = {self.i: self.last_info[self.i]}
        if done[self.i]:
            rew[self.i] = 0
            self.dones.add(self.i)
        self.i = (self.i + 1) % self.num_agents
        done["__all__"] = len(self.dones) == len(self.agents)
        return obs, rew, done, info


MultiAgentCartPole = make_multi_agent("CartPole-v0")
MultiAgentMountainCar = make_multi_agent("MountainCarContinuous-v0")
MultiAgentPendulum = make_multi_agent("Pendulum-v0")
MultiAgentStatelessCartPole = make_multi_agent(
    lambda config: StatelessCartPole(config))
