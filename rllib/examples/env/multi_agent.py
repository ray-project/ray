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
        self.terminateds = set()
        self.truncateds = set()
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)
        self.resetted = False

    def reset(self, *, seed=None, options=None):
        self.resetted = True
        self.terminateds = set()
        self.truncateds = set()
        reset_results = [a.reset() for a in self.agents]
        return (
            {i: oi[0] for i, oi in enumerate(reset_results)},
            {i: oi[1] for i, oi in enumerate(reset_results)},
        )

    def step(self, action_dict):
        obs, rew, terminated, truncated, info = {}, {}, {}, {}, {}
        for i, action in action_dict.items():
            obs[i], rew[i], terminated[i], truncated[i], info[i] = self.agents[i].step(
                action
            )
            if terminated[i]:
                self.terminateds.add(i)
            if truncated[i]:
                self.truncateds.add(i)
        terminated["__all__"] = len(self.terminateds) == len(self.agents)
        truncated["__all__"] = len(self.truncateds) == len(self.agents)
        return obs, rew, terminated, truncated, info

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
        self.terminateds = set()
        self.truncateds = set()
        self.last_obs = {}
        self.last_rew = {}
        self.last_terminated = {}
        self.last_truncated = {}
        self.last_info = {}
        self.i = 0
        self.observation_space = gym.spaces.Discrete(10)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self, *, seed=None, options=None):
        self.terminateds = set()
        self.truncateds = set()
        self.last_obs = {}
        self.last_rew = {}
        self.last_terminated = {}
        self.last_truncated = {}
        self.last_info = {}
        self.i = 0
        for i, a in enumerate(self.agents):
            self.last_obs[i], self.last_info[i] = a.reset()
            self.last_rew[i] = 0
            self.last_terminated[i] = False
            self.last_truncated[i] = False
        obs_dict = {self.i: self.last_obs[self.i]}
        info_dict = {self.i: self.last_info[self.i]}
        self.i = (self.i + 1) % len(self.agents)
        return obs_dict, info_dict

    def step(self, action_dict):
        assert len(self.terminateds) != len(self.agents)
        for i, action in action_dict.items():
            (
                self.last_obs[i],
                self.last_rew[i],
                self.last_terminated[i],
                self.last_truncated[i],
                self.last_info[i],
            ) = self.agents[i].step(action)
        obs = {self.i: self.last_obs[self.i]}
        rew = {self.i: self.last_rew[self.i]}
        terminated = {self.i: self.last_terminated[self.i]}
        truncated = {self.i: self.last_truncated[self.i]}
        info = {self.i: self.last_info[self.i]}
        if terminated[self.i]:
            rew[self.i] = 0
            self.terminateds.add(self.i)
        if truncated[self.i]:
            rew[self.i] = 0
            self.truncateds.add(self.i)
        self.i = (self.i + 1) % len(self.agents)
        terminated["__all__"] = len(self.terminateds) == len(self.agents) - 1
        truncated["__all__"] = len(self.truncateds) == len(self.agents) - 1
        return obs, rew, terminated, truncated, info


class FlexAgentsMultiAgent(MultiAgentEnv):
    """Env of independent agents, each of which exits after n steps."""

    def __init__(self):
        super().__init__()
        self.agents = {}
        self._agent_ids = set()
        self.agentID = 0
        self.terminateds = set()
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
        self.terminateds = set()
        self.truncateds = set()
        obs = {}
        infos = {}
        for i, a in self.agents.items():
            obs[i], infos[i] = a.reset()

        return obs, infos

    def step(self, action_dict):
        obs, rew, terminated, truncated, info = {}, {}, {}, {}, {}
        # Apply the actions.
        for i, action in action_dict.items():
            obs[i], rew[i], terminated[i], truncated[i], info[i] = self.agents[i].step(
                action
            )
            if terminated[i]:
                self.terminateds.add(i)
            if truncated[i]:
                self.truncateds.add(i)

        # Sometimes, add a new agent to the episode.
        if random.random() > 0.75 and len(action_dict) > 0:
            i = self.spawn()
            obs[i], rew[i], terminated[i], truncated[i], info[i] = self.agents[i].step(
                action
            )
            if terminated[i]:
                self.terminateds.add(i)
            if truncated[i]:
                self.truncateds.add(i)

        # Sometimes, kill an existing agent.
        if len(self.agents) > 1 and random.random() > 0.25:
            keys = list(self.agents.keys())
            key = random.choice(keys)
            terminated[key] = True
            del self.agents[key]

        terminated["__all__"] = len(self.terminateds) == len(self.agents)
        truncated["__all__"] = len(self.truncateds) == len(self.agents)
        return obs, rew, terminated, truncated, info


class SometimesZeroAgentsMultiAgent(MultiAgentEnv):
    """Multi-agent env in which sometimes, no agent acts.

    At each timestep, we determine, which agents emit observations (and thereby request
    actions). This set of observing (and action-requesting) agents could be anything
    from the empty set to the full set of all agents.

    For simplicity, all agents terminate after n timesteps.
    """

    def __init__(self, num=3):
        super().__init__()
        self.num_agents = num
        self.agents = [MockEnv(25) for _ in range(self.num_agents)]
        self._agent_ids = set(range(self.num_agents))
        self._observations = {}
        self._infos = {}
        self.terminateds = set()
        self.truncateds = set()
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self, *, seed=None, options=None):
        self.terminateds = set()
        self.truncateds = set()
        self._observations = {}
        self._infos = {}
        for aid in self._get_random_agents():
            self._observations[aid], self._infos[aid] = self.agents[aid].reset()
        return self._observations, self._infos

    def step(self, action_dict):
        rew, terminated, truncated = {}, {}, {}
        # Step those agents, for which we have actions from RLlib.
        for aid, action in action_dict.items():
            (
                self._observations[aid],
                rew[aid],
                terminated[aid],
                truncated[aid],
                self._infos[aid],
            ) = self.agents[aid].step(action)
            if terminated[aid]:
                self.terminateds.add(aid)
            if truncated[aid]:
                self.truncateds.add(aid)
        # Must add the __all__ flag.
        terminated["__all__"] = len(self.terminateds) == self.num_agents
        truncated["__all__"] = len(self.truncateds) == self.num_agents

        # Select some of our observations to be published next (randomly).
        obs = {}
        infos = {}
        for aid in self._get_random_agents():
            if aid not in self._observations:
                self._observations[aid] = self.observation_space.sample()
                self._infos[aid] = {"fourty-two": 42}
            obs[aid] = self._observations.pop(aid)
            infos[aid] = self._infos.pop(aid)

        # Override some of the rewards. Rewards and dones should be always publishable,
        # even if no observation/action for an agent was sent/received.
        # An agent might get a reward because of the action of another agent. In this
        # case, the rewards for that agent are accumulated over the in-between timesteps
        # (in which the other agents step, but not this agent).
        for aid in self._get_random_agents():
            rew[aid] = np.random.rand()

        return obs, rew, terminated, truncated, infos

    def _get_random_agents(self):
        num_observing_agents = np.random.randint(self.num_agents)
        aids = np.random.permutation(self.num_agents)[:num_observing_agents]
        return {
            aid
            for aid in aids
            if aid not in self.terminateds and aid not in self.truncateds
        }


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
        self.terminateds = set()
        self.truncateds = set()

        self.last_obs = {}
        self.last_rew = {}
        self.last_terminated = {}
        self.last_truncated = {}
        self.last_info = {}
        self.i = 0
        self.num = num
        self.observation_space = gym.spaces.Discrete(10)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self, *, seed=None, options=None):
        self.terminateds = set()
        self.truncateds = set()

        self.last_obs = {}
        self.last_rew = {}
        self.last_terminated = {}
        self.last_truncated = {}
        self.last_info = {}
        self.i = 0
        for i, a in enumerate(self.agents):
            self.last_obs[i], self.last_info[i] = a.reset()
            self.last_rew[i] = 0
            self.last_terminated[i] = False
            self.last_truncated[i] = False
        obs_dict = {self.i: self.last_obs[self.i]}
        info_dict = {self.i: self.last_info[self.i]}
        self.i = (self.i + 1) % self.num
        return obs_dict, info_dict

    def step(self, action_dict):
        assert len(self.terminateds) != len(self.agents)
        for i, action in action_dict.items():
            (
                self.last_obs[i],
                self.last_rew[i],
                self.last_terminated[i],
                self.last_truncated[i],
                self.last_info[i],
            ) = self.agents[i].step(action)
        obs = {self.i: self.last_obs[self.i]}
        rew = {self.i: self.last_rew[self.i]}
        terminated = {self.i: self.last_terminated[self.i]}
        truncated = {self.i: self.last_truncated[self.i]}
        info = {self.i: self.last_info[self.i]}
        if terminated[self.i]:
            rew[self.i] = 0
            self.terminateds.add(self.i)
        if truncated[self.i]:
            self.truncateds.add(self.i)
        self.i = (self.i + 1) % self.num
        terminated["__all__"] = len(self.terminateds) == len(self.agents)
        truncated["__all__"] = len(self.truncateds) == len(self.agents)
        return obs, rew, terminated, truncated, info


class GuessTheNumberGame(MultiAgentEnv):
    """
    We have two players, 0 and 1. Agent 0 has to pick a number between 0, MAX-1
    at reset. Agent 1 has to guess the number by asking N questions of whether
    of the form of "a <number> is higher|lower|equal to the picked number. The
    action space is MultiDiscrete [3, MAX]. For the first index 0 means lower,
    1 means higher and 2 means equal. The environment answers with yes (1) or
    no (0) on the reward function. Every time step that agent 1 wastes agent 0
    gets a reward of 1. After N steps the game is terminated. If agent 1
    guesses the number correctly, it gets a reward of 100 points, otherwise it
    gets a reward of 0. On the other hand if agent 0 wins they win 100 points.
    The optimal policy controlling agent 1 should converge to a binary search
    strategy.
    """

    MAX_NUMBER = 3
    MAX_STEPS = 20

    def __init__(self, config):
        super().__init__()
        self._agent_ids = {0, 1}

        self.max_number = config.get("max_number", self.MAX_NUMBER)
        self.max_steps = config.get("max_steps", self.MAX_STEPS)

        self._number = None
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.MultiDiscrete([3, self.max_number])

    def reset(self):
        self._step = 0
        self._number = None
        # agent 0 has to pick a number. So the returned obs does not matter.
        return {0: 0}

    def step(self, action_dict):
        # get agent 0's action
        agent_0_action = action_dict.get(0)

        if agent_0_action is not None:
            # ignore the first part of the action and look at the number
            self._number = agent_0_action[1]
            # next obs should tell agent 1 to start guessing.
            # the returned reward and dones should be on agent 0 who picked a
            # number.
            return {1: 0}, {0: 0}, {0: False, "__all__": False}, {}

        if self._number is None:
            raise ValueError(
                "No number is selected by agent 0. Have you restarted "
                "the environment?"
            )

        # get agent 1's action
        direction, number = action_dict.get(1)
        info = {}
        # always the same, we don't need agent 0 to act ever again, agent 1 should keep
        # guessing.
        obs = {1: 0}
        guessed_correctly = False
        # everytime agent 1 does not guess correctly agent 0 gets a reward of 1.
        if direction == 0:  # lower
            reward = {1: int(number > self._number), 0: 1}
            done = {1: False, "__all__": False}
        elif direction == 1:  # higher
            reward = {1: int(number < self._number), 0: 1}
            done = {1: False, "__all__": False}
        else:  # equal
            guessed_correctly = number == self._number
            reward = {1: guessed_correctly * 100, 0: guessed_correctly * -100}
            done = {1: guessed_correctly, "__all__": guessed_correctly}

        self._step += 1
        if self._step >= self.max_steps:  # max number of steps episode is over
            done["__all__"] = True
            if not guessed_correctly:
                reward[0] = 100  # agent 0 wins
        return obs, reward, done, info


MultiAgentCartPole = make_multi_agent("CartPole-v1")
MultiAgentMountainCar = make_multi_agent("MountainCarContinuous-v0")
MultiAgentPendulum = make_multi_agent("Pendulum-v1")
MultiAgentStatelessCartPole = make_multi_agent(lambda config: StatelessCartPole(config))
