import gymnasium as gym
from typing import Optional

from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.serialization import gym_space_from_dict, gym_space_to_dict


@PublicAPI
class PettingZooEnv(MultiAgentEnv):
    """An interface to the PettingZoo MARL environment library.

    See: https://github.com/Farama-Foundation/PettingZoo

    Inherits from MultiAgentEnv and exposes a given AEC
    (actor-environment-cycle) game from the PettingZoo project via the
    MultiAgentEnv public API.

    Note that the wrapper has some important limitations:

    1. All agents have the same action_spaces and observation_spaces.
       Note: If, within your aec game, agents do not have homogeneous action /
       observation spaces, apply SuperSuit wrappers
       to apply padding functionality: https://github.com/Farama-Foundation/
       SuperSuit#built-in-multi-agent-only-functions
    2. Environments are positive sum games (-> Agents are expected to cooperate
       to maximize reward). This isn't a hard restriction, it just that
       standard algorithms aren't expected to work well in highly competitive
       games.

    Examples:
        >>> from pettingzoo.butterfly import prison_v3
        >>> from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv
        >>> env = PettingZooEnv(prison_v3.env())
        >>> obs, infos = env.reset()
        >>> print(obs)
        # only returns the observation for the agent which should be stepping
        {
            'prisoner_0': array([[[0, 0, 0],
                [0, 0, 0],
                [0, 0, 0],
                ...,
                [0, 0, 0],
                [0, 0, 0],
                [0, 0, 0]]], dtype=uint8)
        }
        >>> obs, rewards, dones, truncateds, infos = env.step({
        ...     "prisoner_0": 1
        ... })
        # only returns the observation, reward, info, etc, for
        # the agent who's turn is next.
        >>> print(obs)
        {
            'prisoner_1': array([[[0, 0, 0],
                [0, 0, 0],
                [0, 0, 0],
                ...,
                [0, 0, 0],
                [0, 0, 0],
                [0, 0, 0]]], dtype=uint8)
        }
        >>> print(rewards)
        {
            'prisoner_1': 0
        }
        >>> print(dones)
        {
            'prisoner_1': False, '__all__': False
        }
        >>> print(truncateds)
        {
            'prisoner_1': False, '__all__': False
        }
        >>> print(infos)
        {
            'prisoner_1': {'map_tuple': (1, 0)}
        }
    """

    def __init__(self, env):
        super().__init__()
        self.env = env
        env.reset()

        # Since all agents have the same spaces, do not provide full observation-
        # and action-spaces as Dicts, mapping agent IDs to the individual
        # agents' spaces. Instead, `self.[action|observation]_space` are the single
        # agent spaces.
        self._obs_space_in_preferred_format = False
        self._action_space_in_preferred_format = False

        # Collect the individual agents' spaces (they should all be the same):
        first_obs_space = self.env.observation_space(self.env.agents[0])
        first_action_space = self.env.action_space(self.env.agents[0])

        for agent in self.env.agents:
            if self.env.observation_space(agent) != first_obs_space:
                raise ValueError(
                    "Observation spaces for all agents must be identical. Perhaps "
                    "SuperSuit's pad_observations wrapper can help (useage: "
                    "`supersuit.aec_wrappers.pad_observations(env)`"
                )
            if self.env.action_space(agent) != first_action_space:
                raise ValueError(
                    "Action spaces for all agents must be identical. Perhaps "
                    "SuperSuit's pad_action_space wrapper can help (usage: "
                    "`supersuit.aec_wrappers.pad_action_space(env)`"
                )

        # Convert from gym to gymnasium, if necessary.
        self.observation_space = gym_space_from_dict(
            gym_space_to_dict(first_obs_space)
        )
        self.action_space = gym_space_from_dict(
            gym_space_to_dict(first_action_space)
        )

        self._agent_ids = set(self.env.agents)

    def reset(self, *, seed: Optional[int] = None, options: Optional[dict] = None):
        if seed is not None:
            self.env.seed(seed)

        info = self.env.reset(seed=seed, return_info=True, options=options)
        return (
            {self.env.agent_selection: self.env.observe(self.env.agent_selection)},
            info or {},
        )

    def step(self, action):
        self.env.step(action[self.env.agent_selection])
        obs_d = {}
        rew_d = {}
        done_d = {}
        truncated_d = {}
        info_d = {}
        while self.env.agents:
            obs, rew, done, truncated, info = self.env.last()
            agent_id = self.env.agent_selection
            obs_d[agent_id] = obs
            rew_d[agent_id] = rew
            done_d[agent_id] = done
            truncated_d[agent_id] = truncated
            info_d[agent_id] = info
            TODO: >???
            if self.env.termination[self.env.agent_selection]:
                self.env.step(None)
            else:
                break

        all_done = not self.env.agents
        done_d["__all__"] = all_done
        truncated_d["__all__"] = all(truncated_d.values())

        return obs_d, rew_d, done_d, truncated_d, info_d

    def close(self):
        self.env.close()

    def render(self):
        return self.env.render(self.render_mode)

    @property
    def get_sub_environments(self):
        return self.env.unwrapped


@PublicAPI
class ParallelPettingZooEnv(MultiAgentEnv):
    def __init__(self, env):
        super().__init__()
        self.par_env = env
        self.par_env.reset()

        # Since all agents have the same spaces, do not provide full observation-
        # and action-spaces as Dicts, mapping agent IDs to the individual
        # agents' spaces. Instead, `self.[action|observation]_space` are the single
        # agent spaces.
        self._obs_space_in_preferred_format = False
        self._action_space_in_preferred_format = False

        # Get first observation space, assuming all agents have equal space
        self.observation_space = self.par_env.observation_space(self.par_env.agents[0])

        # Get first action space, assuming all agents have equal space
        self.action_space = self.par_env.action_space(self.par_env.agents[0])

        assert all(
            self.par_env.observation_space(agent) == self.observation_space
            for agent in self.par_env.agents
        ), (
            "Observation spaces for all agents must be identical. Perhaps "
            "SuperSuit's pad_observations wrapper can help (useage: "
            "`supersuit.aec_wrappers.pad_observations(env)`"
        )

        assert all(
            self.par_env.action_space(agent) == self.action_space
            for agent in self.par_env.agents
        ), (
            "Action spaces for all agents must be identical. Perhaps "
            "SuperSuit's pad_action_space wrapper can help (useage: "
            "`supersuit.aec_wrappers.pad_action_space(env)`"
        )

    def reset(self, *, seed: Optional[int] = None, options: Optional[dict] = None):
        obs, info = self.par_env.reset(seed=seed, return_info=True, options=options)
        return obs, info or {}

    def step(self, action_dict):
        obss, rews, dones, truncateds, infos = self.par_env.step(action_dict)
        dones["__all__"] = all(dones.values())
        truncateds["__all__"] = all(truncateds.values())
        return obss, rews, dones, truncateds, infos

    def close(self):
        self.par_env.close()

    def render(self):
        return self.par_env.render(self.render_mode)

    @property
    def get_sub_environments(self):
        return self.par_env.unwrapped
