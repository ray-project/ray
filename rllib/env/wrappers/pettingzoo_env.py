from typing import Optional

import gymnasium as gym

from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.typing import MultiAgentDict


@PublicAPI
class PettingZooEnv(MultiAgentEnv):
    """An interface to the PettingZoo MARL environment library.

    See: https://github.com/Farama-Foundation/PettingZoo

    Inherits from MultiAgentEnv and exposes a given AEC
    (actor-environment-cycle) game from the PettingZoo project via the
    MultiAgentEnv public API.

    Note that the wrapper has the following important limitation:

    Environments are positive sum games (-> Agents are expected to cooperate
       to maximize reward). This isn't a hard restriction, it just that
       standard algorithms aren't expected to work well in highly competitive
       games.

    Also note that the earlier existing restriction of all agents having the same
    observation- and action spaces has been lifted. Different agents can now have
    different spaces and the entire environment's e.g. `self.action_space` is a Dict
    mapping agent IDs to individual agents' spaces. Same for `self.observation_space`.

    .. testcode::
        :skipif: True

        from pettingzoo.butterfly import prison_v3
        from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv
        env = PettingZooEnv(prison_v3.env())
        obs, infos = env.reset()
        # only returns the observation for the agent which should be stepping
        print(obs)

    .. testoutput::

        {
            'prisoner_0': array([[[0, 0, 0],
                [0, 0, 0],
                [0, 0, 0],
                ...,
                [0, 0, 0],
                [0, 0, 0],
                [0, 0, 0]]], dtype=uint8)
        }

    .. testcode::
        :skipif: True

        obs, rewards, terminateds, truncateds, infos = env.step({
            "prisoner_0": 1
        })
        # only returns the observation, reward, info, etc, for
        # the agent who's turn is next.
        print(obs)

    .. testoutput::

        {
            'prisoner_1': array([[[0, 0, 0],
                [0, 0, 0],
                [0, 0, 0],
                ...,
                [0, 0, 0],
                [0, 0, 0],
                [0, 0, 0]]], dtype=uint8)
        }

    .. testcode::
        :skipif: True

        print(rewards)

    .. testoutput::

        {
            'prisoner_1': 0
        }

    .. testcode::
        :skipif: True

        print(terminateds)

    .. testoutput::

        {
            'prisoner_1': False, '__all__': False
        }

    .. testcode::
        :skipif: True

        print(truncateds)

    .. testoutput::

        {
            'prisoner_1': False, '__all__': False
        }

    .. testcode::
        :skipif: True

        print(infos)

    .. testoutput::

        {
            'prisoner_1': {'map_tuple': (1, 0)}
        }
    """

    def __init__(self, env):
        super().__init__()
        self.env = env
        env.reset()

        self._agent_ids = set(self.env.agents)

        self.observation_space = gym.spaces.Dict(
            {aid: self.env.observation_space(aid) for aid in self._agent_ids}
        )
        self._obs_space_in_preferred_format = True
        self.action_space = gym.spaces.Dict(
            {aid: self.env.action_space(aid) for aid in self._agent_ids}
        )
        self._action_space_in_preferred_format = True

    def observation_space_sample(self, agent_ids: list = None) -> MultiAgentDict:
        sample = self.observation_space.sample()
        if agent_ids is None:
            return sample
        return {aid: sample[aid] for aid in agent_ids}

    def action_space_sample(self, agent_ids: list = None) -> MultiAgentDict:
        sample = self.action_space.sample()
        if agent_ids is None:
            return sample
        return {aid: sample[aid] for aid in agent_ids}

    def reset(self, *, seed: Optional[int] = None, options: Optional[dict] = None):
        info = self.env.reset(seed=seed, options=options)
        return (
            {self.env.agent_selection: self.env.observe(self.env.agent_selection)},
            info or {},
        )

    def step(self, action):
        self.env.step(action[self.env.agent_selection])
        obs_d = {}
        rew_d = {}
        terminated_d = {}
        truncated_d = {}
        info_d = {}
        while self.env.agents:
            obs, rew, terminated, truncated, info = self.env.last()
            agent_id = self.env.agent_selection
            obs_d[agent_id] = obs
            rew_d[agent_id] = rew
            terminated_d[agent_id] = terminated
            truncated_d[agent_id] = truncated
            info_d[agent_id] = info
            if (
                self.env.terminations[self.env.agent_selection]
                or self.env.truncations[self.env.agent_selection]
            ):
                self.env.step(None)
            else:
                break

        all_gone = not self.env.agents
        terminated_d["__all__"] = all_gone and all(terminated_d.values())
        truncated_d["__all__"] = all_gone and all(truncated_d.values())

        return obs_d, rew_d, terminated_d, truncated_d, info_d

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
        self._agent_ids = set(self.par_env.agents)

        self.observation_space = gym.spaces.Dict(
            {aid: self.par_env.observation_space(aid) for aid in self._agent_ids}
        )
        self._obs_space_in_preferred_format = True
        self.action_space = gym.spaces.Dict(
            {aid: self.par_env.action_space(aid) for aid in self._agent_ids}
        )
        self._action_space_in_preferred_format = True

    def reset(self, *, seed: Optional[int] = None, options: Optional[dict] = None):
        obs, info = self.par_env.reset(seed=seed, options=options)
        return obs, info or {}

    def step(self, action_dict):
        obss, rews, terminateds, truncateds, infos = self.par_env.step(action_dict)
        terminateds["__all__"] = all(terminateds.values())
        truncateds["__all__"] = all(truncateds.values())
        return obss, rews, terminateds, truncateds, infos

    def close(self):
        self.par_env.close()

    def render(self):
        return self.par_env.render(self.render_mode)

    @property
    def get_sub_environments(self):
        return self.par_env.unwrapped
