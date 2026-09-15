import copy
from typing import Any, Dict

import chess as ch
import numpy as np
from pettingzoo import AECEnv
from pettingzoo.classic.chess.chess import raw_env as chess_v5

from ray.rllib.env.multi_agent_env import MultiAgentEnv


class MultiAgentChess(MultiAgentEnv):
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

    .. testcode::
        :skipif: True

        from pettingzoo.butterfly import prison_v3
        from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv
        env = PettingZooEnv(prison_v3.env())
        obs = env.reset()
        print(obs)
        # only returns the observation for the agent which should be stepping

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

        obs, rewards, dones, infos = env.step({
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

        print(dones)

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

    def __init__(
        self,
        config: Dict[Any, Any] = None,
        env: AECEnv = None,
    ):
        super().__init__()
        if env is None:
            self.env = chess_v5()
        else:
            self.env = env
        self.env.reset()

        self.config = config
        if self.config is None:
            self.config = {}
        try:
            self.config["random_start"] = self.config["random_start"]
        except KeyError:
            self.config["random_start"] = 4

        # If these important attributes are not set, try to infer them.
        if not self.agents:
            self.agents = list(self._agent_ids)
        if not self.possible_agents:
            self.possible_agents = self.agents.copy()

        # Get first observation space, assuming all agents have equal space
        self.observation_space = self.env.observation_space(self.env.agents[0])

        # Get first action space, assuming all agents have equal space
        self.action_space = self.env.action_space(self.env.agents[0])

        assert all(
            self.env.observation_space(agent) == self.observation_space
            for agent in self.env.agents
        ), (
            "Observation spaces for all agents must be identical. Perhaps "
            "SuperSuit's pad_observations wrapper can help (useage: "
            "`supersuit.aec_wrappers.pad_observations(env)`"
        )

        assert all(
            self.env.action_space(agent) == self.action_space
            for agent in self.env.agents
        ), (
            "Action spaces for all agents must be identical. Perhaps "
            "SuperSuit's pad_action_space wrapper can help (usage: "
            "`supersuit.aec_wrappers.pad_action_space(env)`"
        )
        self._agent_ids = set(self.env.agents)

    def random_start(self, random_moves):
        self.env.board = ch.Board()
        for i in range(random_moves):
            self.env.board.push(np.random.choice(list(self.env.board.legal_moves)))
        return self.env.board

    def observe(self):
        return {
            self.env.agent_selection: self.env.observe(self.env.agent_selection),
            "state": self.get_state(),
        }

    def reset(self, *args, **kwargs):
        self.env.reset()
        if self.config["random_start"] > 0:
            self.random_start(self.config["random_start"])
        return (
            {self.env.agent_selection: self.env.observe(self.env.agent_selection)},
            {self.env.agent_selection: {}},
        )

    def step(self, action):
        try:
            self.env.step(action[self.env.agent_selection])
        except (KeyError, IndexError):
            self.env.step(action)
        except AssertionError:
            # Illegal action
            print(action)
            raise AssertionError("Illegal action")

        obs_d = {}
        rew_d = {}
        done_d = {}
        truncated_d = {}
        info_d = {}
        while self.env.agents:
            obs, rew, done, trunc, info = self.env.last()
            a = self.env.agent_selection
            obs_d[a] = obs
            rew_d[a] = rew
            done_d[a] = done
            truncated_d[a] = trunc
            info_d[a] = info
            if self.env.terminations[self.env.agent_selection]:
                self.env.step(None)
                done_d["__all__"] = True
                truncated_d["__all__"] = True
            else:
                done_d["__all__"] = False
                truncated_d["__all__"] = False
                break

        return obs_d, rew_d, done_d, truncated_d, info_d

    def close(self):
        self.env.close()

    def seed(self, seed=None):
        self.env.seed(seed)

    def render(self, mode="human"):
        return self.env.render(mode)

    @property
    def agent_selection(self):
        return self.env.agent_selection

    @property
    def get_sub_environments(self):
        return self.env.unwrapped

    def get_state(self):
        state = copy.deepcopy(self.env)
        return state

    def set_state(self, state):
        self.env = copy.deepcopy(state)
        return self.env.observe(self.env.agent_selection)
