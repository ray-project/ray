import chess as ch
import numpy as np
import gym.spaces as spaces
from pettingzoo import AECEnv
from pettingzoo.utils import wrappers
from pettingzoo.utils.agent_selector import agent_selector
from pettingzoo.classic.chess import chess_utils
import copy
from ray.rllib.env.multi_agent_env import MultiAgentEnv


def PettingChessEnvWrapper():
    env = PettingChessEnv()
    env = wrappers.CaptureStdoutWrapper(env)
    env = wrappers.TerminateIllegalWrapper(env, illegal_reward=-1)
    env = wrappers.AssertOutOfBoundsWrapper(env)
    env = wrappers.OrderEnforcingWrapper(env)
    return env


class PettingChessEnv(AECEnv):

    metadata = {
        "render_modes": ["human"],
        "name": "chess_v6",
        "is_parallelizable": False,
        "render_fps": 2,
    }

    def __init__(self):
        super().__init__()

        self.board = ch.Board()

        self.agents = [f"player_{i}" for i in range(2)]
        self.possible_agents = self.agents[:]

        self._agent_selector = agent_selector(self.agents)

        self.action_spaces = {name: spaces.Discrete(8 * 8 * 73) for name in self.agents}
        self.observation_spaces = {
            name: spaces.Dict(
                {
                    "observation": spaces.Box(
                        low=0, high=1, shape=(8, 8, 111), dtype=bool
                    ),
                    "action_mask": spaces.Box(
                        low=0, high=1, shape=(4672,), dtype=np.int8
                    ),
                }
            )
            for name in self.agents
        }

        self.rewards = {name: 0 for name in self.agents}
        self.dones = {name: False for name in self.agents}
        self.infos = {name: {} for name in self.agents}

        self.agent_selection = None

        self.board_history = np.zeros((8, 8, 104), dtype=bool)

    def observation_space(self, agent):
        return self.observation_spaces[agent]

    def action_space(self, agent):
        return self.action_spaces[agent]

    def observe(self, agent):
        observation = chess_utils.get_observation(
            self.board, self.possible_agents.index(agent)
        )
        observation = np.dstack((observation[:, :, :7], self.board_history))
        legal_moves = (
            chess_utils.legal_moves(self.board) if agent == self.agent_selection else []
        )

        action_mask = np.zeros(4672, "int8")
        for i in legal_moves:
            action_mask[i] = 1

        return {"observation": observation, "action_mask": action_mask}

    def reset(self, seed=None, return_info=False, options=None):
        self.has_reset = True

        self.agents = self.possible_agents[:]

        self.board = ch.Board()

        self._agent_selector = agent_selector(self.agents)
        self.agent_selection = self._agent_selector.reset()

        self.rewards = {name: 0 for name in self.agents}
        self._cumulative_rewards = {name: 0 for name in self.agents}
        self.dones = {name: False for name in self.agents}
        self.infos = {name: {} for name in self.agents}

        self.board_history = np.zeros((8, 8, 104), dtype=bool)
        self.game_over = False

    def set_game_result(self, result_val):
        for i, name in enumerate(self.agents):
            self.dones[name] = True
            result_coef = 1 if i == 0 else -1
            self.rewards[name] = result_val * result_coef
            self.infos[name] = {"legal_moves": []}

    def step(self, action):
        if action is None or self.dones["player_0"] or self.dones["player_1"]:
            return self._was_done_step(action)
        current_agent = self.agent_selection
        current_index = self.agents.index(current_agent)
        ### This current index mirrors the move
        try:
            chosen_move = chess_utils.action_to_move(self.board, action, current_index)
        except:
            chosen_move = chess_utils.action_to_move(
                self.board, action["player_" + str(current_index)], current_index
            )
        try:
            assert chosen_move in list(self.board.legal_moves)
        except AssertionError:
            print(f"{current_agent} {current_index} chose {chosen_move} {action}")
            print("legal moves:", self.board.legal_moves)
            print(self.board)
            self.game_over = True
            self.set_game_result(not self.board.turn)
            self._accumulate_rewards()
            self.agent_selection = (
                self._agent_selector.next()
            )  # Give turn to the next agent
            self.dones["player_0"], self.dones["player_1"] = True, True
            return None

        self.board.push(chosen_move)

        next_legal_moves = chess_utils.legal_moves(self.board)

        is_stale_or_checkmate = not any(next_legal_moves)

        # claim draw is set to be true to align with normal tournament rules
        is_repetition = self.board.is_repetition(3)
        is_50_move_rule = self.board.can_claim_fifty_moves()
        is_claimable_draw = is_repetition or is_50_move_rule
        game_over = is_claimable_draw or is_stale_or_checkmate

        if game_over:
            self.game_over = True
            result = self.board.result(claim_draw=True)
            result_val = chess_utils.result_to_int(result)
            self.set_game_result(result_val)

        self._accumulate_rewards()

        # Update board after applying action
        next_board = chess_utils.get_observation(self.board, current_agent)
        self.board_history = np.dstack(
            (next_board[:, :, 7:], self.board_history[:, :, :-13])
        )
        self.agent_selection = (
            self._agent_selector.next()
        )  # Give turn to the next agent

    def render(self, mode="human"):
        print(self.board)

    def close(self):
        pass

    def get_state(self):
        return copy.deepcopy(self.env.board)

    def set_state(self, state):
        self.board = state
        return self.board

    def random_start(self, random_moves):
        self.board = ch.Board()
        for i in range(random_moves):
            self.board.push(np.random.choice(list(self.board.legal_moves)))
        return self.board


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
    Examples:
        >>> from pettingzoo.butterfly import prison_v3
        >>> from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv
        >>> env = PettingZooEnv(prison_v3.env())
        >>> obs = env.reset()
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
        >>> obs, rewards, dones, infos = env.step({
        ...                 "prisoner_0": 1
        ...             })
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
        >>> print(infos)
        {
            'prisoner_1': {'map_tuple': (1, 0)}
        }
    """

    def __init__(self, config={"random_start": 4}, env=PettingChessEnv()):
        super().__init__()
        self.env = env
        env.reset()
        # TODO (avnishn): Remove this after making petting zoo env compatible with
        #  check_env.
        self._skip_env_checking = True
        self.config = config
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

    def observe(self):
        return {
            self.env.agent_selection: self.env.observe(self.env.agent_selection),
            "state": self.get_state(),
        }

    def reset(self):
        self.env.reset()
        if self.config["random_start"] > 0:
            self.env.random_start(self.config["random_start"])
        return {self.env.agent_selection: self.env.observe(self.env.agent_selection)}

    def step(self, action):
        try:
            self.env.step(action[self.env.agent_selection])
        except (KeyError, IndexError):
            self.env.step(action)

        obs_d = {}
        rew_d = {}
        done_d = {}
        info_d = {}
        while self.env.agents:
            obs, rew, done, info = self.env.last()
            a = self.env.agent_selection
            obs_d[a] = obs
            rew_d[a] = rew
            done_d[a] = done
            info_d[a] = info
            if self.env.dones[self.env.agent_selection]:

                self.env.step(None)
                done_d["__all__"] = True
            else:
                done_d["__all__"] = False
                break

        return obs_d, rew_d, done_d, info_d

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
