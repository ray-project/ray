from typing import Any

import numpy as np
from gymnasium import spaces

from ray.rllib import env
from rllib.examples.envs.classes.multi_agent.footsies import rl_typing
from rllib.examples.envs.classes.multi_agent.footsies.encoder import FootsiesEncoder
from rllib.examples.envs.classes.multi_agent.footsies.game import constants
from rllib.examples.envs.classes.multi_agent.footsies.game.footsies_game import (
    FootsiesGame,
)


class FootsiesEnv(env.MultiAgentEnv):
    metadata = {"render.modes": ["human"]}
    SPECIAL_CHARGE_FRAMES = 60
    GUARD_BREAK_REWARD = 0.3

    observation_space = spaces.Dict(
        {
            agent: spaces.Box(
                low=-np.inf,
                high=np.inf,
                shape=(FootsiesEncoder.observation_size,),
            )
            for agent in ["p1", "p2"]
        }
    )

    action_space = spaces.Dict(
        {
            agent: spaces.Discrete(
                len(
                    [
                        constants.EnvActions.NONE,
                        constants.EnvActions.BACK,
                        constants.EnvActions.FORWARD,
                        constants.EnvActions.ATTACK,
                        constants.EnvActions.BACK_ATTACK,
                        constants.EnvActions.FORWARD_ATTACK,
                        # This is a special input that holds down
                        # attack for 60 frames. It's just too long of a sequence
                        # to easily learn by holding ATTACK for so long.
                        constants.EnvActions.SPECIAL_CHARGE,
                    ]
                )
            )
            for agent in ["p1", "p2"]
        }
    )

    def __init__(self, config: dict[Any, Any] = None):
        super(FootsiesEnv, self).__init__()

        if config is None:
            config = {}
        self.config = config
        self.use_build_encoding = config.get("use_build_encoding", False)
        self.agents: list[rl_typing.AgentID] = ["p1", "p2"]
        self.possible_agents: list[rl_typing.AgentID] = self.agents.copy()
        self._agent_ids: set[rl_typing.AgentID] = set(self.agents)

        self.evaluation = config.get("evaluation", False)

        self.t: int = 0
        self.max_t: int = config.get("max_t", 1000)
        self.frame_skip = config.get("frame_skip", 4)
        observation_delay = config.get("observation_delay", 16)

        assert (
            observation_delay % self.frame_skip == 0
        ), "observation_delay must be divisible by frame_skip"

        self.encoder = FootsiesEncoder(
            observation_delay=observation_delay // self.frame_skip
        )

        self.game = FootsiesGame(
            host=config["host"],
            port=config["port"],
        )

        self.last_game_state = None
        self.special_charge_queue = {
            "p1": -1,
            "p2": -1,
        }

    def get_obs(self, game_state):
        if self.use_build_encoding:
            encoded_state = self.game.get_encoded_state()
            encoded_state_dict = {
                "p1": np.asarray(encoded_state.player1_encoding, dtype=np.float32),
                "p2": np.asarray(encoded_state.player2_encoding, dtype=np.float32),
            }
            return encoded_state_dict
        else:
            return self.encoder.encode(game_state)

    def reset(
        self,
        *,
        seed: int | None = None,
        options: dict | None = None,
    ) -> tuple[
        dict[rl_typing.AgentID, rl_typing.ObsType], dict[rl_typing.AgentID, Any]
    ]:
        """Resets the environment to the starting state
        and returns the initial observations for all agents.

        :return: Tuple of observations and infos for each agent.
        :rtype: tuple[dict[rl_typing.AgentID, rl_typing.ObsType], dict[rl_typing.AgentID, Any]]
        """
        self.t = 0
        self.game.reset_game()
        self.game.start_game()

        self.encoder.reset()

        if not self.use_build_encoding:
            self.last_game_state = self.game.get_state()

        observations = self.get_obs(self.last_game_state)

        return observations, {agent: {} for agent in self.agents}

    def step(self, actions: dict[rl_typing.AgentID, rl_typing.ActionType]) -> tuple[
        dict[rl_typing.AgentID, rl_typing.ObsType],
        dict[rl_typing.AgentID, float],
        dict[rl_typing.AgentID, bool],
        dict[rl_typing.AgentID, bool],
        dict[rl_typing.AgentID, dict[str, Any]],
    ]:
        """Step the environment with the provided actions for all agents.

        :param actions: Dictionary mapping agent ids to their actions for this step.
        :type actions: dict[rl_typing.AgentID, rl_typing.ActionType]
        :return: Tuple of observations, rewards, terminates, truncateds and infos for all agents.
        :rtype: tuple[ dict[rl_typing.AgentID, rl_typing.ObsType], dict[rl_typing.AgentID, float], dict[rl_typing.AgentID, bool], dict[rl_typing.AgentID, bool], dict[rl_typing.AgentID, dict[str, Any]], ]
        """
        self.t += 1

        for agent_id in self.agents:
            empty_queue = self.special_charge_queue[agent_id] < 0
            action_is_special_charge = (
                actions[agent_id] == constants.EnvActions.SPECIAL_CHARGE
            )

            # Refill the charge queue only if we're not already in a special charge.
            if action_is_special_charge and empty_queue:
                self.special_charge_queue[agent_id] = (
                    self._build_charged_special_queue()
                )

            if self.special_charge_queue[agent_id] >= 0:
                self.special_charge_queue[agent_id] -= 1
                actions[agent_id] = self._convert_to_charge_action(actions[agent_id])

        p1_action = self.game.action_to_bits(actions["p1"], is_player_1=True)
        p2_action = self.game.action_to_bits(actions["p2"], is_player_1=False)

        game_state = self.game.step_n_frames(
            p1_action=p1_action, p2_action=p2_action, n_frames=self.frame_skip
        )
        observations = self.get_obs(game_state)

        terminated = game_state.player1.is_dead or game_state.player2.is_dead

        # Zero-sum game: 1 if other player is dead, -1 if you're dead:
        rewards = {
            "p1": int(game_state.player2.is_dead) - int(game_state.player1.is_dead),
            "p2": int(game_state.player1.is_dead) - int(game_state.player2.is_dead),
        }

        if self.config.get("reward_guard_break", False):
            p1_prev_guard_health = self.last_game_state.player1.guard_health
            p2_prev_guard_health = self.last_game_state.player2.guard_health
            p1_guard_health = game_state.player1.guard_health
            p2_guard_health = game_state.player2.guard_health

            if p2_guard_health < p2_prev_guard_health:
                rewards["p1"] += self.GUARD_BREAK_REWARD
                rewards["p2"] -= self.GUARD_BREAK_REWARD
            if p1_guard_health < p1_prev_guard_health:
                rewards["p2"] += self.GUARD_BREAK_REWARD
                rewards["p1"] -= self.GUARD_BREAK_REWARD

        terminateds = {
            "p1": terminated,
            "p2": terminated,
            "__all__": terminated,
        }

        truncated = self.t >= self.max_t
        truncateds = {
            "p1": truncated,
            "p2": truncated,
            "__all__": truncated,
        }

        self.last_game_state = game_state

        return observations, rewards, terminateds, truncateds, self.get_infos()

    def get_infos(self):
        return {agent: {} for agent in self.agents}

    def _build_charged_special_queue(self):
        assert self.SPECIAL_CHARGE_FRAMES % self.frame_skip == 0
        steps_to_apply_attack = int(self.SPECIAL_CHARGE_FRAMES // self.frame_skip)
        return steps_to_apply_attack

    @staticmethod
    def _convert_to_charge_action(action: int) -> int:
        if action == constants.EnvActions.BACK:
            return constants.EnvActions.BACK_ATTACK
        elif action == constants.EnvActions.FORWARD:
            return constants.EnvActions.FORWARD_ATTACK
        else:
            return constants.EnvActions.ATTACK

    def _build_charged_queue_features(self):
        return {
            "p1": {
                "special_charge_queue": self.special_charge_queue["p1"]
                / self.SPECIAL_CHARGE_FRAMES
            },
            "p2": {
                "special_charge_queue": self.special_charge_queue["p2"]
                / self.SPECIAL_CHARGE_FRAMES
            },
        }
