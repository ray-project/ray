import logging
from typing import Any, Optional

import numpy as np
from gymnasium import spaces
from pettingzoo.utils.env import (
    ActionType,
    AgentID,
    ObsType,
)

from ray.rllib.env import EnvContext
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.examples.envs.classes.multi_agent.footsies.encoder import FootsiesEncoder
from ray.rllib.examples.envs.classes.multi_agent.footsies.game import constants
from ray.rllib.examples.envs.classes.multi_agent.footsies.game.footsies_binary import (
    FootsiesBinary,
)
from ray.rllib.examples.envs.classes.multi_agent.footsies.game.footsies_game import (
    FootsiesGame,
)

import psutil

logger = logging.getLogger("ray.rllib")


class FootsiesEnv(MultiAgentEnv):
    metadata = {"render.modes": ["human"]}
    SPECIAL_CHARGE_FRAMES = 60
    GUARD_BREAK_REWARD = 0.3

    observation_space = spaces.Dict(
        {
            agent: spaces.Box(
                low=-np.inf,
                high=np.inf,
                shape=(constants.OBSERVATION_SPACE_SIZE,),
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

    def __init__(self, config: EnvContext, port: int):
        super().__init__()

        if config is None:
            config = {}
        self.config = config
        self.port = port
        self.footsies_process_pid = (
            None  # Store PID of the running footsies process (we assume one per env)
        )
        self.agents: list[AgentID] = ["p1", "p2"]
        self.possible_agents: list[AgentID] = self.agents.copy()
        self._agent_ids: set[AgentID] = set(self.agents)

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

        # start the game server before initializing the communication between the
        # game server and the Python harness via gRPC
        self._prepare_and_start_game_server()
        self.game = FootsiesGame(
            host=config["host"],
            port=self.port,
        )

        self.last_game_state = None
        self.special_charge_queue = {
            "p1": -1,
            "p2": -1,
        }

    @staticmethod
    def _convert_to_charge_action(action: int) -> int:
        if action == constants.EnvActions.BACK:
            return constants.EnvActions.BACK_ATTACK
        elif action == constants.EnvActions.FORWARD:
            return constants.EnvActions.FORWARD_ATTACK
        else:
            return constants.EnvActions.ATTACK

    def close(self):
        """Terminate Footsies game server process.

        Run to ensure no game servers are left running.
        """
        timeout = 2
        try:
            logger.info(
                f"RLlib {self.__class__.__name__}: Terminating Footsies "
                f"game server process with PID: {self.footsies_process_pid}..."
            )
            p = psutil.Process(self.footsies_process_pid)
            p.terminate()
            p.wait(timeout=timeout)
        except psutil.NoSuchProcess:
            logger.info(
                f"RLlib {self.__class__.__name__}: Process with PID {self.footsies_process_pid} not found, "
                f"it might have been already terminated."
            )
        except psutil.TimeoutExpired:
            logger.warning(
                f"RLlib {self.__class__.__name__}: Process with PID {self.footsies_process_pid} did not terminate "
                f"within {timeout} seconds. "
                f"Sending SIGKILL signal instead.",
            )
            p.kill()
            p.wait(timeout=timeout)

    def get_infos(self):
        return {agent: {} for agent in self.agents}

    def get_obs(self, game_state):
        return self.encoder.encode(game_state)

    def reset(
        self,
        *,
        seed: Optional[int] = None,
        options: Optional[dict] = None,
    ) -> tuple[dict[AgentID, ObsType], dict[AgentID, Any]]:
        """Resets the environment to the starting state
        and returns the initial observations for all agents.

        :return: Tuple of observations and infos for each agent.
        :rtype: tuple[dict[AgentID, ObsType], dict[AgentID, Any]]
        """
        self.t = 0
        self.game.reset_game()
        self.game.start_game()

        self.encoder.reset()
        self.last_game_state = self.game.get_state()

        observations = self.get_obs(self.last_game_state)

        return observations, {agent: {} for agent in self.agents}

    def step(
        self, actions: dict[AgentID, ActionType]
    ) -> tuple[
        dict[AgentID, ObsType],
        dict[AgentID, float],
        dict[AgentID, bool],
        dict[AgentID, bool],
        dict[AgentID, dict[str, Any]],
    ]:
        """Step the environment with the provided actions for all agents.

        :param actions: Dictionary mapping agent ids to their actions for this step.
        :type actions: dict[AgentID, ActionType]
        :return: Tuple of observations, rewards, terminates, truncateds and infos for all agents.
        :rtype: tuple[ dict[AgentID, ObsType], dict[AgentID, float], dict[AgentID, bool], dict[AgentID, bool], dict[AgentID, dict[str, Any]], ]
        """
        self.t += 1

        for agent_id in self.agents:
            empty_queue = self.special_charge_queue[agent_id] < 0
            action_is_special_charge = (
                actions[agent_id] == constants.EnvActions.SPECIAL_CHARGE
            )

            # Refill the charge queue only if we're not already in a special charge.
            if action_is_special_charge and empty_queue:
                self.special_charge_queue[
                    agent_id
                ] = self._build_charged_special_queue()

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

    def _build_charged_special_queue(self):
        assert self.SPECIAL_CHARGE_FRAMES % self.frame_skip == 0
        steps_to_apply_attack = int(self.SPECIAL_CHARGE_FRAMES // self.frame_skip)
        return steps_to_apply_attack

    def _prepare_and_start_game_server(self):
        fb = FootsiesBinary(config=self.config, port=self.port)
        self.footsies_process_pid = fb.start_game_server()


def env_creator(env_config: EnvContext) -> FootsiesEnv:
    """Creates the Footsies environment

    Ensure that each game server runs on a unique port. Training and evaluation env runners have separate port ranges.

    Helper function to create the FootsiesEnv with a unique port based on the worker index and vector index.
    It's usually passed to the `register_env()`, like this: register_env(name="FootsiesEnv", env_creator=env_creator).
    """
    if env_config.get("env-for-evaluation", False):
        port = (
            env_config["eval_start_port"]
            - 1  # "-1" to start with eval_start_port as the first port (eval worker index starts at 1)
            + int(env_config.worker_index) * env_config.get("num_envs_per_worker", 1)
            + env_config.get("vector_index", 0)
        )
    else:
        port = (
            env_config["train_start_port"]
            + int(env_config.worker_index) * env_config.get("num_envs_per_worker", 1)
            + env_config.get("vector_index", 0)
        )
    return FootsiesEnv(config=env_config, port=port)
