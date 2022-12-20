"""Wrap Kaggle's environment

Source: https://github.com/Kaggle/kaggle-environments
"""

from copy import deepcopy
from gymnasium.spaces import (
    Box,
    Dict as DictSpace,
    Discrete,
    MultiBinary,
    MultiDiscrete,
    Space,
    Tuple as TupleSpace,
)

try:
    import kaggle_environments
except (ImportError, ModuleNotFoundError):
    pass
import numpy as np
from typing import Any, Dict, Optional, Tuple

from ray.rllib.env import MultiAgentEnv
from ray.rllib.utils.typing import MultiAgentDict, AgentID


class KaggleFootballMultiAgentEnv(MultiAgentEnv):
    """An interface to the kaggle's football environment.

    See: https://github.com/Kaggle/kaggle-environments
    """

    def __init__(self, configuration: Optional[Dict[str, Any]] = None) -> None:
        """Initializes a Kaggle football environment.

        Args:
            configuration (Optional[Dict[str, Any]]): configuration of the
                football environment. For detailed information, see:
                https://github.com/Kaggle/kaggle-environments/blob/master/kaggle_\
                environments/envs/football/football.json
        """
        super().__init__()
        self.kaggle_env = kaggle_environments.make(
            "football", configuration=configuration or {}
        )
        self.last_cumulative_reward = None

    def reset(
        self,
        *,
        seed: Optional[int] = None,
        options: Optional[dict] = None,
    ) -> Tuple[MultiAgentDict, MultiAgentDict]:
        kaggle_state = self.kaggle_env.reset()
        self.last_cumulative_reward = None
        return {
            f"agent{idx}": self._convert_obs(agent_state["observation"])
            for idx, agent_state in enumerate(kaggle_state)
            if agent_state["status"] == "ACTIVE"
        }, {}

    def step(
        self, action_dict: Dict[AgentID, int]
    ) -> Tuple[
        MultiAgentDict, MultiAgentDict, MultiAgentDict, MultiAgentDict, MultiAgentDict
    ]:
        # Convert action_dict (used by RLlib) to a list of actions (used by
        # kaggle_environments)
        action_list = [None] * len(self.kaggle_env.state)
        for idx, agent_state in enumerate(self.kaggle_env.state):
            if agent_state["status"] == "ACTIVE":
                action = action_dict[f"agent{idx}"]
                action_list[idx] = [action]
        self.kaggle_env.step(action_list)

        # Parse (obs, reward, terminated, truncated, info) from kaggle's "state"
        # representation.
        obs = {}
        cumulative_reward = {}
        terminated = {"__all__": self.kaggle_env.done}
        truncated = {"__all__": False}
        info = {}
        for idx in range(len(self.kaggle_env.state)):
            agent_state = self.kaggle_env.state[idx]
            agent_name = f"agent{idx}"
            if agent_state["status"] == "ACTIVE":
                obs[agent_name] = self._convert_obs(agent_state["observation"])
            cumulative_reward[agent_name] = agent_state["reward"]
            terminated[agent_name] = agent_state["status"] != "ACTIVE"
            truncated[agent_name] = False
            info[agent_name] = agent_state["info"]
        # Compute the step rewards from the cumulative rewards
        if self.last_cumulative_reward is not None:
            reward = {
                agent_id: agent_reward - self.last_cumulative_reward[agent_id]
                for agent_id, agent_reward in cumulative_reward.items()
            }
        else:
            reward = cumulative_reward
        self.last_cumulative_reward = cumulative_reward
        return obs, reward, terminated, truncated, info

    def _convert_obs(self, obs: Dict[str, Any]) -> Dict[str, Any]:
        """Convert raw observations

        These conversions are necessary to make the observations fall into the
        observation space defined below.
        """
        new_obs = deepcopy(obs)
        if new_obs["players_raw"][0]["ball_owned_team"] == -1:
            new_obs["players_raw"][0]["ball_owned_team"] = 2
        if new_obs["players_raw"][0]["ball_owned_player"] == -1:
            new_obs["players_raw"][0]["ball_owned_player"] = 11
        new_obs["players_raw"][0]["steps_left"] = [
            new_obs["players_raw"][0]["steps_left"]
        ]
        return new_obs

    def build_agent_spaces(self) -> Tuple[Space, Space]:
        """Construct the action and observation spaces

        Description of actions and observations:
        https://github.com/google-research/football/blob/master/gfootball/doc/
        observation.md
        """  # noqa: E501
        action_space = Discrete(19)
        # The football field's corners are [+-1., +-0.42]. However, the players
        # and balls may get out of the field. Thus we multiply those limits by
        # a factor of 2.
        xlim = 1.0 * 2
        ylim = 0.42 * 2
        num_players: int = 11
        xy_space = Box(
            np.array([-xlim, -ylim], dtype=np.float32),
            np.array([xlim, ylim], dtype=np.float32),
        )
        xyz_space = Box(
            np.array([-xlim, -ylim, 0], dtype=np.float32),
            np.array([xlim, ylim, np.inf], dtype=np.float32),
        )
        observation_space = DictSpace(
            {
                "controlled_players": Discrete(2),
                "players_raw": TupleSpace(
                    [
                        DictSpace(
                            {
                                # ball information
                                "ball": xyz_space,
                                "ball_direction": Box(-np.inf, np.inf, (3,)),
                                "ball_rotation": Box(-np.inf, np.inf, (3,)),
                                "ball_owned_team": Discrete(3),
                                "ball_owned_player": Discrete(num_players + 1),
                                # left team
                                "left_team": TupleSpace([xy_space] * num_players),
                                "left_team_direction": TupleSpace(
                                    [xy_space] * num_players
                                ),
                                "left_team_tired_factor": Box(0.0, 1.0, (num_players,)),
                                "left_team_yellow_card": MultiBinary(num_players),
                                "left_team_active": MultiBinary(num_players),
                                "left_team_roles": MultiDiscrete([10] * num_players),
                                # right team
                                "right_team": TupleSpace([xy_space] * num_players),
                                "right_team_direction": TupleSpace(
                                    [xy_space] * num_players
                                ),
                                "right_team_tired_factor": Box(
                                    0.0, 1.0, (num_players,)
                                ),
                                "right_team_yellow_card": MultiBinary(num_players),
                                "right_team_active": MultiBinary(num_players),
                                "right_team_roles": MultiDiscrete([10] * num_players),
                                # controlled player information
                                "active": Discrete(num_players),
                                "designated": Discrete(num_players),
                                "sticky_actions": MultiBinary(10),
                                # match state
                                "score": Box(-np.inf, np.inf, (2,)),
                                "steps_left": Box(0, np.inf, (1,)),
                                "game_mode": Discrete(7),
                            }
                        )
                    ]
                ),
            }
        )
        return action_space, observation_space
