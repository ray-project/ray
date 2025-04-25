from typing import Any, Collection, Dict, List, Optional

import gymnasium as gym
from gymnasium.spaces import Box
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import flatten_inputs_to_1d_tensor
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import AgentID, EpisodeType
from ray.util.annotations import PublicAPI


class DoubleXYPosToDiscreteIndex(ConnectorV2):
    """Converts double x/y pos (2 agents) into a discrete index for one of the agents.

    TODO: describe
    """
    @override(ConnectorV2)
    def recompute_output_observation_space(
        self,
        input_observation_space,
        input_action_space,
    ) -> gym.Space:
        spaces = input_observation_space.spaces.copy()
        agent_space = spaces[self._agent_id]

        # Box.high is inclusive.
        self._env_corridor_len = agent_space.high[self._global_obs_slots[1]] + 1
        # Env has always 2 rows (and `self._env_corridor_len` columns).
        num_discrete = int(2 * self._env_corridor_len)
        spaces[self._agent_id] = gym.spaces.Discrete(num_discrete)

        return gym.spaces.Dict(spaces)

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        agent_id: AgentID,
        **kwargs,
    ):
        """Initializes a XYPosToDiscreteIndex instance.

        Args:
            agent_id: The agent ID, for which to convert the global observation,
                consisting of 2 x/y coordinates for the two agents in the env,
                into a single int index for only that agent's x/y position.
        """
        self._agent_id = agent_id
        self._global_obs_slots = [0, 1] if self._agent_id == "agent_0" else [2, 3]

        super().__init__(input_observation_space, input_action_space, **kwargs)

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Dict[str, Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        for sa_episode in self.single_agent_episode_iterator(
            episodes, agents_that_stepped_only=True
        ):
            if sa_episode.agent_id != self._agent_id:
                continue

            # Observations: positions of both agents (row, col).
            # For example: [0.0, 2.0, 1.0, 4.0] means agent_0 is in position (0, 2)
            # and agent_1 is in position (1, 4), where the first number is the row
            # index, the second number is the column index.
            last_global_obs = sa_episode.get_observations(-1)

            # [0/2] = row of agent_0, [1/3] = col of agent_0
            index_obs = (
                last_global_obs[self._global_obs_slots[0]] * self._env_corridor_len
                + last_global_obs[self._global_obs_slots[1]]
            )
            # Write new observation directly back into the episode.
            sa_episode.set_observations(at_indices=-1, new_data=index_obs)

            #  We set the Episode's observation space to ours so that we can safely
            #  set the last obs to the new value (without causing a space mismatch
            #  error).
            sa_episode.observation_space = self.observation_space[self._agent_id]

        return batch


class DoubleXYPosToSingleXYPos(ConnectorV2):
    """Converts double x/y pos (2 agents) into a single x/y pos (for one of the agents).

    TODO: describe
    """
    @override(ConnectorV2)
    def recompute_output_observation_space(
        self,
        input_observation_space,
        input_action_space,
    ) -> gym.Space:
        spaces = input_observation_space
        agent_space = spaces[self._agent_id]
        spaces[self._agent_id] = gym.spaces.Box(
            0,
            agent_space.high[self._global_obs_slots[1]],
            shape=(2,),
            dtype=np.float32,
        )
        return gym.spaces.Dict(spaces)

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        agent_id: AgentID,
        **kwargs,
    ):
        """Initializes a XYPosToDiscreteIndex instance.

        Args:
            agent_id: The agent ID, for which to convert the global observation,
                consisting of 2 x/y coordinates for the two agents in the env,
                into a single int index for only that agent's x/y position.
        """
        self._agent_id = agent_id
        self._global_obs_slots = [0, 1] if self._agent_id == "agent_0" else [2, 3]

        super().__init__(input_observation_space, input_action_space, **kwargs)

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Dict[str, Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        for sa_episode in self.single_agent_episode_iterator(
            episodes, agents_that_stepped_only=True
        ):
            if sa_episode.agent_id != self._agent_id:
                continue

            # Observations: positions of both agents (row, col).
            # For example: [0.0, 2.0, 1.0, 4.0] means agent_0 is in position (0, 2)
            # and agent_1 is in position (1, 4), where the first number is the row
            # index, the second number is the column index.
            last_global_obs = sa_episode.get_observations(-1)

            # [0/2] = row of agent_0, [1/3] = col of agent_0
            single_xy_obs = np.array([
                last_global_obs[self._global_obs_slots[0]],
                last_global_obs[self._global_obs_slots[1]]
            ], dtype=np.float32)
            # Write new observation directly back into the episode.
            sa_episode.set_observations(at_indices=-1, new_data=single_xy_obs)

            #  We set the Episode's observation space to ours so that we can safely
            #  set the last obs to the new value (without causing a space mismatch
            #  error).
            sa_episode.observation_space = self.observation_space[self._agent_id]

        return batch
