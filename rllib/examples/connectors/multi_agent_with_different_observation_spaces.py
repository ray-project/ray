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


class XYPosToDiscreteIndex(ConnectorV2):
    """TODO: describe """
    @override(ConnectorV2)
    def recompute_output_observation_space(
        self,
        input_observation_space,
        input_action_space,
    ) -> gym.Space:
        self._input_obs_base_struct = get_base_struct_from_space(
            self.input_observation_space
        )
        spaces = {}
        for agent_id, space in self._input_obs_base_struct.items():
            if self._agent_ids and agent_id not in self._agent_ids:
                spaces[agent_id] = self._input_obs_base_struct[agent_id]
            else:
                sample = flatten_inputs_to_1d_tensor(
                    tree.map_structure(
                        lambda s: s.sample(),
                        self._input_obs_base_struct[agent_id],
                    ),
                    self._input_obs_base_struct[agent_id],
                    batch_axis=False,
                )
                spaces[agent_id] = Box(
                    float("-inf"), float("inf"), (len(sample),), np.float32
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
            last_obs = sa_episode.get_observations(-1)

            if self._multi_agent:
                if (
                    self._agent_ids is not None
                    and sa_episode.agent_id not in self._agent_ids
                ):
                    flattened_obs = last_obs
                else:
                    flattened_obs = flatten_inputs_to_1d_tensor(
                        inputs=last_obs,
                        # In the multi-agent case, we need to use the specific agent's
                        # space struct, not the multi-agent observation space dict.
                        spaces_struct=self._input_obs_base_struct[sa_episode.agent_id],
                        # Our items are individual observations (no batch axis present).
                        batch_axis=False,
                    )
            else:
                flattened_obs = flatten_inputs_to_1d_tensor(
                    inputs=last_obs,
                    spaces_struct=self._input_obs_base_struct,
                    # Our items are individual observations (no batch axis present).
                    batch_axis=False,
                )

            # Write new observation directly back into the episode.
            sa_episode.set_observations(at_indices=-1, new_data=flattened_obs)
            #  We set the Episode's observation space to ours so that we can safely
            #  set the last obs to the new value (without causing a space mismatch
            #  error).
            sa_episode.observation_space = self.observation_space

        return batch
