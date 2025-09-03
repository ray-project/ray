import gymnasium as gym
import numpy as np
import tree

from typing import Any, Dict, List, Optional
from gymnasium.spaces import Box

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import flatten_inputs_to_1d_tensor
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import EpisodeType

# TODO (simon): Merge this with the `EnvToModule` connector like `FrameStacking`.
class FlattenObservations(ConnectorV2):
    @override(ConnectorV2)
    def recompute_output_observation_space(
        self,
        input_observation_space,
        input_action_space,
    ) -> gym.Space:
        self._input_obs_base_struct = get_base_struct_from_space(
            self.input_observation_space
        )

        sample = flatten_inputs_to_1d_tensor(
            tree.map_structure(
                lambda s: s.sample(),
                self._input_obs_base_struct,
            ),
            self._input_obs_base_struct,
            batch_axis=False,
        )
        return Box(float("-inf"), float("inf"), (len(sample),), np.float32)

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        keys_to_include: Optional[List[str]] = None,
        **kwargs,
    ):
        """Initializes a FlattenObservationsConnector instance.

        Args:

        """
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )

        self._keys_to_include = keys_to_include

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

            def _map_fn(obs, _sa_episode=sa_episode):
                batch_size = len(sa_episode)
                flattened_obs = flatten_inputs_to_1d_tensor(
                    inputs=obs,
                    # In the multi-agent case, we need to use the specific agent's
                    # space struct, not the multi-agent observation space dict.
                    spaces_struct=self._input_obs_base_struct,
                    # Our items are individual observations (no batch axis present).
                    batch_axis=False,
                )
                return flattened_obs.reshape(batch_size, -1).copy()

            self.add_n_batch_items(
                batch=batch,
                column=Columns.OBS,
                items_to_add=_map_fn(
                    sa_episode.get_observations(indices=slice(0, len(sa_episode))),
                    sa_episode,
                ),
                num_items=len(sa_episode),
                single_agent_episode=sa_episode,
            )

        return batch
