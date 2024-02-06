from typing import Any, List, Optional

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree 

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import flatten_inputs_to_1d_tensor
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import EpisodeType


class FlattenObservations(ConnectorV2):
    """A connector piece that flattens all observation components into a 1D array.

    - Only works on data that has already been added to the batch.
    - This connector makes the assumption that under the SampleBatch.OBS key in batch,
    there is either a list of individual env observations to be flattened (single-agent
    case) or a dict mapping agent- and module IDs to lists of data items to be
    flattened (multi-agent case).
    - Does NOT work in a Learner pipeline as it operates on individual observation
    items (as opposed to batched/time-ranked data).
    - Therefore, assumes that the altered (flattened) observations will be written
    back into the episode by a later connector piece in the env-to-module pipeline
    (which this piece is part of as well).
    - Does NOT read any information from the given list of Episode objects.
    - Does NOT write any observations (or other data) to the given Episode objects.

    .. testcode::

        TODO
    """
    @property
    def observation_space(self):
        if self.input_observation_space is None:
            return None
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
        return gym.spaces.Box(
            float("-inf"),
            float("inf"),
            shape=(len(sample),),
            dtype=np.float32,
        )

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        data: Optional[Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        observations = data.get(SampleBatch.OBS)

        if observations is None:
            raise ValueError(
                f"`batch` must already have a column named {SampleBatch.OBS} in it "
                f"for this connector to work!"
            )

        # Single-agent case: There is a list of individual observation items directly
        # under the "obs" key:
        if isinstance(observations, list):
            assert isinstance(episodes[0], SingleAgentEpisode)
            data[SampleBatch.OBS] = [
                flatten_inputs_to_1d_tensor(
                    o,
                    self._input_obs_base_struct,
                    batch_axis=False,
                ) for o in observations
            ]
        # Multi-agent case: There is a dict mapping from agent/module information to
        # lists of individual data items.
        else:
            assert isinstance(episodes[0], MultiAgentEpisode)
            data[SampleBatch.OBS] = {
                k: [flatten_inputs_to_1d_tensor(o, self._input_obs_base_struct, batch_axis=False) for o in o_list]
                for k, o_list in observations.items()
            }
        return data
