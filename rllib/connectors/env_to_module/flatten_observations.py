from typing import Any, List, Optional

import gymnasium as gym
from gymnasium.spaces import Box
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import flatten_inputs_to_1d_tensor
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
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
        # TODO (sven): We should handle this differently. We probably need another
        #  API method for ConnectorV2 in case the `input_observation_space` is changed
        #  after construction (for example, when the connector piece is inserted into
        #  some pipeline).
        self._input_obs_base_struct = get_base_struct_from_space(
            self.input_observation_space
        )
        if self._multi_agent:
            spaces = {}
            for agent_id, space in self._input_obs_base_struct.items():
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
        else:
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
        input_observation_space,
        input_action_space,
        *,
        multi_agent: bool = False,
        **kwargs,
    ):
        """Initializes a FlattenObservations instance.

        Args:
            multi_agent: Whether this connector operates on multi-agent observations,
                in which case, the top-level of the Dict space (where agent IDs are
                mapped to individual agents' observation spaces) is left as-is.
        """
        super().__init__(input_observation_space, input_action_space, **kwargs)

        self._multi_agent = multi_agent

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

        # Process each item under the SampleBatch.OBS key individually and flatten
        # it. We are using the `ConnectorV2.foreach_batch_item_change_in_place` API,
        # allowing us to not worry about multi- or single-agent setups and returning
        # the new version of each item we are iterating over.
        self.foreach_batch_item_change_in_place(
            func=(
                lambda item, agent_id, module_id: flatten_inputs_to_1d_tensor(
                    item,
                    # In the multi-agent case, we need to use the specific agent's space
                    # struct, not the multi-agent observation space dict.
                    (
                        self._input_obs_base_struct
                        if not agent_id
                        else self._input_obs_base_struct[agent_id]
                    ),
                    # Our items are bare observations (no batch axis present).
                    batch_axis=False,
                )
            ),
            batch=data,
            column=SampleBatch.OBS,
        )
        return data
