from typing import Any, List, Optional

import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import unbatch
from ray.rllib.utils.typing import EpisodeType


class UnBatchToIndividualItems(ConnectorV2):
    """TODO"""

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
        memorized_map_structure = shared_data.get("memorized_map_structure")

        # Simple case (no structure stored): Just unbatch.
        if memorized_map_structure is None:
            return tree.map_structure(lambda s: unbatch(s), data)
        # Single agent case: Stored structure is a list, whose indices map to
        # env_vector_idx values.
        elif isinstance(memorized_map_structure, list):
            for column, column_data in data.copy().items():
                column_data = unbatch(column_data)
                new_column_data = {}
                for i, env_vector_idx in enumerate(memorized_map_structure):
                    key = (env_vector_idx,)
                    if key not in new_column_data:
                        new_column_data[key] = []
                    new_column_data[key].append(column_data[i])
                data[column] = new_column_data
        # Multi-agent case: 
        else:
            # TODO(sven)
            raise NotImplementedError

        return data
