from typing import Any, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import EpisodeType


class BatchIndividualItems(ConnectorV2):

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
        is_multi_agent = isinstance(episodes[0], MultiAgentEpisode)

        # TODO
        assert not is_multi_agent

        memorized_map_structure = []

        # Convert lists of individual items into properly batched data.
        for column, column_data in data.copy().items():
            # If `column` is a ModuleID, skip it (user has already provided (some)
            # data for this module and we should NOT touch this data anymore).
            #if is_multi_agent and column in rl_module:
            #    continue

            # Simple case: There is a list directly under `column`:
            # Batch the list.
            if isinstance(column_data, list):
                data[column] = batch(column_data)
            # Single-agent case: There is a dict under `column` mapping
            # `env_vector_idx` to lists of items:
            # Sort by env_vector_idx, concat all these lists, then batch.
            elif not is_multi_agent:
                list_to_be_batched = []
                for (env_vector_idx,) in sorted(column_data.keys()):
                    for item in column_data[(env_vector_idx,)]:
                        memorized_map_structure.append(env_vector_idx)
                        list_to_be_batched.append(item)
                data[column] = batch(list_to_be_batched)
            # Multi-agent case: There is a dict under `column` mapping
            # (env_vector_idx, agent_id, module_id)-tuples to lists of items:
            # Sort by env_vector_idx, concat all these lists, then batch.
            else:
                raise NotImplementedError
            #for (env_idx, agent_id, module_id), items in column_data.items():
            #    if isinstance(items, list):
            #        data[column][env_idx_agent_module_key] = batch(items)

        shared_data["memorized_map_structure"] = memorized_map_structure

        return data
