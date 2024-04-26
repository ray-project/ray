from collections import defaultdict
from typing import Any, List, Optional

import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import unbatch
from ray.rllib.utils.typing import EpisodeType


class UnBatchToIndividualItems(ConnectorV2):
    """Unbatches the given `data` back into the individual-batch-items format."""

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
        # Single agent case: Memorized structure is a list, whose indices map to
        # eps_id values.
        elif isinstance(memorized_map_structure, list):
            for column, column_data in data.copy().items():
                column_data = unbatch(column_data)
                new_column_data = defaultdict(list)
                for i, eps_id in enumerate(memorized_map_structure):
                    # Keys are always tuples to resemble multi-agent keys, which
                    # have the structure (eps_id, agent_id, module_id).
                    key = (eps_id,)
                    new_column_data[key].append(column_data[i])
                data[column] = dict(new_column_data)
        # Multi-agent case: Memorized structure is dict mapping module_ids to lists of
        # (eps_id, agent_id)-tuples, such that the original individual-items-based form
        # can be constructed.
        else:
            for module_id, module_data in data.copy().items():
                if module_id not in memorized_map_structure:
                    raise KeyError(
                        f"ModuleID={module_id} not found in `memorized_map_structure`!"
                    )
                for column, column_data in module_data.items():
                    column_data = unbatch(column_data)
                    new_column_data = defaultdict(list)
                    for i, (eps_id, agent_id) in enumerate(
                        memorized_map_structure[module_id]
                    ):
                        key = (eps_id, agent_id, module_id)
                        # TODO (sven): Support vectorization for MultiAgentEnvRunner.
                        # AgentIDs whose SingleAgentEpisodes are already done, should
                        # not send any data back to the EnvRunner for further
                        # processing.
                        if episodes[0].agent_episodes[agent_id].is_done:
                            continue

                        new_column_data[key].append(column_data[i])
                    module_data[column] = dict(new_column_data)

        return data
