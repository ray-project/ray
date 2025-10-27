from collections import defaultdict
from typing import Any, Dict, List, Optional

import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import unbatch as unbatch_fn
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class UnBatchToIndividualItems(ConnectorV2):
    """Unbatches the given `data` back into the individual-batch-items format.

    Note: This is one of the default module-to-env ConnectorV2 pieces that
    are added automatically by RLlib into every module-to-env connector pipeline,
    unless `config.add_default_connectors_to_module_to_env_pipeline` is set to
    False.

    The default module-to-env connector pipeline is:
    [
        GetActions,
        TensorToNumpy,
        UnBatchToIndividualItems,
        ModuleToAgentUnmapping,  # only in multi-agent setups!
        RemoveSingleTsTimeRankFromBatch,

        [0 or more user defined ConnectorV2 pieces],

        NormalizeAndClipActions,
        ListifyDataForVectorEnv,
    ]
    """

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
        memorized_map_structure = shared_data.get("memorized_map_structure")
        episode_map_structure = shared_data.get("vector_env_episodes_map", {})
        # Simple case (no structure stored): Just unbatch.
        if memorized_map_structure is None:
            return tree.map_structure(lambda s: unbatch_fn(s), batch)
        # Single agent case: Memorized structure is a list, whose indices map to
        # eps_id values.
        elif isinstance(memorized_map_structure, list):
            for column, column_data in batch.copy().items():
                column_data = unbatch_fn(column_data)
                new_column_data = defaultdict(list)
                for i, eps_id in enumerate(memorized_map_structure):
                    # Keys are always tuples to resemble multi-agent keys, which
                    # have the structure (eps_id, agent_id, module_id).
                    key = (eps_id,)
                    new_column_data[key].append(column_data[i])
                batch[column] = dict(new_column_data)
        # Multi-agent case: Memorized structure is dict mapping module_ids to lists of
        # (eps_id, agent_id)-tuples, such that the original individual-items-based form
        # can be constructed.
        else:
            for module_id, module_data in batch.copy().items():
                if module_id not in memorized_map_structure:
                    raise KeyError(
                        f"ModuleID={module_id} not found in `memorized_map_structure`!"
                    )
                for column, column_data in module_data.items():
                    column_data = unbatch_fn(column_data)
                    new_column_data = defaultdict(list)
                    for i, (eps_id, agent_id) in enumerate(
                        memorized_map_structure[module_id]
                    ):
                        key = (eps_id, agent_id, module_id)

                        # Check, if an agent episode is already done. For this we need
                        # to get the corresponding episode in the `EnvRunner`s list of
                        # episodes.
                        eps_id = episode_map_structure.get(eps_id, eps_id)
                        episode = next(
                            (eps for eps in episodes if eps.id_ == eps_id), None
                        )

                        if episode is None:
                            raise ValueError(
                                f"No episode found that matches the ID={eps_id}. Check "
                                "shared_data['vector_env_episodes_map'] for a missing ",
                                "mapping.",
                            )
                        # If an episode has not just started and the agent's episode
                        # is done do not return data.
                        # This should not be `True` for new `MultiAgentEpisode`s.
                        if (
                            episode.agent_episodes
                            and episode.agent_episodes[agent_id].is_done
                            and not episode.is_done
                        ):
                            continue

                        new_column_data[key].append(column_data[i])
                    module_data[column] = dict(new_column_data)

        return batch
