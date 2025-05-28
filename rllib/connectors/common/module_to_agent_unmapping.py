from collections import defaultdict
from typing import Any, Dict, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2, ConnectorV2BatchFormats
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ModuleToAgentUnmapping(ConnectorV2):
    """Performs flipping of `batch` from ModuleID- to AgentID based mapping.

    Before mapping:
    data[module1] -> [col, e.g. ACTIONS]
    -> [dict mapping episode-identifying tuples to lists of data]
    data[module2] -> ...

    After mapping:
    data[ACTIONS]: [dict mapping episode-identifying tuples to lists of data]

    Note that episode-identifying tuples have the form of: (episode_id,) in the
    single-agent case and (ma_episode_id, agent_id, module_id) in the multi-agent
    case.
    """

    # Incoming batches have the format:
    # [moduleID] -> [column name] -> [.. individual items]
    # For more details on the various possible batch formats, see the
    # `ray.rllib.connectors.connector_v2.ConnectorV2BatchFormats` Enum.
    INPUT_BATCH_FORMAT = (
        ConnectorV2BatchFormats.BATCH_FORMAT_MODULE_TO_COLUMN_TO_INDIVIDUAL_ITEMS
    )
    # Returned batches have the format:
    # [column name] -> [(episodeID, agentID, moduleID)-tuple] -> [.. individual items]
    # For more details on the various possible batch formats, see the
    # `ray.rllib.connectors.connector_v2.ConnectorV2BatchFormats` Enum.
    OUTPUT_BATCH_FORMAT = (
        ConnectorV2BatchFormats.BATCH_FORMAT_COLUMN_TO_EPISODE_TO_INDIVIDUAL_ITEMS
    )

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
        memorized_map_structure = (shared_data or {}).get("memorized_map_structure")
        agent_data = defaultdict(dict)

        # Single-agent case.
        if isinstance(memorized_map_structure, list):
            for module_id, module_data in batch.items():
                for column, column_data in module_data.items():
                    new_column_data = defaultdict(list)
                    for i, eps_id in enumerate(memorized_map_structure):
                        # Keys are always tuples to resemble multi-agent keys (which
                        # have the structure (eps_id, agent_id, module_id)).
                        new_column_data[(eps_id,)].append(column_data[i])
                    agent_data[column] = dict(new_column_data)

        # Multi-agent case.
        else:
            episode_map_structure = shared_data.get("vector_env_episodes_map", {})

            for module_id, module_data in batch.items():
                for column, column_data in module_data.items():
                    new_column_data = defaultdict(list)
                    for i, (eps_id, agent_id) in enumerate(
                        memorized_map_structure[module_id]
                    ):
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

                        new_column_data[(eps_id, agent_id, module_id)].append(
                            column_data[i]
                        )
                    agent_data[column].update(dict(new_column_data))

        return dict(agent_data)
