from typing import Any, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType


class ListifyData(ConnectorV2):
    """Performs conversion from ConnectorV2-style format to env/episode insertion.

    Single agent case:
    Convert from:
    [col] -> [(env_vector_idx,)] -> [list of items].
    To:
    [col] -> [list of items, sorted by env vector idx].

    Multi-agent case:
    Convert from:
    [col] -> [(env_vector_idx, agent_id, module_id)] -> list of items.
    To:
    [col] -> [list of multi-agent dicts, sorted by env vector idx].
    """

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
        # Multi-agent case: Create lists of multi-agent dicts under each column.
        if isinstance(episodes[0], MultiAgentEpisode):
            for column, column_data in data.copy().items():
                new_column_data = []
                for key, value in sorted(data[column].items()):
                    env_vector_idx, agent_id, module_id = key
                    if len(new_column_data) <= env_vector_idx:
                        new_column_data.append({})
                    new_column_data[env_vector_idx][agent_id] = value
                data[column] = new_column_data
        # Single-agent case: Create simple lists under each column.
        else:
            for column, column_data in data.copy().items():
                data[column] = [
                    d
                    for key in sorted(data[column].keys())
                    for d in data[column][key]
                ]
        return data
