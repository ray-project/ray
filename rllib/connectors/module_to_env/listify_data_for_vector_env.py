from typing import Any, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import EpisodeType


class ListifyDataForVectorEnv(ConnectorV2):
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
        for column, column_data in data.copy().items():
            # Multi-agent case: Create lists of multi-agent dicts under each column.
            if isinstance(episodes[0], MultiAgentEpisode):
                # TODO (sven): Support vectorized MultiAgentEnv
                assert len(episodes) == 1
                new_column_data = [{}]

                for key, value in data[column].items():
                    assert len(value) == 1
                    eps_id, agent_id, module_id = key
                    new_column_data[0][agent_id] = value[0]
                data[column] = new_column_data
            # Single-agent case: Create simple lists under each column.
            else:
                data[column] = [
                    d for key in data[column].keys() for d in data[column][key]
                ]
                # Batch actions for (single-agent) gym.vector.Env.
                # All other columns, leave listify'ed.
                if column in [Columns.ACTIONS_FOR_ENV, Columns.ACTIONS]:
                    data[column] = batch(data[column])

        return data
