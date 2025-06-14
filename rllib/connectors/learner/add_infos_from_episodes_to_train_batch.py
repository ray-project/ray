from typing import Any, Dict, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2, ConnectorV2BatchFormats
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class AddInfosFromEpisodesToTrainBatch(ConnectorV2):
    """Adds the infos column to th train batch.

    If provided with `episodes` data, this connector piece makes sure that the final
    train batch going into the RLModule for updating (`forward_train()` call) contains
    an `infos` column.

    If the user wants to customize their own data under the given keys (e.g. obs,
    actions, ...), they can extract from the episodes or recompute from `batch`
    their own data and store it in `batch` under those keys. In this case, the default
    connector will not change the data under these keys and simply act as a
    pass-through.
    """

    # Incoming batches have the format:
    # [column name] -> [(episodeID, agentID, moduleID)-tuple] -> [.. individual items]
    # For more details on the various possible batch formats, see the
    # `ray.rllib.connectors.connector_v2.ConnectorV2BatchFormats` Enum.
    INPUT_BATCH_FORMAT = (
        ConnectorV2BatchFormats.BATCH_FORMAT_COLUMN_TO_EPISODE_TO_INDIVIDUAL_ITEMS
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
        batch: Optional[Dict[str, Any]],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        # Infos.
        if Columns.INFOS not in batch:
            for sa_episode in self.single_agent_episode_iterator(
                episodes,
                agents_that_stepped_only=False,
            ):
                self.add_n_batch_items(
                    batch,
                    Columns.INFOS,
                    items_to_add=sa_episode.get_infos(slice(0, len(sa_episode))),
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )

        return batch
