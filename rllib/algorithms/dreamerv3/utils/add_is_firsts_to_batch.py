from typing import Any, List, Optional

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType


class AddIsFirstsToBatch(ConnectorV2):
    """Adds the "is_first" column to the batch."""

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Optional[Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        # If "is_first" already in batch, early out.
        if "is_first" in batch:
            return batch

        for sa_episode in self.single_agent_episode_iterator(episodes):
            self.add_batch_item(
                batch,
                "is_first",
                item_to_add=(
                    1.0 if sa_episode.t_started == 0 and len(sa_episode) == 0 else 0.0
                ),
                single_agent_episode=sa_episode,
            )
        return batch
