from typing import Any, Dict, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class AddInfosFromEpisodesToBatch(ConnectorV2):
    """Adds the infos column to the batch.

    If provided with `episodes` data, this connector piece makes sure that the
    batch going into the RLModule' `forward_...()` calls contains the `infos` column.
    """

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        as_learner_connector: bool = False,
        **kwargs,
    ):
        super().__init__(input_observation_space, input_action_space, **kwargs)
        self._as_learner_connector = as_learner_connector

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

            if self._as_learner_connector:
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
            else:
                for sa_episode in self.single_agent_episode_iterator(
                    episodes,
                    agents_that_stepped_only=True,
                ):
                    self.add_batch_item(
                        batch,
                        Columns.INFOS,
                        item_to_add=sa_episode.get_infos(-1),
                        single_agent_episode=sa_episode,
                    )

        return batch
