import logging
import numpy as np
from pathlib import Path
import ray
from typing import Any, Dict, List

from ray.rllib.core.columns import Columns
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.compression import unpack_if_needed
from ray.rllib.utils.typing import EpisodeType

logger = logging.getLogger(__name__)

SCHEMA = [
    Columns.EPS_ID,
    Columns.AGENT_ID,
    Columns.MODULE_ID,
    Columns.OBS,
    Columns.ACTIONS,
    Columns.REWARDS,
    Columns.INFOS,
    Columns.NEXT_OBS,
    Columns.TERMINATEDS,
    Columns.TRUNCATEDS,
    Columns.T,
    # TODO (simon): Add remove as soon as we are new stack only.
    "agent_index",
    "dones",
    "unroll_id",
]


class OfflineData:
    def __init__(self, config: Dict[str, Any]):

        self.config = config
        self.path = (
            config.get("input_")
            if isinstance(config.get("input_"), list)
            else Path(config.get("input_"))
        )
        # Use `read_json` as default data read method.
        self.data_read_method = config.get("data_read_method", "read_json")
        self.compressed = config.get("compressed", False)
        try:
            self.data = getattr(ray.data, self.data_read_method)(self.path)
            logger.info("Reading data from {}".format(self.path))
            logger.info(self.data.schema())
        except Exception as e:
            logger.error(e)

    def sample(
        self,
        num_samples: int,
        return_iterator: bool = False,
        num_shards: int = 1,
        as_episodes: bool = True,
    ):

        if return_iterator:
            if num_shards > 1:
                return self.data.shards(num_shards)
            else:
                return self.data.iter_batches(
                    batch_size=num_samples,
                    batch_format="numpy",
                    local_shuffle_buffer_size=num_samples * 10,
                )
        else:
            # Return a single batch
            if as_episodes:
                return self._convert_to_episodes(
                    self.data.take_batch(batch_size=num_samples)
                )
            else:
                return self.data.take_batch(batch_size=num_samples)

    def _convert_to_episodes(self, batch: Dict[str, np.ndarray]) -> List[EpisodeType]:
        """Converts a batch of data to episodes."""

        episodes = []
        # TODO (simon): Give users possibility to provide a custom schema.
        for i, obs in enumerate(batch["obs"]):
            episode = SingleAgentEpisode(
                id_=batch[Columns.EPS_ID][i][0],
                agent_id=batch[Columns.AGENT_ID][i][0]
                if Columns.AGENT_ID in batch
                else ("agent_index" if "agent_index" in batch else None),
                observations=[
                    unpack_if_needed(obs)[0],
                    unpack_if_needed(batch[Columns.NEXT_OBS][i])[0],
                ],
                infos=[{}, batch[Columns.INFOS][i][0]],
                actions=[batch[Columns.ACTIONS][i][0]],
                rewards=[batch[Columns.REWARDS][i][0]],
                terminated=batch[
                    Columns.TERMINATEDS if Columns.TERMINATEDS in batch else "dones"
                ][i][0],
                truncated=batch[Columns.TRUNCATEDS][i][0]
                if Columns.TRUNCATEDS in batch
                else False,
                # TODO (simon): Results in zerolength episodes in connector.
                # t_started=batch[Columns.T if Columns.T in batch else
                # "unroll_id"][i][0],
                extra_model_outputs={
                    key: [batch[key][i][0]] for key in batch if key not in SCHEMA
                },
                len_lookback_buffer=0,
            )
            episodes.append(episode)
        return episodes
