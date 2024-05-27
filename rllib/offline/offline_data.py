import functools
import logging
import numpy as np
from pathlib import Path
import ray
from typing import Any, Dict, List, Union

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.columns import Columns
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.compression import unpack_if_needed
from ray.rllib.utils.typing import EpisodeType

logger = logging.getLogger(__name__)

# TODO (simon): Implement schema mapping for users, i.e. user define
# which row name to map to which default schema name below.
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
    def __init__(self, config: Union[Dict[str, Any], AlgorithmConfig]):

        self.config = config
        self.is_multi_agent = (
            config.is_multi_agent()
            if isinstance(config, AlgorithmConfig)
            else len(config.get("policies", {})) > 1
        )
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
        self.batch_iterator = None

    def sample(
        self,
        num_samples: int,
        return_iterator: bool = False,
        num_shards: int = 1,
    ):
        if (
            not return_iterator
            or return_iterator
            and num_shards <= 1
            and not self.batch_iterator
        ):
            self.batch_iterator = self.data.map_batches(
                functools.partial(self._map_to_episodes, self.is_multi_agent)
            ).iter_batches(
                batch_size=num_samples,
                prefetch_batches=1,
                local_shuffle_buffer_size=num_samples * 10,
            )

        if return_iterator:
            if num_shards > 1:
                return self.data.map_batches(
                    functools.partial(self._map_to_episodes, self.is_multi_agent)
                ).streaming_split(n=num_shards, equal=True)
            else:
                return self.batch_iterator
        else:
            # Return a single batch
            return next(iter(self.batch_iterator))["episodes"]

    @staticmethod
    def _map_to_episodes(
        is_multi_agent: bool, batch: Dict[str, np.ndarray]
    ) -> List[EpisodeType]:
        """Maps a batch of data to episodes."""

        episodes = []
        # TODO (simon): Give users possibility to provide a custom schema.
        for i, obs in enumerate(batch["obs"]):

            # If multi-agent we need to extract the agent ID.
            # TODO (simon): Check, what happens with the module ID.
            if is_multi_agent:
                agent_id = (
                    batch[Columns.AGENT_ID][i][0]
                    if Columns.AGENT_ID in batch
                    # The old stack uses "agent_index" instead of "agent_id".
                    # TODO (simon): Remove this as soon as we are new stack only.
                    else (
                        batch["agent_index"][i][0] if "agent_index" in batch else None
                    )
                )
            else:
                agent_id = None

            if is_multi_agent:
                # TODO (simon): Add support for multi-agent episodes.
                pass
            else:
                # Build a single-agent episode with a single row of the batch.
                episode = SingleAgentEpisode(
                    id_=batch[Columns.EPS_ID][i][0],
                    agent_id=agent_id,
                    observations=[
                        unpack_if_needed(obs)[0],
                        unpack_if_needed(batch[Columns.NEXT_OBS][i])[0],
                    ],
                    infos=[
                        {},
                        batch[Columns.INFOS][i][0] if Columns.INFOS in batch else {},
                    ],
                    actions=[batch[Columns.ACTIONS][i][0]],
                    rewards=[batch[Columns.REWARDS][i][0]],
                    terminated=batch[
                        Columns.TERMINATEDS if Columns.TERMINATEDS in batch else "dones"
                    ][i][0],
                    truncated=batch[Columns.TRUNCATEDS][i][0]
                    if Columns.TRUNCATEDS in batch
                    else False,
                    # TODO (simon): Results in zero-length episodes in connector.
                    # t_started=batch[Columns.T if Columns.T in batch else
                    # "unroll_id"][i][0],
                    # TODO (simon): Single-dimensional columns are not supported.
                    extra_model_outputs={
                        key: [batch[key][i][0]] for key in batch if key not in SCHEMA
                    },
                    len_lookback_buffer=0,
                )
            episodes.append(episode)
        return {"episodes": episodes}
