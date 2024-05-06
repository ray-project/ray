import logging
import numpy as np
from pathlib import Path
import ray
from typing import Any, Dict

from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.typing import EpisodeType
logger = logging.getLogger(__name__)


class OfflineData:
    def __init__(self, config: Dict[str, Any]):

        self.config = config
        self.path = Path(config.get("input_"))
        # Use `read_json` as default data read method.
        self.data_read_method = config.get("data_read_method", "read_json")
        try:
            self.data = getattr(ray.data, self.data_read_method)(self.path)
            logger.info("Reading data from {}".format(self.path))
            logger.info(self.data.schema())
        except Exception as e:
            logger.error(e)

    def sample(
        self, num_samples: int, return_iterator: bool = False, num_shards: int = 1
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
            return self.data.take_batch(batch_size=num_samples)
        

    def _convert_to_episodes(self, batch: Dict[str, np.ndarray]):
        """Converts a batch of data to episodes."""

        for i, obs in enumerate(batch["obs"]):
            episode = SingleAgentEpisode(
                episode_id=batch["eps_id"][0],
                agent_id=batch["agent_index"][0]
            )

        

