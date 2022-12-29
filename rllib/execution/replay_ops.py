import logging
from typing import Optional
import random

# Import ray before psutil will make sure we use psutil's bundled version
import ray  # noqa F401
import psutil

from ray.util.debug import log_once
from ray.rllib.utils.typing import SampleBatchType

logger = logging.getLogger(__name__)


# TODO(sven) deprecate this class.
class SimpleReplayBuffer:
    """Simple replay buffer that operates over batches."""

    def __init__(self, num_slots: int, replay_proportion: Optional[float] = None):
        """Initialize SimpleReplayBuffer.

        Args:
            num_slots: Number of batches to store in total.
        """
        self.num_slots = num_slots
        self.replay_batches = []
        self.replay_index = 0

    def add_batch(self, sample_batch: SampleBatchType) -> None:
        self.warn_replay_capacity(item=sample_batch, num_items=self.num_slots)
        if self.num_slots > 0:
            if len(self.replay_batches) < self.num_slots:
                self.replay_batches.append(sample_batch)
            else:
                self.replay_batches[self.replay_index] = sample_batch
                self.replay_index += 1
                self.replay_index %= self.num_slots

    def replay(self) -> SampleBatchType:
        return random.choice(self.replay_batches)

    def __len__(self):
        return len(self.replay_batches)

    @staticmethod
    def warn_replay_capacity(*, item: SampleBatchType, num_items: int) -> None:
        """Warn if the configured replay buffer capacity is too large."""
        if log_once("replay_capacity"):
            item_size = item.size_bytes()
            psutil_mem = psutil.virtual_memory()
            total_gb = psutil_mem.total / 1e9
            mem_size = num_items * item_size / 1e9
            msg = (
                "Estimated max memory usage for replay buffer is {} GB "
                "({} batches of size {}, {} bytes each), "
                "available system memory is {} GB".format(
                    mem_size, num_items, item.count, item_size, total_gb
                )
            )
            if mem_size > total_gb:
                raise ValueError(msg)
            elif mem_size > 0.2 * total_gb:
                logger.warning(msg)
            else:
                logger.info(msg)
