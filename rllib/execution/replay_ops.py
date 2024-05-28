from typing import Optional
import random

from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.replay_buffers.replay_buffer import warn_replay_capacity
from ray.rllib.utils.typing import SampleBatchType


@OldAPIStack
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
        warn_replay_capacity(item=sample_batch, num_items=self.num_slots)
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
