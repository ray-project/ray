import random
from typing import Optional

from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer
from ray.rllib.utils.replay_buffers.utils import warn_replay_buffer_capacity
from ray.rllib.utils.typing import SampleBatchType


class SimpleReplayBuffer(ReplayBuffer):
    """Simple replay buffer that operates over entire batches."""

    def __init__(
        self,
        capacity: int,
        storage_unit: str = "episodes",
        learning_starts: int = 0,
        **kwargs
    ):
        """Initialize a SimpleReplayBuffer instance."""
        super().__init__(capacity=capacity, storage_unit="episodes", **kwargs)
        self.replay_batches = []
        self.replay_index = 0
        self.learning_starts = learning_starts

    @override(ReplayBuffer)
    def add(self, batch: SampleBatchType, **kwargs) -> None:
        warn_replay_buffer_capacity(item=batch, capacity=self.capacity)
        if self.capacity > 0:
            if len(self.replay_batches) < self.capacity:
                self.replay_batches.append(batch)
            else:
                self.replay_batches[self.replay_index] = batch
                self.replay_index += 1
                self.replay_index %= self.capacity

    @override(ReplayBuffer)
    def sample(self, num_items: int, **kwargs) -> Optional[SampleBatchType]:
        # If `learning_starts` not reached yet, return None.
        if self.learning_starts < len(self):
            return None
        return random.choice(self.replay_batches)

    @override(ReplayBuffer)
    def __len__(self):
        return len(self.replay_batches)
