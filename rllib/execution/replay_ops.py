from typing import List, Optional
import random

from ray.rllib.utils.replay_buffers.replay_buffer import warn_replay_capacity
from ray.rllib.utils.typing import SampleBatchType


# TODO(ekl) deprecate this in favor of the replay_sequence_length option.
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


class MixInReplay:
    """This operator adds replay to a stream of experiences.

    It takes input batches, and returns a list of batches that include replayed
    data as well. The number of replayed batches is determined by the
    configured replay proportion. The max age of a batch is determined by the
    number of replay slots.
    """

    def __init__(self, num_slots: int, replay_proportion: float):
        """Initialize MixInReplay.

        Args:
            num_slots: Number of batches to store in total.
            replay_proportion: The input batch will be returned
                and an additional number of batches proportional to this value
                will be added as well.

        Examples:
            # replay proportion 2:1
            >>> from ray.rllib.execution.replay_ops import MixInReplay
            >>> rollouts = ... # doctest: +SKIP
            >>> replay_op = MixInReplay( # doctest: +SKIP
            ...     rollouts, 100, replay_proportion=2)
            >>> print(next(replay_op)) # doctest: +SKIP
            [SampleBatch(<input>), SampleBatch(<replay>), SampleBatch(<rep.>)]
            # replay proportion 0:1, replay disabled
            >>> replay_op = MixInReplay( # doctest: +SKIP
            ...     rollouts, 100, replay_proportion=0)
            >>> print(next(replay_op)) # doctest: +SKIP
            [SampleBatch(<input>)]
        """
        if replay_proportion > 0 and num_slots == 0:
            raise ValueError("You must set num_slots > 0 if replay_proportion > 0.")
        self.replay_buffer = SimpleReplayBuffer(num_slots)
        self.replay_proportion = replay_proportion

    def __call__(self, sample_batch: SampleBatchType) -> List[SampleBatchType]:
        # Put in replay buffer if enabled.
        self.replay_buffer.add_batch(sample_batch)

        # Proportional replay.
        output_batches = [sample_batch]
        f = self.replay_proportion
        while random.random() < f:
            f -= 1
            output_batches.append(self.replay_buffer.replay())
        return output_batches
