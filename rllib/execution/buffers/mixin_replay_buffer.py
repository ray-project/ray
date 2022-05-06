import collections
import platform
import random
from typing import Optional

from ray.rllib.execution.replay_ops import SimpleReplayBuffer
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.typing import PolicyID, SampleBatchType


class MixInMultiAgentReplayBuffer:
    """This buffer adds replayed samples to a stream of new experiences.

    - Any newly added batch (`add_batch()`) is immediately returned upon
    the next `replay` call (close to on-policy) as well as being moved
    into the buffer.
    - Additionally, a certain number of old samples is mixed into the
    returned sample according to a given "replay ratio".
    - If >1 calls to `add_batch()` are made without any `replay()` calls
    in between, all newly added batches are returned (plus some older samples
    according to the "replay ratio").

    Examples:
        >>> from ray.rllib.execution.replay_buffer import MixInMultiAgentReplayBuffer
        # replay ratio 0.66 (2/3 replayed, 1/3 new samples):
        >>> buffer = MixInMultiAgentReplayBuffer(capacity=100, # doctest: +SKIP
        ...                                      replay_ratio=0.66) # doctest: +SKIP
        >>> A, B, C, D = ... # doctest: +SKIP
        >>> buffer.add_batch(A) # doctest: +SKIP
        >>> buffer.add_batch(B) # doctest: +SKIP
        >>> buffer.replay() # doctest: +SKIP
        [A, B, B]
        >>> buffer.add_batch(C) # doctest: +SKIP
        >>> buffer.replay() # doctest: +SKIP
        [C, A, B]
        >>> # or: [C, A, A] or [C, B, B], but always C as it
        >>> # is the newest sample
        >>> buffer.add_batch(D) # doctest: +SKIP
        >>> buffer.replay() # doctest: +SKIP
        [D, A, C]
        >>> # replay proportion 0.0 -> replay disabled:
        >>> from ray.rllib.execution import MixInReplay
        >>> buffer = MixInReplay(capacity=100, replay_ratio=0.0) # doctest: +SKIP
        >>> buffer.add_batch(A) # doctest: +SKIP
        >>> buffer.replay() # doctest: +SKIP
        [A]
        >>> buffer.add_batch(B) # doctest: +SKIP
        >>> buffer.replay() # doctest: +SKIP
        [B]
    """

    def __init__(self, capacity: int, replay_ratio: float):
        """Initializes MixInReplay instance.

        Args:
            capacity (int): Number of batches to store in total.
            replay_ratio (float): Ratio of replayed samples in the returned
                batches. E.g. a ratio of 0.0 means only return new samples
                (no replay), a ratio of 0.5 means always return newest sample
                plus one old one (1:1), a ratio of 0.66 means always return
                the newest sample plus 2 old (replayed) ones (1:2), etc...
        """
        self.capacity = capacity
        self.replay_ratio = replay_ratio
        self.replay_proportion = None
        if self.replay_ratio != 1.0:
            self.replay_proportion = self.replay_ratio / (1.0 - self.replay_ratio)

        def new_buffer():
            return SimpleReplayBuffer(num_slots=capacity)

        self.replay_buffers = collections.defaultdict(new_buffer)

        # Metrics.
        self.add_batch_timer = TimerStat()
        self.replay_timer = TimerStat()
        self.update_priorities_timer = TimerStat()

        # Added timesteps over lifetime.
        self.num_added = 0

        # Last added batch(es).
        self.last_added_batches = collections.defaultdict(list)

    def add_batch(self, batch: SampleBatchType) -> None:
        """Adds a batch to the appropriate policy's replay buffer.

        Turns the batch into a MultiAgentBatch of the DEFAULT_POLICY_ID if
        it is not a MultiAgentBatch. Subsequently adds the individual policy
        batches to the storage.

        Args:
            batch: The batch to be added.
        """
        # Make a copy so the replay buffer doesn't pin plasma memory.
        batch = batch.copy()
        batch = batch.as_multi_agent()

        with self.add_batch_timer:
            for policy_id, sample_batch in batch.policy_batches.items():
                self.replay_buffers[policy_id].add_batch(sample_batch)
                self.last_added_batches[policy_id].append(sample_batch)
        self.num_added += batch.count

    def replay(
        self, policy_id: PolicyID = DEFAULT_POLICY_ID
    ) -> Optional[SampleBatchType]:
        buffer = self.replay_buffers[policy_id]
        # Return None, if:
        # - Buffer empty or
        # - `replay_ratio` < 1.0 (new samples required in returned batch)
        #   and no new samples to mix with replayed ones.
        if len(buffer) == 0 or (
            len(self.last_added_batches[policy_id]) == 0 and self.replay_ratio < 1.0
        ):
            return None

        # Mix buffer's last added batches with older replayed batches.
        with self.replay_timer:
            output_batches = self.last_added_batches[policy_id]
            self.last_added_batches[policy_id] = []

            # No replay desired -> Return here.
            if self.replay_ratio == 0.0:
                return SampleBatch.concat_samples(output_batches)
            # Only replay desired -> Return a (replayed) sample from the
            # buffer.
            elif self.replay_ratio == 1.0:
                return buffer.replay()

            # Replay ratio = old / [old + new]
            # Replay proportion: old / new
            num_new = len(output_batches)
            replay_proportion = self.replay_proportion
            while random.random() < num_new * replay_proportion:
                replay_proportion -= 1
                output_batches.append(buffer.replay())
            return SampleBatch.concat_samples(output_batches)

    def get_host(self) -> str:
        """Returns the computer's network name.

        Returns:
            The computer's networks name or an empty string, if the network
            name could not be determined.
        """
        return platform.node()
