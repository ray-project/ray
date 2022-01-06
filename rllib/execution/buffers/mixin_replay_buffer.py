import collections
import platform
import random
from typing import Optional

from ray.rllib.execution.replay_ops import SimpleReplayBuffer
from ray.rllib.policy.sample_batch import SampleBatch
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
        # replay ratio 0.66 (2/3 replayed, 1/3 new samples):
        >>> buffer = MixInMultiAgentReplayBuffer(capacity=100, replay_ratio=0.66)
        >>> buffer.add_batch(<A>)
        >>> buffer.add_batch(<B>)
        >>> buffer.replay()
        ... [<A>, <B>, <B>]
        >>> buffer.add_batch(<C>)
        >>> buffer.replay()
        ... [<C>, <A>, <B>]
        >>> # or: [<C>, <A>, <A>] or [<C>, <B>, <B>], but always <C> as it
        >>> # is the newest sample

        >>> buffer.add_batch(<D>)
        >>> buffer.replay()
        ... [<D>, <A>, <C>]

        # replay proportion 0.0 -> replay disabled:
        >>> buffer = MixInReplay(capacity=100, replay_ratio=0.0)
        >>> buffer.add_batch(<A>)
        >>> buffer.replay()
        ... [<A>]
        >>> buffer.add_batch(<B>)
        >>> buffer.replay()
        ... [<B>]
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
        self.last_added_batches = []

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
        self.num_added += batch.count

    def replay(self, policy_id: PolicyID) -> Optional[SampleBatchType]:
        if self.num_added == 0:
            return None
        with self.replay_timer:
            output_batches = self.replay_buffers[policy_id].last_added_batches.copy()

            # replay ratio = old / [old + new]
            num_new = len(output_batches)
            num_old = 0
            while random.random() > num_old / (num_old + num_new):
                num_old += 1
                output_batches.append(self.replay_buffers[policy_id].replay())
            return_batch = SampleBatch.concat_samples(output_batches)
            #print(f"returning batch {return_batch}")
            return return_batch

    def get_host(self) -> str:
        """Returns the computer's network name.

        Returns:
            The computer's networks name or an empty string, if the network
            name could not be determined.
        """
        return platform.node()
