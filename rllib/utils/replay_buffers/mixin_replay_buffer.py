import collections
import random
from typing import Optional, Dict, Any

from ray.rllib.policy.sample_batch import (
    DEFAULT_POLICY_ID,
    SampleBatch,
    MultiAgentBatch,
)
from ray.rllib.utils.annotations import override, ExperimentalAPI
from ray.rllib.utils.replay_buffers.multi_agent_replay_buffer import (
    MultiAgentReplayBuffer,
)
from ray.rllib.utils.typing import PolicyID, SampleBatchType
from ray.rllib.execution.buffers.replay_buffer import _ALL_POLICIES


@ExperimentalAPI
class MixInMultiAgentReplayBuffer(MultiAgentReplayBuffer):
    """This buffer adds replayed samples to a stream of new experiences.

    - Any newly added batch (`add_batch()`) is immediately returned upon
    the next `sample` call (close to on-policy) as well as being moved
    into the buffer.
    - Additionally, a certain number of old samples is mixed into the
    returned sample according to a given "replay ratio".
    - If >1 calls to `add()` are made without any `sample()` calls
    in between, all newly added batches are returned (plus some older samples
    according to the "replay ratio").

    Examples:
        # replay ratio 0.66 (2/3 replayed, 1/3 new samples):
        >>> buffer = MixInMultiAgentReplayBuffer(capacity=100,
        ...                                      replay_ratio=0.66)
        >>> buffer.add(<A>)
        >>> buffer.add(<B>)
        >>> buffer.replay()
        ... [<A>, <B>, <B>]
        >>> buffer.add(<C>)
        >>> buffer.sample()
        ... [<C>, <A>, <B>]
        >>> # or: [<C>, <A>, <A>], [<C>, <B>, <A>] or [<C>, <B>, <B>],
        >>> # but always <C> as it is the newest sample

        >>> buffer.add(<D>)
        >>> buffer.sample()
        ... [<D>, <A>, <C>]

        # replay proportion 0.0 -> replay disabled:
        >>> buffer = MixInReplay(capacity=100, replay_ratio=0.0)
        >>> buffer.add(<A>)
        >>> buffer.sample()
        ... [<A>]
        >>> buffer.add(<B>)
        >>> buffer.sample()
        ... [<B>]
    """

    def __init__(
        self,
        capacity: int = 10000,
        storage_unit: str = "timesteps",
        num_shards: int = 1,
        learning_starts: int = 1000,
        replay_batch_size: int = 1,
        prioritized_replay_alpha: float = 0.6,
        prioritized_replay_beta: float = 0.4,
        prioritized_replay_eps: float = 1e-6,
        replay_mode: str = "independent",
        replay_sequence_length: int = 1,
        replay_burn_in: int = 0,
        replay_zero_init_states: bool = True,
        replay_ratio: float = 0.66,
    ):
        """Initializes MixInMultiAgentReplayBuffer instance.

        Args:
            capacity: Number of batches to store in total.
            storage_unit (str): Either 'sequences' or 'timesteps'. Specifies
                how experiences are stored.
            num_shards: The number of buffer shards that exist in total
                (including this one).
            learning_starts: Number of timesteps after which a call to
                `replay()` will yield samples (before that, `replay()` will
                return None).
            capacity: The capacity of the buffer. Note that when
                `replay_sequence_length` > 1, this is the number of sequences
                (not single timesteps) stored.
            replay_batch_size: The batch size to be sampled (in timesteps).
                Note that if `replay_sequence_length` > 1,
                `self.replay_batch_size` will be set to the number of
                sequences sampled (B).
            prioritized_replay_alpha: Alpha parameter for a prioritized
                replay buffer. Use 0.0 for no prioritization.
            prioritized_replay_beta: Beta parameter for a prioritized
                replay buffer.
            prioritized_replay_eps: Epsilon parameter for a prioritized
                replay buffer.
            replay_mode: One of "independent" or "lockstep". Determined,
                whether in the multiagent case, sampling is done across all
                agents/policies equally.
            replay_sequence_length: The sequence length (T) of a single
                sample. If > 1, we will sample B x T from this buffer.
            replay_burn_in: The burn-in length in case
                `replay_sequence_length` > 0. This is the number of timesteps
                each sequence overlaps with the previous one to generate a
                better internal state (=state after the burn-in), instead of
                starting from 0.0 each RNN rollout.
            replay_zero_init_states: Whether the initial states in the
                buffer (if replay_sequence_length > 0) are alwayas 0.0 or
                should be updated with the previous train_batch state outputs.
            replay_ratio: Ratio of replayed samples in the returned
                batches. E.g. a ratio of 0.0 means only return new samples
                (no replay), a ratio of 0.5 means always return newest sample
                plus one old one (1:1), a ratio of 0.66 means always return
                the newest sample plus 2 old (replayed) ones (1:2), etc...
        """
        if not 0 < replay_ratio < 1:
            raise ValueError("Replay ratio must be within [0, 1]")

        MultiAgentReplayBuffer.__init__(
            self,
            capacity,
            storage_unit,
            num_shards,
            learning_starts,
            replay_batch_size,
            prioritized_replay_alpha,
            prioritized_replay_beta,
            prioritized_replay_eps,
            replay_mode,
            replay_sequence_length,
            replay_burn_in,
            replay_zero_init_states,
        )

        self.replay_ratio = replay_ratio
        self.replay_proportion = None
        if self.replay_ratio != 1.0:
            self.replay_proportion = self.replay_ratio / (1.0 - self.replay_ratio)

        # Last added batch(es).
        self.last_added_batches = collections.defaultdict(list)

    @ExperimentalAPI
    @override(MultiAgentReplayBuffer)
    def add(self, batch: SampleBatchType) -> None:
        """Adds a batch to the appropriate policy's replay buffer.

        Turns the batch into a MultiAgentBatch of the DEFAULT_POLICY_ID if
        it is not a MultiAgentBatch. Subsequently adds the individual policy
        batches to the storage.

        Args:
            batch: The batch to be added.
        """
        # Make a copy so the replay buffer doesn't pin plasma memory.
        batch = batch.copy()
        # Handle everything as if multi-agent.
        batch = batch.as_multi_agent()

        with self.add_batch_timer:
            # Lockstep mode: Store under _ALL_POLICIES key (we will always
            # only sample from all policies at the same time).
            if self.replay_mode == "lockstep":
                # Note that prioritization is not supported in this mode.
                for s in batch.timeslices(self.replay_sequence_length):
                    self.replay_buffers[_ALL_POLICIES].add(s, weight=None)
                    self.last_added_batches[_ALL_POLICIES].append(s)
            else:
                for policy_id, sample_batch in batch.policy_batches.items():
                    self._add_to_policy_buffer(policy_id, sample_batch)
                    self.last_added_batches[policy_id].append(sample_batch)
        self._num_added += batch.count

    @ExperimentalAPI
    @override(MultiAgentReplayBuffer)
    def sample(
        self, num_items: int, policy_id: PolicyID = DEFAULT_POLICY_ID
    ) -> Optional[SampleBatchType]:
        """Samples a batch of size `num_items` from a specified buffer.

        If this buffer was given a fake batch, return it, otherwise
        return a MultiAgentBatch with samples. If less than `num_items`
        records are in this buffer, some samples in
        the results may be repeated to fulfil the batch size (`num_items`)
        request.

        Args:
            num_items: Number of items to sample from this buffer.
            policy_id: ID of the policy that produced the experiences to be
            sampled.

        Returns:
            Concatenated batch of items.
        """
        if self._fake_batch:
            if not isinstance(self._fake_batch, MultiAgentBatch):
                self._fake_batch = SampleBatch(self._fake_batch).as_multi_agent()
            return self._fake_batch

        def mix_batches(_policy_id):
            _buffer = self.replay_buffers[policy_id]
            output_batches = self.last_added_batches[_policy_id]
            self.last_added_batches[_policy_id] = []

            # No replay desired
            if self.replay_ratio == 0.0:
                return SampleBatch.concat_samples(output_batches)
            # Only replay desired
            elif self.replay_ratio == 1.0:
                return _buffer.sample(num_items, beta=self.prioritized_replay_beta)

            # Replay ratio = old / [old + new]
            # Replay proportion: old / new
            num_new = len(output_batches)
            replay_proportion = self.replay_proportion
            while random.random() < num_new * replay_proportion:
                replay_proportion -= 1
                output_batches.append(_buffer.sample(num_items))
            return SampleBatch.concat_samples(output_batches)

        def check_buffer_is_ready(_policy_id):
            if (len(self.replay_buffers[policy_id]) == 0) or (
                len(self.last_added_batches[_policy_id]) == 0
                and self.replay_ratio < 1.0
            ):
                return False
            return True

        with self.replay_timer:
            if self.replay_mode == "lockstep":
                assert (
                    policy_id is None
                ), "`policy_id` specifier not allowed in `locksetp` mode!"
                if check_buffer_is_ready(_ALL_POLICIES):
                    return mix_batches(_ALL_POLICIES)
            elif policy_id is not None:
                if check_buffer_is_ready(policy_id):
                    return mix_batches(policy_id)
            else:
                samples = {}
                for policy_id, replay_buffer in self.replay_buffers.items():
                    if check_buffer_is_ready(policy_id):
                        samples[policy_id] = mix_batches(policy_id)
                return MultiAgentBatch(samples, self.replay_batch_size)

    @ExperimentalAPI
    @override(MultiAgentReplayBuffer)
    def get_state(self) -> Dict[str, Any]:
        """Returns all local state.

        Returns:
            The serializable local state.
        """
        data = {
            "last_added_batches": self.last_added_batches,
        }
        parent = MultiAgentReplayBuffer.get_state(self)
        parent.update(data)
        return parent

    @ExperimentalAPI
    @override(MultiAgentReplayBuffer)
    def set_state(self, state: Dict[str, Any]) -> None:
        """Restores all local state to the provided `state`.

        Args:
            state: The new state to set this buffer. Can be obtained by
                calling `self.get_state()`.
        """
        self.last_added_batches = state["last_added_batches"]
        MultiAgentReplayBuffer.set_state(state)
