import collections
import logging
import random
from typing import Any, Dict, Optional

import numpy as np

from ray.rllib.policy.rnn_sequencing import timeslice_along_seq_lens_with_overlap
from ray.rllib.policy.sample_batch import (
    DEFAULT_POLICY_ID,
    SampleBatch,
    concat_samples_into_ma_batch,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.multi_agent_prioritized_replay_buffer import (
    MultiAgentPrioritizedReplayBuffer,
)
from ray.rllib.utils.replay_buffers.multi_agent_replay_buffer import (
    MultiAgentReplayBuffer,
    ReplayMode,
    merge_dicts_with_warning,
)
from ray.rllib.utils.replay_buffers.replay_buffer import _ALL_POLICIES, StorageUnit
from ray.rllib.utils.typing import PolicyID, SampleBatchType
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once

logger = logging.getLogger(__name__)


@DeveloperAPI
class MultiAgentMixInReplayBuffer(MultiAgentPrioritizedReplayBuffer):
    """This buffer adds replayed samples to a stream of new experiences.

    - Any newly added batch (`add()`) is immediately returned upon
    the next `sample` call (close to on-policy) as well as being moved
    into the buffer.
    - Additionally, a certain number of old samples is mixed into the
    returned sample according to a given "replay ratio".
    - If >1 calls to `add()` are made without any `sample()` calls
    in between, all newly added batches are returned (plus some older samples
    according to the "replay ratio").

    .. testcode::
        :skipif: True

        # replay ratio 0.66 (2/3 replayed, 1/3 new samples):
        buffer = MultiAgentMixInReplayBuffer(capacity=100,
                                             replay_ratio=0.66)
        buffer.add(<A>)
        buffer.add(<B>)
        buffer.sample(1)

    .. testoutput::

        ..[<A>, <B>, <B>]

    .. testcode::
        :skipif: True

        buffer.add(<C>)
        buffer.sample(1)

    .. testoutput::

        [<C>, <A>, <B>]
        or: [<C>, <A>, <A>], [<C>, <B>, <A>] or [<C>, <B>, <B>],
        but always <C> as it is the newest sample

    .. testcode::
        :skipif: True

        buffer.add(<D>)
        buffer.sample(1)

    .. testoutput::

        [<D>, <A>, <C>]
        or [<D>, <A>, <A>], [<D>, <B>, <A>] or [<D>, <B>, <C>], etc..
        but always <D> as it is the newest sample

    .. testcode::
        :skipif: True

        # replay proportion 0.0 -> replay disabled:
        buffer = MixInReplay(capacity=100, replay_ratio=0.0)
        buffer.add(<A>)
        buffer.sample()

    .. testoutput::

        [<A>]

    .. testcode::
        :skipif: True

        buffer.add(<B>)
        buffer.sample()

    .. testoutput::

        [<B>]
    """

    def __init__(
        self,
        capacity: int = 10000,
        storage_unit: str = "timesteps",
        num_shards: int = 1,
        replay_mode: str = "independent",
        replay_sequence_override: bool = True,
        replay_sequence_length: int = 1,
        replay_burn_in: int = 0,
        replay_zero_init_states: bool = True,
        replay_ratio: float = 0.66,
        underlying_buffer_config: dict = None,
        prioritized_replay_alpha: float = 0.6,
        prioritized_replay_beta: float = 0.4,
        prioritized_replay_eps: float = 1e-6,
        **kwargs,
    ):
        """Initializes MultiAgentMixInReplayBuffer instance.

        Args:
            capacity: The capacity of the buffer, measured in `storage_unit`.
            storage_unit: Either 'timesteps', 'sequences' or
                'episodes'. Specifies how experiences are stored. If they
                are stored in episodes, replay_sequence_length is ignored.
            num_shards: The number of buffer shards that exist in total
                (including this one).
            replay_mode: One of "independent" or "lockstep". Determines,
                whether batches are sampled independently or to an equal
                amount.
            replay_sequence_override: If True, ignore sequences found in incoming
                batches, slicing them into sequences as specified by
                `replay_sequence_length` and `replay_sequence_burn_in`. This only has
                an effect if storage_unit is `sequences`.
            replay_sequence_length: The sequence length (T) of a single
                sample. If > 1, we will sample B x T from this buffer. This
                only has an effect if storage_unit is 'timesteps'.
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
            underlying_buffer_config: A config that contains all necessary
                constructor arguments and arguments for methods to call on
                the underlying buffers. This replaces the standard behaviour
                of the underlying PrioritizedReplayBuffer. The config
                follows the conventions of the general
                replay_buffer_config. kwargs for subsequent calls of methods
                may also be included. Example:
                "replay_buffer_config": {"type": PrioritizedReplayBuffer,
                "capacity": 10, "storage_unit": "timesteps",
                prioritized_replay_alpha: 0.5, prioritized_replay_beta: 0.5,
                prioritized_replay_eps: 0.5}
            prioritized_replay_alpha: Alpha parameter for a prioritized
                replay buffer. Use 0.0 for no prioritization.
            prioritized_replay_beta: Beta parameter for a prioritized
                replay buffer.
            prioritized_replay_eps: Epsilon parameter for a prioritized
                replay buffer.
            **kwargs: Forward compatibility kwargs.
        """
        if not 0 <= replay_ratio <= 1:
            raise ValueError("Replay ratio must be within [0, 1]")

        MultiAgentPrioritizedReplayBuffer.__init__(
            self,
            capacity=capacity,
            storage_unit=storage_unit,
            num_shards=num_shards,
            replay_mode=replay_mode,
            replay_sequence_override=replay_sequence_override,
            replay_sequence_length=replay_sequence_length,
            replay_burn_in=replay_burn_in,
            replay_zero_init_states=replay_zero_init_states,
            underlying_buffer_config=underlying_buffer_config,
            prioritized_replay_alpha=prioritized_replay_alpha,
            prioritized_replay_beta=prioritized_replay_beta,
            prioritized_replay_eps=prioritized_replay_eps,
            **kwargs,
        )

        self.replay_ratio = replay_ratio

        self.last_added_batches = collections.defaultdict(list)

    @DeveloperAPI
    @override(MultiAgentPrioritizedReplayBuffer)
    def add(self, batch: SampleBatchType, **kwargs) -> None:
        """Adds a batch to the appropriate policy's replay buffer.

        Turns the batch into a MultiAgentBatch of the DEFAULT_POLICY_ID if
        it is not a MultiAgentBatch. Subsequently, adds the individual policy
        batches to the storage.

        Args:
            batch: The batch to be added.
            **kwargs: Forward compatibility kwargs.
        """
        # Make a copy so the replay buffer doesn't pin plasma memory.
        batch = batch.copy()
        # Handle everything as if multi-agent.
        batch = batch.as_multi_agent()

        kwargs = merge_dicts_with_warning(self.underlying_buffer_call_args, kwargs)

        pids_and_batches = self._maybe_split_into_policy_batches(batch)

        # We need to split batches into timesteps, sequences or episodes
        # here already to properly keep track of self.last_added_batches
        # underlying buffers should not split up the batch any further
        with self.add_batch_timer:
            if self.storage_unit == StorageUnit.TIMESTEPS:
                for policy_id, sample_batch in pids_and_batches.items():
                    timeslices = sample_batch.timeslices(1)
                    for time_slice in timeslices:
                        self.replay_buffers[policy_id].add(time_slice, **kwargs)
                        self.last_added_batches[policy_id].append(time_slice)

            elif self.storage_unit == StorageUnit.SEQUENCES:
                for policy_id, sample_batch in pids_and_batches.items():
                    timeslices = timeslice_along_seq_lens_with_overlap(
                        sample_batch=sample_batch,
                        seq_lens=sample_batch.get(SampleBatch.SEQ_LENS)
                        if self.replay_sequence_override
                        else None,
                        zero_pad_max_seq_len=self.replay_sequence_length,
                        pre_overlap=self.replay_burn_in,
                        zero_init_states=self.replay_zero_init_states,
                    )
                    for slice in timeslices:
                        self.replay_buffers[policy_id].add(slice, **kwargs)
                        self.last_added_batches[policy_id].append(slice)

            elif self.storage_unit == StorageUnit.EPISODES:
                for policy_id, sample_batch in pids_and_batches.items():
                    for eps in sample_batch.split_by_episode():
                        # Only add full episodes to the buffer
                        if eps.get(SampleBatch.T)[0] == 0 and (
                            eps.get(SampleBatch.TERMINATEDS, [True])[-1]
                            or eps.get(SampleBatch.TRUNCATEDS, [False])[-1]
                        ):
                            self.replay_buffers[policy_id].add(eps, **kwargs)
                            self.last_added_batches[policy_id].append(eps)
                        else:
                            if log_once("only_full_episodes"):
                                logger.info(
                                    "This buffer uses episodes as a storage "
                                    "unit and thus allows only full episodes "
                                    "to be added to it. Some samples may be "
                                    "dropped."
                                )
            elif self.storage_unit == StorageUnit.FRAGMENTS:
                for policy_id, sample_batch in pids_and_batches.items():
                    self.replay_buffers[policy_id].add(sample_batch, **kwargs)
                    self.last_added_batches[policy_id].append(sample_batch)

        self._num_added += batch.count

    @DeveloperAPI
    @override(MultiAgentReplayBuffer)
    def sample(
        self, num_items: int, policy_id: PolicyID = DEFAULT_POLICY_ID, **kwargs
    ) -> Optional[SampleBatchType]:
        """Samples a batch of size `num_items` from a specified buffer.

        Concatenates old samples to new ones according to
        self.replay_ratio. If not enough new samples are available, mixes in
        less old samples to retain self.replay_ratio on average. Returns
        an empty batch if there are no items in the buffer.

        Args:
            num_items: Number of items to sample from this buffer.
            policy_id: ID of the policy that produced the experiences to be
                sampled.
            **kwargs: Forward compatibility kwargs.

        Returns:
            Concatenated MultiAgentBatch of items.
        """
        # Merge kwargs, overwriting standard call arguments
        kwargs = merge_dicts_with_warning(self.underlying_buffer_call_args, kwargs)

        def mix_batches(_policy_id):
            """Mixes old with new samples.

            Tries to mix according to self.replay_ratio on average.
            If not enough new samples are available, mixes in less old samples
            to retain self.replay_ratio on average.
            """

            def round_up_or_down(value, ratio):
                """Returns an integer averaging to value*ratio."""
                product = value * ratio
                ceil_prob = product % 1
                if random.uniform(0, 1) < ceil_prob:
                    return int(np.ceil(product))
                else:
                    return int(np.floor(product))

            max_num_new = round_up_or_down(num_items, 1 - self.replay_ratio)
            # if num_samples * self.replay_ratio is not round,
            # we need one more sample with a probability of
            # (num_items*self.replay_ratio) % 1

            _buffer = self.replay_buffers[_policy_id]
            output_batches = self.last_added_batches[_policy_id][:max_num_new]
            self.last_added_batches[_policy_id] = self.last_added_batches[_policy_id][
                max_num_new:
            ]

            # No replay desired
            if self.replay_ratio == 0.0:
                return concat_samples_into_ma_batch(output_batches)
            # Only replay desired
            elif self.replay_ratio == 1.0:
                return _buffer.sample(num_items, **kwargs)

            num_new = len(output_batches)

            if np.isclose(num_new, num_items * (1 - self.replay_ratio)):
                # The optimal case, we can mix in a round number of old
                # samples on average
                num_old = num_items - max_num_new
            else:
                # We never want to return more elements than num_items
                num_old = min(
                    num_items - max_num_new,
                    round_up_or_down(
                        num_new, self.replay_ratio / (1 - self.replay_ratio)
                    ),
                )

            output_batches.append(_buffer.sample(num_old, **kwargs))
            # Depending on the implementation of underlying buffers, samples
            # might be SampleBatches
            output_batches = [batch.as_multi_agent() for batch in output_batches]
            return concat_samples_into_ma_batch(output_batches)

        def check_buffer_is_ready(_policy_id):
            if (
                (len(self.replay_buffers[policy_id]) == 0) and self.replay_ratio > 0.0
            ) or (
                len(self.last_added_batches[_policy_id]) == 0
                and self.replay_ratio < 1.0
            ):
                return False
            return True

        with self.replay_timer:
            samples = []

            if self.replay_mode == ReplayMode.LOCKSTEP:
                assert (
                    policy_id is None
                ), "`policy_id` specifier not allowed in `lockstep` mode!"
                if check_buffer_is_ready(_ALL_POLICIES):
                    samples.append(mix_batches(_ALL_POLICIES).as_multi_agent())
            elif policy_id is not None:
                if check_buffer_is_ready(policy_id):
                    samples.append(mix_batches(policy_id).as_multi_agent())
            else:
                for policy_id, replay_buffer in self.replay_buffers.items():
                    if check_buffer_is_ready(policy_id):
                        samples.append(mix_batches(policy_id).as_multi_agent())

            return concat_samples_into_ma_batch(samples)

    @DeveloperAPI
    @override(MultiAgentPrioritizedReplayBuffer)
    def get_state(self) -> Dict[str, Any]:
        """Returns all local state.

        Returns:
            The serializable local state.
        """
        data = {
            "last_added_batches": self.last_added_batches,
        }
        parent = MultiAgentPrioritizedReplayBuffer.get_state(self)
        parent.update(data)
        return parent

    @DeveloperAPI
    @override(MultiAgentPrioritizedReplayBuffer)
    def set_state(self, state: Dict[str, Any]) -> None:
        """Restores all local state to the provided `state`.

        Args:
            state: The new state to set this buffer. Can be obtained by
                calling `self.get_state()`.
        """
        self.last_added_batches = state["last_added_batches"]
        MultiAgentPrioritizedReplayBuffer.set_state(state)
