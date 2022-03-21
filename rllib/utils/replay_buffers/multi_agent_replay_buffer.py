import collections
from typing import Any, Dict, Optional

import numpy as np

import ray
from ray.rllib import SampleBatch
from ray.rllib.execution.buffers.replay_buffer import logger, _ALL_POLICIES
from ray.rllib.policy.rnn_sequencing import timeslice_along_seq_lens_with_overlap
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override, ExperimentalAPI
from ray.rllib.utils.replay_buffers.prioritized_replay_buffer import (
    PrioritizedReplayBuffer,
)
from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.typing import PolicyID, SampleBatchType


@ExperimentalAPI
class MultiAgentReplayBuffer(ReplayBuffer):
    """A replay buffer shard storing data for all policies (in multiagent setup).

    Ray actors are single-threaded, so for scalability, multiple replay actors
    may be created to increase parallelism."""

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
        **kwargs
    ):
        """Initializes a MultiAgentReplayBuffer instance.

        Args:
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
            **kwargs: Forward compatibility kwargs.
        """
        shard_capacity = capacity // num_shards
        ReplayBuffer.__init__(self, shard_capacity, storage_unit)

        self.replay_starts = learning_starts // num_shards
        self.replay_batch_size = replay_batch_size
        self.prioritized_replay_beta = prioritized_replay_beta
        self.prioritized_replay_eps = prioritized_replay_eps
        self.replay_mode = replay_mode
        self.replay_sequence_length = replay_sequence_length
        self.replay_burn_in = replay_burn_in
        self.replay_zero_init_states = replay_zero_init_states

        if replay_sequence_length > 1:
            self.replay_batch_size = int(
                max(1, replay_batch_size // replay_sequence_length)
            )
            logger.info(
                "Since replay_sequence_length={} and replay_batch_size={}, "
                "we will replay {} sequences at a time.".format(
                    replay_sequence_length, replay_batch_size, self.replay_batch_size
                )
            )

        if replay_mode not in ["lockstep", "independent"]:
            raise ValueError("Unsupported replay mode: {}".format(replay_mode))

        def new_buffer():
            if prioritized_replay_alpha == 0.0:
                return ReplayBuffer(self.capacity)
            else:
                return PrioritizedReplayBuffer(
                    self.capacity, alpha=prioritized_replay_alpha
                )

        self.replay_buffers = collections.defaultdict(new_buffer)

        # Metrics.
        self.add_batch_timer = TimerStat()
        self.replay_timer = TimerStat()
        self.update_priorities_timer = TimerStat()
        self._num_added = 0

        # Make externally accessible for testing.
        global _local_replay_buffer
        _local_replay_buffer = self
        # If set, return this instead of the usual data for testing.
        self._fake_batch = None

    @staticmethod
    def get_instance_for_testing():
        """Return a MultiAgentReplayBuffer instance that has been previously
        instantiated.

        Returns:
            _local_replay_buffer: The lastly instantiated
            MultiAgentReplayBuffer.

        """
        global _local_replay_buffer
        return _local_replay_buffer

    @ExperimentalAPI
    @override(ReplayBuffer)
    def add(self, batch: SampleBatchType) -> None:
        """Adds a batch to the appropriate policy's replay buffer.

        Turns the batch into a MultiAgentBatch of the DEFAULT_POLICY_ID if
        it is not a MultiAgentBatch. Subsequently adds the batch to

        Args:
            batch (SampleBatchType): The batch to be added.
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
            else:
                for policy_id, sample_batch in batch.policy_batches.items():
                    self._add_to_policy_buffer(policy_id, sample_batch)
        self._num_added += batch.count

    # Utility method that reduces code redundancy for child classes
    def _add_to_policy_buffer(
        self, policy_id: PolicyID, batch: SampleBatchType
    ) -> None:
        if self.replay_sequence_length == 1:
            timeslices = batch.timeslices(1)
        else:
            timeslices = timeslice_along_seq_lens_with_overlap(
                sample_batch=batch,
                zero_pad_max_seq_len=self.replay_sequence_length,
                pre_overlap=self.replay_burn_in,
                zero_init_states=self.replay_zero_init_states,
            )
        for time_slice in timeslices:
            # If SampleBatch has prio-replay weights, average
            # over these to use as a weight for the entire
            # sequence.
            if "weights" in time_slice and len(time_slice["weights"]):
                weight = np.mean(time_slice["weights"])
            else:
                weight = None
            self.replay_buffers[policy_id].add(time_slice, weight=weight)

    @ExperimentalAPI
    @override(ReplayBuffer)
    def sample(
        self, num_items: int, policy_id: Optional[PolicyID] = None
    ) -> Optional[SampleBatchType]:
        """Samples a batch of size `num_items` from a policy's buffer

        If this buffer was given a fake batch, return it, otherwise
        return a MultiAgentBatch with samples. If less than `num_items`
        records are in the policy's buffer, some samples in
        the results may be repeated to fulfil the batch size (`num_items`)
        request.

        Args:
            num_items: Number of items to sample from a policy's buffer.
            policy_id: ID of the policy that created the experiences we sample

        Returns:
            Concatenated batch of items. None if buffer is empty.
        """
        if self._fake_batch:
            if not isinstance(self._fake_batch, MultiAgentBatch):
                self._fake_batch = SampleBatch(self._fake_batch).as_multi_agent()
            return self._fake_batch

        if self._num_added < self.replay_starts:
            return None
        with self.replay_timer:
            # Lockstep mode: Sample from all policies at the same time an
            # equal amount of steps.
            if self.replay_mode == "lockstep":
                assert (
                    policy_id is None
                ), "`policy_id` specifier not allowed in `locksetp` mode!"
                return self.replay_buffers[_ALL_POLICIES].sample(
                    self.replay_batch_size, beta=self.prioritized_replay_beta
                )
            elif policy_id is not None:
                return self.replay_buffers[policy_id].sample(
                    self.replay_batch_size, beta=self.prioritized_replay_beta
                )
            else:
                samples = {}
                for policy_id, replay_buffer in self.replay_buffers.items():
                    samples[policy_id] = replay_buffer.sample(
                        self.replay_batch_size, beta=self.prioritized_replay_beta
                    )
                return MultiAgentBatch(samples, self.replay_batch_size)

    @ExperimentalAPI
    def update_priorities(self, prio_dict: Dict) -> None:
        """Updates the priorities of underlying replay buffers.

        Computes new priorities from td_errors and prioritized_replay_eps.
        These priorities are used to update underlying replay buffers per
        policy_id.

        Args:
            prio_dict (Dict): A dictionary containing td_errors for
            batches saved in underlying replay buffers.
        """
        with self.update_priorities_timer:
            for policy_id, (batch_indexes, td_errors) in prio_dict.items():
                new_priorities = np.abs(td_errors) + self.prioritized_replay_eps
                self.replay_buffers[policy_id].update_priorities(
                    batch_indexes, new_priorities
                )

    @ExperimentalAPI
    @override(ReplayBuffer)
    def stats(self, debug: bool = False) -> Dict:
        """Returns the stats of this buffer and all underlying buffers.

        Args:
            debug (bool): If True, stats of underlying replay buffers will
            be fetched with debug=True.

        Returns:
            stat: Dictionary of buffer stats.
        """
        stat = {
            "add_batch_time_ms": round(1000 * self.add_batch_timer.mean, 3),
            "replay_time_ms": round(1000 * self.replay_timer.mean, 3),
            "update_priorities_time_ms": round(
                1000 * self.update_priorities_timer.mean, 3
            ),
        }
        for policy_id, replay_buffer in self.replay_buffers.items():
            stat.update(
                {"policy_{}".format(policy_id): replay_buffer.stats(debug=debug)}
            )
        return stat

    @ExperimentalAPI
    @override(ReplayBuffer)
    def get_state(self) -> Dict[str, Any]:
        """Returns all local state.

        Returns:
            The serializable local state.
        """
        state = {"num_added": self._num_added, "replay_buffers": {}}
        for policy_id, replay_buffer in self.replay_buffers.items():
            state["replay_buffers"][policy_id] = replay_buffer.get_state()
        return state

    @ExperimentalAPI
    @override(ReplayBuffer)
    def set_state(self, state: Dict[str, Any]) -> None:
        """Restores all local state to the provided `state`.

        Args:
            state: The new state to set this buffer. Can be obtained by
                calling `self.get_state()`.
        """
        self._num_added = state["num_added"]
        buffer_states = state["replay_buffers"]
        for policy_id in buffer_states.keys():
            self.replay_buffers[policy_id].set_state(buffer_states[policy_id])


ReplayActor = ray.remote(num_cpus=0)(MultiAgentReplayBuffer)
