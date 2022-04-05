import logging
import collections
from typing import Any, Dict, Optional
from enum import Enum

import ray
from ray.rllib.utils.replay_buffers.replay_buffer import _ALL_POLICIES
from ray.rllib.policy.rnn_sequencing import timeslice_along_seq_lens_with_overlap
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override, ExperimentalAPI
from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.typing import PolicyID, SampleBatchType
from ray.rllib.utils.replay_buffers.replay_buffer import StorageUnit
from ray.rllib.utils.from_config import from_config
from ray.util.debug import log_once
from ray.rllib.utils.deprecation import Deprecated

logger = logging.getLogger(__name__)


@ExperimentalAPI
class ReplayMode(Enum):
    LOCKSTEP = "lockstep"
    INDEPENDENT = "independent"


@ExperimentalAPI
def merge_dicts_with_warning(args_on_init, args_on_call):
    """Merge argument dicts, overwriting args_on_call with warning.

    The MultiAgentReplayBuffer supports setting standard arguments for calls
    of methods of the underlying buffers. These arguments can be
    overwritten. Such overwrites trigger a warning to the user.
    """
    for arg_name, arg_value in args_on_call.items():
        if arg_name in args_on_init:
            if log_once("overwrite_argument_{}".format((str(arg_name)))):
                logger.warning(
                    "Replay Buffer was initialized to have "
                    "underlying buffers methods called with "
                    "argument `{}={}`, but was subsequently called "
                    "with `{}={}`.".format(
                        arg_name,
                        args_on_init[arg_name],
                        arg_name,
                        arg_value,
                    )
                )
    return {**args_on_init, **args_on_call}


@ExperimentalAPI
class MultiAgentReplayBuffer(ReplayBuffer):
    """A replay buffer shard for multiagent setups.

    This buffer is meant to be run in parallel to distribute experiences
    across `num_shards` shards. Unlike simpler buffers, it holds a set of
    buffers - one for each policy ID.
    """

    def __init__(
        self,
        capacity: int = 10000,
        storage_unit: str = "timesteps",
        num_shards: int = 1,
        replay_batch_size: int = 1,
        learning_starts: int = 1000,
        replay_mode: str = "independent",
        replay_sequence_length: int = 1,
        replay_burn_in: int = 0,
        replay_zero_init_states: bool = True,
        underlying_buffer_config: dict = None,
        **kwargs
    ):
        """Initializes a MultiAgentReplayBuffer instance.

        Args:
            num_shards: The number of buffer shards that exist in total
                (including this one).
            storage_unit: Either 'timesteps', 'sequences' or
                'episodes'. Specifies how experiences are stored. If they
                are stored in episodes, replay_sequence_length is ignored.
            learning_starts: Number of timesteps after which a call to
                `sample()` will yield samples (before that, `sample()` will
                return None).
            capacity: Max number of total timesteps in all policy buffers.
                After reaching this number, older samples will be
                dropped to make space for new ones.
            replay_batch_size: The batch size to be sampled (in timesteps).
                Note that if `replay_sequence_length` > 1,
                `self.replay_batch_size` will be set to the number of
                sequences sampled (B).
            replay_mode: One of "independent" or "lockstep". Determines,
                whether batches are sampled independently or to an equal
                amount.
            replay_sequence_length: The sequence length (T) of a single
                sample. If > 1, we will sample B x T from this buffer. This
                only has an effect if storage_unit is 'timesteps'.
            replay_burn_in: The burn-in length in case
                `replay_sequence_length` > 0. This is the number of timesteps
                each sequence overlaps with the previous one to generate a
                better internal state (=state after the burn-in), instead of
                starting from 0.0 each RNN rollout. This only has an effect
                if storage_unit is 'timesteps'.
            replay_zero_init_states: Whether the initial states in the
                buffer (if replay_sequence_length > 0) are alwayas 0.0 or
                should be updated with the previous train_batch state outputs.
            underlying_buffer_config: A config that contains all necessary
                constructor arguments and arguments for methods to call on
                the underlying buffers.
            **kwargs: Forward compatibility kwargs.
        """
        shard_capacity = capacity // num_shards
        ReplayBuffer.__init__(self, capacity, storage_unit)

        # If the user provides an underlying buffer config, we use to
        # instantiate and interact with underlying buffers
        self.underlying_buffer_config = underlying_buffer_config
        if self.underlying_buffer_config is not None:
            self.underlying_buffer_call_args = self.underlying_buffer_config
        else:
            self.underlying_buffer_call_args = {}

        self.replay_batch_size = replay_batch_size
        self.replay_starts = learning_starts // num_shards
        self.replay_mode = replay_mode
        self.replay_sequence_length = replay_sequence_length
        self.replay_burn_in = replay_burn_in
        self.replay_zero_init_states = replay_zero_init_states

        if replay_mode in ["lockstep", ReplayMode.LOCKSTEP]:
            self.replay_mode = ReplayMode.LOCKSTEP
            if self._storage_unit in [StorageUnit.EPISODES, StorageUnit.SEQUENCES]:
                raise ValueError(
                    "MultiAgentReplayBuffer does not support "
                    "lockstep mode with storage unit `episodes`"
                    "or `sequences`."
                )
        elif replay_mode in ["independent", ReplayMode.INDEPENDENT]:
            self.replay_mode = ReplayMode.INDEPENDENT
        else:
            raise ValueError("Unsupported replay mode: {}".format(replay_mode))

        if self.underlying_buffer_config:
            ctor_args = {
                **{"capacity": shard_capacity, "storage_unit": storage_unit},
                **self.underlying_buffer_config,
            }

            def new_buffer():
                return from_config(self.underlying_buffer_config["type"], ctor_args)

        else:
            # Default case
            def new_buffer():
                self.underlying_buffer_call_args = {}
                return ReplayBuffer(
                    self.capacity,
                    storage_unit=storage_unit,
                )

        self.replay_buffers = collections.defaultdict(new_buffer)

        # Metrics.
        self.add_batch_timer = TimerStat()
        self.replay_timer = TimerStat()
        self._num_added = 0

    def __len__(self) -> int:
        """Returns the number of items currently stored in this buffer."""
        return sum(len(buffer._storage) for buffer in self.replay_buffers.values())

    @ExperimentalAPI
    @Deprecated(old="replay", new="sample", error=False)
    def replay(self, num_items: int = None, **kwargs) -> Optional[SampleBatchType]:
        """Deprecated in favor of new ReplayBuffer API."""
        if num_items is None:
            num_items = self.replay_batch_size
        return self.sample(num_items, **kwargs)

    @ExperimentalAPI
    @override(ReplayBuffer)
    def add(self, batch: SampleBatchType, **kwargs) -> None:
        """Adds a batch to the appropriate policy's replay buffer.

        Turns the batch into a MultiAgentBatch of the DEFAULT_POLICY_ID if
        it is not a MultiAgentBatch. Subsequently, adds the individual policy
        batches to the storage.

        Args:
            batch : The batch to be added.
            **kwargs: Forward compatibility kwargs.
        """
        # Make a copy so the replay buffer doesn't pin plasma memory.
        batch = batch.copy()
        # Handle everything as if multi-agent.
        batch = batch.as_multi_agent()

        with self.add_batch_timer:
            if self.replay_mode == ReplayMode.LOCKSTEP:
                # Lockstep mode: Store under _ALL_POLICIES key (we will always
                # only sample from all policies at the same time).
                # This means storing a MultiAgentBatch to the underlying buffer
                self._add_to_underlying_buffer(_ALL_POLICIES, batch, **kwargs)
            else:
                # Store independent SampleBatches
                for policy_id, sample_batch in batch.policy_batches.items():
                    self._add_to_underlying_buffer(policy_id, sample_batch, **kwargs)
        self._num_added += batch.count

    @ExperimentalAPI
    def _add_to_underlying_buffer(
        self, policy_id: PolicyID, batch: SampleBatchType, **kwargs
    ) -> None:
        """Add a batch of experiences to the underlying buffer of a policy.

        If the storage unit is `timesteps`, cut the batch into timeslices
        before adding them to the appropriate buffer. Otherwise, let the
        underlying buffer decide how slice batches.

        Args:
            policy_id: ID of the policy that corresponds to the underlying
            buffer
            batch: SampleBatch to add to the underlying buffer
            **kwargs: Forward compatibility kwargs.
        """
        # Merge kwargs, overwriting standard call arguments
        kwargs = merge_dicts_with_warning(self.underlying_buffer_call_args, kwargs)

        # For the storage unit `timesteps`, the underlying buffer will
        # simply store the samples how they arrive. For sequences and
        # episodes, the underlying buffer may split them itself.
        if self._storage_unit is StorageUnit.TIMESTEPS:
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
                self.replay_buffers[policy_id].add(time_slice, **kwargs)
        else:
            self.replay_buffers[policy_id].add(batch, **kwargs)

    @ExperimentalAPI
    @override(ReplayBuffer)
    def sample(
        self, num_items: int, policy_id: Optional[PolicyID] = None, **kwargs
    ) -> Optional[SampleBatchType]:
        """Samples a MultiAgentBatch of `num_items` per one policy's buffer.

        If less than `num_items` records are in the policy's buffer,
        some samples in the results may be repeated to fulfil the batch size
        `num_items` request. Returns an empty batch if there are no items in
        the buffer.

        Args:
            num_items: Number of items to sample from a policy's buffer.
            policy_id: ID of the policy that created the experiences we sample.
                If none is given, sample from all policies.

        Returns:
            Concatenated MultiAgentBatch of items.
            **kwargs: Forward compatibility kwargs.
        """
        # Merge kwargs, overwriting standard call arguments
        kwargs = merge_dicts_with_warning(self.underlying_buffer_call_args, kwargs)

        if self._num_added < self.replay_starts:
            return MultiAgentBatch({}, 0)
        with self.replay_timer:
            # Lockstep mode: Sample from all policies at the same time an
            # equal amount of steps.
            if self.replay_mode == ReplayMode.LOCKSTEP:
                assert (
                    policy_id is None
                ), "`policy_id` specifier not allowed in `lockstep` mode!"
                # In lockstep mode we sample MultiAgentBatches
                return self.replay_buffers[_ALL_POLICIES].sample(num_items, **kwargs)
            elif policy_id is not None:
                sample = self.replay_buffers[policy_id].sample(num_items, **kwargs)
                return MultiAgentBatch({policy_id: sample}, sample.count)
            else:
                samples = {}
                for policy_id, replay_buffer in self.replay_buffers.items():
                    samples[policy_id] = replay_buffer.sample(num_items, **kwargs)
                return MultiAgentBatch(samples, sum(s.count for s in samples.values()))

    @ExperimentalAPI
    @override(ReplayBuffer)
    def stats(self, debug: bool = False) -> Dict:
        """Returns the stats of this buffer and all underlying buffers.

        Args:
            debug: If True, stats of underlying replay buffers will
            be fetched with debug=True.

        Returns:
            stat: Dictionary of buffer stats.
        """
        stat = {
            "add_batch_time_ms": round(1000 * self.add_batch_timer.mean, 3),
            "replay_time_ms": round(1000 * self.replay_timer.mean, 3),
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
