import collections
import logging
from enum import Enum
from typing import Any, Dict, Optional

from ray.util.timer import _Timer
from ray.rllib.policy.rnn_sequencing import timeslice_along_seq_lens_with_overlap
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.replay_buffers.replay_buffer import (
    _ALL_POLICIES,
    ReplayBuffer,
    StorageUnit,
)
from ray.rllib.utils.typing import PolicyID, SampleBatchType
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once

logger = logging.getLogger(__name__)


@DeveloperAPI
class ReplayMode(Enum):
    LOCKSTEP = "lockstep"
    INDEPENDENT = "independent"


@DeveloperAPI
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


@DeveloperAPI
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
        replay_mode: str = "independent",
        replay_sequence_override: bool = True,
        replay_sequence_length: int = 1,
        replay_burn_in: int = 0,
        replay_zero_init_states: bool = True,
        underlying_buffer_config: dict = None,
        **kwargs
    ):
        """Initializes a MultiAgentReplayBuffer instance.

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
            replay_burn_in: This is the number of timesteps
                each sequence overlaps with the previous one to generate a
                better internal state (=state after the burn-in), instead of
                starting from 0.0 each RNN rollout. This only has an effect
                if storage_unit is `sequences`.
            replay_zero_init_states: Whether the initial states in the
                buffer (if replay_sequence_length > 0) are alwayas 0.0 or
                should be updated with the previous train_batch state outputs.
            underlying_buffer_config: A config that contains all necessary
                constructor arguments and arguments for methods to call on
                the underlying buffers.
            ``**kwargs``: Forward compatibility kwargs.
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
        self.replay_sequence_override = replay_sequence_override
        self.replay_mode = replay_mode
        self.replay_sequence_length = replay_sequence_length
        self.replay_burn_in = replay_burn_in
        self.replay_zero_init_states = replay_zero_init_states
        self.replay_sequence_override = replay_sequence_override

        if (
            replay_sequence_length > 1
            and self.storage_unit is not StorageUnit.SEQUENCES
        ):
            logger.warning(
                "MultiAgentReplayBuffer configured with "
                "`replay_sequence_length={}`, but `storage_unit={}`. "
                "replay_sequence_length will be ignored and set to 1.".format(
                    replay_sequence_length, storage_unit
                )
            )
            self.replay_sequence_length = 1

        if replay_sequence_length == 1 and self.storage_unit is StorageUnit.SEQUENCES:
            logger.warning(
                "MultiAgentReplayBuffer configured with "
                "`replay_sequence_length={}`, but `storage_unit={}`. "
                "This will result in sequences equal to timesteps.".format(
                    replay_sequence_length, storage_unit
                )
            )

        if replay_mode in ["lockstep", ReplayMode.LOCKSTEP]:
            self.replay_mode = ReplayMode.LOCKSTEP
            if self.storage_unit in [StorageUnit.EPISODES, StorageUnit.SEQUENCES]:
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
                **{"capacity": shard_capacity, "storage_unit": StorageUnit.FRAGMENTS},
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
                    storage_unit=StorageUnit.FRAGMENTS,
                )

        self.replay_buffers = collections.defaultdict(new_buffer)

        # Metrics.
        self.add_batch_timer = _Timer()
        self.replay_timer = _Timer()
        self._num_added = 0

    def __len__(self) -> int:
        """Returns the number of items currently stored in this buffer."""
        return sum(len(buffer._storage) for buffer in self.replay_buffers.values())

    @DeveloperAPI
    @Deprecated(
        old="ReplayBuffer.replay()",
        new="ReplayBuffer.sample(num_items)",
        error=True,
    )
    def replay(self, num_items: int = None, **kwargs) -> Optional[SampleBatchType]:
        """Deprecated in favor of new ReplayBuffer API."""
        pass

    @DeveloperAPI
    @override(ReplayBuffer)
    def add(self, batch: SampleBatchType, **kwargs) -> None:
        """Adds a batch to the appropriate policy's replay buffer.

        Turns the batch into a MultiAgentBatch of the DEFAULT_POLICY_ID if
        it is not a MultiAgentBatch. Subsequently, adds the individual policy
        batches to the storage.

        Args:
            batch : The batch to be added.
            ``**kwargs``: Forward compatibility kwargs.
        """
        if batch is None:
            if log_once("empty_batch_added_to_buffer"):
                logger.info(
                    "A batch that is `None` was added to {}. This can be "
                    "normal at the beginning of execution but might "
                    "indicate an issue.".format(type(self).__name__)
                )
            return
        # Make a copy so the replay buffer doesn't pin plasma memory.
        batch = batch.copy()
        # Handle everything as if multi-agent.
        batch = batch.as_multi_agent()

        with self.add_batch_timer:
            pids_and_batches = self._maybe_split_into_policy_batches(batch)
            for policy_id, sample_batch in pids_and_batches.items():
                self._add_to_underlying_buffer(policy_id, sample_batch, **kwargs)

        self._num_added += batch.count

    @DeveloperAPI
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
            ``**kwargs``: Forward compatibility kwargs.
        """
        # Merge kwargs, overwriting standard call arguments
        kwargs = merge_dicts_with_warning(self.underlying_buffer_call_args, kwargs)

        # For the storage unit `timesteps`, the underlying buffer will
        # simply store the samples how they arrive. For sequences and
        # episodes, the underlying buffer may split them itself.
        if self.storage_unit is StorageUnit.TIMESTEPS:
            timeslices = batch.timeslices(1)
        elif self.storage_unit is StorageUnit.SEQUENCES:
            timeslices = timeslice_along_seq_lens_with_overlap(
                sample_batch=batch,
                seq_lens=batch.get(SampleBatch.SEQ_LENS)
                if self.replay_sequence_override
                else None,
                zero_pad_max_seq_len=self.replay_sequence_length,
                pre_overlap=self.replay_burn_in,
                zero_init_states=self.replay_zero_init_states,
            )
        elif self.storage_unit == StorageUnit.EPISODES:
            timeslices = []
            for eps in batch.split_by_episode():
                if (
                    eps.get(SampleBatch.T)[0] == 0
                    and eps.get(SampleBatch.DONES)[-1] == True  # noqa E712
                ):
                    # Only add full episodes to the buffer
                    timeslices.append(eps)
                else:
                    if log_once("only_full_episodes"):
                        logger.info(
                            "This buffer uses episodes as a storage "
                            "unit and thus allows only full episodes "
                            "to be added to it. Some samples may be "
                            "dropped."
                        )
        elif self.storage_unit == StorageUnit.FRAGMENTS:
            timeslices = [batch]
        else:
            raise ValueError("Unknown `storage_unit={}`".format(self.storage_unit))

        for slice in timeslices:
            self.replay_buffers[policy_id].add(slice, **kwargs)

    @DeveloperAPI
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
            policy_id: ID of the policy that created the experiences we sample. If
            none is given, sample from all policies.

        Returns:
            Concatenated MultiAgentBatch of items.
            ``**kwargs``: Forward compatibility kwargs.
        """
        # Merge kwargs, overwriting standard call arguments
        kwargs = merge_dicts_with_warning(self.underlying_buffer_call_args, kwargs)

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

    @DeveloperAPI
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

    @DeveloperAPI
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

    @DeveloperAPI
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

    def _maybe_split_into_policy_batches(self, batch: SampleBatchType):
        """Returns a dict of policy IDs and batches, depending on our replay mode.

        This method helps with splitting up MultiAgentBatches only if the
        self.replay_mode requires it.
        """
        if self.replay_mode == ReplayMode.LOCKSTEP:
            return {_ALL_POLICIES: batch}
        else:
            return batch.policy_batches
