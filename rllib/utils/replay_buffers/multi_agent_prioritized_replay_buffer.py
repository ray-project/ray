from typing import Dict
import logging
import math
import numpy as np

from ray.util.timer import _Timer
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.multi_agent_replay_buffer import (
    MultiAgentReplayBuffer,
    ReplayMode,
    merge_dicts_with_warning,
)
from ray.rllib.utils.replay_buffers.prioritized_replay_buffer import (
    PrioritizedReplayBuffer,
)
from ray.rllib.utils.replay_buffers.replay_buffer import (
    StorageUnit,
)
from ray.rllib.utils.typing import PolicyID, SampleBatchType
from ray.rllib.policy.sample_batch import SampleBatch
from ray.util.debug import log_once
from ray.util.annotations import DeveloperAPI
from ray.rllib.policy.rnn_sequencing import timeslice_along_seq_lens_with_overlap

logger = logging.getLogger(__name__)


@DeveloperAPI
class MultiAgentPrioritizedReplayBuffer(
    MultiAgentReplayBuffer, PrioritizedReplayBuffer
):
    """A prioritized replay buffer shard for multiagent setups.

    This buffer is meant to be run in parallel to distribute experiences
    across `num_shards` shards. Unlike simpler buffers, it holds a set of
    buffers - one for each policy ID.
    """

    def __init__(
        self,
        capacity_items=10000,
        capacity_ts: int = math.inf,
        capacity_bytes: int = math.inf,
        storage_unit: str = "timesteps",
        storage_location: str = "memory",
        num_shards: int = 1,
        replay_mode: str = "independent",
        replay_sequence_override: bool = True,
        replay_sequence_length: int = 1,
        replay_burn_in: int = 0,
        replay_zero_init_states: bool = True,
        underlying_buffer_config: dict = None,
        prioritized_replay_alpha: float = 0.6,
        prioritized_replay_beta: float = 0.4,
        prioritized_replay_eps: float = 1e-6,
        **kwargs
    ):
        """Initializes a MultiAgentReplayBuffer instance.

        Args:
            capacity_items: Maximum number of items to store in this FIFO buffer.
                After reaching this number, older samples will be dropped to make space
                for new ones. The number has to be finite in order to keep track of the
                item hit count.
            capacity_ts: Maximum number of timesteps to store in this FIFO buffer.
                After reaching this number, older samples will be dropped to make space
                for new ones.
            capacity_bytes: Maximum number of bytes to store in this FIFO buffer.
                After reaching this number, older samples will be dropped to make space
                for new ones.
            storage_unit: Either 'timesteps', 'sequences' or
                'episodes'. Specifies how experiences are stored. If they
                are stored in episodes, replay_sequence_length is ignored.
                If they are stored in episodes, replay_sequence_length is
                ignored.
            storage_location: Either 'memory' or 'disk'.
                Specifies where experiences are stored.
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
                sample. If > 1, we will sample B x T from this buffer.
            replay_burn_in: The burn-in length in case
                `replay_sequence_length` > 0. This is the number of timesteps
                each sequence overlaps with the previous one to generate a
                better internal state (=state after the burn-in), instead of
                starting from 0.0 each RNN rollout.
            replay_zero_init_states: Whether the initial states in the
                buffer (if replay_sequence_length > 0) are alwayas 0.0 or
                should be updated with the previous train_batch state outputs.
            underlying_buffer_config: A config that contains all necessary
                constructor arguments and arguments for methods to call on
                the underlying buffers. This replaces the standard behaviour
                of the underlying PrioritizedReplayBuffer. The config
                follows the conventions of the general
                replay_buffer_config. kwargs for subsequent calls of methods
                may also be included. Example:
                "replay_buffer_config": {"type": PrioritizedReplayBuffer,
                "capacity_items": 10, "storage_unit": "timesteps",
                prioritized_replay_alpha: 0.5, prioritized_replay_beta: 0.5,
                prioritized_replay_eps: 0.5}
            prioritized_replay_alpha: Alpha parameter for a prioritized
                replay buffer. Use 0.0 for no prioritization.
            prioritized_replay_beta: Beta parameter for a prioritized
                replay buffer.
            prioritized_replay_eps: Epsilon parameter for a prioritized
                replay buffer.
            ``**kwargs``: Forward compatibility kwargs.
        """
        if "replay_mode" in kwargs and (kwargs["replay_mode"] == ReplayMode.LOCKSTEP):
            if log_once("lockstep_mode_not_supported"):
                logger.error(
                    "Replay mode `lockstep` is not supported for "
                    "MultiAgentPrioritizedReplayBuffer. "
                    "This buffer will run in `independent` mode."
                )
            kwargs["replay_mode"] = "independent"

        if underlying_buffer_config is not None:
            if log_once("underlying_buffer_config_not_supported"):
                logger.info(
                    "PrioritizedMultiAgentReplayBuffer instantiated "
                    "with underlying_buffer_config. This will "
                    "overwrite the standard behaviour of the "
                    "underlying PrioritizedReplayBuffer."
                )
            prioritized_replay_buffer_config = underlying_buffer_config
        else:
            prioritized_replay_buffer_config = {
                "type": PrioritizedReplayBuffer,
                "alpha": prioritized_replay_alpha,
                "beta": prioritized_replay_beta,
            }

        MultiAgentReplayBuffer.__init__(
            self,
            capacity_items=capacity_items,
            capacity_ts=capacity_ts,
            capacity_bytes=capacity_bytes,
            storage_unit=storage_unit,
            storage_location=storage_location,
            replay_sequence_override=replay_sequence_override,
            replay_mode=replay_mode,
            replay_sequence_length=replay_sequence_length,
            replay_burn_in=replay_burn_in,
            replay_zero_init_states=replay_zero_init_states,
            underlying_buffer_config=prioritized_replay_buffer_config,
            **kwargs,
        )

        self.prioritized_replay_eps = prioritized_replay_eps
        self.update_priorities_timer = _Timer()

    @DeveloperAPI
    @override(MultiAgentReplayBuffer)
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
        if self.storage_unit == StorageUnit.TIMESTEPS:
            timeslices = batch.timeslices(1)
        elif self.storage_unit == StorageUnit.SEQUENCES:
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
                if eps.get(SampleBatch.T)[0] == 0 and (
                    eps.get(SampleBatch.TERMINATEDS, [True])[-1]
                    or eps.get(SampleBatch.TRUNCATEDS, [False])[-1]
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
            # If SampleBatch has prio-replay weights, average
            # over these to use as a weight for the entire
            # sequence.
            if self.replay_mode is ReplayMode.INDEPENDENT:
                if "weights" in slice and len(slice["weights"]):
                    weight = np.mean(slice["weights"])
                else:
                    weight = None

                if "weight" in kwargs and weight is not None:
                    if log_once("overwrite_weight"):
                        logger.warning(
                            "Adding batches with column "
                            "`weights` to this buffer while "
                            "providing weights as a call argument "
                            "to the add method results in the "
                            "column being overwritten."
                        )

                kwargs = {"weight": weight, **kwargs}
            else:
                if "weight" in kwargs:
                    if log_once("lockstep_no_weight_allowed"):
                        logger.warning(
                            "Settings weights for batches in "
                            "lockstep mode is not allowed."
                            "Weights are being ignored."
                        )

                kwargs = {**kwargs, "weight": None}
            self.replay_buffers[policy_id].add(slice, **kwargs)

    @DeveloperAPI
    @override(PrioritizedReplayBuffer)
    def update_priorities(self, prio_dict: Dict) -> None:
        """Updates the priorities of underlying replay buffers.

        Computes new priorities from td_errors and prioritized_replay_eps.
        These priorities are used to update underlying replay buffers per
        policy_id.

        Args:
            prio_dict: A dictionary containing td_errors for
            batches saved in underlying replay buffers.
        """
        with self.update_priorities_timer:
            for policy_id, (batch_indexes, td_errors) in prio_dict.items():
                new_priorities = np.abs(td_errors) + self.prioritized_replay_eps
                self.replay_buffers[policy_id].update_priorities(
                    batch_indexes, new_priorities
                )

    @DeveloperAPI
    @override(MultiAgentReplayBuffer)
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
            "update_priorities_time_ms": round(
                1000 * self.update_priorities_timer.mean, 3
            ),
        }
        for policy_id, replay_buffer in self.replay_buffers.items():
            stat.update(
                {"policy_{}".format(policy_id): replay_buffer.stats(debug=debug)}
            )
        return stat
