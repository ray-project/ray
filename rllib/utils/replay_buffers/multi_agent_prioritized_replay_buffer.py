from typing import Dict
import logging
import numpy as np

import ray
from ray.rllib.policy.rnn_sequencing import timeslice_along_seq_lens_with_overlap
from ray.rllib.utils.annotations import override, ExperimentalAPI
from ray.rllib.utils.replay_buffers.multi_agent_replay_buffer import (
    MultiAgentReplayBuffer,
    ReplayMode,
)
from ray.rllib.utils.replay_buffers.prioritized_replay_buffer import (
    PrioritizedReplayBuffer,
)
from ray.rllib.utils.replay_buffers.replay_buffer import StorageUnit
from ray.rllib.utils.typing import PolicyID, SampleBatchType
from ray.rllib.utils.timer import TimerStat
from ray.util.debug import log_once

logger = logging.getLogger(__name__)


@ExperimentalAPI
class MultiAgentPrioritizedReplayBuffer(MultiAgentReplayBuffer):
    """A prioritized replay buffer shard for multiagent setups.

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
        prioritized_replay_alpha: float = 0.6,
        prioritized_replay_beta: float = 0.4,
        prioritized_replay_eps: float = 1e-6,
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
                If they are stored in episodes, replay_sequence_length is
                ignored.
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
                "capacity": 10, "storage_unit": "timesteps",
                prioritized_replay_alpha: 0.5, prioritized_replay_beta: 0.5,
                prioritized_replay_eps: 0.5}
            **kwargs: Forward compatibility kwargs.
        """
        if "replay_mode" in kwargs and (
            kwargs["replay_mode"] == "lockstep"
            or kwargs["replay_mode"] == ReplayMode.LOCKSTEP
        ):
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

        shard_capacity = capacity // num_shards
        MultiAgentReplayBuffer.__init__(
            self,
            shard_capacity,
            storage_unit,
            **kwargs,
            underlying_buffer_config=prioritized_replay_buffer_config,
            replay_batch_size=replay_batch_size,
            learning_starts=learning_starts,
            replay_mode=replay_mode,
            replay_sequence_length=replay_sequence_length,
            replay_burn_in=replay_burn_in,
            replay_zero_init_states=replay_zero_init_states,
        )

        self.prioritized_replay_eps = prioritized_replay_eps
        self.update_priorities_timer = TimerStat()

    @ExperimentalAPI
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
            **kwargs: Forward compatibility kwargs.
        """
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
                # If SampleBatch has prio-replay weights, average
                # over these to use as a weight for the entire
                # sequence.
                if self.replay_mode is ReplayMode.INDEPENDENT:
                    if "weights" in time_slice and len(time_slice["weights"]):
                        weight = np.mean(time_slice["weights"])
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
                self.replay_buffers[policy_id].add(time_slice, **kwargs)
        else:
            self.replay_buffers[policy_id].add(batch, **kwargs)

    @ExperimentalAPI
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

    @ExperimentalAPI
    @override(MultiAgentReplayBuffer)
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


ReplayActor = ray.remote(num_cpus=0)(MultiAgentPrioritizedReplayBuffer)
