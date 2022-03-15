import logging
import platform
from typing import Any, Dict, List, Optional
import numpy as np
import random
from enum import Enum
from ray.util.debug import log_once

# Import ray before psutil will make sure we use psutil's bundled version
import ray  # noqa F401
import psutil  # noqa E402

from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.metrics.window_stat import WindowStat
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.execution.buffers.replay_buffer import warn_replay_capacity

# Constant that represents all policies in lockstep replay mode.
_ALL_POLICIES = "__all__"

logger = logging.getLogger(__name__)


@ExperimentalAPI
class StorageUnit(Enum):
    TIMESTEPS = "timesteps"
    SEQUENCES = "sequences"
    EPISODES = "episodes"


@ExperimentalAPI
class ReplayBuffer:
    def __init__(
        self, capacity: int = 10000, storage_unit: str = "timesteps", **kwargs
    ):
        """Initializes a (FIFO) ReplayBuffer instance.

        Args:
            capacity: Max number of timesteps to store in this FIFO
                buffer. After reaching this number, older samples will be
                dropped to make space for new ones.
            storage_unit: Either 'timesteps', `sequences` or
                `episodes`. Specifies how experiences are stored.
            **kwargs: Forward compatibility kwargs.
        """

        if storage_unit in ["timesteps", StorageUnit.TIMESTEPS]:
            self._storage_unit = StorageUnit.TIMESTEPS
        elif storage_unit in ["sequences", StorageUnit.SEQUENCES]:
            self._storage_unit = StorageUnit.SEQUENCES
        elif storage_unit in ["episodes", StorageUnit.EPISODES]:
            self._storage_unit = StorageUnit.EPISODES
        else:
            raise ValueError(
                "storage_unit must be either 'timesteps', `sequences` or `episodes`."
            )

        # The actual storage (list of SampleBatches or MultiAgentBatches).
        self._storage = []

        # Caps the number of timesteps stored in this buffer
        self.capacity = capacity
        # The next index to override in the buffer.
        self._next_idx = 0
        # len(self._hit_count) must always be less than len(capacity)
        self._hit_count = np.zeros(self.capacity)

        # Whether we have already hit our capacity (and have therefore
        # started to evict older samples).
        self._eviction_started = False

        # Number of (single) timesteps that have been added to the buffer
        # over its lifetime. Note that each added item (batch) may contain
        # more than one timestep.
        self._num_timesteps_added = 0
        self._num_timesteps_added_wrap = 0

        # Number of (single) timesteps that have been sampled from the buffer
        # over its lifetime.
        self._num_timesteps_sampled = 0

        self._evicted_hit_stats = WindowStat("evicted_hit", 1000)
        self._est_size_bytes = 0

        self.batch_size = None

    def __len__(self) -> int:
        """Returns the number of items currently stored in this buffer."""
        return len(self._storage)

    @ExperimentalAPI
    def add(self, batch: SampleBatchType, **kwargs) -> None:
        """Adds a batch of experiences to this buffer.

        Also splits experiences into chunks of timesteps, sequences
        or episodes, depending on self._storage_unit. Calls
        self._add_single_batch.

        Args:
            batch: Batch to add to this buffer's storage.
            **kwargs: Forward compatibility kwargs.
        """
        assert batch.count > 0, batch
        warn_replay_capacity(item=batch, num_items=self.capacity / batch.count)

        if (
            type(batch) == MultiAgentBatch
            and self._storage_unit != StorageUnit.TIMESTEPS
        ):
            raise ValueError(
                "Can not add MultiAgentBatch to ReplayBuffer "
                "with storage_unit {}"
                "".format(str(self._storage_unit))
            )

        if self._storage_unit == StorageUnit.TIMESTEPS:
            self._add_single_batch(batch, **kwargs)

        elif self._storage_unit == StorageUnit.SEQUENCES:
            timestep_count = 0
            for seq_len in batch.get(SampleBatch.SEQ_LENS):
                start_seq = timestep_count
                end_seq = timestep_count + seq_len
                self._add_single_batch(batch[start_seq:end_seq], **kwargs)
                timestep_count = end_seq

        elif self._storage_unit == StorageUnit.EPISODES:
            for eps in batch.split_by_episode():
                if (
                    eps.get(SampleBatch.T)[0] == 0
                    and eps.get(SampleBatch.DONES)[-1] == True  # noqa E712
                ):
                    # Only add full episodes to the buffer
                    self._add_single_batch(eps, **kwargs)
                else:
                    if log_once("only_full_episodes"):
                        logger.info(
                            "This buffer uses episodes as a storage "
                            "unit and thus allows only full episodes "
                            "to be added to it. Some samples may be "
                            "dropped."
                        )

    @ExperimentalAPI
    def _add_single_batch(self, item: SampleBatchType, **kwargs) -> None:
        """Add a SampleBatch of experiences to self._storage.

        An item consists of either one or more timesteps, a sequence or an
        episode. Differs from add() in that it does not consider the storage
        unit or type of batch and simply stores it.

        Args:
            item: The batch to be added.
            **kwargs: Forward compatibility kwargs.
        """
        self._num_timesteps_added += item.count
        self._num_timesteps_added_wrap += item.count

        if self._next_idx >= len(self._storage):
            self._storage.append(item)
            self._est_size_bytes += item.size_bytes()
        else:
            self._storage[self._next_idx] = item

        # Eviction of older samples has already started (buffer is "full").
        if self._eviction_started:
            self._evicted_hit_stats.push(self._hit_count[self._next_idx])
            self._hit_count[self._next_idx] = 0

        # Wrap around storage as a circular buffer once we hit capacity.
        if self._num_timesteps_added_wrap >= self.capacity:
            self._eviction_started = True
            self._num_timesteps_added_wrap = 0
            self._next_idx = 0
        else:
            self._next_idx += 1

    @ExperimentalAPI
    def sample(self, num_items: int, **kwargs) -> Optional[SampleBatchType]:
        """Samples `num_items` items from this buffer.

        Samples in the results may be repeated.

        Examples for storage of SamplesBatches:
        - If storage unit `timesteps` has been chosen and batches of
        size 5 have been added, sample(5) will yield a concatenated batch of
        15 timesteps.
        - If storage unit 'sequences' has been chosen and sequences of
        different lengths have been added, sample(5) will yield a concatenated
        batch with a number of timesteps equal to the sum of timesteps in
        the 5 sampled sequences.
        - If storage unit 'episodes' has been chosen and episodes of
        different lengths have been added, sample(5) will yield a concatenated
        batch with a number of timesteps equal to the sum of timesteps in
        the 5 sampled episodes.

        Args:
            num_items: Number of items to sample from this buffer.
            **kwargs: Forward compatibility kwargs.

        Returns:
            Concatenated batch of items.
        """
        idxes = [random.randint(0, len(self) - 1) for _ in range(num_items)]
        sample = self._encode_sample(idxes)
        self._num_timesteps_sampled += sample.count
        return sample

    @ExperimentalAPI
    def stats(self, debug: bool = False) -> dict:
        """Returns the stats of this buffer.

        Args:
            debug: If True, adds sample eviction statistics to the returned
                stats dict.

        Returns:
            A dictionary of stats about this buffer.
        """
        data = {
            "added_count": self._num_timesteps_added,
            "added_count_wrapped": self._num_timesteps_added_wrap,
            "eviction_started": self._eviction_started,
            "sampled_count": self._num_timesteps_sampled,
            "est_size_bytes": self._est_size_bytes,
            "num_entries": len(self._storage),
        }
        if debug:
            data.update(self._evicted_hit_stats.stats())
        return data

    @ExperimentalAPI
    def get_state(self) -> Dict[str, Any]:
        """Returns all local state.

        Returns:
            The serializable local state.
        """
        state = {"_storage": self._storage, "_next_idx": self._next_idx}
        state.update(self.stats(debug=False))
        return state

    @ExperimentalAPI
    def set_state(self, state: Dict[str, Any]) -> None:
        """Restores all local state to the provided `state`.

        Args:
            state: The new state to set this buffer. Can be
                obtained by calling `self.get_state()`.
        """
        # The actual storage.
        self._storage = state["_storage"]
        self._next_idx = state["_next_idx"]
        # Stats and counts.
        self._num_timesteps_added = state["added_count"]
        self._num_timesteps_added_wrap = state["added_count_wrapped"]
        self._eviction_started = state["eviction_started"]
        self._num_timesteps_sampled = state["sampled_count"]
        self._est_size_bytes = state["est_size_bytes"]

    def _encode_sample(self, idxes: List[int]) -> SampleBatchType:
        """Fetches concatenated samples at given indeces from the storage."""
        samples = [self._storage[i] for i in idxes]

        if samples:
            # Assume all samples are of same type
            sample_type = type(samples[0])
            out = sample_type.concat_samples(samples)
        else:
            out = SampleBatch()
        out.decompress_if_needed()
        return out

    def get_host(self) -> str:
        """Returns the computer's network name.

        Returns:
            The computer's networks name or an empty string, if the network
            name could not be determined.
        """
        return platform.node()
