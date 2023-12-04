from enum import Enum
import logging
import numpy as np
import random
from typing import Any, Dict, List, Optional, Union

# Import ray before psutil will make sure we use psutil's bundled version
import ray  # noqa F401
import psutil

from ray.rllib.policy.sample_batch import SampleBatch, concat_samples
from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.metrics.window_stat import WindowStat
from ray.rllib.utils.replay_buffers.base import ReplayBufferInterface
from ray.rllib.utils.typing import SampleBatchType
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once

# Constant that represents all policies in lockstep replay mode.
_ALL_POLICIES = "__all__"

logger = logging.getLogger(__name__)


@DeveloperAPI
class StorageUnit(Enum):
    """Specifies how batches are structured in a ReplayBuffer.

    timesteps: One buffer slot per timestep.
    sequences: One buffer slot per sequence.
    episodes: One buffer slot per episode.
    fragemts: One buffer slot per incoming batch.
    """

    TIMESTEPS = "timesteps"
    SEQUENCES = "sequences"
    EPISODES = "episodes"
    FRAGMENTS = "fragments"


@DeveloperAPI
def warn_replay_capacity(*, item: SampleBatchType, num_items: int) -> None:
    """Warn if the configured replay buffer capacity is too large."""
    if log_once("replay_capacity"):
        item_size = item.size_bytes()
        psutil_mem = psutil.virtual_memory()
        total_gb = psutil_mem.total / 1e9
        mem_size = num_items * item_size / 1e9
        msg = (
            "Estimated max memory usage for replay buffer is {} GB "
            "({} batches of size {}, {} bytes each), "
            "available system memory is {} GB".format(
                mem_size, num_items, item.count, item_size, total_gb
            )
        )
        if mem_size > total_gb:
            raise ValueError(msg)
        elif mem_size > 0.2 * total_gb:
            logger.warning(msg)
        else:
            logger.info(msg)


@DeveloperAPI
class ReplayBuffer(ReplayBufferInterface, FaultAwareApply):
    """The lowest-level replay buffer interface used by RLlib.

    This class implements a basic ring-type of buffer with random sampling.
    ReplayBuffer is the base class for advanced types that add functionality while
    retaining compatibility through inheritance.

    The following examples show how buffers behave with different storage_units
    and capacities. This behaviour is generally similar for other buffers, although
    they might not implement all storage_units.

    Examples:

    .. testcode::

        from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer
        from ray.rllib.utils.replay_buffers.replay_buffer import StorageUnit
        from ray.rllib.policy.sample_batch import SampleBatch

        # Store any batch as a whole
        buffer = ReplayBuffer(capacity=10, storage_unit=StorageUnit.FRAGMENTS)
        buffer.add(SampleBatch({"a": [1], "b": [2, 3, 4]}))
        buffer.sample(1)

        # Store only complete episodes
        buffer = ReplayBuffer(capacity=10,
                                storage_unit=StorageUnit.EPISODES)
        buffer.add(SampleBatch({"c": [1, 2, 3, 4],
                                SampleBatch.T: [0, 1, 0, 1],
                                SampleBatch.TERMINATEDS: [False, True, False, True],
                                SampleBatch.EPS_ID: [0, 0, 1, 1]}))
        buffer.sample(1)

        # Store single timesteps
        buffer = ReplayBuffer(capacity=2, storage_unit=StorageUnit.TIMESTEPS)
        buffer.add(SampleBatch({"a": [1, 2], SampleBatch.T: [0, 1]}))
        buffer.sample(1)

        buffer.add(SampleBatch({"a": [3], SampleBatch.T: [2]}))
        print(buffer._eviction_started)
        buffer.sample(1)

        buffer = ReplayBuffer(capacity=10, storage_unit=StorageUnit.SEQUENCES)
        buffer.add(SampleBatch({"c": [1, 2, 3], SampleBatch.SEQ_LENS: [1, 2]}))
        buffer.sample(1)

    .. testoutput::

        True

    `True` is not the output of the above testcode, but an artifact of unexpected
    behaviour of sphinx doctests.
    (see https://github.com/ray-project/ray/pull/32477#discussion_r1106776101)
    """

    def __init__(
        self,
        capacity: int = 10000,
        storage_unit: Union[str, StorageUnit] = "timesteps",
        **kwargs,
    ):
        """Initializes a (FIFO) ReplayBuffer instance.

        Args:
            capacity: Max number of timesteps to store in this FIFO
                buffer. After reaching this number, older samples will be
                dropped to make space for new ones.
            storage_unit: If not a StorageUnit, either 'timesteps', 'sequences' or
                'episodes'. Specifies how experiences are stored.
            ``**kwargs``: Forward compatibility kwargs.
        """

        if storage_unit in ["timesteps", StorageUnit.TIMESTEPS]:
            self.storage_unit = StorageUnit.TIMESTEPS
        elif storage_unit in ["sequences", StorageUnit.SEQUENCES]:
            self.storage_unit = StorageUnit.SEQUENCES
        elif storage_unit in ["episodes", StorageUnit.EPISODES]:
            self.storage_unit = StorageUnit.EPISODES
        elif storage_unit in ["fragments", StorageUnit.FRAGMENTS]:
            self.storage_unit = StorageUnit.FRAGMENTS
        else:
            raise ValueError(
                f"storage_unit must be either '{StorageUnit.TIMESTEPS}', "
                f"'{StorageUnit.SEQUENCES}', '{StorageUnit.EPISODES}' "
                f"or '{StorageUnit.FRAGMENTS}', but is {storage_unit}"
            )

        # The actual storage (list of SampleBatches or MultiAgentBatches).
        self._storage = []

        # Caps the number of timesteps stored in this buffer
        if capacity <= 0:
            raise ValueError(
                "Capacity of replay buffer has to be greater than zero "
                "but was set to {}.".format(capacity)
            )
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

    @override(ReplayBufferInterface)
    def __len__(self) -> int:
        return len(self._storage)

    @override(ReplayBufferInterface)
    def add(self, batch: SampleBatchType, **kwargs) -> None:
        """Adds a batch of experiences or other data to this buffer.

        Splits batch into chunks of timesteps, sequences or episodes, depending on
        `self._storage_unit`. Calls `self._add_single_batch` to add resulting slices
        to the buffer storage.

        Args:
            batch: The batch to add.
            ``**kwargs``: Forward compatibility kwargs.
        """
        if not batch.count > 0:
            return

        warn_replay_capacity(item=batch, num_items=self.capacity / batch.count)

        if self.storage_unit == StorageUnit.TIMESTEPS:
            timeslices = batch.timeslices(1)
            for t in timeslices:
                self._add_single_batch(t, **kwargs)

        elif self.storage_unit == StorageUnit.SEQUENCES:
            timestep_count = 0
            for seq_len in batch.get(SampleBatch.SEQ_LENS):
                start_seq = timestep_count
                end_seq = timestep_count + seq_len
                self._add_single_batch(batch[start_seq:end_seq], **kwargs)
                timestep_count = end_seq

        elif self.storage_unit == StorageUnit.EPISODES:
            for eps in batch.split_by_episode():
                if eps.get(SampleBatch.T, [0])[0] == 0 and (
                    eps.get(SampleBatch.TERMINATEDS, [True])[-1]
                    or eps.get(SampleBatch.TRUNCATEDS, [False])[-1]
                ):
                    # Only add full episodes to the buffer
                    # Check only if info is available
                    self._add_single_batch(eps, **kwargs)
                else:
                    if log_once("only_full_episodes"):
                        logger.info(
                            "This buffer uses episodes as a storage "
                            "unit and thus allows only full episodes "
                            "to be added to it (starting from T=0 and ending in "
                            "`terminateds=True` or `truncateds=True`. "
                            "Some samples may be dropped."
                        )

        elif self.storage_unit == StorageUnit.FRAGMENTS:
            self._add_single_batch(batch, **kwargs)

    @DeveloperAPI
    def _add_single_batch(self, item: SampleBatchType, **kwargs) -> None:
        """Add a SampleBatch of experiences to self._storage.

        An item consists of either one or more timesteps, a sequence or an
        episode. Differs from add() in that it does not consider the storage
        unit or type of batch and simply stores it.

        Args:
            item: The batch to be added.
            ``**kwargs``: Forward compatibility kwargs.
        """
        self._num_timesteps_added += item.count
        self._num_timesteps_added_wrap += item.count

        if self._next_idx >= len(self._storage):
            self._storage.append(item)
            self._est_size_bytes += item.size_bytes()
        else:
            item_to_be_removed = self._storage[self._next_idx]
            self._est_size_bytes -= item_to_be_removed.size_bytes()
            self._storage[self._next_idx] = item
            self._est_size_bytes += item.size_bytes()

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

    @override(ReplayBufferInterface)
    def sample(
        self, num_items: Optional[int] = None, **kwargs
    ) -> Optional[SampleBatchType]:
        """Samples `num_items` items from this buffer.

        The items depend on the buffer's storage_unit.
        Samples in the results may be repeated.

        Examples for sampling results:

        1) If storage unit 'timesteps' has been chosen and batches of
        size 5 have been added, sample(5) will yield a concatenated batch of
        15 timesteps.

        2) If storage unit 'sequences' has been chosen and sequences of
        different lengths have been added, sample(5) will yield a concatenated
        batch with a number of timesteps equal to the sum of timesteps in
        the 5 sampled sequences.

        3) If storage unit 'episodes' has been chosen and episodes of
        different lengths have been added, sample(5) will yield a concatenated
        batch with a number of timesteps equal to the sum of timesteps in
        the 5 sampled episodes.

        Args:
            num_items: Number of items to sample from this buffer.
            ``**kwargs``: Forward compatibility kwargs.

        Returns:
            Concatenated batch of items.
        """
        if len(self) == 0:
            raise ValueError("Trying to sample from an empty buffer.")
        idxes = [random.randint(0, len(self) - 1) for _ in range(num_items)]
        sample = self._encode_sample(idxes)
        self._num_timesteps_sampled += sample.count
        return sample

    @DeveloperAPI
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

    @override(ReplayBufferInterface)
    def get_state(self) -> Dict[str, Any]:
        state = {"_storage": self._storage, "_next_idx": self._next_idx}
        state.update(self.stats(debug=False))
        return state

    @override(ReplayBufferInterface)
    def set_state(self, state: Dict[str, Any]) -> None:
        # The actual storage.
        self._storage = state["_storage"]
        self._next_idx = state["_next_idx"]
        # Stats and counts.
        self._num_timesteps_added = state["added_count"]
        self._num_timesteps_added_wrap = state["added_count_wrapped"]
        self._eviction_started = state["eviction_started"]
        self._num_timesteps_sampled = state["sampled_count"]
        self._est_size_bytes = state["est_size_bytes"]

    @DeveloperAPI
    def _encode_sample(self, idxes: List[int]) -> SampleBatchType:
        """Fetches concatenated samples at given indices from the storage."""
        samples = []
        for i in idxes:
            self._hit_count[i] += 1
            samples.append(self._storage[i])

        if samples:
            # We assume all samples are of same type
            out = concat_samples(samples)
        else:
            out = SampleBatch()
        out.decompress_if_needed()
        return out

    @Deprecated(
        help="ReplayBuffers could be iterated over by default before. "
        "Making a buffer an iterator has been deprecated. Switch your Algorithm to "
        "override the `training_step()` method (instead of `execution_plan()`) to "
        "resolve this.",
        error=True,
    )
    def make_iterator(self, num_items_to_replay: int):
        pass
