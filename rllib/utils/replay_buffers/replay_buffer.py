from enum import Enum
import logging
import platform
from typing import Any, Dict, List, Optional, Union
import math

import random
from enum import Enum, unique

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.replay_buffers.storage import (
    InMemoryStorage,
    LocalStorage,
    OnDiskStorage,
)
from ray.rllib.utils.typing import SampleBatchType, T
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once
from ray.util.iter import ParallelIteratorWorker

# Constant that represents all policies in lockstep replay mode.
_ALL_POLICIES = "__all__"

logger = logging.getLogger(__name__)


@DeveloperAPI
@unique
class StorageUnit(str, Enum):
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
@unique
class StorageLocation(str, Enum):
    ON_DISK = "disk"
    IN_MEMORY = "memory"


# TODO (artur): Remove ParallelIteratorWorker once we no longer support executionplans
@DeveloperAPI
class ReplayBuffer(ParallelIteratorWorker, FaultAwareApply):
    """The lowest-level replay buffer interface used by RLlib.

    This class implements a basic ring-type of buffer with random sampling.
    ReplayBuffer is the base class for advanced types that add functionality while
    retaining compatibility through inheritance.

    The following examples show how buffers behave with different storage_units
    and capacities. This behaviour is generally similar for other buffers, although
    they might not implement all storage_units.

    Examples:
        >>> from ray.rllib.utils.replay_buffers import ReplayBuffer, # doctest: +SKIP
        ...                         StorageUnit # doctest: +SKIP
        >>> from ray.rllib.policy.sample_batch import SampleBatch # doctest: +SKIP
        >>> # Store any batch as a whole
        >>> buffer = ReplayBuffer(capacity=10,
        ...                         storage_unit=StorageUnit.FRAGMENTS) # doctest: +SKIP
        >>> buffer.add(SampleBatch({"a": [1], "b": [2, 3, 4]})) # doctest: +SKIP
        >>> print(buffer.sample(1)) # doctest: +SKIP
        >>> # SampleBatch(1: ['a', 'b'])
        >>> # Store only complete episodes
        >>> buffer = ReplayBuffer(capacity=10,
        ...                         storage_unit=StorageUnit.EPISODES) # doctest: +SKIP
        >>> buffer.add(SampleBatch({"c": [1, 2, 3, 4], # doctest: +SKIP
        ...                        SampleBatch.T: [0, 1, 0, 1],
        ...                        SampleBatch.TERMINATEDS: [False, True, False, True],
        ...                        SampleBatch.EPS_ID: [0, 0, 1, 1]})) # doctest: +SKIP
        >>> eps_n = buffer.sample(1) # doctest: +SKIP
        >>> print(eps_n[SampleBatch.EPS_ID]) # doctest: +SKIP
        >>> # [1 1]
        >>> # Store single timesteps
        >>> buffer = ReplayBuffer(capacity=2,  # doctest: +SKIP
        ...                         storage_unit=StorageUnit.TIMESTEPS) # doctest: +SKIP
        >>> buffer.add(SampleBatch({"a": [1, 2],
        ...                         SampleBatch.T: [0, 1]})) # doctest: +SKIP
        >>> t_n = buffer.sample(1) # doctest: +SKIP
        >>> print(t_n["a"]) # doctest: +SKIP
        >>> # [2]
        >>> buffer.add(SampleBatch({"a": [3], SampleBatch.T: [2]})) # doctest: +SKIP
        >>> print(buffer._eviction_started) # doctest: +SKIP
        >>> # True
        >>> t_n = buffer.sample(1) # doctest: +SKIP
        >>> print(t_n["a"]) # doctest: +SKIP
        >>> # [3] # doctest: +SKIP
        >>> buffer = ReplayBuffer(capacity=10, # doctest: +SKIP
        ...                         storage_unit=StorageUnit.SEQUENCES) # doctest: +SKIP
        >>> buffer.add(SampleBatch({"c": [1, 2, 3], # doctest: +SKIP
        ...                        SampleBatch.SEQ_LENS: [1, 2]})) # doctest: +SKIP
        >>> seq_n = buffer.sample(1) # doctest: +SKIP
        >>> print(seq_n["c"]) # doctest: +SKIP
        >>> # [1]
    """

    def __init__(
        self,
        capacity_items: int = 10000,
        capacity_ts: int = math.inf,
        capacity_bytes: int = math.inf,
        storage_unit: Union[str, StorageUnit] = "timesteps",
        storage_location: Union[str, StorageLocation] = "memory",
        **kwargs,
    ):
        """Initializes a (FIFO) ReplayBuffer instance.

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
            storage_unit: If not a StorageUnit, either 'timesteps',
            'sequences' or 'episodes'. Specifies how experiences are stored.
            ``**kwargs``: Forward compatibility kwargs.
            storage_location: Either 'memory' or 'disk'.
                Specifies where experiences are stored.
            **kwargs: Forward compatibility kwargs.
        """
        if storage_unit not in list(StorageUnit):
            raise ValueError(
                "storage_unit must be one of {}.".format(
                    ", ".join(f"'{s}'" for s in StorageUnit)
                )
            )
        self.storage_unit = storage_unit

        # Caps the number of timesteps stored in this buffer
        if capacity_ts <= 0:
            raise ValueError(
                "Capacity of replay buffer has to be greater than zero "
                "but was set to {}.".format(capacity_ts)
            )

        # The actual storage (stores SampleBatches or MultiAgentBatches).
        if storage_location not in list(StorageLocation):
            raise ValueError(
                "storage_location must be one of {}.".format(
                    ", ".join(f"'{s}'" for s in StorageLocation)
                )
            )
        self._storage_location = storage_location
        self._storage = self._create_storage(
            capacity_items=capacity_items,
            capacity_ts=capacity_ts,
            capacity_bytes=capacity_bytes,
        )

        # Number of (single) timesteps that have been sampled from the buffer
        # over its lifetime.
        self._num_timesteps_sampled = 0

        self.batch_size = None

    def __len__(self) -> int:
        """Returns the number of items currently stored in this buffer."""
        return len(self._storage)

    @property
    def capacity_ts(self) -> int:
        """Maximum number of timesteps the storage may contain."""
        return self._storage._capacity_ts

    @property
    def capacity_items(self) -> int:
        """Maximum number of items the storage may contain."""
        return self._storage._capacity_items

    @property
    def capacity_bytes(self) -> int:
        """Maximum number of bytes the storage may contain."""
        return self._storage._capacity_bytes

    @DeveloperAPI
    def add(self, batch: SampleBatchType, **kwargs) -> None:
        """Adds a batch of experiences to this buffer.

        Splits batch into chunks of timesteps, sequences or episodes, depending on
        `self.storage_unit`. Calls `self._add_single_batch` to add resulting slices
        to the buffer storage.

        Args:
            batch: Batch to add.
            ``**kwargs``: Forward compatibility kwargs.
        """
        if not batch.count > 0:
            return

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
        self._storage.add(item)

    @DeveloperAPI
    def sample(self, num_items: int, **kwargs) -> Optional[SampleBatchType]:
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
            "sampled_count": self._num_timesteps_sampled,
            "added_count": self._storage.num_timesteps_added,
            "current_count": self._storage.num_timesteps,
            "eviction_started": self._storage.eviction_started,
            "size_bytes": self._storage.size_bytes,
            "num_entries": len(self._storage),
        }
        if debug:
            data.update(self._storage.evicted_hit_stats)
        return data

    @DeveloperAPI
    def get_state(self) -> Dict[str, Any]:
        """Returns all local state.

        Returns:
            The serializable local state.
        """
        state = {
            "_storage": self._storage.get_state(),
            "storage_unit": self.storage_unit,
            "_storage_location": self._storage_location,
        }
        state.update(self.stats(debug=False))
        return state

    @DeveloperAPI
    def set_state(self, state: Dict[str, Any]) -> None:
        """Restores all local state to the provided `state`.

        Args:
            state: The new state to set this buffer. Can be
                obtained by calling `self.get_state()`.
        """
        # The actual storage.
        self.storage_unit = state["storage_unit"]
        self._storage_location = state["_storage_location"]
        self._storage = self._create_storage(capacity_items=1)
        self._storage.set_state(state["_storage"])
        # Stats and counts.
        self._num_timesteps_sampled = state["sampled_count"]

    def _create_storage(self, **kwargs) -> LocalStorage:
        if self._storage_location == StorageLocation.IN_MEMORY:
            return InMemoryStorage(**kwargs)
        elif self._storage_location == StorageLocation.ON_DISK:
            return OnDiskStorage(**kwargs)
        else:
            raise ValueError(
                "Unknown storage location: {}".format(self._storage_location)
            )

    @DeveloperAPI
    def _encode_sample(self, idxes: List[int]) -> SampleBatchType:
        """Fetches concatenated samples at given indices from the storage."""
        samples = []
        for i in idxes:
            samples.append(self._storage[i])

        if samples:
            # We assume all samples are of same type
            sample_type = type(samples[0])
            out = sample_type.concat_samples(samples)
        else:
            out = SampleBatch()
        out.decompress_if_needed()
        return out

    @DeveloperAPI
    def get_host(self) -> str:
        """Returns the computer's network name.

        Returns:
            The computer's networks name or an empty string, if the network
            name could not be determined.
        """
        return platform.node()

    @Deprecated(new="ReplayBuffer.add()", error=True)
    def add_batch(self, *args, **kwargs):
        pass

    @Deprecated(
        old="ReplayBuffer.replay(num_items)",
        new="ReplayBuffer.sample(num_items)",
        error=True,
    )
    def replay(self, num_items):
        pass

    @Deprecated(
        help="ReplayBuffers could be iterated over by default before. "
        "Making a buffer an iterator will soon "
        "be deprecated altogether. Consider switching to the training "
        "iteration API to resolve this.",
        error=False,
    )
    def make_iterator(self, num_items_to_replay: int):
        """Make this buffer a ParallelIteratorWorker to retain compatibility.

        Execution plans have made heavy use of buffers as ParallelIteratorWorkers.
        This method provides an easy way to support this for now.
        """

        def gen_replay():
            while True:
                yield self.sample(num_items_to_replay)

        ParallelIteratorWorker.__init__(self, gen_replay, False)
