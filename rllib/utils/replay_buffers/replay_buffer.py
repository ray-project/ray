import logging
import platform
from typing import Any, Dict, List, Optional, Callable, Union

import random
from enum import Enum, unique

from ray.util.debug import log_once
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.replay_buffers.storage import (
    InMemoryStorage,
    LocalStorage,
    OnDiskStorage,
)
from ray.rllib.utils.typing import SampleBatchType, T
from ray.util.annotations import DeveloperAPI
from ray.util.iter import ParallelIteratorWorker

# Constant that represents all policies in lockstep replay mode.
_ALL_POLICIES = "__all__"

logger = logging.getLogger(__name__)


@DeveloperAPI
@unique
class StorageUnit(str, Enum):
    TIMESTEPS = "timesteps"
    SEQUENCES = "sequences"
    EPISODES = "episodes"
    FRAGMENTS = "fragments"


@DeveloperAPI
@unique
class StorageLocation(str, Enum):
    ON_DISK = "on_disk"
    IN_MEMORY = "in_memory"


@DeveloperAPI
class ReplayBuffer:
    def __init__(
        self,
        capacity: int = 10000,
        storage_unit: Union[str, StorageUnit] = "timesteps",
        storage_location: Union[str, StorageLocation] = "in_memory",
        **kwargs,
    ):
        """Initializes a (FIFO) ReplayBuffer instance.

        Args:
            capacity: Max number of timesteps to store in this FIFO
                buffer. After reaching this number, older samples will be
                dropped to make space for new ones.
            storage_unit: Either 'timesteps', `sequences` or
                `episodes`. Specifies how experiences are stored.
            storage_location: Either 'in_memory' or 'on_disk'.
                Specifies where experiences are stored.
            **kwargs: Forward compatibility kwargs.
        """
        if storage_unit not in list(StorageUnit):
            raise ValueError(
                "storage_unit must be one of {}.".format(
                    ", ".join(f"'{s}'" for s in StorageUnit)
                )
            )
        self._storage_unit = storage_unit

        # Caps the number of timesteps stored in this buffer
        if capacity <= 0:
            raise ValueError(
                "Capacity of replay buffer has to be greater than zero "
                "but was set to {}.".format(capacity)
            )

        # The actual storage (stores SampleBatches or MultiAgentBatches).
        if storage_location not in list(StorageLocation):
            raise ValueError(
                "storage_location must be one of {}.".format(
                    ", ".join(f"'{s}'" for s in StorageLocation)
                )
            )
        self._storage_location = storage_location
        self._storage = self._create_storage(capacity)

        # Number of (single) timesteps that have been sampled from the buffer
        # over its lifetime.
        self._num_timesteps_sampled = 0

        self.batch_size = None

    def __len__(self) -> int:
        """Returns the number of items currently stored in this buffer."""
        return len(self._storage)

    @DeveloperAPI
    @property
    def capacity(self) -> int:
        """Capacity of the replay buffer (`int`, read-only)."""
        return self._storage.capacity

    @DeveloperAPI
    def add(self, batch: SampleBatchType, **kwargs) -> None:
        """Adds a batch of experiences to this buffer.

        Also splits experiences into chunks of timesteps, sequences
        or episodes, depending on self._storage_unit. Calls
        self._add_single_batch.

        Args:
            batch: Batch to add to this buffer's storage.
            **kwargs: Forward compatibility kwargs.
        """
        if not batch.count > 0:
            return

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
        elif self._storage_unit == StorageUnit.FRAGMENTS:
            self._add_single_batch(batch, **kwargs)

    @DeveloperAPI
    def _add_single_batch(self, item: SampleBatchType, **kwargs) -> None:
        """Add a SampleBatch of experiences to self._storage.

        An item consists of either one or more timesteps, a sequence or an
        episode. Differs from add() in that it does not consider the storage
        unit or type of batch and simply stores it.

        Args:
            item: The batch to be added.
            **kwargs: Forward compatibility kwargs.
        """
        self._storage.add(item)

    @DeveloperAPI
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
            "_storage_unit": self._storage_unit,
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
        self._storage_unit = state["_storage_unit"]
        self._storage_location = state["_storage_location"]
        self._storage = self._create_storage(1)
        self._storage.set_state(state["_storage"])
        # Stats and counts.
        self._num_timesteps_sampled = state["sampled_count"]

    def _create_storage(self, capacity: int) -> LocalStorage:
        if self._storage_location == StorageLocation.IN_MEMORY:
            return InMemoryStorage(capacity)
        elif self._storage_location == StorageLocation.ON_DISK:
            return OnDiskStorage(capacity)
        raise ValueError("Unknown storage location: {}".format(self._storage_location))

    @DeveloperAPI
    def _encode_sample(self, idxes: List[int]) -> SampleBatchType:
        """Fetches concatenated samples at given indeces from the storage."""
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

    @DeveloperAPI
    def apply(
        self,
        func: Callable[["ReplayBuffer", Optional[Any], Optional[Any]], T],
        *_args,
        **kwargs,
    ) -> T:
        """Calls the given function with this ReplayBuffer instance.

        This is useful if we want to apply a function to a set of remote actors.

        Args:
            func: A callable that accepts the replay buffer itself, args and kwargs
            *_arkgs: Any args to pass to func
            **kwargs: Any kwargs to pass to func

        Returns:
            Return value of the induced function call
        """
        return func(self, *_args, **kwargs)

    @Deprecated(old="ReplayBuffer.add_batch()", new="ReplayBuffer.add()", error=False)
    def add_batch(self, *args, **kwargs):
        return self.add(*args, **kwargs)

    @Deprecated(
        old="ReplayBuffer.replay(num_items)",
        new="ReplayBuffer.sample(num_items)",
        error=False,
    )
    def replay(self, num_items):
        return self.sample(num_items)

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
