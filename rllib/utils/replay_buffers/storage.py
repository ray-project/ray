import logging
import numpy as np
import os
import pickle
import shelve

# Import ray before psutil will make sure we use psutil's bundled version
import ray  # noqa F401
import psutil
import shutil

from abc import abstractmethod
from collections.abc import Sized, Iterable
from enum import Enum, unique
from typing import Optional, Dict, Any, Iterator, Union, overload
from tempfile import NamedTemporaryFile

from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.metrics.window_stat import WindowStat
from ray.rllib.utils.typing import SampleBatchType
from ray.util.debug import log_once

logger = logging.getLogger(__name__)


@ExperimentalAPI
@unique
class AllocationPlan(str, Enum):
    ONE_TIME = "one-time"
    DYNAMIC = "dynamic"


@ExperimentalAPI
class LocalStorage(Sized, Iterable):
    @ExperimentalAPI
    def __init__(
        self,
        capacity: int = 10000,
        allocation_plan: Union[str, AllocationPlan] = "one-time",
    ) -> None:
        """Initializes an empty LocalStorage instance for storing timesteps in a ring buffer.

        The storage is indexed for fast random access of stored items and takes care
        of properly adding and removing items with respect to its capacity.

        Args:
            capacity: Maximum number of timesteps to store in this FIFO
                buffer. After reaching this number, older samples will be
                dropped to make space for new ones.
            allocation_plan: Either `one-time` or `dynamic`. Specifies how
                space for the ring buffer is allocated by the storage.
        """
        if capacity < 1:
            raise ValueError("Storage capacity must be strictly positive")
        # Capacity of the storage in terms of timesteps
        # Each item stored in the storage may consist of multiple timesteps
        self._capacity = capacity

        if allocation_plan == AllocationPlan.ONE_TIME:
            initial_size = capacity
        elif allocation_plan == AllocationPlan.DYNAMIC:
            initial_size = 0
        else:
            raise ValueError(
                "allocation_plan must be one of {}.".format(
                    ", ".join(f"'{s}'" for s in AllocationPlan)
                )
            )

        # Whether we have already hit our capacity (and have therefore
        # started to evict older samples).
        self._eviction_started = False
        # Maximum number of items that can be stored in ring buffer
        # (max_items <= capacity)
        # max_items is increasing while capacity is not reached but it never decreases
        self._max_items = initial_size
        # Number of items currently in storage (num_items <= max_items)
        self._num_items = 0
        # Index of first, i.e. oldest, item in storage (offset_idx < max_items)
        self._offset_idx = 0

        # Number of (single) timesteps that have been added to the buffer
        # over its lifetime. Note that each added item (batch) may contain
        # more than one timestep.
        self._num_timesteps_added = 0
        # Number of timesteps currently in storage
        # (num_items <= num_timesteps <= capacity)
        self._num_timesteps = 0

        # Statistics
        # len(self._hit_count) == capacity
        self._hit_count = np.zeros(self.capacity, dtype=np.int64)
        self._evicted_hit_stats = WindowStat("evicted_hit", 1000)
        self._size_bytes = 0

    @ExperimentalAPI
    @property
    def capacity(self) -> int:
        """Maximum number of timesteps the storage may contain
        (`int`, read-only).
        """
        return self._capacity

    @ExperimentalAPI
    @property
    def size_bytes(self) -> int:
        """Current size of the data inside the storage in bytes
        (`int`, read-only).
        """
        return self._size_bytes

    @ExperimentalAPI
    @property
    def evicted_hit_stats(self) -> Dict[str, Any]:
        """Hit statistics for items in storage including mean,
        std, and quantiles (`dict`, read-only).
        """
        return self._evicted_hit_stats.stats()

    @ExperimentalAPI
    @property
    def eviction_started(self) -> bool:
        """Whether eviction of items started, i.e. storage
        is "full" (`bool`, read-only).
        """
        return self._eviction_started

    @ExperimentalAPI
    @property
    def num_timesteps_added(self) -> int:
        """Total number of timesteps added to the storage
        over its lifetime (`int`, read-only).
        """
        return self._num_timesteps_added

    @ExperimentalAPI
    @property
    def num_timesteps(self) -> int:
        """Number of timesteps currently in the storage
        (`int`, read-only).
        """
        return self._num_timesteps

    @ExperimentalAPI
    def get_state(self) -> Dict[str, Any]:
        """Returns all local state.

        Returns:
            The serializable local state.
        """
        state = {
            "_capacity": self._capacity,
            "_max_items": self._max_items,
            "_num_items": self._num_items,
            "_offset_idx": self._offset_idx,
            "_eviction_started": self._eviction_started,
            "_num_timesteps_added": self._num_timesteps_added,
            "_num_timesteps": self._num_timesteps,
            "_size_bytes": self._size_bytes,
        }
        return state

    @ExperimentalAPI
    def set_state(self, state: Dict[str, Any]) -> None:
        """Restores all local state to the provided `state`.

        Args:
            state: The new state to set this buffer. Can be
                obtained by calling `self.get_state()`.
        """
        self._capacity = state["_capacity"]
        self._max_items = state["_max_items"]
        self._num_items = state["_num_items"]
        self._offset_idx = state["_offset_idx"]
        self._eviction_started = state["_eviction_started"]
        self._num_timesteps_added = state["_num_timesteps_added"]
        self._num_timesteps = state["_num_timesteps"]
        self._size_bytes = state["_size_bytes"]
        self._hit_count = np.zeros(self.capacity, dtype=np.int64)

    @ExperimentalAPI
    def __len__(self) -> int:
        return self._num_items

    @ExperimentalAPI
    def __iter__(self) -> Iterator[SampleBatchType]:
        for i in range(len(self)):
            yield self[i]
    
    @overload
    def __getitem__(self, key: int) -> SampleBatchType: ...

    @overload
    def __getitem__(self, key: slice) -> "StorageView": ...

    @ExperimentalAPI
    def __getitem__(self, key):
        if isinstance(key, int):
            i = key
            while i < 0:
                i += len(self)
            while i >= len(self):
                i -= len(self)
            idx = self._get_internal_index(i)
            self._hit_count[idx] += 1
            return self._get(idx)
        elif isinstance(key, slice):
            s = key
            return StorageView(self, s)
        else:
            raise TypeError("Only single integer indices or slices are supported.")

    @ExperimentalAPI
    def __setitem__(self, i: int, item: SampleBatchType) -> None:
        if not isinstance(i, int):
            raise ValueError(
                "Only single integer indices supported for setting values."
            )
        if i >= len(self) or i < 0:
            raise IndexError("Buffer index out of range.")
        if not self.eviction_started:
            raise RuntimeError(
                "Assigning items to an index is only allowed "
                "after eviction has been started. Use .add(item) instead."
            )
        idx = self._get_internal_index(i)
        drop_item = self._del(idx)
        if drop_item.count < item.count:
            logger.warning(
                "New item consists of more timesteps than "
                "the replaced item. This violates storage capacity."
            )
        self._evicted_hit_stats.push(self._hit_count[idx])
        self._num_timesteps -= drop_item.count
        self._size_bytes -= drop_item.size_bytes()
        self._hit_count[idx] = 0
        self._num_timesteps_added += item.count
        self._num_timesteps += item.count
        self._size_bytes += item.size_bytes()
        self._set(idx, item)

    @ExperimentalAPI
    def add(self, item: SampleBatchType) -> None:
        """Add a new item to the storage. The index of the new item
        will be assigned automatically. Moreover, old items may be
        dropped with respect to the storage's capacity.

        Args:
            item: Item (batch) to add to the storage.
        """
        if item.count > self.capacity:
            logger.warning(
                "The batch to be added consists of {} timesteps "
                "which is larger than the storage capacity of {}. "
                "Therefore, the batch has not been added.".format(
                    item.count, self.capacity
                )
            )
            return

        self._num_timesteps_added += item.count
        self._num_timesteps += item.count
        self._size_bytes += item.size_bytes()

        # Drop old items.
        # May require multiple drops if newly added item
        # contains more timesteps than the old items.
        while self._num_timesteps > self.capacity:
            assert self._num_items > 0
            self._eviction_started = True
            self._evicted_hit_stats.push(self._hit_count[self._offset_idx])
            self._hit_count[self._offset_idx] = 0
            drop_item = self._del(self._offset_idx)
            self._num_timesteps -= drop_item.count
            self._size_bytes -= drop_item.size_bytes()
            self._num_items -= 1
            self._offset_idx = self._get_internal_index(1)  # Increase offset

        # Insert new item.
        # Compute index to set new item at in circular storage.
        # Wrap around once we hit capacity.
        assert self._num_items <= self._max_items
        if self._num_items == self._max_items:
            # Storage with dynamic space allocation
            assert self._max_items < self.capacity
            self._max_items += 1
            if self._offset_idx != 0:
                # When items have been evicted previously, we need to
                # increment the indices the oldest items are stored at
                # to free the index for the new item (since items live
                # in a ring buffer).
                # This is an expensive operation but should rarely happen
                # in practice, e.g. when items contain full episodes and
                # episode length decreases during training.
                for i in reversed(range(self._offset_idx + 1, self._max_items)):
                    it = self._get(i - 1)
                    self._set(i, it)
                    self._hit_count[i] = self._hit_count[i - 1]
                self._hit_count[self._offset_idx] = 0
                self._offset_idx = self._get_internal_index(1)  # Increase offset
        new_idx = self._get_internal_index(self._num_items)
        self._set(new_idx, item)
        self._num_items += 1
        assert self._num_items <= self.capacity

    def _get_internal_index(self, idx: int):
        """Translate the given external storage index into
        the internal index space of the circular buffer.

        Args:
            idx: External storage index (0 <= idx < len(storage)).

        Returns:
            Internal index from interval [0, max_items)
        """
        if idx < 0:
            raise IndexError("Buffer index out of range")
        return (self._offset_idx + idx) % self._max_items

    def _get_external_index(self, idx: int):
        """Translate the given internal circular buffer index into
        the external index space of the storage.

        Args:
            idx: Internal circular Buffer index (0 <= idx < max_items).

        Returns:
            External index from interval [0, len(storage))
        """
        if idx < 0:
            raise IndexError("Buffer index out of range")
        if idx >= self._offset_idx:
            return idx - self._offset_idx
        else:
            return idx + self._max_items - self._offset_idx

    @abstractmethod
    def _get(self, idx: int) -> SampleBatchType:
        """Get the item at the specified index / key.

        This method must be implementend by subclasses
        using an actual data structure for storing the data.
        This data structure must be capable of dealing with
        indices between 0 <= idx < `self._max_items`.

        Args:
            idx: Index of the item of interest.

        Returns:
            Item at index.
        """
        raise NotImplementedError()

    @abstractmethod
    def _set(self, idx: int, item: SampleBatchType) -> None:
        """Store the given item at the specified index / key.

        This method must be implementend by subclasses
        using an actual data structure for storing the data.
        This data structure must be capable of dealing with
        indices between 0 <= idx < `self._max_items`.

        In case of dynamic space allocation, `self._max_items`
        increases when new items are added to the storage.
        The subclass is responsible for inceasing the index
        range of its data structure as required.

        Args:
            idx: Index to store the item at.
            item: Item to store at specified index.
        """
        raise NotImplementedError()

    @abstractmethod
    def _del(self, idx: int) -> SampleBatchType:
        """Remove and return the item at the specified index / key.

        This method may be overridden by subclasses
        using an actual data structure for storing the data.
        This data structure must be capable of dealing with
        indices between 0 <= idx < `self._max_items`.

        Note: Removing the item from the actual data structure is
        not required for a properly working storage but is highly
        recommended to reduce its memory footprint.

        Args:
            idx: Index of the item of interest.

        Returns:
            Item at index that has been removed.
        """
        return self._get(idx)


@ExperimentalAPI
class StorageView(LocalStorage):
    @ExperimentalAPI
    @override(LocalStorage)
    def __init__(
        self,
        storage: LocalStorage,
        storage_slice: slice,
    ) -> None:
        """Initializes a read-only StorageView instance of a LocalStorage.

        Args:
            storage: Underlying storage.
            storage_slice: Slice of the storage
        """
        self._storage = storage
        start = storage_slice.start or 0
        stop = storage_slice.stop or len(storage)
        step = storage_slice.step or 1
        self._slice = slice(start, stop, step)
        self._idx_map = list(
            range(self._slice.start, self._slice.stop, self._slice.step)
        )

    @ExperimentalAPI
    @property
    def slice(self) -> slice:
        """Slice of the StorageView (`slice`, read-only)."""
        return self._slice

    @ExperimentalAPI
    @property
    def capacity(self) -> int:
        """Maximum number of timesteps the storage may contain
        (`int`, read-only).
        """
        return self._storage.capacity

    @ExperimentalAPI
    @property
    def size_bytes(self) -> int:
        """Current size of the data inside the storage in bytes
        (`int`, read-only).
        """
        return self._storage.size_bytes

    @ExperimentalAPI
    @property
    def evicted_hit_stats(self) -> Dict[str, Any]:
        """Hit statistics for items in storage including mean,
        std, and quantiles (`dict`, read-only).
        """
        return self._storage.evicted_hit_stats

    @ExperimentalAPI
    @property
    def eviction_started(self) -> bool:
        """Whether eviction of items started, i.e. storage
        is "full" (`bool`, read-only).
        """
        return self._storage.eviction_started

    @ExperimentalAPI
    @property
    def num_timesteps_added(self) -> int:
        """Total number of timesteps added to the storage
        over its lifetime (`int`, read-only).
        """
        return self._storage.num_timesteps_added

    @ExperimentalAPI
    @property
    def num_timesteps(self) -> int:
        """Number of timesteps currently in the storage
        (`int`, read-only).
        """
        return self._storage.num_timesteps

    @override(LocalStorage)
    def get_state(self) -> Dict[str, Any]:
        raise RuntimeError("The view of a storage is stateless.")

    @override(LocalStorage)
    def set_state(self, state: Dict[str, Any]) -> None:
        raise RuntimeError("The view of a storage is stateless.")

    @ExperimentalAPI
    @override(LocalStorage)
    def __len__(self) -> int:
        return len(self._idx_map)

    @ExperimentalAPI
    @override(LocalStorage)
    def __getitem__(self, key):
        if isinstance(key, int):
            i = key
            while i < 0:
                i += len(self)
            while i >= len(self):
                i -= len(self)
            idx = self._idx_map[i]
            return self._storage[idx]
        elif isinstance(key, slice):
            s = key
            return StorageView(self, s)
        else:
            raise TypeError("Only single integer indices or slices are supported.")

    @override(LocalStorage)
    def __setitem__(self, i: int, item: SampleBatchType) -> None:
        raise RuntimeError("The view of a storage is read-only.")

    @override(LocalStorage)
    def add(self, item: SampleBatchType) -> None:
        raise RuntimeError("The view of a storage is read-only.")

    @override(LocalStorage)
    def _get(self, idx: int) -> SampleBatchType:
        raise RuntimeError("The view of a storage is read-only.")

    @override(LocalStorage)
    def _set(self, idx: int, item: SampleBatchType) -> None:
        raise RuntimeError("The view of a storage is read-only.")

    @override(LocalStorage)
    def _del(self, idx: int) -> SampleBatchType:
        raise RuntimeError("The view of a storage is read-only.")


@ExperimentalAPI
class InMemoryStorage(LocalStorage):
    @ExperimentalAPI
    @override(LocalStorage)
    def __init__(
        self,
        capacity: int = 10000,
        allocation_plan: str = "one-time",
    ) -> None:
        """Initializes an InMemoryStorage instance for storing timesteps in memory.

        The storage uses Python's list as data structure.

        Args:
            capacity: Maximum number of timesteps to store in this FIFO
                buffer. After reaching this number, older samples will be
                dropped to make space for new ones.
            allocation_plan: Either `one-time` or `dynamic`. Specifies how
                space for the ring buffer is allocated by the storage.
        """
        super().__init__(capacity, allocation_plan)
        self._samples = [None] * self._max_items

    @ExperimentalAPI
    @override(LocalStorage)
    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state["_samples"] = self._samples
        return state

    @ExperimentalAPI
    @override(LocalStorage)
    def set_state(self, state: Dict[str, Any]) -> None:
        self._samples = state["_samples"]
        super().set_state(state)

    @override(LocalStorage)
    def _get(self, i: int) -> SampleBatchType:
        return self._samples[i]

    @override(LocalStorage)
    def _set(self, idx: int, item: SampleBatchType) -> None:
        self._warn_replay_capacity(item, self.capacity / item.count)
        if idx == len(self._samples):
            self._samples.append(item)
        else:
            self._samples[idx] = item

    @override(LocalStorage)
    def _del(self, i: int) -> SampleBatchType:
        del_sample = self._samples[i]
        self._samples[i] = None
        return del_sample

    def _warn_replay_capacity(self, item: SampleBatchType, num_items: int) -> None:
        """Warn if the configured replay buffer capacity is too large."""
        if log_once("replay_capacity_memory"):
            item_size = item.size_bytes()
            psutil_mem = psutil.virtual_memory()
            free_gb = psutil_mem.available / 1e9
            mem_size = num_items * item_size / 1e9
            remainder = mem_size - self.size_bytes / 1e9
            msg = (
                "Estimated memory usage for replay buffer is {} GB "
                "({} batches of size {}, {} bytes each), "
                "of which {} GB are pending for allocation. "
                "Available memory is {} GB.".format(
                    mem_size, num_items, item.count, item_size, remainder, free_gb
                )
            )
            if remainder > free_gb:
                raise ValueError(msg)
            elif remainder > 0.2 * free_gb:
                logger.warning(msg)
            else:
                logger.info(msg)


@ExperimentalAPI
class OnDiskStorage(LocalStorage):
    @ExperimentalAPI
    @override(LocalStorage)
    def __init__(
        self, capacity: int = 10000, buffer_file: Optional[str] = None
    ) -> None:
        """Initializes an OnDiskStorage instance for storing timesteps on disk.
        This allows replay buffers larger than memory.

        The storage uses Python's shelve as data structure.

        Args:
            capacity: Maximum number of timesteps to store in this FIFO
                buffer. After reaching this number, older samples will be
                dropped to make space for new ones.
            buffer_file: Optional buffer file to wite the data to. The file must not
                exist and the file name must end with an `.dat` extension.
        """
        super().__init__(capacity, AllocationPlan.DYNAMIC.value)
        self._buffer_file = buffer_file
        self._rm_file_on_del = False
        if not self._buffer_file:
            self._rm_file_on_del = True
            with NamedTemporaryFile(prefix="replay_buffer_", suffix=".dat") as f:
                self._buffer_file = f.name
        if os.path.exists(self._buffer_file):
            raise ValueError("buffer_file must not exist: {}".format(self._buffer_file))
        if not self._buffer_file.endswith(".dat"):
            raise ValueError("buffer_file must end with '.dat' extension")
        self._buffer_file = os.path.abspath(self._buffer_file)

        # The actual storage (shelf / dict of SampleBatches).
        if pickle.HIGHEST_PROTOCOL < 5:
            logger.warning(
                "Recommended pickle protocol is at least 5 "
                "for fast zero-copy access of arrays"
            )
        self._samples = shelve.open(
            self._buffer_file[:-4], flag="c", protocol=pickle.HIGHEST_PROTOCOL
        )
        # Make sure shelve created correct file for storage
        assert os.path.exists(self._buffer_file)

    @ExperimentalAPI
    @override(LocalStorage)
    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        state["_buffer_file"] = self._buffer_file
        state["_rm_file_on_del"] = self._rm_file_on_del
        state["_pkl_proto"] = pickle.HIGHEST_PROTOCOL
        # Never delete file since it will be reused later
        self._rm_file_on_del = False
        return state

    @ExperimentalAPI
    @override(LocalStorage)
    def set_state(self, state: Dict[str, Any]) -> None:
        # Clean up existing storage
        self._samples.close()
        if self._buffer_file != state["_buffer_file"] and self._rm_file_on_del:
            os.remove(self._buffer_file)
        # Restore given storage
        self._buffer_file = state["_buffer_file"]
        self._rm_file_on_del = state["_rm_file_on_del"]
        self._samples = shelve.open(
            self._buffer_file[:-4], flag="w", protocol=state["_pkl_proto"]
        )
        super().set_state(state)

    @override(LocalStorage)
    def _get(self, i: int) -> SampleBatchType:
        return self._samples[str(i)]

    @override(LocalStorage)
    def _set(self, idx: int, item: SampleBatchType) -> None:
        self._warn_replay_capacity(item, self.capacity / item.count)
        self._samples[str(idx)] = item
        self._samples.sync()

    @override(LocalStorage)
    def _del(self, i: int) -> SampleBatchType:
        # Do not delete item since this leads to continuously
        # increasing file size
        # https://github.com/python/cpython/blob/4153f2cbcb41a1a9057bfba28d5f65d48ea39283/Lib/dbm/dumb.py#L11-L12
        drop_item = self._samples[str(i)]
        # del self._samples[str(i)]
        return drop_item

    def __del__(self) -> None:
        if self._samples is not None:
            self._samples.close()
        if self._rm_file_on_del and os.path.exists(self._buffer_file):
            os.remove(self._buffer_file)

    def _warn_replay_capacity(self, item: SampleBatchType, num_items: int) -> None:
        """Warn if the configured replay buffer capacity is too large."""
        if log_once("replay_capacity_disk"):
            item_size = item.size_bytes()
            shutil_du = shutil.disk_usage(os.path.dirname(self._buffer_file))
            free_gb = shutil_du.free / 1e9
            mem_size = num_items * item_size / 1e9
            remainder = mem_size - self.size_bytes / 1e9
            msg = (
                "Estimated disk usage for replay buffer is {} GB "
                "({} batches of size {}, {} bytes each), "
                "of which {} GB are pending for allocation. "
                "Available disk space is {} GB.".format(
                    mem_size, num_items, item.count, item_size, remainder, free_gb
                )
            )
            if remainder > free_gb:
                raise ValueError(msg)
            elif remainder > 0.2 * free_gb:
                logger.warning(msg)
            else:
                logger.info(msg)
