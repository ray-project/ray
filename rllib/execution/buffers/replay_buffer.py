import logging
import numpy as np
import random
from typing import Any, Dict, List, Optional

# Import ray before psutil will make sure we use psutil's bundled version
import ray  # noqa F401
import psutil  # noqa E402

from ray.rllib.execution.segment_tree import SumSegmentTree, MinSegmentTree
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.util.debug import log_once
from ray.rllib.utils.deprecation import Deprecated, DEPRECATED_VALUE, \
    deprecation_warning
from ray.rllib.utils.metrics.window_stat import WindowStat
from ray.rllib.utils.typing import SampleBatchType

# Constant that represents all policies in lockstep replay mode.
_ALL_POLICIES = "__all__"

logger = logging.getLogger(__name__)


def warn_replay_capacity(*, item: SampleBatchType, num_items: int) -> None:
    """Warn if the configured replay buffer capacity is too large."""
    if log_once("replay_capacity"):
        item_size = item.size_bytes()
        psutil_mem = psutil.virtual_memory()
        total_gb = psutil_mem.total / 1e9
        mem_size = num_items * item_size / 1e9
        msg = ("Estimated max memory usage for replay buffer is {} GB "
               "({} batches of size {}, {} bytes each), "
               "available system memory is {} GB".format(
                   mem_size, num_items, item.count, item_size, total_gb))
        if mem_size > total_gb:
            raise ValueError(msg)
        elif mem_size > 0.2 * total_gb:
            logger.warning(msg)
        else:
            logger.info(msg)


@Deprecated(new="warn_replay_capacity", error=False)
def warn_replay_buffer_size(*, item: SampleBatchType, num_items: int) -> None:
    return warn_replay_capacity(item=item, num_items=num_items)


@DeveloperAPI
class ReplayBuffer:
    @DeveloperAPI
    def __init__(self,
                 capacity: int = 10000,
                 size: Optional[int] = DEPRECATED_VALUE):
        """Initializes a ReplayBuffer instance.

        Args:
            capacity: Max number of timesteps to store in the FIFO
                buffer. After reaching this number, older samples will be
                dropped to make space for new ones.
        """
        # Deprecated args.
        if size != DEPRECATED_VALUE:
            deprecation_warning(
                "ReplayBuffer(size)", "ReplayBuffer(capacity)", error=False)
            capacity = size

        # The actual storage (list of SampleBatches).
        self._storage = []

        self.capacity = capacity
        # The next index to override in the buffer.
        self._next_idx = 0
        self._hit_count = np.zeros(self.capacity)

        # Whether we have already hit our capacity (and have therefore
        # started to evict older samples).
        self._eviction_started = False

        self._num_timesteps_added = 0
        self._num_timesteps_added_wrap = 0
        self._num_timesteps_sampled = 0
        self._evicted_hit_stats = WindowStat("evicted_hit", 1000)
        self._est_size_bytes = 0

    def __len__(self) -> int:
        """Returns the number of items currently stored in this buffer."""
        return len(self._storage)

    @DeveloperAPI
    def add(self, item: SampleBatchType, weight: float) -> None:
        """Add a batch of experiences.

        Args:
            item: SampleBatch to add to this buffer's storage.
            weight: The weight of the added sample used in subsequent
                sampling steps. Only relevant if this ReplayBuffer is
                a PrioritizedReplayBuffer.
        """
        assert item.count > 0, item
        warn_replay_capacity(item=item, num_items=self.capacity / item.count)

        self._num_timesteps_added += item.count
        self._num_timesteps_added_wrap += item.count

        if self._next_idx >= len(self._storage):
            self._storage.append(item)
            self._est_size_bytes += item.size_bytes()
        else:
            self._storage[self._next_idx] = item

        # Wrap around storage as a circular buffer once we hit capacity.
        if self._num_timesteps_added_wrap >= self.capacity:
            self._eviction_started = True
            self._num_timesteps_added_wrap = 0
            self._next_idx = 0
        else:
            self._next_idx += 1

        if self._eviction_started:
            self._evicted_hit_stats.push(self._hit_count[self._next_idx])
            self._hit_count[self._next_idx] = 0

    def _encode_sample(self, idxes: List[int]) -> SampleBatchType:
        out = SampleBatch.concat_samples([self._storage[i] for i in idxes])
        out.decompress_if_needed()
        return out

    @DeveloperAPI
    def sample(self, num_items: int) -> SampleBatchType:
        """Sample a batch of experiences.

        Args:
            num_items: Number of items to sample from this buffer.

        Returns:
            Concatenated batch of items.
        """
        idxes = [
            random.randint(0,
                           len(self._storage) - 1) for _ in range(num_items)
        ]
        self._num_sampled += num_items
        return self._encode_sample(idxes)

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

    @DeveloperAPI
    def get_state(self) -> Dict[str, Any]:
        """Returns all local state.

        Returns:
            The serializable local state.
        """
        state = {"_storage": self._storage, "_next_idx": self._next_idx}
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
        self._storage = state["_storage"]
        self._next_idx = state["_next_idx"]
        # Stats and counts.
        self._num_timesteps_added = state["added_count"]
        self._num_timesteps_added_wrap = state["added_count_wrapped"]
        self._eviction_started = state["eviction_started"]
        self._num_timesteps_sampled = state["sampled_count"]
        self._est_size_bytes = state["est_size_bytes"]


@DeveloperAPI
class PrioritizedReplayBuffer(ReplayBuffer):
    @DeveloperAPI
    def __init__(self,
                 capacity: int = 10000,
                 alpha: float = 1.0,
                 size: Optional[int] = DEPRECATED_VALUE):
        """Initializes a PrioritizedReplayBuffer instance.

        Args:
            capacity: Max number of timesteps to store in the FIFO
                buffer. After reaching this number, older samples will be
                dropped to make space for new ones.
            alpha: How much prioritization is used
                (0.0=no prioritization, 1.0=full prioritization).
        """
        super(PrioritizedReplayBuffer, self).__init__(capacity, size)
        assert alpha > 0
        self._alpha = alpha

        it_capacity = 1
        while it_capacity < self.capacity:
            it_capacity *= 2

        self._it_sum = SumSegmentTree(it_capacity)
        self._it_min = MinSegmentTree(it_capacity)
        self._max_priority = 1.0
        self._prio_change_stats = WindowStat("reprio", 1000)

    @DeveloperAPI
    @override(ReplayBuffer)
    def add(self, item: SampleBatchType, weight: float) -> None:
        """Add a batch of experiences.

        Args:
            item: SampleBatch to add to this buffer's storage.
            weight: The weight of the added sample used in subsequent sampling
                steps.
        """
        idx = self._next_idx
        super(PrioritizedReplayBuffer, self).add(item, weight)
        if weight is None:
            weight = self._max_priority
        self._it_sum[idx] = weight**self._alpha
        self._it_min[idx] = weight**self._alpha

    def _sample_proportional(self, num_items: int) -> List[int]:
        res = []
        for _ in range(num_items):
            # TODO(szymon): should we ensure no repeats?
            mass = random.random() * self._it_sum.sum(0, len(self._storage))
            idx = self._it_sum.find_prefixsum_idx(mass)
            res.append(idx)
        return res

    @DeveloperAPI
    @override(ReplayBuffer)
    def sample(self, num_items: int, beta: float) -> SampleBatchType:
        """Sample `num_items` items from this buffer, including prio. weights.

        If less than `num_items` records are in this buffer, some samples in
        the results may be repeated to fulfil the batch size (`num_items`)
        request.

        Args:
            num_items: Number of items to sample from this buffer.
            beta: To what degree to use importance weights
                (0 - no corrections, 1 - full correction).

        Returns:
            Concatenated batch of items including "weights" and
            "batch_indexes" fields denoting IS of each sampled
            transition and original idxes in buffer of sampled experiences.
        """
        assert beta >= 0.0

        idxes = self._sample_proportional(num_items)

        weights = []
        batch_indexes = []
        p_min = self._it_min.min() / self._it_sum.sum()
        max_weight = (p_min * len(self))**(-beta)

        for idx in idxes:
            p_sample = self._it_sum[idx] / self._it_sum.sum()
            weight = (p_sample * len(self))**(-beta)
            count = self._storage[idx].count
            # If zero-padded, count will not be the actual batch size of the
            # data.
            if isinstance(self._storage[idx], SampleBatch) and \
                    self._storage[idx].zero_padded:
                actual_size = self._storage[idx].max_seq_len
            else:
                actual_size = count
            weights.extend([weight / max_weight] * actual_size)
            batch_indexes.extend([idx] * actual_size)
            self._num_timesteps_sampled += count
        batch = self._encode_sample(idxes)

        # Note: prioritization is not supported in lockstep replay mode.
        if isinstance(batch, SampleBatch):
            batch["weights"] = np.array(weights)
            batch["batch_indexes"] = np.array(batch_indexes)

        return batch

    @DeveloperAPI
    def update_priorities(self, idxes: List[int],
                          priorities: List[float]) -> None:
        """Update priorities of sampled transitions.

        Sets priority of transition at index idxes[i] in buffer
        to priorities[i].

        Args:
            idxes: List of indices of sampled transitions
            priorities: List of updated priorities corresponding to
                transitions at the sampled idxes denoted by
                variable `idxes`.
        """
        # Making sure we don't pass in e.g. a torch tensor.
        assert isinstance(idxes, (list, np.ndarray)), \
            "ERROR: `idxes` is not a list or np.ndarray, but " \
            "{}!".format(type(idxes).__name__)
        assert len(idxes) == len(priorities)
        for idx, priority in zip(idxes, priorities):
            assert priority > 0
            assert 0 <= idx < len(self._storage)
            delta = priority**self._alpha - self._it_sum[idx]
            self._prio_change_stats.push(delta)
            self._it_sum[idx] = priority**self._alpha
            self._it_min[idx] = priority**self._alpha

            self._max_priority = max(self._max_priority, priority)

    @DeveloperAPI
    @override(ReplayBuffer)
    def stats(self, debug: bool = False) -> Dict:
        """Returns the stats of this buffer.

        Args:
            debug: If true, adds sample eviction statistics to the
                returned stats dict.

        Returns:
            A dictionary of stats about this buffer.
        """
        parent = ReplayBuffer.stats(self, debug)
        if debug:
            parent.update(self._prio_change_stats.stats())
        return parent

    @DeveloperAPI
    @override(ReplayBuffer)
    def get_state(self) -> Dict[str, Any]:
        """Returns all local state.

        Returns:
            The serializable local state.
        """
        # Get parent state.
        state = super().get_state()
        # Add prio weights.
        state.update({
            "sum_segment_tree": self._it_sum.get_state(),
            "min_segment_tree": self._it_min.get_state(),
            "max_priority": self._max_priority,
        })
        return state

    @DeveloperAPI
    @override(ReplayBuffer)
    def set_state(self, state: Dict[str, Any]) -> None:
        """Restores all local state to the provided `state`.

        Args:
            state: The new state to set this buffer. Can be obtained by
                calling `self.get_state()`.
        """
        super().set_state(state)
        self._it_sum.set_state(state["sum_segment_tree"])
        self._it_min.set_state(state["min_segment_tree"])
        self._max_priority = state["max_priority"]


# Visible for testing.
_local_replay_buffer = None
