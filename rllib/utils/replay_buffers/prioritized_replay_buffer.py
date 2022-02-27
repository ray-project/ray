import random
from typing import Any, Dict, List, Optional
import numpy as np

# Import ray before psutil will make sure we use psutil's bundled version
import ray  # noqa F401
import psutil  # noqa E402

from ray.rllib.execution.buffers.replay_buffer import warn_replay_capacity
from ray.rllib.execution.segment_tree import SumSegmentTree, MinSegmentTree
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override, ExperimentalAPI
from ray.rllib.utils.metrics.window_stat import WindowStat
from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer
from ray.rllib.utils.typing import SampleBatchType


@ExperimentalAPI
class PrioritizedReplayBuffer(ReplayBuffer):
    @ExperimentalAPI
    def __init__(
        self,
        capacity: int = 10000,
        storage_unit: str = "timesteps",
        alpha: float = 1.0,
    ):
        """Initializes a PrioritizedReplayBuffer instance.

        Args:
            capacity: Max number of timesteps to store in the FIFO
                buffer. After reaching this number, older samples will be
                dropped to make space for new ones.
            storage_unit: Either 'sequences' or 'timesteps'. Specifies how
                experiences are stored.
            alpha: How much prioritization is used
                (0.0=no prioritization, 1.0=full prioritization).
        """
        ReplayBuffer.__init__(self, capacity, storage_unit)

        assert alpha > 0
        self._alpha = alpha

        it_capacity = 1
        while it_capacity < self.capacity:
            it_capacity *= 2

        self._it_sum = SumSegmentTree(it_capacity)
        self._it_min = MinSegmentTree(it_capacity)
        self._max_priority = 1.0
        self._prio_change_stats = WindowStat("reprio", 1000)

    @ExperimentalAPI
    @override(ReplayBuffer)
    def add(self, batch: SampleBatchType, weight: float) -> None:
        """Add a batch of experiences.

        Args:
            batch: SampleBatch to add to this buffer's storage.
            weight: The weight of the added sample used in subsequent sampling
                steps.
        """
        idx = self._next_idx

        assert batch.count > 0, batch
        warn_replay_capacity(item=batch, num_items=self.capacity / batch.count)

        # Update our timesteps counts.
        self._num_timesteps_added += batch.count
        self._num_timesteps_added_wrap += batch.count

        if self._next_idx >= len(self._storage):
            self._storage.append(batch)
            self._est_size_bytes += batch.size_bytes()
        else:
            self._storage[self._next_idx] = batch

        # Wrap around storage as a circular buffer once we hit capacity.
        if self._num_timesteps_added_wrap >= self.capacity:
            self._eviction_started = True
            self._num_timesteps_added_wrap = 0
            self._next_idx = 0
        else:
            self._next_idx += 1

        # Eviction of older samples has already started (buffer is "full").
        if self._eviction_started:
            self._evicted_hit_stats.push(self._hit_count[self._next_idx])
            self._hit_count[self._next_idx] = 0

        if weight is None:
            weight = self._max_priority
        self._it_sum[idx] = weight ** self._alpha
        self._it_min[idx] = weight ** self._alpha

    def _sample_proportional(self, num_items: int) -> List[int]:
        res = []
        for _ in range(num_items):
            # TODO(szymon): should we ensure no repeats?
            mass = random.random() * self._it_sum.sum(0, len(self._storage))
            idx = self._it_sum.find_prefixsum_idx(mass)
            res.append(idx)
        return res

    @ExperimentalAPI
    @override(ReplayBuffer)
    def sample(self, num_items: int, beta: float) -> Optional[SampleBatchType]:
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
        # If we don't have any samples yet in this buffer, return None.
        if len(self) == 0:
            return None

        assert beta >= 0.0

        idxes = self._sample_proportional(num_items)

        weights = []
        batch_indexes = []
        p_min = self._it_min.min() / self._it_sum.sum()
        max_weight = (p_min * len(self)) ** (-beta)

        for idx in idxes:
            p_sample = self._it_sum[idx] / self._it_sum.sum()
            weight = (p_sample * len(self)) ** (-beta)
            count = self._storage[idx].count
            # If zero-padded, count will not be the actual batch size of the
            # data.
            if (
                isinstance(self._storage[idx], SampleBatch)
                and self._storage[idx].zero_padded
            ):
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

    @ExperimentalAPI
    def update_priorities(self, idxes: List[int], priorities: List[float]) -> None:
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
        assert isinstance(
            idxes, (list, np.ndarray)
        ), "ERROR: `idxes` is not a list or np.ndarray, but " "{}!".format(
            type(idxes).__name__
        )
        assert len(idxes) == len(priorities)
        for idx, priority in zip(idxes, priorities):
            assert priority > 0
            assert 0 <= idx < len(self._storage)
            delta = priority ** self._alpha - self._it_sum[idx]
            self._prio_change_stats.push(delta)
            self._it_sum[idx] = priority ** self._alpha
            self._it_min[idx] = priority ** self._alpha

            self._max_priority = max(self._max_priority, priority)

    @ExperimentalAPI
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

    @ExperimentalAPI
    @override(ReplayBuffer)
    def get_state(self) -> Dict[str, Any]:
        """Returns all local state.

        Returns:
            The serializable local state.
        """
        # Get parent state.
        state = super().get_state()
        # Add prio weights.
        state.update(
            {
                "sum_segment_tree": self._it_sum.get_state(),
                "min_segment_tree": self._it_min.get_state(),
                "max_priority": self._max_priority,
            }
        )
        return state

    @ExperimentalAPI
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
