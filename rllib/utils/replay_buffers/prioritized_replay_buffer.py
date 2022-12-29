import random
from typing import Any, Dict, List, Optional
import numpy as np
import math

from ray.rllib.execution.segment_tree import SumSegmentTree, MinSegmentTree
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics.window_stat import WindowStat
from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer
from ray.rllib.utils.typing import SampleBatchType
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class PrioritizedReplayBuffer(ReplayBuffer):
    """This buffer implements Prioritized Experience Replay

    The algorithm has been described by Tom Schaul et. al. in "Prioritized
    Experience Replay". See https://arxiv.org/pdf/1511.05952.pdf for
    the full paper.
    """

    def __init__(
        self,
        capacity_items=10000,
        capacity_ts: int = math.inf,
        capacity_bytes: int = math.inf,
        storage_unit: str = "timesteps",
        storage_location: str = "memory",
        alpha: float = 1.0,
        **kwargs
    ):
        """Initializes a PrioritizedReplayBuffer instance.

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
                'episodes'. Specifies how experiences are stored.
            storage_location: Either 'memory' or 'disk'.
                Specifies where experiences are stored.
            alpha: How much prioritization is used
                (0.0=no prioritization, 1.0=full prioritization).
            ``**kwargs``: Forward compatibility kwargs.
        """
        ReplayBuffer.__init__(
            self,
            capacity_items=capacity_items,
            capacity_ts=capacity_ts,
            capacity_bytes=capacity_bytes,
            storage_unit=storage_unit,
            storage_location=storage_location,
            **kwargs
        )

        assert alpha > 0
        self._alpha = alpha

        # Segment tree must have capacity that is a power of 2
        it_capacity = 1
        while it_capacity < self.capacity_items:
            it_capacity *= 2

        self._it_sum = SumSegmentTree(it_capacity)
        self._it_min = MinSegmentTree(it_capacity)
        self._max_priority = 1.0
        self._prio_change_stats = WindowStat("reprio", 1000)

    @DeveloperAPI
    @override(ReplayBuffer)
    def _add_single_batch(self, item: SampleBatchType, **kwargs) -> None:
        """Add a batch of experiences to self._storage with weight.

        An item consists of either one or more timesteps, a sequence or an
        episode. Differs from add() in that it does not consider the storage
        unit or type of batch and simply stores it.

        Args:
            item: The item to be added.
            ``**kwargs``: Forward compatibility kwargs.
        """
        weight = kwargs.get("weight", None)

        if weight is None:
            weight = self._max_priority

        len_before_add = len(self._storage)
        add_idx = self._storage._get_internal_index(len_before_add)

        ReplayBuffer._add_single_batch(self, item)

        if len(self._storage) <= len_before_add:
            num_del = len_before_add - len(self._storage) + 1
            for i in range(1, num_del + 1):
                del_idx = (
                    self._storage._get_internal_index(0) - i
                ) % self.capacity_items
                if del_idx != add_idx:
                    # Set deleted item's priorities to neutral element
                    self._it_sum[del_idx] = self._it_sum.neutral_element
                    self._it_min[del_idx] = self._it_min.neutral_element

        self._it_sum[add_idx] = weight ** self._alpha
        self._it_min[add_idx] = weight ** self._alpha

    def _sample_proportional(self, num_items: int) -> List[int]:
        start_idx = self._storage._get_internal_index(0)
        end_idx = self._storage._get_internal_index(len(self._storage))
        if end_idx > start_idx:
            mass = self._it_sum.sum(start_idx, end_idx)
        else:
            mass = self._it_sum.sum(0, end_idx)
            mass += self._it_sum.sum(start_idx, len(self._storage))
        res = []
        for _ in range(num_items):
            # TODO(szymon): should we ensure no repeats?
            idx = self._it_sum.find_prefixsum_idx(mass * random.random())
            res.append(idx)
        return res

    @DeveloperAPI
    @override(ReplayBuffer)
    def sample(
        self, num_items: int, beta: float, **kwargs
    ) -> Optional[SampleBatchType]:
        """Sample `num_items` items from this buffer, including prio. weights.

        Samples in the results may be repeated.

        Examples for storage of SamplesBatches:
        - If storage unit `timesteps` has been chosen and batches of
        size 5 have been added, sample(5) will yield a concatenated batch of
        25 timesteps.
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
            beta: To what degree to use importance weights (0 - no corrections,
            1 - full correction).
            ``**kwargs``: Forward compatibility kwargs.

        Returns:
            Concatenated SampleBatch of items including "weights" and
            "batch_indexes" fields denoting IS of each sampled
            transition and original idxes in buffer of sampled experiences.
        """
        assert beta >= 0.0

        if len(self) == 0:
            raise ValueError("Trying to sample from an empty buffer.")

        internal_idxes = self._sample_proportional(num_items)

        ext_idxes = [0] * len(internal_idxes)
        weights = []
        batch_indexes = []
        p_min = self._it_min.min() / self._it_sum.sum()
        max_weight = (p_min * len(self)) ** (-beta)

        for i, idx in enumerate(internal_idxes):
            p_sample = self._it_sum[idx] / self._it_sum.sum()
            weight = (p_sample * len(self)) ** (-beta)
            ext_idx = self._storage._get_external_index(idx)
            sample = self._storage[ext_idx]
            # If zero-padded, count will not be the actual batch size of the
            # data.
            if isinstance(sample, SampleBatch) and sample.zero_padded:
                actual_size = sample.max_seq_len
            else:
                actual_size = sample.count
            ext_idxes[i] = ext_idx
            weights.extend([weight / max_weight] * actual_size)
            batch_indexes.extend([ext_idx] * actual_size)
            self._num_timesteps_sampled += sample.count
        batch = self._encode_sample(ext_idxes)

        # Note: prioritization is not supported in multi agent lockstep
        if isinstance(batch, SampleBatch):
            batch["weights"] = np.array(weights)
            batch["batch_indexes"] = np.array(batch_indexes)

        return batch

    @DeveloperAPI
    def update_priorities(self, idxes: List[int], priorities: List[float]) -> None:
        """Update priorities of items at given indices.

        Sets priority of item at index idxes[i] in buffer
        to priorities[i].

        Args:
            idxes: List of indices of items
            priorities: List of updated priorities corresponding to items at the
            idxes denoted by variable `idxes`.
        """
        # Making sure we don't pass in e.g. a torch tensor.
        assert isinstance(
            idxes, (list, np.ndarray)
        ), "ERROR: `idxes` is not a list or np.ndarray, but {}!".format(
            type(idxes).__name__
        )
        assert len(idxes) == len(priorities)
        for idx, priority in zip(idxes, priorities):
            assert priority > 0
            assert 0 <= idx < len(self._storage)
            int_idx = self._storage._get_internal_index(idx)
            delta = priority**self._alpha - self._it_sum[int_idx]
            self._prio_change_stats.push(delta)
            assert self._it_sum[int_idx] != self._it_sum.neutral_element
            assert self._it_min[int_idx] != self._it_min.neutral_element
            self._it_sum[int_idx] = priority**self._alpha
            self._it_min[int_idx] = priority**self._alpha

            self._max_priority = max(self._max_priority, priority)

    @DeveloperAPI
    @override(ReplayBuffer)
    def stats(self, debug: bool = False) -> Dict:
        """Returns the stats of this buffer.

        Args:
            debug: If true, adds sample eviction statistics to the returned stats dict.

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
        state.update(
            {
                "sum_segment_tree": self._it_sum.get_state(),
                "min_segment_tree": self._it_min.get_state(),
                "max_priority": self._max_priority,
            }
        )
        return state

    @DeveloperAPI
    @override(ReplayBuffer)
    def set_state(self, state: Dict[str, Any]) -> None:
        """Restores all local state to the provided `state`.

        Args:
            state: The new state to set this buffer. Can be obtained by calling
            `self.get_state()`.
        """
        super().set_state(state)
        self._it_sum.set_state(state["sum_segment_tree"])
        self._it_min.set_state(state["min_segment_tree"])
        self._max_priority = state["max_priority"]
