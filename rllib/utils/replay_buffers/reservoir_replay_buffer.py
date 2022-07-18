from typing import Any, Dict
import random
import math

from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer
from ray.rllib.utils.typing import SampleBatchType


# __sphinx_doc_reservoir_buffer__begin__
@ExperimentalAPI
class ReservoirReplayBuffer(ReplayBuffer):
    """This buffer implements reservoir sampling.

    The algorithm has been described by Jeffrey S. Vitter in "Random sampling
    with a reservoir".
    """

    def __init__(
        self,
        capacity_items=10000,
        capacity_ts: int = math.inf,
        capacity_bytes: int = math.inf,
        storage_unit: str = "timesteps",
        storage_location: str = "memory",
        **kwargs
    ):
        """Initializes a ReservoirBuffer instance.

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
        """
        ReplayBuffer.__init__(
            self,
            capacity_items=capacity_items,
            capacity_ts=capacity_ts,
            capacity_bytes=capacity_bytes,
            storage_unit=storage_unit,
            storage_location=storage_location,
        )
        self._num_add_calls = 0
        self._num_evicted = 0

    @ExperimentalAPI
    @override(ReplayBuffer)
    def _add_single_batch(self, item: SampleBatchType, **kwargs) -> None:
        """Add a SampleBatch of experiences to self._storage.

        An item consists of either one or more timesteps, a sequence or an
        episode. Differs from add() in that it does not consider the storage
        unit or type of batch and simply stores it.

        Args:
            item: The batch to be added.
            ``**kwargs``: Forward compatibility kwargs.
        """
        # Update add counts.
        self._num_add_calls += 1

        if self._storage.eviction_started:
            idx = random.randint(0, self._num_add_calls - 1)
            if idx < len(self._storage):
                self._num_evicted += 1
                self._storage[idx] = item
        else:
            ReplayBuffer._add_single_batch(self, item, **kwargs)

    @ExperimentalAPI
    @override(ReplayBuffer)
    def stats(self, debug: bool = False) -> dict:
        """Returns the stats of this buffer.

        Args:
            debug: If True, adds sample eviction statistics to the returned
                    stats dict.

        Returns:
            A dictionary of stats about this buffer.
        """
        data = {
            "num_evicted": self._num_evicted,
            "num_add_calls": self._num_add_calls,
        }
        parent = ReplayBuffer.stats(self, debug)
        parent.update(data)
        return parent

    @ExperimentalAPI
    @override(ReplayBuffer)
    def get_state(self) -> Dict[str, Any]:
        """Returns all local state.

        Returns:
            The serializable local state.
        """
        parent = ReplayBuffer.get_state(self)
        parent.update(self.stats())
        return parent

    @ExperimentalAPI
    @override(ReplayBuffer)
    def set_state(self, state: Dict[str, Any]) -> None:
        """Restores all local state to the provided `state`.

        Args:
            state: The new state to set this buffer. Can be
                    obtained by calling `self.get_state()`.
        """
        self._num_evicted = state["num_evicted"]
        self._num_add_calls = state["num_add_calls"]
        ReplayBuffer.set_state(self, state)


# __sphinx_doc_reservoir_buffer__end__
