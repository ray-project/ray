from typing import Any, Dict
import random

# Import ray before psutil will make sure we use psutil's bundled version
import ray  # noqa F401
import psutil  # noqa E402

from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.replay_buffers.replay_buffer import (
    ReplayBuffer,
    warn_replay_capacity,
)
from ray.rllib.utils.typing import SampleBatchType


# __sphinx_doc_reservoir_buffer__begin__
@ExperimentalAPI
class ReservoirReplayBuffer(ReplayBuffer):
    """This buffer implements reservoir sampling.

    The algorithm has been described by Jeffrey S. Vitter in "Random sampling
    with a reservoir". See https://www.cs.umd.edu/~samir/498/vitter.pdf for
    the full paper.
    """

    def __init__(
        self, capacity: int = 10000, storage_unit: str = "timesteps", **kwargs
    ):
        """Initializes a ReservoirBuffer instance.

        Args:
            capacity: Max number of timesteps to store in the FIFO
                    buffer. After reaching this number, older samples will be
                    dropped to make space for new ones.
            storage_unit: Either 'timesteps', 'sequences' or
                    'episodes'. Specifies how experiences are stored.
        """
        ReplayBuffer.__init__(self, capacity, storage_unit)
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
        self._num_timesteps_added += item.count
        self._num_timesteps_added_wrap += item.count

        # Update add counts.
        self._num_add_calls += 1
        # Update our timesteps counts.

        if self._num_timesteps_added < self.capacity:
            self._storage.append(item)
            self._est_size_bytes += item.size_bytes()
        else:
            # Eviction of older samples has already started (buffer is "full")
            self._eviction_started = True
            idx = random.randint(0, self._num_add_calls - 1)
            if idx < len(self._storage):
                self._num_evicted += 1
                self._evicted_hit_stats.push(self._hit_count[idx])
                self._hit_count[idx] = 0
                # This is a bit of a hack: ReplayBuffer always inserts at
                # self._next_idx
                self._next_idx = idx
                self._evicted_hit_stats.push(self._hit_count[idx])
                self._hit_count[idx] = 0

                item_to_be_removed = self._storage[idx]
                self._est_size_bytes -= item_to_be_removed.size_bytes()
                self._storage[idx] = item
                self._est_size_bytes += item.size_bytes()

                assert item.count > 0, item
                warn_replay_capacity(item=item, num_items=self.capacity / item.count)

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
