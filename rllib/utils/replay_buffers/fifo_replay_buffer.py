import numpy as np
from typing import Any, Dict, Optional

from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer, StorageUnit
from ray.rllib.utils.typing import SampleBatchType
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class FifoReplayBuffer(ReplayBuffer):
    """This replay buffer implements a FIFO queue.

    Sometimes, e.g. for offline use cases, it may be desirable to use
    off-policy algorithms without a Replay Buffer.
    This FifoReplayBuffer can be used in-place to achieve the same effect
    without having to introduce separate algorithm execution branches.

    For simplicity and efficiency reasons, this replay buffer stores incoming
    sample batches as-is, and returns them one at time.
    This is to avoid any additional load when this replay buffer is used.
    """

    def __init__(self, *args, **kwargs):
        """Initializes a FifoReplayBuffer.

        Args:
            ``*args``   : Forward compatibility args.
            ``**kwargs``: Forward compatibility kwargs.
        """
        # Completely by-passing underlying ReplayBuffer by setting its
        # capacity to 1 (lowest allowed capacity).
        ReplayBuffer.__init__(self, 1, StorageUnit.FRAGMENTS, **kwargs)

        self._queue = []

    @DeveloperAPI
    @override(ReplayBuffer)
    def add(self, batch: SampleBatchType, **kwargs) -> None:
        return self._queue.append(batch)

    @DeveloperAPI
    @override(ReplayBuffer)
    def sample(self, *args, **kwargs) -> Optional[SampleBatchType]:
        """Sample a saved training batch from this buffer.

        Args:
            ``*args``   : Forward compatibility args.
            ``**kwargs``: Forward compatibility kwargs.

        Returns:
            A single training batch from the queue.
        """
        if len(self._queue) <= 0:
            # Return empty SampleBatch if queue is empty.
            return MultiAgentBatch({}, 0)
        batch = self._queue.pop(0)
        # Equal weights of 1.0.
        batch["weights"] = np.ones(len(batch))
        return batch

    @DeveloperAPI
    def update_priorities(self, *args, **kwargs) -> None:
        """Update priorities of items at given indices.

        No-op for this replay buffer.

        Args:
            ``*args``   : Forward compatibility args.
            ``**kwargs``: Forward compatibility kwargs.
        """
        pass

    @DeveloperAPI
    @override(ReplayBuffer)
    def stats(self, debug: bool = False) -> Dict:
        """Returns the stats of this buffer.

        Args:
            debug: If true, adds sample eviction statistics to the returned stats dict.

        Returns:
            A dictionary of stats about this buffer.
        """
        # As if this replay buffer has never existed.
        return {}

    @DeveloperAPI
    @override(ReplayBuffer)
    def get_state(self) -> Dict[str, Any]:
        """Returns all local state.

        Returns:
            The serializable local state.
        """
        # Pass through replay buffer does not save states.
        return {}

    @DeveloperAPI
    @override(ReplayBuffer)
    def set_state(self, state: Dict[str, Any]) -> None:
        """Restores all local state to the provided `state`.

        Args:
            state: The new state to set this buffer. Can be obtained by calling
            `self.get_state()`.
        """
        pass
