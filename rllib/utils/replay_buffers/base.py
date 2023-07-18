from abc import ABCMeta, abstractmethod
import platform
from typing import Any, Dict, Optional

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class ReplayBufferInterface(metaclass=ABCMeta):
    """Abstract base class for all of RLlib's replay buffers.

    Mainly defines the `add()` and `sample()` methods that every buffer class
    must implement to be usable by an Algorithm.
    Buffers may determine on all the implementation details themselves, e.g.
    whether to store single timesteps, episodes, or episode fragments or whether
    to return fixed batch sizes or per-call defined ones.
    """

    @abstractmethod
    @DeveloperAPI
    def __len__(self) -> int:
        """Returns the number of items currently stored in this buffer."""

    @abstractmethod
    @DeveloperAPI
    def add(self, batch: Any, **kwargs) -> None:
        """Adds a batch of experiences or other data to this buffer.

        Args:
            batch: Batch or data to add.
            ``**kwargs``: Forward compatibility kwargs.
        """

    @abstractmethod
    @DeveloperAPI
    def sample(self, num_items: Optional[int] = None, **kwargs) -> Any:
        """Samples `num_items` items from this buffer.

        The exact shape of the returned data depends on the buffer's implementation.

        Args:
            num_items: Number of items to sample from this buffer.
            ``**kwargs``: Forward compatibility kwargs.

        Returns:
            A batch of items.
        """

    @abstractmethod
    @DeveloperAPI
    def get_state(self) -> Dict[str, Any]:
        """Returns all local state in a dict.

        Returns:
            The serializable local state.
        """

    @abstractmethod
    @DeveloperAPI
    def set_state(self, state: Dict[str, Any]) -> None:
        """Restores all local state to the provided `state`.

        Args:
            state: The new state to set this buffer. Can be obtained by calling
                `self.get_state()`.
        """

    @DeveloperAPI
    def get_host(self) -> str:
        """Returns the computer's network name.

        Returns:
            The computer's networks name or an empty string, if the network
            name could not be determined.
        """
        return platform.node()
