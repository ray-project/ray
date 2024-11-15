import abc
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


class BundleQueue(abc.ABC):
    @abc.abstractmethod
    def __len__(self) -> int:
        """Return the number of bundles in the queue."""
        ...

    @abc.abstractmethod
    def __contains__(self, bundle: "RefBundle") -> bool:
        """Return whether the bundle is in the queue."""
        ...

    @abc.abstractmethod
    def add(self, bundle: "RefBundle") -> None:
        """Add a bundle to the queue."""
        ...

    @abc.abstractmethod
    def pop(self) -> "RefBundle":
        """Remove and return the head of the queue.

        Raises:
            IndexError: If the queue is empty.
        """
        ...

    @abc.abstractmethod
    def peek(self) -> Optional["RefBundle"]:
        """Return the head of the queue without removing it.

        If the queue is empty, return `None`.
        """
        ...

    @abc.abstractmethod
    def remove(self, bundle: "RefBundle"):
        """Remove a bundle from the queue."""
        ...

    @abc.abstractmethod
    def clear(self):
        """Remove all bundles from the queue."""
        ...

    @abc.abstractmethod
    def estimate_size_bytes(self) -> int:
        """Return an estimate of the total size of objects in the queue."""
        ...

    @abc.abstractmethod
    def is_empty(self):
        """Return whether this queue and all of its internal data structures are empty.

        This method is used for testing.
        """
        ...
