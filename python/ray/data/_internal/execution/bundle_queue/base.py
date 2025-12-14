from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Protocol,
    Tuple,
    runtime_checkable,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


class _QueueMetricRecorderMixin:
    """Mixin for recording stats about a queue. Subclasses
    may choose to use the _on_dequeue and _on_enqueue methods to
    track num_blocks, nbytes, etc... If not, they should override
    those methods."""

    def __init__(self):
        self._nbytes: int = 0
        self._num_blocks: int = 0
        self._num_bundles: int = 0
        self._num_rows: int = 0

    def _on_enqueue(self, bundle: RefBundle):
        self._nbytes += bundle.size_bytes()
        self._num_blocks += len(bundle.block_refs)
        self._num_bundles += 1
        self._num_rows += bundle.num_rows() or 0

    def _on_dequeue(self, bundle: RefBundle):
        self._nbytes -= bundle.size_bytes()
        self._num_blocks -= len(bundle.block_refs)
        self._num_bundles -= 1
        # Fix this for None count
        self._num_rows -= bundle.num_rows() or 0

    def estimate_size_bytes(self) -> int:
        """Return the estimated size in bytes of all bundles."""
        return self._nbytes

    def num_blocks(self) -> int:
        """Return the total # of blocks across all bundles."""
        return self._num_blocks

    def num_rows(self) -> int:
        """Return the total # of rows across all bundles."""
        return self._num_rows

    def _reset_metrics(self):
        self._num_rows = 0
        self._num_blocks = 0
        self._num_bundles = 0
        self._nbytes = 0

    def __len__(self) -> int:
        """Return the total # bundles."""
        return self._num_bundles


class BaseBundleQueue(_QueueMetricRecorderMixin):
    """Base class for storing bundles. Here and subclasses should adhere to the mental
    model that "first", "front", or "head" is the next bundle to be dequeued. Consequently,
    "last", "back", or "tail" is the last bundle to be dequeued.
    """

    @abc.abstractmethod
    def add(self, bundle: RefBundle, **kwargs: Any) -> None:
        """Add a bundle to the tail(end) of the queue.

        Args:
            bundle: The bundle to add.
            **kwargs: Additional queue-specific parameters (e.g., `key` for ordered queues).
                This is used for `finalize`.
        """
        ...

    @abc.abstractmethod
    def get_next(self) -> RefBundle:
        """Remove and return the head of the queue.

        Raises:
            ValueError: If the queue is empty.

        Returns:
            A Refbundle if has_next() is True
        """
        ...

    @abc.abstractmethod
    def peek_next(self) -> Optional[RefBundle]:
        """Return the head of the queue. The only invariant is
        that the # of blocks, rows, and bytes must be in remain unchanged
        before and after this method call.

        If queue.has_next() == False, return `None`.
        """
        ...

    @abc.abstractmethod
    def has_next(self) -> bool:
        """Check if the queue has a valid bundle."""
        ...

    @abc.abstractmethod
    def clear(self):
        """Remove all bundles from the queue."""
        ...

    @abc.abstractmethod
    def finalize(self, **kwargs: Any):
        """Signal that no additional bundles will be added to the bundler so
        the bundler can be finalized. The keys of kwargs provided should be the same
        as the ones passed into the `add()` method. This is important for ordered
        queues."""
        ...


@runtime_checkable
class SupportsDeque(Protocol):
    """Protocol for queues that support deque operations (add to front, get from back)."""

    def add_to_front(self, bundle: RefBundle):
        ...

    def get_last(self) -> RefBundle:
        ...

    def peek_last(self) -> Optional[RefBundle]:
        ...


@runtime_checkable
class SupportsRemoval(Protocol):
    """Protocol for storing bundles AND supporting remove(bundle)
    and contains(bundle) operations."""

    def __contains__(self, bundle: RefBundle) -> bool:
        """Return whether the key is in the queue."""
        ...

    def remove(self, bundle: RefBundle) -> RefBundle:
        """Remove the specified bundle from the queue. If multiple instances exist, remove the first one."""
        ...

    def remove_last(self, bundle: RefBundle) -> RefBundle:
        """Remove the specified bundle from the queue. If multiple instances exist, remove the last one."""
        ...


# TODO(Justin): What I wrote below is not ideal, and will be removed
# once we are able to track metrics in the queues themselves (as opposed
# to what we currently do -- track metrics in the operators). We need this method
# to surface the original bundles to the operators so they can track the bundles
# correctly.
@runtime_checkable
class SupportsRebundling(Protocol):
    """Protocol for queues that rebundle their input"""

    def get_next_with_original(self) -> Tuple[List[RefBundle], RefBundle]:
        """Gets the next bundle.

        Returns:
            A two-tuple. The first element is a list of bundles that were combined into
            the output bundle. The second element is the output bundle.
        """
        ...
