from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

from ray.data._internal.execution.util import memory_string

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


class BaseBundleQueue:
    """Base class for storing bundles. Here and subclasses should adhere to the mental
    model that "first", "front", or "head" is the next bundle to be dequeued. Consequently,
    "last", "back", or "tail" is the last bundle to be dequeued.

    Subclasses may choose to use the _on_dequeue_bundle and _on_enqueue_bundle methods to
    track num_blocks, nbytes, etc... If not, they should override those methods.
    """

    def __init__(self):
        self._nbytes: int = 0
        self._num_blocks: int = 0
        self._num_bundles: int = 0
        self._num_rows: int = 0

    def _on_enqueue_bundle(self, bundle: RefBundle):
        self._nbytes += bundle.size_bytes()
        self._num_blocks += len(bundle.block_refs)
        self._num_bundles += 1
        self._num_rows += bundle.num_rows() or 0

    def _on_dequeue_bundle(self, bundle: RefBundle):
        self._nbytes -= bundle.size_bytes()
        self._num_blocks -= len(bundle.block_refs)
        self._num_bundles -= 1
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

    def __repr__(self) -> str:
        """Return a string representation showing queue metrics."""
        nbytes = memory_string(self.estimate_size_bytes())
        return (
            f"{self.__class__.__name__}("
            f"num_bundles={len(self)}, "
            f"num_blocks={self.num_blocks()}, "
            f"num_rows={self.num_rows()}, "
            f"nbytes={nbytes})"
        )

    def add(self, bundle: RefBundle, **kwargs: Any):
        """Add a bundle to the tail(end) of the queue. Base classes should override
        the `_add_inner` method for simple use cases. For more complex metrics tracking,
        they can override this method.

        Args:
            bundle: The bundle to add.
            **kwargs: Additional queue-specific parameters (e.g., `key` for ordered queues).
                This is used for `finalize`.
        """
        self._on_enqueue_bundle(bundle)
        self._add_inner(bundle, **kwargs)

    def _add_inner(self, bundle: RefBundle, **kwargs: Any):
        raise NotImplementedError

    def get_next(self) -> RefBundle:
        """Remove and return the head of the queue. Base classes should override
        the `_get_next_inner` method for simple use cases. For more complex metrics tracking,
        they can override this method.

        Raises:
            IndexError: If the queue is empty.

        Returns:
            The `RefBundle` at the head of the queue.
        """
        bundle = self._get_next_inner()
        self._on_dequeue_bundle(bundle)
        return bundle

    def _get_next_inner(self) -> RefBundle:
        raise NotImplementedError

    @abc.abstractmethod
    def peek_next(self) -> Optional[RefBundle]:
        """Return the head of the queue. The only invariant is
        that the # of blocks, rows, and bytes must remain unchanged
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

    def finalize(self, **kwargs: Any):
        """Signal that no additional bundles will be added to the bundler so
        the bundler can be finalized. The keys of kwargs provided should be the same
        as the ones passed into the `add()` method. This is important for ordered
        queues."""
        return None


class QueueWithRemoval(BaseBundleQueue):
    """Base class for storing bundles AND supporting remove(bundle)
    and contains(bundle) operations."""

    def __contains__(self, bundle: RefBundle) -> bool:
        """Return whether the key is in the queue."""
        ...

    def remove(self, bundle: RefBundle) -> RefBundle:
        """Remove the specified bundle from the queue. If multiple instances exist, remove the first one."""
        bundle = self._remove_inner(bundle)
        self._on_dequeue_bundle(bundle)
        return bundle

    def _remove_inner(self, bundle: RefBundle) -> RefBundle:
        raise NotImplementedError
