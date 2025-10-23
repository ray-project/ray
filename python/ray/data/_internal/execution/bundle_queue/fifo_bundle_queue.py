from collections import defaultdict, deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional

from .bundle_queue import BundleQueue

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


@dataclass
class _Node:
    value: "RefBundle"
    next: Optional["_Node"] = None
    prev: Optional["_Node"] = None


class FIFOBundleQueue(BundleQueue):
    """A bundle queue that follows a first-in-first-out policy."""

    def __init__(self):
        # We manually implement a linked list because we need to remove elements
        # efficiently, and Python's built-in data structures have O(n) removal time.
        self._head: Optional[_Node] = None
        self._tail: Optional[_Node] = None
        # We use a dictionary to keep track of the nodes corresponding to each bundle.
        # This allows us to remove a bundle from the queue in O(1) time. We need a list
        # because a bundle can be added to the queue multiple times. Nodes in each list
        # are insertion-ordered.
        self._bundle_to_nodes: Dict["RefBundle", List[_Node]] = defaultdict(deque)

        self._nbytes = 0
        self._num_blocks = 0
        self._num_bundles = 0

    def __len__(self) -> int:
        return self._num_bundles

    def __contains__(self, bundle: "RefBundle") -> bool:
        return bundle in self._bundle_to_nodes

    def add(self, bundle: "RefBundle") -> None:
        """Add a bundle to the end (right) of the queue."""
        new_node = _Node(value=bundle, next=None, prev=self._tail)
        # Case 1: The queue is empty.
        if self._head is None:
            assert self._tail is None
            self._head = new_node
            self._tail = new_node
        # Case 2: The queue has at least one element.
        else:
            self._tail.next = new_node
            self._tail = new_node

        self._bundle_to_nodes[bundle].append(new_node)

        self._nbytes += bundle.size_bytes()
        self._num_blocks += len(bundle.block_refs)
        self._num_bundles += 1

    def get_next(self) -> "RefBundle":
        """Return the first (left) bundle in the queue."""
        # Case 1: The queue is empty.
        if not self._head:
            raise IndexError("You can't pop from an empty queue")

        bundle = self._head.value
        self.remove(bundle)

        return bundle

    def has_next(self) -> bool:
        return self._num_bundles > 0

    def peek_next(self) -> Optional["RefBundle"]:
        """Return the first (left) bundle in the queue without removing it."""
        if self._head is None:
            return None

        return self._head.value

    def remove(self, bundle: "RefBundle"):
        """Remove a bundle from the queue.

        If there are multiple instances of the bundle in the queue, this method only
        removes the first one.
        """
        # Case 1: The queue is empty.
        if bundle not in self._bundle_to_nodes:
            raise ValueError(f"The bundle {bundle} is not in the queue.")

        node = self._bundle_to_nodes[bundle].popleft()
        if not self._bundle_to_nodes[bundle]:
            del self._bundle_to_nodes[bundle]

        # Case 2: The bundle is the only element in the queue.
        if self._head is self._tail:
            self._head = None
            self._tail = None
        # Case 3: The bundle is the first element in the queue.
        elif node is self._head:
            self._head = node.next
            self._head.prev = None
        # Case 4: The bundle is the last element in the queue.
        elif node is self._tail:
            self._tail = node.prev
            self._tail.next = None
        # Case 5: The bundle is in the middle of the queue.
        else:
            node.prev.next = node.next
            node.next.prev = node.prev

        self._num_bundles -= 1
        self._num_blocks -= len(bundle)
        self._nbytes -= bundle.size_bytes()

        assert self._nbytes >= 0, (
            "Expected the total size of objects in the queue to be non-negative, but "
            f"got {self._nbytes} bytes instead."
        )

        return node.value

    def clear(self):
        self._head = None
        self._tail = None
        self._bundle_to_nodes.clear()
        self._num_bundles = 0
        self._num_blocks = 0
        self._nbytes = 0

    def estimate_size_bytes(self) -> int:
        return self._nbytes

    def num_blocks(self) -> int:
        return self._num_blocks

    def is_empty(self):
        return not self._bundle_to_nodes and self._head is None and self._tail is None
