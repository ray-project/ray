from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Deque, Dict, Iterator, Optional

from typing_extensions import override

from .base import QueueWithRemoval

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


@dataclass
class _Node:
    value: RefBundle
    next: Optional[_Node] = None
    prev: Optional[_Node] = None


class HashLinkedQueue(QueueWithRemoval):
    """A bundle queue that supports these operations quickly:
    - contains(bundle)
    - remove(bundle)
    """

    def __init__(self):
        super().__init__()
        # We manually implement a linked list because we need to remove elements
        # efficiently, and Python's built-in data structures have O(n) removal time.
        self._head: Optional[_Node] = None
        self._tail: Optional[_Node] = None
        # We use a dictionary to keep track of the nodes corresponding to each bundle.
        # This allows us to remove a bundle from the queue in O(1) time. We need a list
        # because a bundle can be added to the queue multiple times. Nodes in each list
        # are insertion-ordered.
        self._bundle_to_nodes: Dict[RefBundle, Deque[_Node]] = defaultdict(deque)

    @override
    def __contains__(self, bundle: RefBundle) -> bool:
        return bundle in self._bundle_to_nodes

    @override
    def _add_inner(self, bundle: RefBundle, **kwargs: Any):
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

    @override
    def _get_next_inner(self) -> RefBundle:
        # Case 1: The queue is empty.
        if not self._head:
            raise IndexError("You can't pop from an empty queue")

        bundle = self._head.value
        self._remove_inner(bundle)

        return bundle

    @override
    def has_next(self) -> bool:
        return self._num_bundles > 0

    @override
    def peek_next(self) -> Optional[RefBundle]:
        if self._head is None:
            return None

        return self._head.value

    @override
    def _remove_inner(self, bundle: RefBundle) -> RefBundle:
        # Case 1: The queue is empty.
        if bundle not in self._bundle_to_nodes:
            raise ValueError(f"The bundle {bundle} is not in the queue.")

        node = self._bundle_to_nodes[bundle].popleft()
        if not self._bundle_to_nodes[bundle]:
            del self._bundle_to_nodes[bundle]

        node = self._remove_node(node)
        return node.value

    def _remove_node(self, node: _Node) -> _Node:
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

        return node

    def __iter__(self) -> Iterator[RefBundle]:
        curr = self._head
        while curr:
            yield curr.value
            curr = curr.next

    def clear(self):
        self._reset_metrics()
        self._bundle_to_nodes.clear()
        self._head = None
        self._tail = None
