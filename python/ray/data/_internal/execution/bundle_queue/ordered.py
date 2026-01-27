from __future__ import annotations

from collections import defaultdict, deque
from typing import TYPE_CHECKING, DefaultDict, Deque, Optional, Set

from typing_extensions import override

from ray.data._internal.execution.bundle_queue import BaseBundleQueue

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


class ReorderingBundleQueue(BaseBundleQueue):
    """A queue that iterates over the bundles in the key order rather than
    insertion order.

    For bundles inserted with the same key, insertion order is used.
    """

    def __init__(self):
        super().__init__()

        self._inner: DefaultDict[int, Deque[RefBundle]] = defaultdict(lambda: deque())
        self._current_index: int = 0
        self._finalized_keys: Set[int] = set()
        self._finished: bool = False

    def _move_to_next_key(self):
        """Move the output index to the next task.

        This method should only be called when the current task is complete and all
        outputs have been taken.
        """
        assert len(self._inner[self._current_index]) == 0
        assert self._current_index in self._finalized_keys

        if self._current_index < len(self._inner) - 1:
            self._current_index += 1
        else:
            self._current_index = -1
            self._finished = True

    @override
    def _add_inner(self, bundle: RefBundle, key: int) -> None:
        assert key is not None
        self._inner[key].append(bundle)

    @override
    def has_next(self) -> bool:
        if (
            self._current_index in self._finalized_keys and
            len(self._inner[self._current_index]) == 0
        ):
            self._move_to_next_key()

        return len(self._inner[self._current_index]) > 0

    @override
    def _get_next_inner(self) -> RefBundle:
        if not self._inner[self._current_index]:
            raise ValueError("Cannot pop from empty queue.")

        return self._inner[self._current_index].popleft()

    @override
    def peek_next(self) -> Optional[RefBundle]:
        return (
            self._inner[self._current_index][0]
            if self._inner[self._current_index]
            else None
        )

    @override
    def finalize(self, key: int):
        assert key is not None and key >= self._current_index
        self._finalized_keys.add(key)

    @override
    def clear(self):
        self._reset_metrics()
        self._inner.clear()
        self._finalized_keys.clear()
        self._current_index = 0
