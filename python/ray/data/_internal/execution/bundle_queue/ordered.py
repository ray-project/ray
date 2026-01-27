from __future__ import annotations

from collections import defaultdict, deque
from typing import TYPE_CHECKING, DefaultDict, Deque, Optional, Set

from typing_extensions import override

from ray.data._internal.execution.bundle_queue import BaseBundleQueue

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


class ReorderingBundleQueue(BaseBundleQueue):
    """A queue that supports adding in index order."""

    def __init__(self):
        super().__init__()

        self._inner: DefaultDict[int, Deque[RefBundle]] = defaultdict(lambda: deque())
        self._current_index: int = 0
        self._completed_keys: Set[int] = set()

    def _move_to_next_index(self):
        """Move the output index to the next task.

        This method should only be called when the current task is complete and all
        outputs have been taken.
        """
        assert len(self._inner[self._current_index]) == 0
        assert self._current_index in self._completed_keys
        del self._inner[self._current_index]
        self._completed_keys.remove(self._current_index)
        self._current_index += 1

    @override
    def _add_inner(self, bundle: RefBundle, key: int) -> None:
        assert key is not None
        self._inner[key].append(bundle)

    @override
    def has_next(self) -> bool:
        return len(self._inner[self._current_index]) > 0

    @override
    def _get_next_inner(self) -> RefBundle:
        if not self._inner[self._current_index]:
            raise ValueError("Cannot pop from empty queue.")
        next_bundle = self._inner[self._current_index].popleft()
        if len(self._inner[self._current_index]) == 0:
            if self._current_index in self._completed_keys:
                self._move_to_next_index()
        return next_bundle

    @override
    def peek_next(self) -> Optional[RefBundle]:
        if not self._inner[self._current_index]:
            return None
        return self._inner[self._current_index][0]

    @override
    def finalize(self, key: int):
        assert key is not None and key >= self._current_index
        self._completed_keys.add(key)
        if key == self._current_index:
            if len(self._inner[key]) == 0:
                self._move_to_next_index()

    @override
    def clear(self):
        self._reset_metrics()
        self._inner.clear()
        self._completed_keys.clear()
        self._current_index = 0
