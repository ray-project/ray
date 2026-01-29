from __future__ import annotations

from collections import defaultdict, deque
from typing import TYPE_CHECKING, DefaultDict, Deque, Optional, Set

from typing_extensions import override

from ray.data._internal.execution.bundle_queue import BaseBundleQueue

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


class ReorderingBundleQueue(BaseBundleQueue):
    """A queue that iterates over the bundles in the order of provided "keys" rather than
    insertion order (for bundles inserted with the same key, insertion order is used)

    User of this queue has to adhere to following invariants of this queue:

    1. (!) Used keys have to be a *contiguous* range of `[0, N]`

    Failure to follow this requirement might result in this queue getting
    irreversibly stuck.
    """

    def __init__(self):
        super().__init__()

        self._inner: DefaultDict[int, Deque[RefBundle]] = defaultdict(lambda: deque())
        self._current_key: int = 0
        self._finalized_keys: Set[int] = set()

    def _move_to_next_key(self):
        """Move the output index to the next task.

        This method should only be called when the current task is complete and all
        outputs have been taken.
        """
        assert len(self._inner[self._current_key]) == 0
        assert self._current_key in self._finalized_keys

        self._current_key += 1

    @override
    def _add_inner(self, bundle: RefBundle, key: int) -> None:
        assert key is not None
        self._inner[key].append(bundle)

    @override
    def has_next(self) -> bool:
        while (
            self._current_key in self._finalized_keys
            and len(self._inner[self._current_key]) == 0
        ):
            self._move_to_next_key()

        return len(self._inner[self._current_key]) > 0

    @override
    def _get_next_inner(self) -> RefBundle:
        # It's vital to invoke `has_next` here, to potentially advance the pointer
        # to the next key
        if not self.has_next():
            raise ValueError("Cannot pop from empty queue.")

        return self._inner[self._current_key].popleft()

    @override
    def peek_next(self) -> Optional[RefBundle]:
        # It's vital to invoke `has_next` here, to potentially advance the pointer
        # to the next key
        if not self.has_next():
            return None

        return self._inner[self._current_key][0]

    @override
    def finalize(self, key: int):
        assert key is not None and key >= self._current_key
        self._finalized_keys.add(key)

    @override
    def clear(self):
        self._reset_metrics()
        self._inner.clear()
        self._finalized_keys.clear()
        self._current_key = 0
