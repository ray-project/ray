from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Any, Deque, Iterator, List, Optional

from typing_extensions import override

from .base import BaseBundleQueue

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


class FIFOBundleQueue(BaseBundleQueue):
    """A bundle queue that follows fifo-policy. Conceptually
    [ ] <- [ ] <- [ ] ...
     ^ where the leftmost is popped first
    """

    def __init__(self, bundles: Optional[List[RefBundle]] = None):
        super().__init__()
        self._inner: Deque[RefBundle] = deque([])
        if bundles is not None:
            for bundle in bundles:
                self.add(bundle)

    @override
    def _add_inner(self, bundle: RefBundle, **kwargs: Any):
        self._inner.append(bundle)

    @override
    def _get_next_inner(self) -> RefBundle:
        if not self.has_next():
            raise ValueError(
                f"Popping from empty {self.__class__.__name__} is prohibited"
            )

        bundle = self._inner.popleft()
        return bundle

    @override
    def peek_next(self) -> Optional[RefBundle]:
        if not self.has_next():
            return None
        return self._inner[0]

    @override
    def has_next(self) -> bool:
        return len(self) > 0

    @override
    def finalize(self, **kwargs: Any):
        pass

    def __iter__(self) -> Iterator[RefBundle]:
        yield from self._inner

    def to_list(self) -> List[RefBundle]:
        return list(self._inner)

    @override
    def clear(self):
        self._reset_metrics()
        self._inner.clear()
