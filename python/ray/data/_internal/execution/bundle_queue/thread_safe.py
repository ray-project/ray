from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any, Optional

from .base import BaseBundleQueue, BundleQueue

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle


class ThreadSafeBundleQueue(BundleQueue):
    """A thread-safe wrapper for a ``BundleQueue``.

    Delegates all operations to the wrapped queue while a lock.

    NOTE: Not safe to use with ``__contains__``/``remove`` from ``QueueWithRemoval``.
    """

    def __init__(self, inner: BundleQueue):
        self._inner = inner
        self._lock = threading.Lock()

    def estimate_size_bytes(self) -> int:
        with self._lock:
            return self._inner.estimate_size_bytes()

    def num_blocks(self) -> int:
        with self._lock:
            return self._inner.num_blocks()

    def num_bundles(self) -> int:
        with self._lock:
            return self._inner.num_bundles()

    def num_rows(self) -> int:
        with self._lock:
            return self._inner.num_rows()

    def add(self, bundle: RefBundle, **kwargs: Any):
        with self._lock:
            self._inner.add(bundle, **kwargs)

    def get_next(self) -> RefBundle:
        with self._lock:
            return self._inner.get_next()

    def peek_next(self) -> Optional[RefBundle]:
        with self._lock:
            return self._inner.peek_next()

    def has_next(self) -> bool:
        with self._lock:
            return self._inner.has_next()

    def clear(self):
        with self._lock:
            self._inner.clear()

    def finalize(self, **kwargs: Any):
        with self._lock:
            return self._inner.finalize(**kwargs)
