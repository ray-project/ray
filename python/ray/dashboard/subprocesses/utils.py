import asyncio
import threading
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


def assert_not_in_asyncio_loop():
    try:
        asyncio.get_running_loop()
        raise AssertionError(
            "This function should not be called from within an asyncio loop"
        )
    except RuntimeError:
        pass


class ThreadSafeDict(Generic[K, V]):
    """A thread-safe dictionary that only allows certain operations."""

    def __init__(self):
        self._lock = threading.Lock()
        self._dict: dict[K, V] = {}

    def put_new(self, key: K, value: V):
        with self._lock:
            if key in self._dict:
                raise KeyError(f"Key {key} already exists in {self._dict}")
            self._dict[key] = value

    def get_or_raise(self, key: K) -> V:
        with self._lock:
            value = self._dict.get(key)
            if value is None:
                raise KeyError(f"Key {key} not found in {self._dict}")
            return value

    def pop_or_raise(self, key: K) -> V:
        with self._lock:
            value = self._dict.pop(key)
            if value is None:
                raise KeyError(f"Key {key} not found in {self._dict}")
            return value
