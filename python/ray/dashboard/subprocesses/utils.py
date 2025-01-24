import asyncio
import os
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


def module_logging_filename(
    module_name: str, incarnation: int, logging_filename: str
) -> str:
    """
    Parse logging_filename = STEM EXTENSION,
    return STEM - MODULE_NAME - INCARNATION EXTENSION

    If logging_filename is empty, return empty. This means the logs go to stderr.

    Example:
    module_name = "TestModule"
    incarnation = 5
    logging_filename = "dashboard.log"
    STEM = "dashboard"
    EXTENSION = ".log"
    return "dashboard-TestModule-5.log"
    """
    if not logging_filename:
        return ""
    stem, extension = os.path.splitext(logging_filename)
    return f"{stem}-{module_name}-{incarnation}{extension}"


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

    def pop_all(self) -> dict[K, V]:
        with self._lock:
            d = self._dict
            self._dict = {}
            return d
