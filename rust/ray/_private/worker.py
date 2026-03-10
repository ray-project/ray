"""Worker global state for the Rust Ray backend.

Provides ``global_worker`` with an ``rdt_manager`` attribute that
GPU worker processes populate during initialization.
"""


class _Worker:
    """Minimal worker state holder, similar to C++ backend's Worker class."""

    def __init__(self):
        self.rdt_manager = None
        self._core_worker = None
        self._actor_handle = None

    def reset(self):
        self.rdt_manager = None
        self._core_worker = None
        self._actor_handle = None


global_worker = _Worker()
