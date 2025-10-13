import gc
import logging
import threading
import time
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class PythonGCThread(threading.Thread):
    """A background thread that triggers Python garbage collection.

    This thread waits for GC events from CoreWorker and triggers `gc.collect()` when
    requested, ensuring that collections are spaced out by at least
    `min_interval_s` seconds."""

    def __init__(
        self, *, min_interval_s: int = 5, gc_collect_func: Optional[Callable] = None
    ):
        logger.debug("Starting Python GC thread")
        super().__init__(name="PythonGCThread", daemon=True)
        self._should_exit = False
        self._last_gc_time = float("-inf")
        self._min_gc_interval = min_interval_s
        self._gc_event = threading.Event()
        # Set the gc_collect_func for UT, defaulting to gc.collect if None
        self._gc_collect_func = gc_collect_func or gc.collect

    def trigger_gc(self) -> None:
        self._gc_event.set()

    def run(self):
        while not self._should_exit:
            self._gc_event.wait()
            self._gc_event.clear()

            if self._should_exit:
                break

            time_since_last_gc = time.monotonic() - self._last_gc_time
            if time_since_last_gc < self._min_gc_interval:
                logger.debug(
                    f"Skipping GC, only {time_since_last_gc:.2f}s since last GC"
                )
                continue

            try:
                start = time.monotonic()
                num_freed = self._gc_collect_func()
                self._last_gc_time = time.monotonic()
                if num_freed > 0:
                    logger.debug(
                        "gc.collect() freed {} refs in {} seconds".format(
                            num_freed, self._last_gc_time - start
                        )
                    )
            except Exception as e:
                logger.error(f"Error during GC: {e}")
                self._last_gc_time = time.monotonic()

    def stop(self):
        logger.debug("Stopping Python GC thread")
        self._should_exit = True
        self._gc_event.set()
        self.join()
