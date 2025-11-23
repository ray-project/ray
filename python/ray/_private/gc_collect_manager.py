import gc
import logging
import threading
import time
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class PythonGCThread(threading.Thread):
    """A background thread that triggers Python garbage collection.

    This thread waits for GC events from CoreWorker and triggers `gc.collect()` when
    when requested."""

    def __init__(self, *, gc_collect_func: Optional[Callable] = None):
        logger.debug("Starting Python GC thread")
        super().__init__(name="PythonGCThread", daemon=True)
        self._should_exit = False
        self._gc_event = threading.Event()
        # Sets the gc_collect_func (only for testing), defaults to gc.collect
        self._gc_collect_func = gc_collect_func or gc.collect

    def trigger_gc(self) -> None:
        self._gc_event.set()

    def run(self):
        while not self._should_exit:
            self._gc_event.wait()
            self._gc_event.clear()

            if self._should_exit:
                break

            try:
                start = time.monotonic()
                num_freed = self._gc_collect_func()
                if num_freed > 0:
                    logger.debug(
                        "gc.collect() freed {} refs in {} seconds".format(
                            num_freed, time.monotonic() - start
                        )
                    )
            except Exception as e:
                logger.error(f"Error during GC: {e}")

    def stop(self):
        logger.debug("Stopping Python GC thread")
        self._should_exit = True
        self._gc_event.set()
        self.join()
