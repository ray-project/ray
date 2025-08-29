import gc
import logging
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

_gc_event = threading.Event()
_gc_thread: Optional[threading.Thread] = None
# avoid multiple threads triggering GC at the same time
_gc_lock = threading.Lock()


class PythonGCThread(threading.Thread):
    def __init__(self, min_interval: int = 5):
        super().__init__(name="GCManagerThread", daemon=True)
        self._running = True
        self._last_gc_time = float("-inf")  # ensures first GC runs immediately
        self._min_gc_interval = min_interval

    def run(self):
        while self._running:
            _gc_event.wait()
            _gc_event.clear()

            if not self._running:
                break

            time_since_last_gc = time.perf_counter() - self._last_gc_time
            if time_since_last_gc < self._min_gc_interval:
                logger.debug(
                    f"Skipping GC, only {time_since_last_gc:.2f}s since last GC"
                )
                continue

            if not self.gc_in_progress:
                self.gc_in_progress = True
                try:
                    start = time.perf_counter()
                    num_freed = gc.collect()
                    self._last_gc_time = time.perf_counter()
                    if num_freed > 0:
                        logger.debug(
                            "gc.collect() freed {} refs in {} seconds".format(
                                num_freed, self._last_gc_time - start
                            )
                        )
                except Exception as e:
                    logger.error(f"Error during GC: {e}")
                finally:
                    self.gc_in_progress = False
            else:
                logger.debug("GC already in progress, skipping this trigger")

    def stop(self):
        self._running = False
        _gc_event.set()
        self.join()


def init_gc_collect_manager(min_interval: int = 5) -> None:
    global _gc_thread
    with _gc_lock:
        if _gc_thread is not None and _gc_thread.is_alive():
            logger.warning("GC thread already initialized")
            return

        _gc_thread = GCManagerThread(min_interval)
        _gc_thread.start()
        logger.debug(f"GC thread initialized with min interval {min_interval}s")


def stop_gc_collect_manager_if_needed() -> None:
    global _gc_thread
    with _gc_lock:
        if _gc_thread is not None:
            _gc_thread.stop()
            _gc_thread = None
            logger.debug("GC thread stopped")
        else:
            logger.debug("No GC thread to stop")


def trigger_gc() -> None:
    _gc_event.set()
