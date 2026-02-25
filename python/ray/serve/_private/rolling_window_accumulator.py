import threading
import time
from typing import List


class _ThreadBuckets:
    """Per-thread bucket storage for rolling window accumulator.

    Each thread gets its own instance to avoid lock contention on the hot path.
    """

    # This is a performance optimization to avoid creating a dictionary for the instance.
    __slots__ = ("buckets", "current_bucket_idx", "last_rotation_time")

    def __init__(self, num_buckets: int):
        self.buckets = [0.0] * num_buckets
        self.current_bucket_idx = 0
        self.last_rotation_time = time.time()


class _ThreadLocalRef(threading.local):
    """Thread-local reference to the thread's _ThreadBuckets instance."""

    def __init__(self):
        super().__init__()
        # by using threading.local, each thread gets its own instance of _ThreadBuckets.
        self.data: _ThreadBuckets = None


class RollingWindowAccumulator:
    """Tracks cumulative values over a rolling time window.

    Uses bucketing for memory efficiency - divides the window into N buckets
    and rotates them as time passes. This allows efficient tracking of values
    over a sliding window without storing individual data points.

    Uses thread-local storage for lock-free writes on the hot path (add()).
    Only get_total() requires synchronization to aggregate across threads.

    Example:
        # Create a 10-minute rolling window with 60 buckets (10s each)
        accumulator = RollingWindowAccumulator(
            window_duration_s=600.0,
            num_buckets=60,
        )

        # Add values (lock-free, safe from multiple threads)
        accumulator.add(100.0)
        accumulator.add(50.0)

        # Get total (aggregates across all threads)
        total = accumulator.get_total()

    Thread Safety:
        - add() is lock-free after the first call from each thread
        - get_total() acquires a lock to aggregate across threads
        - Safe to call from multiple threads concurrently
    """

    def __init__(
        self,
        window_duration_s: float,
        num_buckets: int = 60,
    ):
        """Initialize the rolling window accumulator.

        Args:
            window_duration_s: Total duration of the rolling window in seconds.
                Values older than this are automatically expired.
            num_buckets: Number of buckets to divide the window into. More buckets
                gives finer granularity but uses slightly more memory. Default is 60,
                which for a 10-minute window gives 10-second granularity.

        Raises:
            ValueError: If window_duration_s <= 0 or num_buckets <= 0.
        """
        if window_duration_s <= 0:
            raise ValueError(
                f"window_duration_s must be positive, got {window_duration_s}"
            )
        if num_buckets <= 0:
            raise ValueError(f"num_buckets must be positive, got {num_buckets}")

        self._window_duration_s = window_duration_s
        self._num_buckets = num_buckets
        self._bucket_duration_s = window_duration_s / num_buckets

        # Thread-local reference to per-thread bucket data
        self._local = _ThreadLocalRef()

        # Track all per-thread bucket instances for aggregation
        self._all_thread_data: List[_ThreadBuckets] = []
        self._registry_lock = threading.Lock()

    @property
    def window_duration_s(self) -> float:
        """The total duration of the rolling window in seconds."""
        return self._window_duration_s

    @property
    def num_buckets(self) -> int:
        """The number of buckets in the rolling window."""
        return self._num_buckets

    @property
    def bucket_duration_s(self) -> float:
        """The duration of each bucket in seconds."""
        return self._bucket_duration_s

    def _ensure_initialized(self) -> _ThreadBuckets:
        """Ensure thread-local storage is initialized for the current thread.

        This is called on every add() but the fast path (already initialized)
        is just a single attribute check with no locking.

        Returns:
            The _ThreadBuckets instance for the current thread.
        """
        data = self._local.data
        if data is not None:
            return data

        # Slow path: first call from this thread
        data = _ThreadBuckets(self._num_buckets)
        self._local.data = data

        # Register for aggregation (only happens once per thread)
        with self._registry_lock:
            self._all_thread_data.append(data)

        return data

    def _rotate_buckets_if_needed(self, data: _ThreadBuckets) -> None:
        """Rotate buckets for the given thread's storage.

        Advances the current bucket index and clears old buckets as time passes.
        """
        now = time.time()
        elapsed = now - data.last_rotation_time
        buckets_to_advance = int(elapsed / self._bucket_duration_s)

        if buckets_to_advance > 0:
            if buckets_to_advance >= self._num_buckets:
                # All buckets have expired, reset everything
                data.buckets = [0.0] * self._num_buckets
                data.current_bucket_idx = 0
            else:
                # Clear old buckets as we advance
                for _ in range(buckets_to_advance):
                    data.current_bucket_idx = (
                        data.current_bucket_idx + 1
                    ) % self._num_buckets
                    data.buckets[data.current_bucket_idx] = 0.0

            data.last_rotation_time = now

    def add(self, value: float) -> None:
        """Add a value to the current bucket.

        This operation is lock-free for the calling thread after the first call.
        Safe to call from multiple threads concurrently.

        Args:
            value: The value to add to the accumulator.
        """
        # Fast path: just check if initialized (no lock)
        data = self._ensure_initialized()

        # Lock-free: only touches thread-local data
        self._rotate_buckets_if_needed(data)
        data.buckets[data.current_bucket_idx] += value

    def get_total(self) -> float:
        """Get total value across all buckets in the window.

        This aggregates values from all threads that have called add().
        Expired buckets (older than window_duration_s) are not included.

        Note: We are accepting some inaccuracy in the total value to avoid the overhead of a lock.
        This is acceptable because we are only using this for utilization metrics, which are not
        critical for the overall system. Given that the default window duration is 600s and the
        default report interval is 10s, the inaccuracy is less than 0.16%.

        Returns:
            The sum of all non-expired values in the rolling window.
        """
        total = 0.0
        now = time.time()

        with self._registry_lock:
            for data in self._all_thread_data:
                # Calculate which buckets are still valid for this thread's data
                elapsed = now - data.last_rotation_time
                buckets_expired = int(elapsed / self._bucket_duration_s)

                if buckets_expired >= self._num_buckets:
                    # All buckets have expired for this thread
                    continue

                # Sum buckets that haven't expired
                # Buckets are arranged in a circular buffer, with current_bucket_idx
                # being the most recent. We need to skip buckets that have expired.
                for i in range(self._num_buckets - buckets_expired):
                    # Go backwards from current bucket
                    idx = (data.current_bucket_idx - i) % self._num_buckets
                    total += data.buckets[idx]

        return total

    def get_num_registered_threads(self) -> int:
        """Get the number of threads that have called add().

        Useful for debugging and testing.

        Returns:
            The number of threads registered with this accumulator.
        """
        with self._registry_lock:
            return len(self._all_thread_data)
