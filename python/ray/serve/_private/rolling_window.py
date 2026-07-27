import threading
import time
from collections import deque
from typing import Any, Optional


class _ThreadBuckets:
    """Per-thread bucket storage for rolling window.

    Each thread gets its own instance to avoid lock contention on the hot path.
    """

    # This is a performance optimization to avoid creating a dictionary for the instance.
    __slots__ = ("buckets", "current_bucket_idx", "last_rotation_time")

    def __init__(self, num_buckets: int):
        self.buckets = [0.0] * num_buckets
        self.current_bucket_idx = 0
        self.last_rotation_time = time.time()


class _ThreadDequeData:
    """Per-thread monotonic deque storage for RollingWindowMax and RollingWindowMin.

    Separates the "current bucket" accumulation from the committed deque so that
    the deque is bounded to O(num_buckets) entries regardless of how many values
    are added per bucket, rather than O(num_values_in_window).

    Attributes:
        deque: Monotonic deque of ``(bucket_seq, bucket_value)`` pairs. Each
            ``bucket_seq`` appears at most once because values within the same
            bucket are pre-aggregated into ``current_bucket_val`` before being
            committed here when the bucket rolls over.
            For RollingWindowMax values are in descending order (front = max).
            For RollingWindowMin values are in ascending order (front = min).
        current_seq: Sequence number of the bucket currently being accumulated.
            ``-1`` means no value has been added yet.
        current_bucket_val: Running max (or min) for the current ``current_seq``
            bucket.  Initialized to ``float('-inf')`` for max or ``float('inf')``
            for min so that any real value overwrites it on the first add(), and
            so that get_max()/get_min() safely ignore it until an actual value
            has been recorded.
    """

    __slots__ = ("deque", "current_seq", "current_bucket_val")

    def __init__(self, sentinel: float) -> None:
        """Initialize the thread deque data.

        Args:
            sentinel: Initial value for the current bucket's running scalar.
        """
        self.deque: deque = deque()
        self.current_seq: int = -1
        self.current_bucket_val: float = sentinel


class _ThreadLocalRef(threading.local):
    """Thread-local reference to the thread's _ThreadBuckets instance."""

    def __init__(self) -> None:
        super().__init__()
        # by using threading.local, each thread gets its own instance of _ThreadBuckets.
        self.data: Optional[object] = None


class _RollingWindowBase:
    """Base class for rolling window trackers.

    Provides the shared infrastructure: bucketing, rotation, thread-local
    storage, and thread registration. Subclasses define how values are
    recorded into buckets and how buckets are aggregated.

    Uses bucketing for memory efficiency - divides the window into N buckets
    and rotates them as time passes. This allows efficient tracking of values
    over a sliding window without storing individual data points.
    """

    def __init__(
        self,
        window_duration_s: float,
        num_buckets: int = 60,
    ):
        if window_duration_s <= 0:
            raise ValueError(
                f"window_duration_s must be positive, got {window_duration_s}"
            )
        if num_buckets <= 0:
            raise ValueError(f"num_buckets must be positive, got {num_buckets}")

        self._window_duration_s = window_duration_s
        self._num_buckets = num_buckets
        self._bucket_duration_s = window_duration_s / num_buckets
        self._start_time = time.time()

        # Thread-local reference to per-thread bucket data
        self._local = _ThreadLocalRef()

        # Track all per-thread bucket instances for aggregation.
        # Type is intentionally unparameterized: RollingWindowAccumulator stores
        # _ThreadBuckets while RollingWindowMax/Min store _ThreadDequeData.
        self._all_thread_data: list = []
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

    def _ensure_initialized(self) -> Any:
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

    def _get_current_seq(self, now: float) -> int:
        """Return the monotonic bucket sequence number for the given timestamp.

        Sequence numbers increase monotonically as time advances, with one new
        sequence number assigned per ``bucket_duration_s`` interval. Used by
        RollingWindowMax and RollingWindowMin to identify which bucket a value
        belongs to and to detect bucket roll-overs without mutable shared state.

        Args:
            now: Current time in seconds (e.g. from time.time()).

        Returns:
            A non-negative integer bucket sequence number.
        """
        return int((now - self._start_time) / self._bucket_duration_s)

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

    def get_num_registered_threads(self) -> int:
        """Get the number of threads that have called add().

        Useful for debugging and testing.

        Returns:
            The number of threads registered with this accumulator.
        """
        with self._registry_lock:
            return len(self._all_thread_data)


class RollingWindowAccumulator(_RollingWindowBase):
    """Tracks cumulative values over a rolling time window.

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


class RollingWindowMax(_RollingWindowBase):
    """Tracks the maximum value over a rolling time window.

    Uses a bucketed rolling window approach with a monotonic deque for O(1)
    max query per thread. Within each bucket period, only the maximum value is
    tracked via a per-thread scalar accumulator. When the bucket rolls over that
    maximum is committed to a monotonic descending deque, bounding per-thread
    memory to O(num_buckets) regardless of how many values are added per bucket.

    Example:
        # Create a 30-second rolling window with 6 buckets (5s each)
        tracker = RollingWindowMax(
            window_duration_s=30.0,
            num_buckets=6,
        )

        # Record values (lock-free, safe from multiple threads)
        tracker.add(100.0)
        tracker.add(500.0)
        tracker.add(50.0)

        # Get max in the window (aggregates across all threads)
        maximum = tracker.get_max()  # returns 500.0

    Thread Safety:
        - add() is lock-free after the first call from each thread
        - get_max() acquires a lock to aggregate across threads; it also
          opportunistically evicts expired entries from each thread's deque
          without holding the thread's own lock (safe under CPython's GIL
          since individual deque operations are atomic). Minor inaccuracies
          are possible during concurrent add()/get_max() calls, similar to
          RollingWindowAccumulator.
        - Safe to call from multiple threads concurrently
    """

    def _ensure_initialized(self) -> Any:
        """Ensure thread-local deque data is initialized for the current thread."""
        data = self._local.data
        if data is not None:
            return data

        # sentinel = float('-inf') so that any real add() value overwrites it.
        data = _ThreadDequeData(sentinel=float("-inf"))
        self._local.data = data

        with self._registry_lock:
            self._all_thread_data.append(data)

        return data

    def add(self, value: float) -> None:
        """Record a value, updating the current bucket's running maximum.

        This operation is lock-free for the calling thread after the first call.
        Safe to call from multiple threads concurrently.

        Within each bucket period only the maximum value is tracked via a scalar
        accumulator. When the bucket rolls over, that maximum is committed to the
        monotonic deque, keeping deque size bounded to O(num_buckets) across the
        window regardless of call frequency.

        Args:
            value: The value to record.
        """
        data = self._ensure_initialized()
        now = time.time()
        seq = self._get_current_seq(now)

        if seq == data.current_seq:
            # Same bucket: just track the running max — no deque modification.
            if value > data.current_bucket_val:
                data.current_bucket_val = value
            return

        # Bucket has advanced: commit the previous bucket's max to the deque.
        if data.current_seq >= 0:
            bucket_val = data.current_bucket_val
            # Maintain monotonic descending order; pop entries that are now
            # dominated by the new bucket's max (they can never be the future max
            # while this newer, higher value is still in the window).
            while data.deque and data.deque[-1][1] <= bucket_val:
                data.deque.pop()
            data.deque.append((data.current_seq, bucket_val))

        # Evict expired committed buckets from the front.
        min_valid_seq = seq - self._num_buckets + 1
        while data.deque and data.deque[0][0] < min_valid_seq:
            data.deque.popleft()

        # Start accumulating the new bucket.
        data.current_seq = seq
        data.current_bucket_val = value

    def get_max(self) -> float:
        """Get max value across all non-expired buckets in the window.

        This aggregates values from all threads that have called add().
        Expired buckets (older than window_duration_s) are not included.

        Returns:
            The maximum value observed in the rolling window, or 0.0
            if no values have been recorded.
        """
        result = 0.0
        now = time.time()
        seq = self._get_current_seq(now)
        min_valid_seq = seq - self._num_buckets + 1

        with self._registry_lock:
            for data in self._all_thread_data:
                # Evict expired committed buckets from the front.
                while data.deque and data.deque[0][0] < min_valid_seq:
                    data.deque.popleft()
                # Max from committed buckets (front of monotonic descending deque).
                if data.deque and data.deque[0][1] > result:
                    result = data.deque[0][1]
                # Also consider the uncommitted current bucket if it is still
                # within the valid window. This bucket has not yet been committed
                # to the deque because no newer bucket has arrived yet.
                if (
                    data.current_seq >= 0
                    and data.current_seq >= min_valid_seq
                    and data.current_bucket_val > result
                ):
                    result = data.current_bucket_val

        return result


class RollingWindowMin(_RollingWindowBase):
    """Tracks the minimum value over a rolling time window.

    Uses a bucketed rolling window approach with a monotonic deque for O(1)
    min query per thread. Within each bucket period, only the minimum value is
    tracked via a per-thread scalar accumulator. When the bucket rolls over that
    minimum is committed to a monotonic ascending deque, bounding per-thread
    memory to O(num_buckets) regardless of how many values are added per bucket.

    Example:
        # Create a 30-second rolling window with 6 buckets (5s each)
        tracker = RollingWindowMin(
            window_duration_s=30.0,
            num_buckets=6,
        )

        # Record values (lock-free, safe from multiple threads)
        tracker.add(100.0)
        tracker.add(50.0)
        tracker.add(500.0)

        # Get min in the window (aggregates across all threads)
        minimum = tracker.get_min()  # returns 50.0

    Thread Safety:
        - add() is lock-free after the first call from each thread
        - get_min() acquires a lock to aggregate across threads; it also
          opportunistically evicts expired entries from each thread's deque
          without holding the thread's own lock (safe under CPython's GIL
          since individual deque operations are atomic). Minor inaccuracies
          are possible during concurrent add()/get_min() calls, similar to
          RollingWindowAccumulator.
        - Safe to call from multiple threads concurrently
    """

    def _ensure_initialized(self) -> Any:
        """Ensure thread-local deque data is initialized for the current thread."""
        data = self._local.data
        if data is not None:
            return data

        # sentinel = float('inf') so that any real add() value overwrites it.
        data = _ThreadDequeData(sentinel=float("inf"))
        self._local.data = data

        with self._registry_lock:
            self._all_thread_data.append(data)

        return data

    def add(self, value: float) -> None:
        """Record a value, updating the current bucket's running minimum.

        This operation is lock-free for the calling thread after the first call.
        Safe to call from multiple threads concurrently.

        Within each bucket period only the minimum value is tracked via a scalar
        accumulator. When the bucket rolls over, that minimum is committed to the
        monotonic deque, keeping deque size bounded to O(num_buckets) across the
        window regardless of call frequency.

        Args:
            value: The value to record.
        """
        data = self._ensure_initialized()
        now = time.time()
        seq = self._get_current_seq(now)

        if seq == data.current_seq:
            # Same bucket: just track the running min — no deque modification.
            if value < data.current_bucket_val:
                data.current_bucket_val = value
            return

        # Bucket has advanced: commit the previous bucket's min to the deque.
        if data.current_seq >= 0:
            bucket_val = data.current_bucket_val
            # Maintain monotonic ascending order; pop entries that are now
            # dominated by the new bucket's min (they can never be the future min
            # while this newer, lower value is still in the window).
            while data.deque and data.deque[-1][1] >= bucket_val:
                data.deque.pop()
            data.deque.append((data.current_seq, bucket_val))

        # Evict expired committed buckets from the front.
        min_valid_seq = seq - self._num_buckets + 1
        while data.deque and data.deque[0][0] < min_valid_seq:
            data.deque.popleft()

        # Start accumulating the new bucket.
        data.current_seq = seq
        data.current_bucket_val = value

    def get_min(self) -> Optional[float]:
        """Get min value across all non-expired buckets in the window.

        This aggregates values from all threads that have called add().
        Expired buckets (older than window_duration_s) are not included.

        Returns:
            The minimum value observed in the rolling window, or None
            if no values have been recorded.
        """
        result: Optional[float] = None
        now = time.time()
        seq = self._get_current_seq(now)
        min_valid_seq = seq - self._num_buckets + 1

        with self._registry_lock:
            for data in self._all_thread_data:
                # Evict expired committed buckets from the front.
                while data.deque and data.deque[0][0] < min_valid_seq:
                    data.deque.popleft()
                # Min from committed buckets (front of monotonic ascending deque).
                if data.deque:
                    val = data.deque[0][1]
                    if result is None or val < result:
                        result = val
                # Also consider the uncommitted current bucket if it is still
                # within the valid window. This bucket has not yet been committed
                # to the deque because no newer bucket has arrived yet.
                if data.current_seq >= 0 and data.current_seq >= min_valid_seq:
                    val = data.current_bucket_val
                    if result is None or val < result:
                        result = val

        return result
