"""
[1] IMPACT: Importance Weighted Asynchronous Architectures with Clipped Target Networks.
Luo et al. 2020
https://arxiv.org/pdf/1912.00167
"""
import threading
import time
from collections import deque
from typing import Any, Optional

import numpy as np

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.metrics.ray_metrics import (
    DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
    TimerAndPrometheusLogger,
)
from ray.util.metrics import Counter, Histogram

POLICY_SCOPE = "func"
TARGET_POLICY_SCOPE = "target_func"


class _ImpactItem:
    """Internal wrapper to track reuse count for each item."""

    def __init__(self, data: Any):
        self.data = data
        self.reuse_count = 0


class ImpactRingBuffer:
    """
    A thread-safe ring buffer implementing the TRUE IMPACT reuse strategy.

    - Implements round-robin reuse by re-queuing to the BACK.
    - Uses a single lock for correctness.
    - Evicts the oldest item (from the front) when full.
    """

    def __init__(self, capacity: int, max_reuse: int = 1):
        if capacity <= 0:
            raise ValueError("Capacity must be positive")
        if max_reuse <= 0:
            raise ValueError("max_reuse must be positive")

        self._capacity = capacity
        self._max_reuse = max_reuse
        self._buffer = deque()

        # A single lock is required for all buffer/deque operations.
        self._lock = threading.Lock()

        # Semaphore tracks the number of *available* items.
        self._items_available = threading.Semaphore(0)

        # Statistics
        self._total_puts = 0
        self._total_gets = 0
        self._total_evictions = 0
        self._total_spent = 0

    def put(self, item: Any, block: int) -> Optional[Any]:
        """
        Add a new, unseen item to the END of the buffer.
        If full, the oldest item (from the FRONT) is evicted.
        """
        wrapped_item = _ImpactItem(item)
        evicted_data = None

        with self._lock:
            self._total_puts += 1

            # 1. Eviction: Evict from the FRONT if full
            if len(self._buffer) >= self._capacity:
                evicted_item = self._buffer.popleft()
                evicted_data = evicted_item.data
                self._total_evictions += 1
                # The semaphore count stays the same because one
                # item is removed, but another is about to be added.
            else:
                # We are adding a new item, so release the semaphore.
                self._items_available.release()

            # 2. Add new item to the END
            self._buffer.append(wrapped_item)
            return evicted_data

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        """
        Get the oldest item (from the FRONT).
        If the item is not "spent" (reuse_count < max_reuse),
        it is re-queued at the END of the buffer.
        """
        # 1. Acquire right to get an item. This blocks if empty.
        acquired = self._items_available.acquire(blocking=block, timeout=timeout)
        if not acquired:
            if block:
                raise TimeoutError("Timeout waiting for item")
            else:
                raise IndexError("Buffer is empty")

        # 2. Get item from the FRONT (thread-safe)
        with self._lock:
            if not self._buffer:
                # Safety check, should not happen if semaphore is correct
                self._items_available.release()  # Release the sem we acquired
                raise IndexError("Buffer is empty")

            wrapped_item = self._buffer.popleft()
            self._total_gets += 1

        # 3. Process item logic *outside* the lock
        wrapped_item.reuse_count += 1

        # 4. Check for re-queue
        if wrapped_item.reuse_count < self._max_reuse:
            # Item is not spent, put it back at the END
            with self._lock:
                self._buffer.append(wrapped_item)

            # Release semaphore again, as item is still available (just at the back)
            self._items_available.release()
        else:
            # Item is spent. Do not re-queue. Do not release semaphore.
            # The total number of available items has decreased by 1.
            self._total_spent += 1

        return wrapped_item.data

    def qsize(self) -> int:
        with self._lock:
            return len(self._buffer)

    def __len__(self) -> int:
        return self.qsize()

    def task_done(self):
        pass

    def get_stats(self) -> dict:
        """Get buffer statistics for monitoring."""
        with self._lock:
            avg_reuse = 0
            if self._buffer:
                counts = [item.reuse_count for item in self._buffer]
                avg_reuse = sum(counts) / len(counts)

            return {
                "size": len(self._buffer),
                "capacity": self._capacity,
                "max_reuse": self._max_reuse,
                "total_puts": self._total_puts,
                "total_gets": self._total_gets,
                "total_evictions": self._total_evictions,
                "total_spent_items": self._total_spent,
                "avg_reuse_in_buffer": avg_reuse,
            }


class CircularBuffer:
    """A circular batch-wise buffer as described in [1] for APPO.

    The buffer holds at most N batches, which are sampled at random (uniformly).
    If full and a new batch is added, the oldest batch is discarded. Also, each batch
    currently in the buffer can be sampled at most K times (after which it is also
    discarded).
    """

    def __init__(self, num_batches: int, iterations_per_batch: int):
        # N from the paper (buffer size).
        self.num_batches = num_batches
        # K ("replay coefficient") from the paper.
        self.iterations_per_batch = iterations_per_batch

        self._NxK = self.num_batches * self.iterations_per_batch
        self._num_added = 0

        self._buffer = deque([None for _ in range(self._NxK)], maxlen=self._NxK)
        self._indices = set()
        self._offset = self._NxK
        self._lock = threading.Lock()

        self._rng = np.random.default_rng()

        # Ray metrics
        self._metrics_circular_buffer_add_time = Histogram(
            name="rllib_utils_circular_buffer_add_time",
            description="Time spent in CircularBuffer.add()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_circular_buffer_add_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_circular_buffer_add_ts_dropped = Counter(
            name="rllib_utils_circular_buffer_add_ts_dropped_counter",
            description="Total number of env steps dropped by the CircularBuffer.",
            tag_keys=("rllib",),
        )
        self._metrics_circular_buffer_add_ts_dropped.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_circular_buffer_sample_time = Histogram(
            name="rllib_utils_circular_buffer_sample_time",
            description="Time spent in CircularBuffer.sample()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_circular_buffer_sample_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

    def put(self, batch, block):
        # Add buffer and k=0 information to the deque.
        with TimerAndPrometheusLogger(self._metrics_circular_buffer_add_time):
            with self._lock:
                dropped_entry = self._buffer[0]
                for _ in range(self.iterations_per_batch):
                    self._buffer.append(batch)
                    self._indices.add(self._offset)
                    self._indices.discard(self._offset - self._NxK)
                    self._offset += 1
                self._num_added += 1

            # A valid entry (w/ a batch whose k has not been reach K yet) was dropped.
            dropped_ts = 0
            if dropped_entry is not None:
                dropped_ts = dropped_entry[0].env_steps()
                if dropped_ts > 0:
                    self._metrics_circular_buffer_add_ts_dropped.inc(value=dropped_ts)

        return dropped_ts

    def get(self):
        # Only initially, the buffer may be empty -> Just wait for some time.
        with TimerAndPrometheusLogger(self._metrics_circular_buffer_sample_time):
            while len(self) == 0:
                time.sleep(0.0001)

            # Sample a random buffer index.
            with self._lock:
                idx = self._rng.choice(list(self._indices))
                actual_buffer_idx = idx - self._offset + self._NxK
                batch = self._buffer[actual_buffer_idx]
                assert batch is not None, (
                    idx,
                    actual_buffer_idx,
                    self._offset,
                    self._indices,
                    [b is None for b in self._buffer],
                )
                self._buffer[actual_buffer_idx] = None
                self._indices.discard(idx)

        # Return the sampled batch.
        return batch

    @property
    def filled(self):
        """Whether the buffer has been filled once with at least `self.num_batches`."""
        with self._lock:
            return self._num_added >= self.num_batches

    def task_done(self):
        pass

    def __len__(self) -> int:
        """Returns the number of actually valid (non-expired) batches in the buffer."""
        with self._lock:
            return len(self._indices)


class FastRingBuffer:
    """
    High-performance ring buffer optimized for multi-producer, single-consumer scenarios.

    Key optimizations:
    - Separate read/write locks minimize contention
    - Semaphore-based blocking is more efficient than condition variables
    - Manual capacity management prevents semaphore desync issues
    - Optimized for IMPALA/APPO learner workloads with many env-runners

    Best for: 50+ producers, 1 consumer, high throughput (10k+ items/sec)
    """

    def __init__(self, capacity: int):
        """
        Initialize ring buffer with fixed capacity.

        Args:
            capacity: Maximum number of items in buffer
                     For IMPALA: Use smaller buffer (32-64) for freshness
                     Use larger buffer (256+) for maximum throughput
        """
        if capacity <= 0:
            raise ValueError("Capacity must be positive")

        self._capacity = capacity
        self._buffer = deque()
        self._ages = deque()  # Track insertion order for monitoring
        self._insertion_counter = 0
        # Separate locks for read/write operations reduce contention
        self._write_lock = threading.Lock()
        self._read_lock = threading.Lock()
        # Semaphore for efficient blocking (OS-level, not Python spin-wait)
        self._items_available = threading.Semaphore(0)
        # Statistics
        self._total_puts = 0
        self._total_gets = 0
        self._total_evictions = 0
        self._total_stale_drops = 0  # For compatibility with get_stats()

    def put(self, item: Any, block: bool) -> Optional[Any]:
        """
        Add item to buffer. If full, oldest item is evicted.
        Non-blocking, optimized for high-throughput producers.

        Args:
            item: Item to add

        Returns:
            Evicted item if buffer was full, None otherwise
        """
        with self._write_lock:
            evicted = None
            self._insertion_counter += 1
            self._total_puts += 1

            # Manual eviction ensures semaphore stays in sync
            if len(self._buffer) >= self._capacity:
                evicted = self._buffer.popleft()
                self._ages.popleft()
                self._total_evictions += 1
                # Don't increment semaphore since we're replacing, not adding
            else:
                # Only release semaphore when actually adding (not replacing)
                self._items_available.release()

            self._buffer.append(item)
            self._ages.append(self._insertion_counter)
            return evicted

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        """
        Remove and return oldest item from buffer.

        Args:
            block: If True, block until item is available
            timeout: Maximum time to wait in seconds (None = wait forever)

        Returns:
            Oldest item in buffer

        Raises:
            IndexError: If buffer is empty and block=False
            TimeoutError: If timeout expires while waiting
        """
        if block:
            acquired = self._items_available.acquire(blocking=True, timeout=timeout)
            if not acquired:
                raise TimeoutError("Timeout waiting for item")
        else:
            acquired = self._items_available.acquire(blocking=False)
            if not acquired:
                raise IndexError("Buffer is empty")

        # Item guaranteed available after semaphore acquisition
        with self._read_lock:
            if not self._buffer:
                # Edge case: shouldn't happen if semaphore is correct
                raise IndexError("Buffer is empty")

            self._total_gets += 1
            self._ages.popleft()
            return self._buffer.popleft()

    def get_nowait(self) -> Any:
        """Non-blocking get. Alias for get(block=False)."""
        return self.get(block=False)

    def qsize(self) -> int:
        """Return current number of items in buffer. Alias for __len__."""
        return len(self)

    def __len__(self) -> int:
        """Return current number of items in buffer."""
        with self._write_lock:
            return len(self._buffer)

    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        with self._write_lock:
            return len(self._buffer) == 0

    def is_full(self) -> bool:
        """Check if buffer is at capacity."""
        with self._write_lock:
            return len(self._buffer) >= self._capacity

    def get_stats(self) -> dict:
        """
        Get buffer statistics for monitoring.

        Returns:
            Dictionary with buffer statistics
        """
        with self._write_lock:
            oldest_age = None
            newest_age = None
            avg_age = None
            num_very_stale = 0  # Items older than 2x capacity

            if self._ages:
                oldest_age = self._insertion_counter - self._ages[0]
                newest_age = self._insertion_counter - self._ages[-1]

                # Calculate average age
                ages = [self._insertion_counter - age for age in self._ages]
                avg_age = sum(ages) / len(ages)

                # Count "very stale" items (older than 2x capacity)
                stale_threshold = self._capacity * 2
                num_very_stale = sum(1 for age in ages if age > stale_threshold)

            return {
                "size": len(self._buffer),
                "capacity": self._capacity,
                "total_puts": self._total_puts,
                "total_gets": self._total_gets,
                "total_evictions": self._total_evictions,
                "total_stale_drops": self._total_stale_drops,
                "oldest_item_age": oldest_age,
                "newest_item_age": newest_age,
                "avg_item_age": avg_age,
                "num_very_stale_items": num_very_stale,
                "insertion_counter": self._insertion_counter,
            }

    def task_done(self):
        """Compatibility method for queue.Queue interface. No-op for RingBuffer."""
        pass

    def join(self):
        """Compatibility method for queue.Queue interface. No-op for RingBuffer."""
        pass


@OldAPIStack
def make_appo_models(policy) -> ModelV2:
    """Builds model and target model for APPO.

    Returns:
        ModelV2: The Model for the Policy to use.
            Note: The target model will not be returned, just assigned to
            `policy.target_model`.
    """
    # Get the num_outputs for the following model construction calls.
    _, logit_dim = ModelCatalog.get_action_dist(
        policy.action_space, policy.config["model"]
    )

    # Construct the (main) model.
    policy.model = ModelCatalog.get_model_v2(
        policy.observation_space,
        policy.action_space,
        logit_dim,
        policy.config["model"],
        name=POLICY_SCOPE,
        framework=policy.framework,
    )
    policy.model_variables = policy.model.variables()

    # Construct the target model.
    policy.target_model = ModelCatalog.get_model_v2(
        policy.observation_space,
        policy.action_space,
        logit_dim,
        policy.config["model"],
        name=TARGET_POLICY_SCOPE,
        framework=policy.framework,
    )
    policy.target_model_variables = policy.target_model.variables()

    # Return only the model (not the target model).
    return policy.model
