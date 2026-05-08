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


class CircularBuffer:
    """A circular batch-wise buffer with Queue-like interface.

    The buffer holds at most N batches, which are sampled at random (uniformly).
    If full and a new batch is added, the oldest batch is discarded. Each batch
    can be sampled at most K times (after which it is also discarded).

    This version implements Queue-like put/get methods with blocking support.
    """

    def __init__(self, num_batches: int, iterations_per_batch: int):
        """
        Args:
            num_batches: N from the paper (queue buffer size).
            iterations_per_batch: K ("replay coefficient") from the paper. Defines
                how often a single batch can sampled before being discarded. If a
                new batch is added when the buffer is full, the oldest batch is
                discarded entirely (regardless of how often it has been sampled).
        """
        self.num_batches = num_batches
        self.iterations_per_batch = iterations_per_batch

        self._NxK = self.num_batches * self.iterations_per_batch
        self._num_added = 0

        self._buffer = deque([None for _ in range(self._NxK)], maxlen=self._NxK)
        self._indices = set()
        self._offset = self._NxK
        self._lock = threading.Lock()

        # Semaphore tracks the number of *available* samples.
        self._items_available = threading.Semaphore(0)

        self._rng = np.random.default_rng()

        # Statistics
        self._total_puts = 0
        self._total_gets = 0
        self._total_dropped = 0

        # Ray metrics
        self._metrics_circular_buffer_put_time = Histogram(
            name="rllib_utils_circular_buffer_put_time",
            description="Time spent in CircularBuffer.put()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_circular_buffer_put_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_circular_buffer_put_ts_dropped = Counter(
            name="rllib_utils_circular_buffer_put_ts_dropped_counter",
            description="Total number of env steps dropped by the CircularBuffer.",
            tag_keys=("rllib",),
        )
        self._metrics_circular_buffer_put_ts_dropped.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

        self._metrics_circular_buffer_get_time = Histogram(
            name="rllib_utils_circular_buffer_get_time",
            description="Time spent in CircularBuffer.get()",
            boundaries=DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS,
            tag_keys=("rllib",),
        )
        self._metrics_circular_buffer_get_time.set_default_tags(
            {"rllib": self.__class__.__name__}
        )

    def put(
        self, item: Any, block: bool = True, timeout: Optional[float] = None
    ) -> int:
        """Add a new batch to the buffer.

        The batch is added K times (iterations_per_batch) to allow for K samples.
        If full, the oldest batch entries are dropped.

        Args:
            item: The batch to add
            block: Not used (always non-blocking for puts)
            timeout: Not used

        Returns:
            Number of dropped entries (0 or iterations_per_batch)
        """
        with TimerAndPrometheusLogger(self._metrics_circular_buffer_put_time):
            with self._lock:
                self._total_puts += 1

                # Check if we'll drop old entries
                dropped_entry = self._buffer[0]

                # Add buffer K times with new indices
                for _ in range(self.iterations_per_batch):
                    self._buffer.append(item)
                    self._indices.add(self._offset)
                    self._indices.discard(self._offset - self._NxK)
                    self._offset += 1

                    # Release semaphore for each available sample
                    self._items_available.release()

                self._num_added += 1

                # A valid entry (w/ a batch whose k has not been reach K yet) was dropped.
                dropped_ts = 0
                if dropped_entry is not None:
                    dropped_ts = (
                        dropped_entry[0].env_steps()
                        if isinstance(dropped_entry, tuple)
                        else dropped_entry.env_steps()
                    )
                    if dropped_ts > 0:
                        self._metrics_circular_buffer_put_ts_dropped.inc(
                            value=dropped_ts
                        )

        return dropped_ts

    def put_nowait(self, item: Any) -> int:
        """Equivalent to self.put(block=False)."""
        return self.put(item, block=False)

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        """Sample a random batch from the buffer.

        The sampled entry is removed and won't be sampled again.
        Blocks if the buffer is empty (when block=True).

        Args:
            block: If True, block until an item is available
            timeout: Maximum time to wait (only used when block=True)

        Returns:
            A randomly sampled batch

        Raises:
            TimeoutError: If timeout expires while blocking
            IndexError: If buffer is empty and block=False
        """
        # Only initially, the buffer may be empty -> Just wait for some time.
        with TimerAndPrometheusLogger(self._metrics_circular_buffer_get_time):
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

    def get_nowait(self) -> Any:
        """Equivalent to self.get(block=False)."""
        return self.get(block=False)

    @property
    def filled(self) -> bool:
        """Whether the buffer has been filled once with at least `self.num_batches`."""
        with self._lock:
            return self._num_added >= self.num_batches

    def qsize(self) -> int:
        """Returns the number of actually valid (non-expired) batches in the buffer."""
        with self._lock:
            return len(self._indices)

    def __len__(self) -> int:
        return self.qsize()

    def task_done(self):
        """No-op for Queue compatibility."""
        pass

    def get_stats(self) -> dict:
        """Get buffer statistics for monitoring."""
        with self._lock:
            return {
                "size": len(self._indices),
                "capacity": self._NxK,
                "num_batches": self.num_batches,
                "iterations_per_batch": self.iterations_per_batch,
                "total_puts": self._total_puts,
                "total_gets": self._total_gets,
                "total_dropped": self._total_dropped,
                "filled": self._num_added >= self.num_batches,
            }


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
