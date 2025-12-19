"""
[1] IMPACT: Importance Weighted Asynchronous Architectures with Clipped Target Networks.
Luo et al. 2020
https://arxiv.org/pdf/1912.00167
"""
import threading
import time
from collections import deque

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

    def add(self, batch):
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
                dropped_ts = dropped_entry.env_steps()
                if dropped_ts > 0:
                    self._metrics_circular_buffer_add_ts_dropped.inc(value=dropped_ts)

        return dropped_ts

    def sample(self):
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

    def __len__(self) -> int:
        """Returns the number of actually valid (non-expired) batches in the buffer."""
        with self._lock:
            return len(self._indices)


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
