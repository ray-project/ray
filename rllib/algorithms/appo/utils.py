"""
[1] IMPACT: Importance Weighted Asynchronous Architectures with Clipped Target Networks.
Luo et al. 2020
https://arxiv.org/pdf/1912.00167
"""
from collections import deque
import threading
import time

import numpy as np

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import OldAPIStack


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

    def add(self, batch):
        # Add buffer and k=0 information to the deque.
        with self._lock:
            dropped_entry = self._buffer[0]
            for _ in range(self.iterations_per_batch):
                self._buffer.append(batch)
                self._indices.add(self._offset)
                self._indices.discard(self._offset - self._NxK)
                self._offset += 1

        # A valid entry (w/ a batch whose k has not been reach K yet) was dropped.
        dropped_ts = 0
        if dropped_entry is not None:
            dropped_ts = dropped_entry.env_steps()

        self._num_added += 1

        return dropped_ts

    def sample(self):
        # Only initially, the buffer may be empty -> Just wait for some time.
        while not self._indices:
            time.sleep(0.00001)

        # Sample a random buffer index.
        with self._lock:
            idx = self._rng.choice(list(self._indices))
            actual_buffer_idx = idx - self._offset + self._NxK
            batch = self._buffer[actual_buffer_idx]
            self._buffer[actual_buffer_idx] = None
            self._indices.discard(idx)

        # Return the sampled batch.
        return batch

    @property
    def filled(self):
        """Whether the buffer has been filled once with at least `self.num_batches`."""
        return self._num_added >= self.num_batches

    def __len__(self) -> int:
        """Returns the number of actually valid (non-expired) batches in the buffer."""
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
