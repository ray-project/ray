from collections import deque
import random
import threading
import time

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import OldAPIStack


POLICY_SCOPE = "func"
TARGET_POLICY_SCOPE = "target_func"


class CircularBuffer:
    def __init__(self, num_batches: int, iterations_per_batch: int):
        # N from the paper (buffer size).
        self.num_batches = num_batches
        # K ("replay coefficient") from the paper.
        self.iterations_per_batch = iterations_per_batch

        self._buffer = deque(maxlen=self.num_batches)
        self._lock = threading.Lock()

    def add(self, batch):
        dropped_ts = 0

        # Add buffer and k=0 information to the deque.
        with self._lock:
            len_ = len(self._buffer)
            if len_ == self.num_batches:
                dropped_batch = self._buffer[0][0]
                if dropped_batch is not None:
                    dropped_ts += (
                        dropped_batch.env_steps()
                        * (self.iterations_per_batch - self._buffer[0][1])
                    )
            self._buffer.append([batch, 0])

        return dropped_ts

    def sample(self):
        k = entry = batch = None

        while True:
            # Only initially, the buffer may be empty -> Just wait for some time.
            if len(self._buffer) == 0:
                time.sleep(0.001)
                continue
            # Sample a random buffer index.
            with self._lock:
                entry = self._buffer[random.randint(0, len(self._buffer) - 1)]
            batch, k = entry
            # Ignore batches that have already been invalidated.
            if batch is not None:
                break

        # Increase k += 1 for this batch.
        assert k is not None
        entry[1] += 1

        # This batch has been exhausted (k == K) -> Invalidate it in the buffer.
        if k == self.iterations_per_batch:
            entry[0] = None
            entry[1] = None

        # Return the sampled batch.
        return batch

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
