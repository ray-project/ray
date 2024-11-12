from collections import deque
import random

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import OldAPIStack


POLICY_SCOPE = "func"
TARGET_POLICY_SCOPE = "target_func"


class CircularBuffer:
    def __init__(self, capacity: int, max_picks_per_batch: int):
        # N from the paper (buffer size).
        self.capacity = capacity
        # K ("replay coefficient") from the paper.
        self.max_picks_per_batch = max_picks_per_batch

        self._buffer = deque(maxlen=self.capacity)
        self._ks = deque(maxlen=self.capacity)

    def add(self, batch):
        self._buffer.append(batch)
        self._ks.append(0)

    def sample(self):
        index = random.randint(0, len(self._buffer) - 1)
        self._ks[index] += 1
        batch = self._buffer[index]
        # This batch has been exhausted -> Remove it from the buffer.
        if self._ks[index] == self.max_picks_per_batch:

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
