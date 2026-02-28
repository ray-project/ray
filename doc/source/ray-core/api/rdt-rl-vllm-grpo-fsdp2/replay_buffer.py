from typing import Any

import numpy as np
import ray
from settings import MAX_BUFFER_SIZE


@ray.remote
class ReplayBuffer:
    """Store scored groups for reuse during learning.

    This class stores the past experiences (AKA trajectories, or slices) of the model.
    This allows the learner to sample and learn from the same experiences multiple times
    by comparing the latest model with previous models.

    The sampler weights the trajectories by the policy version, such that trajectories produced
    by more recent versions of the model are more likely to be sampled.
    """

    def __init__(self) -> None:
        self.storage: list[dict[str, Any]] = []

    def put(self, slice: dict[str, Any]) -> None:
        """Add a new slice to the buffer.

        The buffer discards the oldest slices if the buffer gets too large to prevent memory leaks,
        and so that the latest model can gradually explore further from the initial policy.
        """
        self.storage.append(slice)
        if len(self.storage) > MAX_BUFFER_SIZE:
            self.storage = self.storage[-MAX_BUFFER_SIZE:]

    def sample_groups(self, n: int) -> list[dict[str, Any]]:
        """Sample n scored trajectory slices."""
        # The probability of sampling a slice is proportional to its policy version.
        if len(self.storage) == 0:
            return []
        total = sum(group["policy_version"] for group in self.storage)
        probs = [group["policy_version"] / total for group in self.storage]
        # Sample with replacement without exceeding the buffer's size.
        n = min(n, self.size())
        chosen = np.random.choice(self.size(), size=n, p=probs, replace=True)
        return [self.storage[i] for i in chosen]

    def size(self) -> int:
        return len(self.storage)
