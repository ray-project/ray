import gymnasium as gym

from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import AlgorithmConfigDict


class MemoryLeakingPolicy(RandomPolicy):
    """A Policy that leaks very little memory.

    Useful for proving that our memory-leak tests can catch the
    slightest leaks.
    """

    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        config: AlgorithmConfigDict,
    ):
        super().__init__(observation_space, action_space, config)
        self._leakage_size = config.get("leakage_size", "small")
        self._leak = []

    @override(RandomPolicy)
    def compute_actions(self, *args, **kwargs):
        # Leak.
        if self._leakage_size == "small":
            self._leak.append(1.5)
        else:
            self._leak.append([1.5] * 100)
        return super().compute_actions(*args, **kwargs)

    @override(RandomPolicy)
    def compute_actions_from_input_dict(self, *args, **kwargs):
        # Leak.
        if self._leakage_size == "small":
            self._leak.append(1)
        else:
            self._leak.append([1] * 100)
        return super().compute_actions_from_input_dict(*args, **kwargs)

    @override(RandomPolicy)
    def learn_on_batch(self, samples):
        # Leak.
        if self._leakage_size == "small":
            self._leak.append(False)
        else:
            self._leak.append([False] * 100)
        return super().learn_on_batch(samples)

    @override(RandomPolicy)
    def compute_log_likelihoods(self, *args, **kwargs):
        # Leak.
        if self._leakage_size == "small":
            self._leak.append("test")
        else:
            self._leak.append(["test"] * 100)
        return super().compute_log_likelihoods(*args, **kwargs)
