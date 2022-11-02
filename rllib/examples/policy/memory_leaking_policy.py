from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.utils.annotations import override


class MemoryLeakingPolicy(RandomPolicy):
    """A Policy that leaks very little memory.

    Useful for proving that our memory-leak tests can catch the
    slightest leaks.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._leak = []

    @override(RandomPolicy)
    def compute_actions(self, *args, **kwargs):
        # Leak.
        self._leak.append(1.5)
        return super().compute_actions(*args, **kwargs)

    @override(RandomPolicy)
    def compute_actions_from_input_dict(self, *args, **kwargs):
        # Leak.
        self._leak.append(1)
        return super().compute_actions_from_input_dict(*args, **kwargs)

    @override(RandomPolicy)
    def learn_on_batch(self, samples):
        # Leak.
        self._leak.append(False)
        return super().learn_on_batch(samples)

    @override(RandomPolicy)
    def compute_log_likelihoods(self, *args, **kwargs):
        # Leak.
        self._leak.append("test")
        return super().compute_log_likelihoods(*args, **kwargs)
