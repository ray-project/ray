from ray.train import FailureConfig
from ray.train.v2._internal.execution.failure_handling import (
    DefaultFailurePolicy,
    FailurePolicy,
)


def create_failure_policy(failure_config: FailureConfig) -> FailurePolicy:
    """Create a failure policy from the given failure config.

    Defaults to the `DefaultFailurePolicy` implementation.
    """
    return DefaultFailurePolicy(failure_config=failure_config)
