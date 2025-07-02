from ray.train.v2._internal.execution.failure_handling import (
    DefaultFailurePolicy,
    FailurePolicy,
)
from ray.train.v2.api.config import FailureConfig


def create_failure_policy(failure_config: FailureConfig) -> FailurePolicy:
    """Create a failure policy from the given failure config.

    Defaults to the `DefaultFailurePolicy` implementation.
    """
    return DefaultFailurePolicy(failure_config=failure_config)
