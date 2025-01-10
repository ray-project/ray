from ray.train import FailureConfig
from ray.train.v2._internal.execution.failure_handling import FailurePolicy


def create_failure_policy(failure_config: FailureConfig) -> FailurePolicy:
    from ray.anyscale.train._internal.execution.failure_handling.anyscale_failure_policy import (  # noqa: E501
        AnyscaleFailurePolicy,
    )

    return AnyscaleFailurePolicy(failure_config=failure_config)
