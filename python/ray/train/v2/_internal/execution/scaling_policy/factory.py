from ray.train.v2._internal.execution.scaling_policy import (
    ElasticScalingPolicy,
    FixedScalingPolicy,
    ScalingPolicy,
)
from ray.train.v2.api.config import ScalingConfig


def create_scaling_policy(scaling_config: ScalingConfig) -> ScalingPolicy:
    """Create a scaling policy from the given scaling config.

    Defaults to the `FixedScalingPolicy` implementation.
    """
    if scaling_config.elasticity_enabled:
        return ElasticScalingPolicy(scaling_config=scaling_config)
    return FixedScalingPolicy(scaling_config=scaling_config)
