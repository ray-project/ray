from ray.anyscale.train.api.config import ScalingConfig as AnyscaleScalingConfig
from ray.train.v2._internal.execution.scaling_policy import (
    FixedScalingPolicy,
    ScalingPolicy,
)
from ray.train.v2.api.config import ScalingConfig as RayScalingConfig


def create_scaling_policy(scaling_config: RayScalingConfig) -> ScalingPolicy:
    """Create a scaling policy from the given scaling config.

    If elasticity is enabled by passing num_workers=(min_workers, max_workers),
    return an `ElasticScalingPolicy` instance.
    Otherwise, return the default `FixedScalingPolicy`.
    """
    from ray.anyscale.train._internal.execution.scaling_policy import (
        ElasticScalingPolicy,
    )
    from ray.train.v2._internal.execution.scaling_policy.factory import (
        create_scaling_policy as create_scaling_policy_ray,
    )

    # Fallback to the Ray implementation.
    # This is useful for keeping Ray tests separate from runtime tests.
    if not isinstance(scaling_config, AnyscaleScalingConfig):
        return create_scaling_policy_ray(scaling_config)

    return (
        ElasticScalingPolicy(scaling_config=scaling_config)
        if scaling_config.elasticity_enabled
        else FixedScalingPolicy(scaling_config=scaling_config)
    )
