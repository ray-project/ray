import pytest


def test_scaling_config():
    """The `ScalingConfig` should be patched to accept min/max workers."""
    # isort: off
    from ray.train import ScalingConfig
    from ray.anyscale.train.api.config import ScalingConfig as AnyscaleScalingConfig

    # isort: on

    assert ScalingConfig is AnyscaleScalingConfig


def test_scaling_policy():
    """The `create_scaling_policy` function should be patched to return
    an elastic scaling policy if a min/max workers is set."""
    from ray.anyscale.train._internal.execution.scaling_policy.elastic import (
        ElasticScalingPolicy,
    )
    from ray.train import ScalingConfig
    from ray.train.v2._internal.execution.scaling_policy import (
        FixedScalingPolicy,
        create_scaling_policy,
    )
    from ray.train.v2.api.config import ScalingConfig as RayScalingConfig

    fixed_policy = create_scaling_policy(ScalingConfig(num_workers=2))
    assert isinstance(fixed_policy, FixedScalingPolicy)

    elastic_policy = create_scaling_policy(ScalingConfig(num_workers=(2, 4)))
    assert isinstance(elastic_policy, ElasticScalingPolicy)

    fixed_policy = create_scaling_policy(RayScalingConfig(num_workers=2))


if __name__ == "__main__":
    pytest.main(["-v", "-x", __file__])
