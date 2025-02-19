import pytest

import ray.train


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


@pytest.mark.parametrize(
    "num_workers, excluded_resources", [(2, {"CPU": 2}), [(2, 4), {}]]
)
def test_dataset_setup_callback_patch(num_workers, excluded_resources):
    """Ray Train resources should be excluded from the total resources unless
    an elastic number of workers is enabled."""
    from ray.train.v2._internal.callbacks import DatasetsSetupCallback  # isort: skip
    from ray.anyscale.train._internal.callbacks.datasets import (
        AnyscaleDatasetsSetupCallback,
    )

    assert DatasetsSetupCallback is AnyscaleDatasetsSetupCallback

    scaling_config = ray.train.ScalingConfig(
        num_workers=num_workers, resources_per_worker={"CPU": 1}
    )
    callback = DatasetsSetupCallback(
        datasets={}, data_config=None, scaling_config=scaling_config
    )
    assert callback.get_train_total_resources(scaling_config) == excluded_resources


if __name__ == "__main__":
    pytest.main(["-v", "-x", __file__])
