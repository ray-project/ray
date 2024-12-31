import importlib

import pytest

import ray.train
from ray.train import FailureConfig, RunConfig, ScalingConfig


@pytest.mark.parametrize(
    "operation, raise_error",
    [
        (lambda: FailureConfig(fail_fast=True), True),
        (lambda: RunConfig(verbose=0), True),
        (lambda: FailureConfig(), False),
        (lambda: RunConfig(), False),
        (lambda: ScalingConfig(trainer_resources={"CPU": 1}), True),
        (lambda: ScalingConfig(), False),
    ],
)
def test_api_configs(operation, raise_error):
    if raise_error:
        with pytest.raises(NotImplementedError):
            operation()
    else:
        try:
            operation()
        except Exception as e:
            pytest.fail(f"Default Operation raised an exception: {e}")


def test_scaling_config_total_resources():
    """Test the patched scaling config total resources calculation."""
    num_workers = 2
    num_cpus_per_worker = 1
    num_gpus_per_worker = 1

    scaling_config = ScalingConfig(
        num_workers=num_workers,
        use_gpu=True,
        resources_per_worker={"CPU": num_cpus_per_worker, "GPU": num_gpus_per_worker},
    )
    scaling_config.total_resources == {
        "CPU": num_workers * num_cpus_per_worker,
        "GPU": num_workers * num_gpus_per_worker,
    }


@pytest.mark.parametrize("env_v2_enabled", [True, False])
def test_train_v2_import(monkeypatch, env_v2_enabled):
    monkeypatch.setenv("RAY_TRAIN_V2_ENABLED", str(int(env_v2_enabled)))

    # Load from the public `ray.train` module
    # isort: off
    importlib.reload(ray.train)
    from ray.train import FailureConfig, Result, RunConfig

    # isort: on

    # Import from the absolute module paths as references
    from ray.train.v2.api.config import ScalingConfig as ScalingConfigV2
    from ray.train.v2.api.config import FailureConfig as FailureConfigV2
    from ray.train.v2.api.config import RunConfig as RunConfigV2
    from ray.train.v2.api.result import Result as ResultV2

    if env_v2_enabled:
        assert ScalingConfig is ScalingConfigV2
        assert RunConfig is RunConfigV2
        assert FailureConfig is FailureConfigV2
        assert Result is ResultV2
    else:
        assert ScalingConfig is not ScalingConfigV2
        assert RunConfig is not RunConfigV2
        assert FailureConfig is not FailureConfigV2
        assert Result is not ResultV2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
