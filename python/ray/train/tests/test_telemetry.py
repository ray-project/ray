import sys

import pytest

import ray
import ray._private.usage.usage_lib as ray_usage_lib
from ray._common.test_utils import TelemetryCallsite, check_library_usage_telemetry
from ray.train.data_parallel_trainer import DataParallelTrainer


@pytest.fixture
def reset_usage_lib():
    yield
    ray.shutdown()
    ray_usage_lib.reset_global_state()


@pytest.mark.parametrize("callsite", list(TelemetryCallsite))
def test_not_used_on_import(reset_usage_lib, callsite: TelemetryCallsite):
    def _import_ray_train():
        from ray import train  # noqa: F401

    check_library_usage_telemetry(
        _import_ray_train, callsite=callsite, expected_library_usages=[set(), {"core"}]
    )


@pytest.mark.parametrize("callsite", list(TelemetryCallsite))
def test_used_on_train_fit(reset_usage_lib, callsite: TelemetryCallsite):
    def _call_train_fit():
        def train_fn():
            pass

        trainer = DataParallelTrainer(train_fn)
        trainer.fit()

    check_library_usage_telemetry(
        _call_train_fit,
        callsite=callsite,
        expected_library_usages=[{"train", "tune"}, {"core", "train", "tune"}],
        expected_extra_usage_tags={
            "air_entrypoint": "Trainer.fit",
            "air_storage_configuration": "local",
            "air_trainer": "Custom",
        },
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
