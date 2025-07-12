import sys

import pytest

import ray
import ray._private.usage.usage_lib as ray_usage_lib
from ray import tune
from ray._common.test_utils import TelemetryCallsite, check_library_usage_telemetry


@pytest.fixture
def reset_usage_lib():
    yield
    ray.shutdown()
    ray_usage_lib.reset_global_state()


@pytest.mark.parametrize("callsite", list(TelemetryCallsite))
def test_not_used_on_import(reset_usage_lib, callsite: TelemetryCallsite):
    if callsite in {TelemetryCallsite.ACTOR, TelemetryCallsite.TASK}:
        pytest.skip("TODO: train usage is exported when importing in an actor or task.")

    def _import_ray_tune():
        from ray import tune  # noqa: F401

    check_library_usage_telemetry(
        _import_ray_tune, callsite=callsite, expected_library_usages=[set(), {"core"}]
    )


@pytest.mark.parametrize("callsite", list(TelemetryCallsite))
def test_used_on_tuner_fit(reset_usage_lib, callsite: TelemetryCallsite):
    def _call_tuner_fit():
        def objective(*args):
            pass

        tuner = tune.Tuner(objective)
        tuner.fit()

    check_library_usage_telemetry(
        _call_tuner_fit,
        callsite=callsite,
        expected_library_usages=[{"tune"}, {"core", "tune"}],
        expected_extra_usage_tags={
            "tune_scheduler": "FIFOScheduler",
            "tune_searcher": "BasicVariantGenerator",
            "air_entrypoint": "Tuner.fit",
            "air_storage_configuration": "local",
        },
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
