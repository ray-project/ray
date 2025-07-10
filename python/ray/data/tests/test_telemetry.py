import sys

import pytest

import ray
import ray._private.usage.usage_lib as ray_usage_lib
from ray import data
from ray._common.test_utils import TelemetryCallsite, check_library_usage_telemetry


@pytest.fixture
def reset_usage_lib():
    yield
    ray.shutdown()
    ray_usage_lib.reset_global_state()


@pytest.mark.parametrize("callsite", list(TelemetryCallsite))
def test_not_used_on_import(reset_usage_lib, callsite: TelemetryCallsite):
    def _import_ray_data():
        from ray import data  # noqa: F401

    check_library_usage_telemetry(
        _import_ray_data, callsite=callsite, expected_library_usages=[set(), {"core"}]
    )


@pytest.mark.parametrize("callsite", list(TelemetryCallsite))
def test_used_on_data_range(reset_usage_lib, callsite: TelemetryCallsite):
    def _call_data_range():
        data.range(10)

    check_library_usage_telemetry(
        _call_data_range,
        callsite=callsite,
        expected_library_usages=[{"core", "dataset"}],
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
