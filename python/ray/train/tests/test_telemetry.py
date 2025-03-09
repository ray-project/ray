import sys

import pytest

import ray
import ray._private.usage.usage_lib as ray_usage_lib
from ray._private.test_utils import check_library_usage_telemetry, TelemetryCallsite


@pytest.fixture
def reset_usage_lib():
    yield
    ray.shutdown()
    ray_usage_lib.reset_global_state()


@pytest.mark.skip(reason="Usage currently marked on import.")
@pytest.mark.parametrize("callsite", list(TelemetryCallsite))
def test_not_used_on_import(reset_usage_lib, callsite: TelemetryCallsite):
    def _import_ray_train():
        from ray import train  # noqa: F401

    check_library_usage_telemetry(
        _import_ray_train, callsite=callsite, expected_library_usages=[set(), {"core"}]
    )


@pytest.mark.parametrize("callsite", list(TelemetryCallsite))
def test_used_on_import(reset_usage_lib, callsite: TelemetryCallsite):
    def _use_ray_train():
        # TODO(edoakes): this test currently fails if we don't call `ray.init()`
        # prior to the import. This is a bug.
        if callsite == TelemetryCallsite.DRIVER:
            ray.init()

        from ray import train  # noqa: F401

    check_library_usage_telemetry(
        _use_ray_train,
        callsite=callsite,
        expected_library_usages=[{"train"}, {"core", "train"}],
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
