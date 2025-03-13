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
    def _import_rllib():
        from ray import rllib  # noqa: F401

    check_library_usage_telemetry(
        _import_rllib, callsite=callsite, expected_library_usages=[set(), {"core"}]
    )


@pytest.mark.parametrize("callsite", list(TelemetryCallsite))
def test_used_on_import(reset_usage_lib, callsite: TelemetryCallsite):
    def _use_rllib():
        # TODO(edoakes): this test currently fails if we don't call `ray.init()`
        # prior to the import. This is a bug.
        if callsite == TelemetryCallsite.DRIVER:
            ray.init()

        from ray import rllib  # noqa: F401

    check_library_usage_telemetry(
        _use_rllib,
        callsite=callsite,
        # TODO(edoakes): train shouldn't be marked used every time rllib is used.
        expected_library_usages=[{"rllib", "train"}, {"core", "rllib", "train"}],
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
