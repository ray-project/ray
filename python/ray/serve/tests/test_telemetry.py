import sys

import pytest

import ray
import ray._private.usage.usage_lib as ray_usage_lib
from ray._private.test_utils import check_library_usage_telemetry, TelemetryCallsite

from ray import serve


@pytest.fixture
def reset_usage_lib(ray_shutdown):
    yield
    ray.shutdown()
    ray_usage_lib.reset_global_state()


@pytest.mark.parametrize("callsite", list(TelemetryCallsite))
def test_not_used_on_import(reset_usage_lib, callsite: TelemetryCallsite):
    def _import_ray_serve():
        from ray import serve  # noqa: F401

    check_library_usage_telemetry(
        _import_ray_serve, callsite=callsite, expected_library_usages=[set(), {"core"}]
    )


@pytest.mark.parametrize("callsite", list(TelemetryCallsite))
def test_used_on_serve_start(reset_usage_lib, callsite: TelemetryCallsite):
    def _call_serve_start():
        serve.start()

    check_library_usage_telemetry(
        _call_serve_start,
        callsite=callsite,
        expected_library_usages=[{"serve"}, {"core", "serve"}],
    )


@pytest.mark.parametrize("callsite", list(TelemetryCallsite))
def test_used_on_serve_run(reset_usage_lib, callsite: TelemetryCallsite):
    @serve.deployment
    class D:
        def __call__(self):
            pass

    app = D.bind()

    def _call_serve_run():
        serve.run(app)

    check_library_usage_telemetry(
        _call_serve_run,
        callsite=callsite,
        expected_library_usages=[{"serve"}, {"core", "serve"}],
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
