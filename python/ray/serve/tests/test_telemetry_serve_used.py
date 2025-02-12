import sys

import pytest

import ray
import ray._private.usage.usage_lib as ray_usage_lib
from ray import serve


def _is_serve_used() -> bool:
    return "serve" in ray_usage_lib.get_library_usages_to_report(
        ray.experimental.internal_kv.internal_kv_get_gcs_client()
    )


@pytest.fixture
def reset_usage_lib(ray_shutdown):
    ray_usage_lib.reset_global_state()


def test_serve_start(reset_usage_lib):
    assert not _is_serve_used()
    serve.start()
    assert _is_serve_used()


def test_serve_run(reset_usage_lib):
    assert not _is_serve_used()

    @serve.deployment
    class A:
        def __call__(self):
            pass

    serve.run(A.bind())

    assert _is_serve_used()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
