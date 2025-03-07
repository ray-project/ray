import sys
from typing import Set

import pytest

import ray
import ray._private.usage.usage_lib as ray_usage_lib
from ray import serve


def _get_library_usages() -> Set[str]:
    return set(
        ray_usage_lib.get_library_usages_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
    )


@pytest.fixture
def reset_usage_lib(ray_shutdown):
    yield
    ray.shutdown()
    ray_usage_lib.reset_global_state()


@pytest.mark.parametrize("callsite", ["driver", "actor", "task"])
def test_serve_start(reset_usage_lib, callsite: str):
    assert len(_get_library_usages()) == 0
    if callsite == "driver":
        serve.start()
    elif callsite == "actor":

        @ray.remote
        class A:
            def __init__(self):
                serve.start()

        a = A.remote()
        ray.get(a.__ray_ready__.remote())

    elif callsite == "task":

        @ray.remote
        def f():
            serve.start()

        ray.get(f.remote())

    assert _get_library_usages() == {"core", "serve"}


@pytest.mark.parametrize("callsite", ["driver", "actor", "task"])
def test_serve_run(reset_usage_lib, callsite: str):
    assert len(_get_library_usages()) == 0

    @serve.deployment
    class D:
        def __call__(self):
            pass

    app = D.bind()

    if callsite == "driver":
        serve.run(app)
    elif callsite == "actor":

        @ray.remote
        class A:
            def __init__(self):
                serve.run(app)

        a = A.remote()
        ray.get(a.__ray_ready__.remote())

    elif callsite == "task":

        @ray.remote
        def f():
            serve.run(app)

        ray.get(f.remote())

    assert _get_library_usages() == {"core", "serve"}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
