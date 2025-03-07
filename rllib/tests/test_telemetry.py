import sys
from typing import Set

import pytest

import ray
import ray._private.usage.usage_lib as ray_usage_lib


@pytest.fixture
def reset_usage_lib():
    yield
    ray.shutdown()
    ray_usage_lib.reset_global_state()


def _get_library_usages() -> Set[str]:
    return set(
        ray_usage_lib.get_library_usages_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
    )


@pytest.mark.parametrize("callsite", ["driver", "actor", "task"])
def test_import(reset_usage_lib, callsite: str):
    assert len(_get_library_usages()) == 0

    if callsite == "driver":
        # NOTE(edoakes): this test currently fails if we don't call `ray.init()`
        # prior to the import. This is a bug in the telemetry collection.
        ray.init()
        from ray import rllib
    elif callsite == "actor":

        @ray.remote
        class A:
            def __init__(self):
                from ray import rllib

        a = A.remote()
        ray.get(a.__ray_ready__.remote())

    elif callsite == "task":

        @ray.remote
        def f():
            from ray import rllib

        ray.get(f.remote())

    assert _get_library_usages() in [{"rllib", "train"}, {"core", "rllib", "train"}]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
