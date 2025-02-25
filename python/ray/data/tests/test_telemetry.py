import sys

import pytest

import ray
import ray._private.usage.usage_lib as ray_usage_lib


def _is_data_used() -> bool:
    return "dataset" in ray_usage_lib.get_library_usages_to_report(
        ray.experimental.internal_kv.internal_kv_get_gcs_client()
    )


def test_data_range():
    assert not _is_data_used()
    ray.data.range(10)
    assert _is_data_used()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
