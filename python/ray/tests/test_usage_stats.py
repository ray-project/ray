import pytest
import sys
import ray

import ray._private.usage.usage_lib as ray_usage_lib


def test_usage_lib_cluster_metadata_generation(shutdown_only):
    ray.init(num_cpus=0)
    """
    Test metadata stored is equivalent to `_generate_cluster_metadata`.
    """
    meta = ray_usage_lib._generate_cluster_metadata()
    cluster_metadata = ray_usage_lib.get_cluster_metadata(
        ray.experimental.internal_kv.internal_kv_get_gcs_client(), num_retries=20
    )
    # Session id is random.
    meta.pop("session_id")
    cluster_metadata.pop("session_id")
    assert meta == cluster_metadata

    """
    Make sure put & get works properly.
    """
    cluster_metadata = ray_usage_lib.put_cluster_metadata(
        ray.experimental.internal_kv.internal_kv_get_gcs_client(), num_retries=20
    )
    assert cluster_metadata == ray_usage_lib.get_cluster_metadata(
        ray.experimental.internal_kv.internal_kv_get_gcs_client(), num_retries=20
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
