import os

import pytest
from ray.serve.storage.kv_store import (RayInternalKVStore, RayLocalKVStore,
                                        RayS3KVStore)


def test_ray_internal_kv(serve_instance):  # noqa: F811
    with pytest.raises(TypeError):
        RayInternalKVStore(namespace=1)
        RayInternalKVStore(namespace=b"")

    kv = RayInternalKVStore()

    with pytest.raises(TypeError):
        kv.put(1, b"1")
    with pytest.raises(TypeError):
        kv.put("1", 1)
    with pytest.raises(TypeError):
        kv.put("1", "1")

    kv.put("1", b"2")
    assert kv.get("1") == b"2"
    kv.put("2", b"4")
    assert kv.get("2") == b"4"
    kv.put("1", b"3")
    assert kv.get("1") == b"3"
    assert kv.get("2") == b"4"


def test_ray_internal_kv_collisions(serve_instance):  # noqa: F811
    kv1 = RayInternalKVStore()
    kv1.put("1", b"1")
    assert kv1.get("1") == b"1"

    kv2 = RayInternalKVStore("namespace")

    assert kv2.get("1") is None

    kv2.put("1", b"-1")
    assert kv2.get("1") == b"-1"
    assert kv1.get("1") == b"1"


def _test_operations(kv_store):
    # Trival get & put
    kv_store.put("1", b"1")
    assert kv_store.get("1") == b"1"
    kv_store.put("2", b"2")
    assert kv_store.get("1") == b"1"
    assert kv_store.get("2") == b"2"

    # Overwrite same key
    kv_store.put("1", b"-1")
    assert kv_store.get("1") == b"-1"

    # Get non-existing key
    assert kv_store.get("3") is None

    # Delete existing key
    kv_store.delete("1")
    kv_store.delete("2")
    assert kv_store.get("1") is None
    assert kv_store.get("2") is None

    # Delete non-existing key
    kv_store.delete("3")


def test_external_kv_local_disk():
    kv_store = RayLocalKVStore("namespace", "test_kv_store.db")

    _test_operations(kv_store)


@pytest.mark.skip(reason="Need to figure out credentials for testing")
def test_external_kv_aws_s3():
    kv_store = RayS3KVStore(
        "namespace",
        bucket="jiao-test",
        s3_path="/checkpoint",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", None),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", None),
        aws_session_token=os.environ.get("AWS_SESSION_TOKEN", None),
    )

    _test_operations(kv_store)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
