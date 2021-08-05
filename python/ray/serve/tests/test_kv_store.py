import pytest
import os

from ray.serve.kv_store import RayInternalKVStore, RayExternalKVStore


def test_ray_internal_kv(serve_instance):
    with pytest.raises(TypeError):
        RayInternalKVStore(namespace=1)
        RayInternalKVStore(namespace=b"")

    kv = RayInternalKVStore()

    with pytest.raises(TypeError):
        kv.put(1, b"1")
    with pytest.raises(TypeError):
        kv.put("1", 1)

    kv.put("1", b"2")
    assert kv.get("1") == b"2"
    kv.put("2", b"4")
    assert kv.get("2") == b"4"
    kv.put("1", b"3")
    assert kv.get("1") == b"3"
    assert kv.get("2") == b"4"

    # Test that the value can be a string.
    kv.put("10", "value")
    assert kv.get("10") == b"value"


def test_ray_internal_kv_collisions(serve_instance):
    kv1 = RayInternalKVStore()
    kv1.put("1", b"1")
    assert kv1.get("1") == b"1"

    kv2 = RayInternalKVStore("namespace")

    assert kv2.get("1") is None

    kv2.put("1", b"-1")
    assert kv2.get("1") == b"-1"
    assert kv1.get("1") == b"1"

def test_ray_serve_external_kv_local_disk():
    kv_store = RayExternalKVStore("namespace")
    kv_store.put("1", b"1")
    assert kv_store.get("1") == b"1"

    kv_store.put("2", b"2")
    assert kv_store.get("1") == b"1"
    assert kv_store.get("2") == b"2"

    if os.path.exists("/tmp/ray_serve_checkpoint_key.txt"):
        os.remove("/tmp/ray_serve_checkpoint_key.txt")
    if os.path.exists("/tmp/ray_serve_checkpoint_val.txt"):
        os.remove("/tmp/ray_serve_checkpoint_val.txt")

def test_ray_serve_external_kv_aws_s3():
    kv_store = RayExternalKVStore(
        "namespace",
        bucket="test-jiao",
        s3_path="/ray_serve_checkpoint",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", None),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", None),
        local_mode=False,
    )
    kv_store.put("1", b"1")
    assert kv_store.get("1") == b"1"

    kv_store.put("2", b"2")
    assert kv_store.get("1") == b"1"
    assert kv_store.get("2") == b"2"



if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
