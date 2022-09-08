import pytest

from ray.serve._private.storage.kv_store import RayInternalKVStore


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
