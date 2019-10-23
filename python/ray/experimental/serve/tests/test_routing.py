from ray.experimental.serve.kv_store_service import (InMemoryKVStore,
                                                     RayInternalKVStore)


def test_default_in_memory_kv():
    kv = InMemoryKVStore("")
    kv.put("1", 2)
    assert kv.get("1") == 2
    kv.put("1", 3)
    assert kv.get("1") == 3
    assert kv.as_dict() == {"1": 3}


def test_ray_interal_kv(ray_instance):
    kv = RayInternalKVStore("")
    kv.put("1", 2)
    assert kv.get("1") == 2
    kv.put("1", 3)
    assert kv.get("1") == 3
    assert kv.as_dict() == {"1": 3}

    kv = RayInternalKVStore("othernamespace")
    kv.put("1", 2)
    assert kv.get("1") == 2
    kv.put("1", 3)
    assert kv.get("1") == 3
    assert kv.as_dict() == {"1": 3}
