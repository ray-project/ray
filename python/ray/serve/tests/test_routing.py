import os
import tempfile

from ray.serve.kv_store_service import (InMemoryKVStore, RayInternalKVStore,
                                        SQLiteKVStore)


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


def test_sqlite_kv():
    _, path = tempfile.mkstemp()

    # Test get
    kv = SQLiteKVStore("routing_table", db_path=path)
    kv.put("/api", "api-endpoint")
    assert kv.get("/api") == "api-endpoint"
    assert kv.get("not-exist") is None

    # Test namespace
    kv2 = SQLiteKVStore("other_table", db_path=path)
    kv2.put("/api", "api-endpoint-two")
    assert kv2.get("/api") == "api-endpoint-two"

    # Test as dict
    assert kv.as_dict() == {"/api": "api-endpoint"}

    # Test override
    kv.put("/api", "api-new")
    assert kv.get("/api") == "api-new"

    os.remove(path)
