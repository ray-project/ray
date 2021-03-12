import ray


def test_basic_kv(shutdown_only):
    ray.init()
    core_worker = ray.worker.global_worker.core_worker
    core_worker.kv_put(b"TEST_KEY", b"TEST_VAL")
    assert b"TEST_VAL" == core_worker.kv_get(b"TEST_KEY")
    assert not core_worker.kv_exists(b"TEST_KEY2")
    core_worker.kv_del(b"TEST_KEY")
    assert not core_worker.kv_exists(b"TEST_KEY")
    assert core_worker.kv_get(b"TEST_KEY") is None
