import ray


def test_basic_kv(shutdown_only):
    ray.init()
    gcs_client = ray.worker.global_worker.core_worker.get_gcs_client()
    gcs_client.kv_put(b"TEST_KEY", b"TEST_VAL")
    assert b"TEST_VAL" == gcs_client.kv_get(b"TEST_KEY")
    assert not gcs_client.kv_exists(b"TEST_KEY2")
    gcs_client.kv_del(b"TEST_KEY")
    assert not gcs_client.kv_exists(b"TEST_KEY")
    assert gcs_client.kv_get(b"TEST_KEY") is None
