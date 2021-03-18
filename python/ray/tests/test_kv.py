import ray
from ray import ray_constants
from ray._raylet import connect_to_gcs


def test_gcs_client_core_worker(shutdown_only):
    ray.init()
    gcs_client = ray.worker.global_worker.core_worker.get_gcs_client()
    gcs_client.kv_put(b"TEST_KEY", b"TEST_VAL")
    assert b"TEST_VAL" == gcs_client.kv_get(b"TEST_KEY")
    assert not gcs_client.kv_exists(b"TEST_KEY2")
    gcs_client.kv_del(b"TEST_KEY")
    assert not gcs_client.kv_exists(b"TEST_KEY")
    assert gcs_client.kv_get(b"TEST_KEY") is None
    gcs_client.kv_put(b"TEST_KEY_1", b"TEST_VAL_1")
    gcs_client.kv_put(b"TEST_KEY_2", b"TEST_VAL_2")
    gcs_client.kv_put(b"TEST_KEY_3", b"TEST_VAL_3")
    gcs_client.kv_put(b"TEST_KEY_4", b"TEST_VAL_4")
    keys = gcs_client.kv_keys(b"TEST_KEY_")
    assert keys == ["TEST_KEY_1", "TEST_KEY_2", "TEST_KEY_3", "TEST_KEY_4"]


def test_gcs_client_address(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    ip, port = cluster.address.split(":")
    password = ray_constants.REDIS_DEFAULT_PASSWORD
    gcs_client = connect_to_gcs(ip, int(port), password)
    gcs_client.kv_put(b"TEST_KEY", b"TEST_VAL")
    assert b"TEST_VAL" == gcs_client.kv_get(b"TEST_KEY")
    assert not gcs_client.kv_exists(b"TEST_KEY2")
    gcs_client.kv_del(b"TEST_KEY")
    assert not gcs_client.kv_exists(b"TEST_KEY")
    assert gcs_client.kv_get(b"TEST_KEY") is None
    gcs_client.kv_put(b"TEST_KEY_1", b"TEST_VAL_1")
    gcs_client.kv_put(b"TEST_KEY_2", b"TEST_VAL_2")
    gcs_client.kv_put(b"TEST_KEY_3", b"TEST_VAL_3")
    gcs_client.kv_put(b"TEST_KEY_4", b"TEST_VAL_4")
    keys = gcs_client.kv_keys(b"TEST_KEY_")
    assert keys == ["TEST_KEY_1", "TEST_KEY_2", "TEST_KEY_3", "TEST_KEY_4"]
