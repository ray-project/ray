import ray
from ray import ray_constants
from ray._raylet import connect_to_gcs


def run_kv_test(gcs_client):
    assert gcs_client.kv_put(b"TEST_KEY", b"TEST_VAL", True)
    assert b"TEST_VAL" == gcs_client.kv_get(b"TEST_KEY")
    assert not gcs_client.kv_exists(b"TEST_KEY2")
    gcs_client.kv_del(b"TEST_KEY")
    assert not gcs_client.kv_exists(b"TEST_KEY")
    assert gcs_client.kv_put(b"TEST_KEY", b"TEST_VAL", False)
    assert not gcs_client.kv_put(b"TEST_KEY", b"TEST_VAL2", False)
    assert gcs_client.kv_get(b"TEST_KEY") == b"TEST_VAL"
    assert not gcs_client.kv_put(b"TEST_KEY", b"TEST_VAL2", True)
    assert gcs_client.kv_get(b"TEST_KEY") == b"TEST_VAL2"
    gcs_client.kv_del(b"TEST_KEY")

    assert gcs_client.kv_get(b"TEST_KEY") is None
    assert gcs_client.kv_put(b"TEST_KEY_1", b"TEST_VAL_1", True)
    assert gcs_client.kv_put(b"TEST_KEY_2", b"TEST_VAL_2", True)
    assert gcs_client.kv_put(b"TEST_KEY_3", b"TEST_VAL_3", True)
    assert gcs_client.kv_put(b"TEST_KEY_4", b"TEST_VAL_4", True)
    keys = set(gcs_client.kv_keys(b"TEST_KEY_"))
    assert keys == {b"TEST_KEY_1", b"TEST_KEY_2", b"TEST_KEY_3", b"TEST_KEY_4"}


def test_gcs_client_core_worker(shutdown_only):
    ray.init()
    gcs_client = ray.worker.global_worker.core_worker.get_gcs_client()
    run_kv_test(gcs_client)


def test_gcs_client_address(ray_start_cluster_head):
    cluster = ray_start_cluster_head
    ip, port = cluster.address.split(":")
    password = ray_constants.REDIS_DEFAULT_PASSWORD
    gcs_client = connect_to_gcs(ip, int(port), password)
    run_kv_test(gcs_client)
