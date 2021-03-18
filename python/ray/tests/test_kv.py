import ray
from ray import ray_constants
from ray._raylet import connect_to_gcs
import importlib
from ray.experimental import internal_kv


def run_kv_test(gcs_client):
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
    keys = set(gcs_client.kv_keys(b"TEST_KEY_"))
    assert keys == set(
        [b"TEST_KEY_1", b"TEST_KEY_2", b"TEST_KEY_3", b"TEST_KEY_4"])


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


def run_internal_kv_test():
    importlib.reload(internal_kv)
    internal_kv._internal_kv_initialized()
    internal_kv._internal_kv_put(b"TEST_KEY", b"TEST_VAL")
    assert b"TEST_VAL" == internal_kv._internal_kv_get(b"TEST_KEY")
    assert not internal_kv._internal_kv_exists(b"TEST_KEY2")
    internal_kv._internal_kv_del(b"TEST_KEY")
    assert not internal_kv._internal_kv_exists(b"TEST_KEY")
    assert internal_kv._internal_kv_get(b"TEST_KEY") is None
    internal_kv._internal_kv_put(b"TEST_KEY_1", b"TEST_VAL_1")
    internal_kv._internal_kv_put(b"TEST_KEY_2", b"TEST_VAL_2")
    internal_kv._internal_kv_put(b"TEST_KEY_3", b"TEST_VAL_3")
    internal_kv._internal_kv_put(b"TEST_KEY_4", b"TEST_VAL_4")
    keys = set(internal_kv._internal_kv_list(b"TEST_KEY_"))
    assert keys == set(
        [b"TEST_KEY_1", b"TEST_KEY_2", b"TEST_KEY_3", b"TEST_KEY_4"])


def test_internal_kv_deprecate(ray_start_cluster_head):
    import os
    if "USE_GSE_KV" in os.environ:
        os.environ.pop("USE_GSE_KV")
    run_internal_kv_test()


def test_internal_kv_gcs(ray_start_cluster_head):
    import os
    if "USE_GSE_KV" not in os.environ:
        os.environ["USE_GSE_KV"] = "1"
    run_internal_kv_test()
    os.environ.pop("USE_GSE_KV")
