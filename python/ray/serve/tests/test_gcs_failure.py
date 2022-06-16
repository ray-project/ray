import os
import sys

import grpc
import pytest
import requests

import ray
import ray.serve as serve
from ray._private.test_utils import wait_for_condition
from ray.serve.storage.kv_store import KVStoreError, RayInternalKVStore
from ray.tests.conftest import external_redis  # noqa: F401


@pytest.fixture(scope="function")
def serve_ha(external_redis, monkeypatch):  # noqa: F811
    monkeypatch.setenv("RAY_SERVE_KV_TIMEOUT_S", "1")
    address_info = ray.init(
        num_cpus=36,
        namespace="default_test_namespace",
        _metrics_export_port=9999,
        _system_config={"metrics_report_interval_ms": 1000, "task_retry_delay_ms": 50},
    )
    yield (address_info, serve.start(detached=True))
    ray.shutdown()


def test_ray_internal_kv_timeout(serve_ha):  # noqa: F811
    # Firstly make sure it's working
    kv1 = RayInternalKVStore()
    kv1.put("1", b"1")
    assert kv1.get("1") == b"1"

    # Kill the GCS
    ray.worker._global_node.kill_gcs_server()

    with pytest.raises(KVStoreError) as e:
        kv1.put("2", b"2")
    assert e.value.args[0] in (
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    )


@pytest.mark.parametrize("use_handle", [False, True])
def test_controller_gcs_failure(serve_ha, use_handle):  # noqa: F811
    @serve.deployment(version="1")
    def d(*args):
        return f"1|{os.getpid()}"

    def call():
        if use_handle:
            ret = ray.get(d.get_handle().remote())
        else:
            ret = requests.get("http://localhost:8000/d").text
        print("RET=", ret)
        return ret.split("|")[0], ret.split("|")[1]

    d.deploy()
    val1, pid1 = call()

    assert val1 == "1"
    # Kill the GCS
    print("Kill GCS")
    ray.worker._global_node.kill_gcs_server()

    # Make sure it's still working even when GCS is killed
    val1, pid1 = call()
    assert val1 == "1"

    # Redeploy should fail
    with pytest.raises(KVStoreError):
        d.options(version="2").deploy()

    # Make sure nothing changed
    val1, pid1 = call()
    assert val1 == "1"
    print("Start GCS")
    # Bring GCS back
    ray.worker._global_node.start_gcs_server()

    # Make sure nothing changed even when GCS is back

    with pytest.raises(Exception):
        # TODO: We also need to check PID, but there is
        # a bug in ray core which will restart the actor
        # when GCS restarts.
        wait_for_condition(lambda: call()[0] != val1, timeout=4)

    @serve.deployment(version="2")
    def d(*args):
        return f"2|{os.getpid()}"

    # Redeploying with the same version and new code should do nothing.
    d.deploy()

    assert call()[0] == "2"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", "--forked", __file__]))
