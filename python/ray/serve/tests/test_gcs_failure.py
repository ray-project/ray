import os
import sys

import grpc
import pytest
import requests

import ray
import ray.serve as serve
from ray._private.test_utils import wait_for_condition
from ray.serve._private.storage.kv_store import KVStoreError, RayInternalKVStore
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


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failing on Windows, 'ForkedFunc' object has no attribute 'pid'",
)
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


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Failing on Windows, 'ForkedFunc' object has no attribute 'pid'",
)
@pytest.mark.parametrize("use_handle", [False, True])
def test_controller_gcs_failure(serve_ha, use_handle):  # noqa: F811
    @serve.deployment
    def d(*args):
        return f"{os.getpid()}"

    def call():
        if use_handle:
            ret = ray.get(d.get_handle().remote())
        else:
            ret = requests.get("http://localhost:8000/d").text
        return ret

    serve.run(d.bind())
    pid = call()

    # Kill the GCS.
    print("Kill GCS")
    ray.worker._global_node.kill_gcs_server()

    # Make sure pid doesn't change within 5s.
    with pytest.raises(Exception):
        wait_for_condition(lambda: pid != call(), timeout=5, retry_interval_ms=1)

    print("Start GCS")
    ray.worker._global_node.start_gcs_server()

    # Make sure nothing changed even when GCS is back.
    with pytest.raises(Exception):
        wait_for_condition(lambda: call() != pid, timeout=4)

    serve.run(d.bind())

    # Make sure redeploy happens.
    for _ in range(10):
        assert pid != call()

    pid = call()

    print("Kill GCS")
    ray.worker._global_node.kill_gcs_server()

    # Redeploy should fail without a change going through.
    with pytest.raises(KVStoreError):
        serve.run(d.options().bind())

    for _ in range(10):
        assert pid == call()


if __name__ == "__main__":
    # When GCS is down, right now some core worker members are not cleared
    # properly in ray.shutdown. Given that this is not hi-pri issue,
    # using --forked for isolation.
    sys.exit(pytest.main(["-v", "-s", "--forked", __file__]))
