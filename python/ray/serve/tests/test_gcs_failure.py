import importlib
import os
import sys

import pytest
import requests

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve._private.storage.kv_store import KVStoreError, RayInternalKVStore
from ray.serve.context import _get_global_client
from ray.serve.handle import DeploymentHandle
from ray.tests.conftest import external_redis  # noqa: F401


@pytest.fixture(scope="function")
def serve_ha(external_redis, monkeypatch):  # noqa: F811
    monkeypatch.setenv("RAY_SERVE_KV_TIMEOUT_S", "1")
    importlib.reload(ray.serve._private.constants)  # to reload the constants set above
    address_info = ray.init(
        num_cpus=36,
        namespace="default_test_namespace",
        _metrics_export_port=9999,
        _system_config={"metrics_report_interval_ms": 1000, "task_retry_delay_ms": 50},
    )
    serve.start()
    yield (address_info, _get_global_client())
    ray.shutdown()
    # Clear cache and global serve client
    serve.shutdown()


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
    assert e.value.rpc_code in (
        ray._raylet.GRPC_STATUS_CODE_UNAVAILABLE,
        ray._raylet.GRPC_STATUS_CODE_DEADLINE_EXCEEDED,
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
            handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
            ret = handle.remote().result()
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


@pytest.mark.parametrize("use_proxy", [True, False])
def test_router_on_gcs_failure(serve_ha, use_proxy: bool):
    """Test that a new router can send requests to replicas when GCS is down.

    Specifically, if a proxy was just brought up or a deployment handle
    was just created, and the GCS goes down BEFORE the router is able to
    send its first request, new incoming requests should successfully get
    sent to replicas during GCS downtime.
    """

    def router_populated_with_replicas(handle: DeploymentHandle):
        replicas = handle._router._replica_scheduler._replica_id_set
        assert len(replicas) > 0
        return True

    @serve.deployment
    class Dummy:
        def __call__(self):
            return os.getpid()

    h = serve.run(Dummy.options(num_replicas=2).bind())
    # TODO(zcin): We want to test the behavior for when the router
    # didn't get a chance to send even a single request yet. However on
    # the very first request we record telemetry for whether the
    # deployment handle API was used, which will hang when the GCS is
    # down. As a workaround for now, avoid recording telemetry so we
    # can properly test router behavior when GCS is down. We should look
    # into adding a timeout on the kv cache operation.
    h._recorded_telemetry = True
    # Eagerly create router so it receives the replica set instead of
    # waiting for the first request
    h._get_or_create_router()

    wait_for_condition(router_populated_with_replicas, handle=h)

    # Kill GCS server before a single request is sent.
    ray.worker._global_node.kill_gcs_server()

    returned_pids = set()
    if use_proxy:
        for _ in range(10):
            returned_pids.add(
                int(requests.get("http://localhost:8000", timeout=0.1).text)
            )
    else:
        for _ in range(10):
            returned_pids.add(int(h.remote().result(timeout_s=0.1)))

    print("Returned pids:", returned_pids)
    assert len(returned_pids) == 2


if __name__ == "__main__":
    # When GCS is down, right now some core worker members are not cleared
    # properly in ray.shutdown. Given that this is not hi-pri issue,
    # using --forked for isolation.
    sys.exit(pytest.main(["-v", "-s", "--forked", __file__]))
