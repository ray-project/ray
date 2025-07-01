import importlib
import os
import sys
from typing import Callable, Optional

import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve._private.storage.kv_store import KVStoreError, RayInternalKVStore
from ray.serve._private.test_utils import check_apps_running
from ray.serve.context import _get_global_client
from ray.serve.handle import DeploymentHandle
from ray.serve.schema import ServeDeploySchema
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

    # When GCS is down, right now some core worker members are not cleared
    # properly in ray.shutdown.
    ray.worker._global_node.start_gcs_server()

    # Clear cache and global serve client
    serve.shutdown()
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
            ret = httpx.get("http://localhost:8000/d").text
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

    # TODO(abrar): The following block of code causes the pytest process to crash
    # abruptly. It's unclear why this is happening. Check with ray core team.
    # Skipping this for now to unblock CI.

    # # Redeploy should fail without a change going through.
    # with pytest.raises(KVStoreError):
    #     serve.run(d.options().bind())

    for _ in range(10):
        assert pid == call()


def router_populated_with_replicas(
    threshold: int,
    handle: Optional[DeploymentHandle] = None,
    get_replicas_func: Optional[Callable] = None,
    check_cache_populated: bool = False,
):
    """Either get router's replica set from `handle` directly, or use
    `get_replicas_func` to get replica set. Then check that the number
    of replicas in set is at least `threshold`.
    """
    if handle:
        router = handle._router._asyncio_router
        replicas = router._request_router._replica_id_set
    else:
        replicas = get_replicas_func()

    print(f"Replica set in router: {replicas}")
    assert len(replicas) >= threshold

    # Return early if we don't need to check cache
    if not check_cache_populated:
        return True

    router = handle._router._asyncio_router
    cache = router._request_router.replica_queue_len_cache
    for replica_id in replicas:
        assert (
            cache.get(replica_id) is not None
        ), f"{replica_id} missing from cache {cache._cache}"

    return True


@pytest.mark.parametrize("use_proxy", [True, False])
def test_new_router_on_gcs_failure(serve_ha, use_proxy: bool):
    """Test that a new router can send requests to replicas when GCS is down.

    Specifically, if a proxy was just brought up or a deployment handle
    was just created, and the GCS goes down BEFORE the router is able to
    send its first request, new incoming requests should successfully get
    sent to replicas during GCS downtime.
    """

    _, client = serve_ha

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
    # into adding a timeout on the kv cache operation. For now, the proxy
    # doesn't run into this because we don't record telemetry on proxy
    h._recorded_telemetry = True
    # Eagerly create router so it receives the replica set instead of
    # waiting for the first request
    h._init()

    if use_proxy:
        proxy_handles = ray.get(client._controller.get_proxies.remote())
        proxy_handle = list(proxy_handles.values())[0]
        wait_for_condition(
            router_populated_with_replicas,
            threshold=2,
            get_replicas_func=lambda: ray.get(
                proxy_handle._dump_ingress_replicas_for_testing.remote("/")
            ),
        )
    else:
        wait_for_condition(router_populated_with_replicas, threshold=2, handle=h)

    # Kill GCS server before a single request is sent.
    ray.worker._global_node.kill_gcs_server()

    returned_pids = set()
    if use_proxy:
        for _ in range(10):
            returned_pids.add(int(httpx.get("http://localhost:8000", timeout=3.0).text))
    else:
        for _ in range(10):
            returned_pids.add(int(h.remote().result(timeout_s=3.0)))

    print("Returned pids:", returned_pids)
    assert len(returned_pids) == 2


def test_handle_router_updated_replicas_then_gcs_failure(serve_ha):
    """Test the router's replica set is updated from 1 to 2 replicas, with the first
    replica staying the same. Verify that if the GCS goes down before the router
    gets a chance to send a request to the second replica, requests can be handled
    during GCS failure.

    This test uses a plain handle to send requests.
    """

    _, client = serve_ha

    config = {
        "name": "default",
        "import_path": "ray.serve._private.test_utils:get_pid_entrypoint",
        "route_prefix": "/",
        "deployments": [{"name": "GetPID", "num_replicas": 1}],
    }
    client.deploy_apps(ServeDeploySchema(**{"applications": [config]}))
    wait_for_condition(check_apps_running, apps=["default"])

    h = serve.get_app_handle("default")
    print(h.remote().result())

    config["deployments"][0]["num_replicas"] = 2
    client.deploy_apps(ServeDeploySchema(**{"applications": [config]}))

    wait_for_condition(
        router_populated_with_replicas,
        threshold=2,
        handle=h,
        check_cache_populated=True,
    )

    # Kill GCS server before router gets to send request to second replica
    ray.worker._global_node.kill_gcs_server()

    returned_pids = set()
    for _ in range(20):
        returned_pids.add(int(h.remote().result(timeout_s=1.0)))

    print("Returned pids:", returned_pids)
    assert len(returned_pids) == 2


def test_proxy_router_updated_replicas_then_gcs_failure(serve_ha):
    """Test the router's replica set is updated from 1 to 2 replicas, with the first
    replica staying the same. Verify that if the GCS goes down before the router
    gets a chance to send a request to the second replica, requests can be handled
    during GCS failure.

    This test sends http requests to the proxy.
    """
    _, client = serve_ha

    config = {
        "name": "default",
        "import_path": "ray.serve._private.test_utils:get_pid_entrypoint",
        "route_prefix": "/",
        "deployments": [{"name": "GetPID", "num_replicas": 1}],
    }
    client.deploy_apps(ServeDeploySchema(**{"applications": [config]}))
    wait_for_condition(check_apps_running, apps=["default"])

    r = httpx.post("http://localhost:8000")
    assert r.status_code == 200, r.text
    print(r.text)

    config["deployments"][0]["num_replicas"] = 2
    client.deploy_apps(ServeDeploySchema(**{"applications": [config]}))

    proxy_handles = ray.get(client._controller.get_proxies.remote())
    proxy_handle = list(proxy_handles.values())[0]

    wait_for_condition(
        router_populated_with_replicas,
        threshold=2,
        get_replicas_func=lambda: ray.get(
            proxy_handle._dump_ingress_replicas_for_testing.remote("/")
        ),
    )

    # Kill GCS server before router gets to send request to second replica
    ray.worker._global_node.kill_gcs_server()

    returned_pids = set()
    for _ in range(20):
        r = httpx.post("http://localhost:8000")
        assert r.status_code == 200
        returned_pids.add(int(r.text))

    print("Returned pids:", returned_pids)
    assert len(returned_pids) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
