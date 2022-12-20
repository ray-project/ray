import contextlib
import os
import time
import signal
import sys

import grpc
import pytest
import ray
from ray._private.gcs_utils import GcsClient
import ray._private.gcs_utils as gcs_utils
from ray._private.test_utils import (
    enable_external_redis,
    find_free_port,
    generate_system_config_map,
    async_wait_for_condition_async_predicate,
)
import ray._private.ray_constants as ray_constants


@contextlib.contextmanager
def stop_gcs_server():
    process = ray._private.worker._global_node.all_processes[
        ray._private.ray_constants.PROCESS_TYPE_GCS_SERVER
    ][0].process
    pid = process.pid
    os.kill(pid, signal.SIGSTOP)
    yield
    os.kill(pid, signal.SIGCONT)


def test_kv_basic(ray_start_regular, monkeypatch):
    monkeypatch.setenv("TEST_RAY_COLLECT_KV_FREQUENCY", "1")
    gcs_address = ray._private.worker.global_worker.gcs_client.address
    gcs_client = gcs_utils.GcsClient(address=gcs_address, nums_reconnect_retry=0)
    # Wait until all other calls finished
    time.sleep(2)
    # reset the counter
    gcs_utils._called_freq = {}
    assert gcs_client.internal_kv_get(b"A", b"NS") is None
    assert gcs_client.internal_kv_put(b"A", b"B", False, b"NS") == 1
    assert gcs_client.internal_kv_get(b"A", b"NS") == b"B"
    assert gcs_client.internal_kv_put(b"A", b"C", False, b"NS") == 0
    assert gcs_client.internal_kv_get(b"A", b"NS") == b"B"
    assert gcs_client.internal_kv_put(b"A", b"C", True, b"NS") == 0
    assert gcs_client.internal_kv_get(b"A", b"NS") == b"C"
    assert gcs_client.internal_kv_put(b"AA", b"B", False, b"NS") == 1
    assert gcs_client.internal_kv_put(b"AB", b"B", False, b"NS") == 1
    assert set(gcs_client.internal_kv_keys(b"A", b"NS")) == {b"A", b"AA", b"AB"}
    assert gcs_client.internal_kv_del(b"A", False, b"NS") == 1
    assert set(gcs_client.internal_kv_keys(b"A", b"NS")) == {b"AA", b"AB"}
    assert gcs_client.internal_kv_keys(b"A", b"NSS") == []
    assert gcs_client.internal_kv_del(b"A", True, b"NS") == 2
    assert gcs_client.internal_kv_keys(b"A", b"NS") == []
    assert gcs_client.internal_kv_del(b"A", False, b"NSS") == 0
    assert gcs_utils._called_freq["internal_kv_get"] == 4
    assert gcs_utils._called_freq["internal_kv_put"] == 5


@pytest.mark.skipif(sys.platform == "win32", reason="Windows doesn't have signals.")
def test_kv_timeout(ray_start_regular):
    gcs_address = ray._private.worker.global_worker.gcs_client.address
    gcs_client = gcs_utils.GcsClient(address=gcs_address, nums_reconnect_retry=0)

    assert gcs_client.internal_kv_put(b"A", b"", False, b"") == 1

    with stop_gcs_server():
        with pytest.raises(grpc.RpcError, match="Deadline Exceeded"):
            gcs_client.internal_kv_put(b"A", b"B", False, b"NS", timeout=2)

        with pytest.raises(grpc.RpcError, match="Deadline Exceeded"):
            gcs_client.internal_kv_get(b"A", b"NS", timeout=2)

        with pytest.raises(grpc.RpcError, match="Deadline Exceeded"):
            gcs_client.internal_kv_keys(b"A", b"NS", timeout=2)

        with pytest.raises(grpc.RpcError, match="Deadline Exceeded"):
            gcs_client.internal_kv_del(b"A", True, b"NS", timeout=2)


@pytest.mark.asyncio
async def test_kv_basic_aio(ray_start_regular):
    gcs_client = gcs_utils.GcsAioClient(
        address=ray._private.worker.global_worker.gcs_client.address
    )

    assert await gcs_client.internal_kv_get(b"A", b"NS") is None
    assert await gcs_client.internal_kv_put(b"A", b"B", False, b"NS") == 1
    assert await gcs_client.internal_kv_get(b"A", b"NS") == b"B"
    assert await gcs_client.internal_kv_put(b"A", b"C", False, b"NS") == 0
    assert await gcs_client.internal_kv_get(b"A", b"NS") == b"B"
    assert await gcs_client.internal_kv_put(b"A", b"C", True, b"NS") == 0
    assert await gcs_client.internal_kv_get(b"A", b"NS") == b"C"
    assert await gcs_client.internal_kv_put(b"AA", b"B", False, b"NS") == 1
    assert await gcs_client.internal_kv_put(b"AB", b"B", False, b"NS") == 1
    keys = await gcs_client.internal_kv_keys(b"A", b"NS")
    assert set(keys) == {b"A", b"AA", b"AB"}
    assert await gcs_client.internal_kv_del(b"A", False, b"NS") == 1
    keys = await gcs_client.internal_kv_keys(b"A", b"NS")
    assert set(keys) == {b"AA", b"AB"}
    assert await gcs_client.internal_kv_keys(b"A", b"NSS") == []
    assert await gcs_client.internal_kv_del(b"A", True, b"NS") == 2
    assert await gcs_client.internal_kv_keys(b"A", b"NS") == []
    assert await gcs_client.internal_kv_del(b"A", False, b"NSS") == 0


@pytest.mark.skipif(sys.platform == "win32", reason="Windows doesn't have signals.")
@pytest.mark.asyncio
async def test_kv_timeout_aio(ray_start_regular):
    gcs_client = gcs_utils.GcsAioClient(
        address=ray._private.worker.global_worker.gcs_client.address
    )
    # Make sure gcs_client is connected
    assert await gcs_client.internal_kv_put(b"A", b"", False, b"") == 1

    with stop_gcs_server():
        with pytest.raises(grpc.RpcError, match="Deadline Exceeded"):
            await gcs_client.internal_kv_put(b"A", b"B", False, b"NS", timeout=2)

        with pytest.raises(grpc.RpcError, match="Deadline Exceeded"):
            await gcs_client.internal_kv_get(b"A", b"NS", timeout=2)

        with pytest.raises(grpc.RpcError, match="Deadline Exceeded"):
            await gcs_client.internal_kv_keys(b"A", b"NS", timeout=2)

        with pytest.raises(grpc.RpcError, match="Deadline Exceeded"):
            await gcs_client.internal_kv_del(b"A", True, b"NS", timeout=2)


@pytest.mark.skipif(
    not enable_external_redis(), reason="Only valid when start with an external redis"
)
def test_external_storage_namespace_isolation(shutdown_only):
    addr = ray.init(
        namespace="a", _system_config={"external_storage_namespace": "c1"}
    ).address_info["address"]
    gcs_client = GcsClient(address=addr)

    assert gcs_client.internal_kv_put(b"ABC", b"DEF", True, None) == 1

    assert gcs_client.internal_kv_get(b"ABC", None) == b"DEF"

    ray.shutdown()

    addr = ray.init(
        namespace="a", _system_config={"external_storage_namespace": "c2"}
    ).address_info["address"]
    gcs_client = GcsClient(address=addr)
    assert gcs_client.internal_kv_get(b"ABC", None) is None
    assert gcs_client.internal_kv_put(b"ABC", b"XYZ", True, None) == 1

    assert gcs_client.internal_kv_get(b"ABC", None) == b"XYZ"
    ray.shutdown()

    addr = ray.init(
        namespace="a", _system_config={"external_storage_namespace": "c1"}
    ).address_info["address"]
    gcs_client = GcsClient(address=addr)
    assert gcs_client.internal_kv_get(b"ABC", None) == b"DEF"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pull_based,ray_start_cluster",
    [
        (
            False,
            generate_system_config_map(
                RAY_num_heartbeats_timeout=2, pull_based_healthcheck=False
            ),
        ),
        (
            True,
            generate_system_config_map(
                health_check_initial_delay_ms=0,
                health_check_period_ms=1000,
                health_check_failure_threshold=2,
                pull_based_healthcheck=True,
            ),
        ),
    ],
    indirect=["ray_start_cluster"],
)
async def test_check_liveness(pull_based, monkeypatch, ray_start_cluster):
    if pull_based:
        monkeypatch.setenv("RAY_pull_based_healthcheck", "true")
        monkeypatch.setenv("RAY_health_check_initial_delay_ms", "0")
        monkeypatch.setenv("RAY_health_check_period_ms", "1000")
        monkeypatch.setenv("RAY_health_check_failure_threshold", "2")
    else:
        monkeypatch.setenv("RAY_pull_based_healthcheck", "false")
        monkeypatch.setenv("RAY_num_heartbeats_timeout", "2")

    cluster = ray_start_cluster
    h = cluster.add_node(node_manager_port=find_free_port())
    n1 = cluster.add_node(node_manager_port=find_free_port())
    n2 = cluster.add_node(node_manager_port=find_free_port())
    gcs_client = gcs_utils.GcsAioClient(address=cluster.address)
    node_manager_addresses = [
        f"{n.raylet_ip_address}:{n.node_manager_port}" for n in [h, n1, n2]
    ]

    ret = await gcs_client.check_alive(node_manager_addresses)
    assert ret == [True, True, True]

    cluster.remove_node(n1)

    async def check(expect_liveness):
        ret = await gcs_client.check_alive(node_manager_addresses)
        return ret == expect_liveness

    await async_wait_for_condition_async_predicate(
        check, expect_liveness=[True, False, True]
    )

    n2_raylet_process = n2.all_processes[ray_constants.PROCESS_TYPE_RAYLET][0].process
    n2_raylet_process.kill()

    # GCS hasn't marked it as dead yet.
    ret = await gcs_client.check_alive(node_manager_addresses)
    assert ret == [True, False, True]

    # GCS will notice node dead soon
    await async_wait_for_condition_async_predicate(
        check, expect_liveness=[True, False, False]
    )


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
