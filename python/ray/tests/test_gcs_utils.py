import contextlib
import os
import signal
import sys

import grpc
import pytest

import ray
import ray._private.gcs_utils as gcs_utils


@contextlib.contextmanager
def stop_gcs_server():
    process = ray._private.worker._global_node.all_processes[
        ray._private.ray_constants.PROCESS_TYPE_GCS_SERVER
    ][0].process
    pid = process.pid
    os.kill(pid, signal.SIGSTOP)
    yield
    os.kill(pid, signal.SIGCONT)


def test_kv_basic(ray_start_regular):
    gcs_address = ray._private.worker.global_worker.gcs_client.address
    gcs_client = gcs_utils.GcsClient(address=gcs_address, nums_reconnect_retry=0)

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


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
