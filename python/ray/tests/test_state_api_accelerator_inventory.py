"""Tests for the accelerator-inventory data path in the State API: the
StateDataSourceClient RPC (deadline, channel lifecycle, UNIMPLEMENTED), the
StateAPIManager.list_nodes enrichment (detail gating, filtering/limiting,
dead-node preservation, shared deadline/bounded concurrency), and a real-cluster
end-to-end test that the head-node State API reports exactly what the reporter
agent discovers."""

import asyncio
import json
import sys
import types
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock

import grpc
import pytest
from click.testing import CliRunner

import ray.dashboard.state_aggregator as state_aggregator
from ray._private.state_api_test_utils import create_api_options
from ray.core.generated import reporter_pb2
from ray.core.generated.gcs_pb2 import GcsNodeInfo
from ray.core.generated.reporter_pb2 import GetAcceleratorInventoryReply
from ray.dashboard.modules.reporter.reporter_agent import (
    _accelerator_inventory_to_reply,
    _discover_accelerator_inventory,
)
from ray.dashboard.state_aggregator import (
    ACCELERATOR_INVENTORY_MAX_CONCURRENCY,
    StateAPIManager,
    _accelerator_inventory_reply_to_dict,
)
from ray.tests.test_state_api import generate_node_data
from ray.util.state import get_node, list_nodes
from ray.util.state.exception import DataSourceUnavailable
from ray.util.state.state_cli import ray_list
from ray.util.state.state_manager import (
    StateDataSourceClient,
    _close_grpc_channel,
)

# ---------------------------------------------------------------------------
# StateDataSourceClient.get_accelerator_inventory
# ---------------------------------------------------------------------------


async def test_get_accelerator_inventory_no_agent_returns_none():
    client = object.__new__(StateDataSourceClient)
    client.get_reporter_service_stub = AsyncMock(return_value=None)
    assert await client.get_accelerator_inventory("abcd") is None


async def test_get_accelerator_inventory_unimplemented_is_unsupported():
    client = object.__new__(StateDataSourceClient)
    stub = AsyncMock()
    err = grpc.aio.AioRpcError(grpc.StatusCode.UNIMPLEMENTED, None, None)
    stub.GetAcceleratorInventory = AsyncMock(side_effect=err)
    channel = AsyncMock()
    channel.close = AsyncMock(return_value=None)
    client.get_reporter_service_stub = AsyncMock(return_value=(stub, channel))

    reply = await client.get_accelerator_inventory("abcd")
    assert reply.status == reporter_pb2.ACCELERATOR_INVENTORY_STATUS_UNSUPPORTED
    assert "does not implement" in reply.status_message
    channel.close.assert_awaited_once()


async def test_get_accelerator_inventory_closes_channel_on_success():
    client = object.__new__(StateDataSourceClient)
    stub = AsyncMock()
    reply = GetAcceleratorInventoryReply(
        status=reporter_pb2.ACCELERATOR_INVENTORY_STATUS_OK
    )
    stub.GetAcceleratorInventory = AsyncMock(return_value=reply)
    channel = AsyncMock()
    channel.close = AsyncMock(return_value=None)
    client.get_reporter_service_stub = AsyncMock(return_value=(stub, channel))

    result = await client.get_accelerator_inventory("abcd")
    assert result is reply
    channel.close.assert_awaited_once()


async def test_get_accelerator_inventory_network_error_is_unavailable():
    client = object.__new__(StateDataSourceClient)
    stub = AsyncMock()
    err = grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, None, None)
    stub.GetAcceleratorInventory = AsyncMock(side_effect=err)
    channel = AsyncMock()
    channel.close = AsyncMock(return_value=None)
    client.get_reporter_service_stub = AsyncMock(return_value=(stub, channel))

    with pytest.raises(DataSourceUnavailable):
        await client.get_accelerator_inventory("abcd")
    channel.close.assert_awaited_once()


async def test_get_accelerator_inventory_deadline():
    client = object.__new__(StateDataSourceClient)

    async def slow_stub_lookup(_node_id):
        await asyncio.sleep(10)
        return None

    client.get_reporter_service_stub = AsyncMock(side_effect=slow_stub_lookup)
    with pytest.raises(asyncio.TimeoutError):
        await client.get_accelerator_inventory("abcd", timeout=0.05)


async def test_close_grpc_channel_survives_cancellation():
    """The close is scheduled once and shielded, so cancelling the caller never
    reschedules (or drops) the close coroutine."""
    closed = asyncio.Event()

    async def slow_close():
        await asyncio.sleep(0.05)
        closed.set()

    calls = []

    class FakeChannel:
        def close(self):
            calls.append(True)
            return slow_close()

    channel = FakeChannel()
    task = asyncio.create_task(_close_grpc_channel(channel))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # The close still completes in the background.
    await asyncio.wait_for(closed.wait(), timeout=1)
    # close() was invoked exactly once (no coroutine reuse).
    assert len(calls) == 1


async def test_get_accelerator_inventory_cancellation_still_closes_channel():
    client = object.__new__(StateDataSourceClient)
    closed = asyncio.Event()

    async def slow_rpc(_request):
        await asyncio.sleep(10)

    class FakeChannel:
        async def close(self):
            closed.set()

    stub = AsyncMock()
    stub.GetAcceleratorInventory = AsyncMock(side_effect=slow_rpc)
    channel = FakeChannel()
    client.get_reporter_service_stub = AsyncMock(return_value=(stub, channel))

    task = asyncio.create_task(client.get_accelerator_inventory("abcd", timeout=0.05))
    with pytest.raises(asyncio.TimeoutError):
        await task
    # The channel is still closed even though the deadline cancelled the RPC.
    await asyncio.wait_for(closed.wait(), timeout=1)


async def test_get_reporter_service_stub_no_agent_returns_none():
    client = object.__new__(StateDataSourceClient)

    class FakeGcsClient:
        async def async_internal_kv_get(self, *args, **kwargs):
            return None

    class FakeNodeID:
        def hex(self):
            return "abcd"

    client._gcs_client = FakeGcsClient()
    assert await client.get_reporter_service_stub(FakeNodeID()) is None


# ---------------------------------------------------------------------------
# StateAPIManager.list_nodes enrichment
# ---------------------------------------------------------------------------


def _manager(client):
    return StateAPIManager(client, ThreadPoolExecutor())


def _ok_reply():
    reply = GetAcceleratorInventoryReply(
        status=reporter_pb2.ACCELERATOR_INVENTORY_STATUS_OK
    )
    device = reply.devices.add()
    device.vendor = "nvidia"
    device.resource_family = "GPU"
    device.model = "A100"
    device.uuid = "GPU-1"
    device.memory_total_bytes = 1234
    device.pci_bus_id = "0000:3b:00.0"
    device.numa_node = 1
    device.kind = "gpu"
    return reply


async def test_list_nodes_detail_false_no_inventory_rpc():
    client = AsyncMock(StateDataSourceClient)
    node = generate_node_data(b"1234")
    client.get_all_node_info.return_value = ({node.node_id: node}, 0)
    result = await _manager(client).list_nodes(option=create_api_options(detail=False))

    assert len(result.result) == 1
    assert "accelerator_inventory" not in result.result[0]
    client.get_accelerator_inventory.assert_not_awaited()


async def test_list_nodes_detail_true_attaches_inventory():
    client = AsyncMock(StateDataSourceClient)
    node = generate_node_data(b"1234")
    client.get_all_node_info.return_value = ({node.node_id: node}, 0)
    client.get_accelerator_inventory.return_value = _ok_reply()

    result = await _manager(client).list_nodes(option=create_api_options(detail=True))

    inventory = result.result[0]["accelerator_inventory"]
    assert inventory["status"] == "ok"
    assert len(inventory["devices"]) == 1
    assert inventory["devices"][0]["uuid"] == "GPU-1"
    assert inventory["devices"][0]["numa_node"] == 1
    client.get_accelerator_inventory.assert_awaited_once()


async def test_list_nodes_only_alive_nodes_queried_after_limit():
    client = AsyncMock(StateDataSourceClient)
    alive = generate_node_data(b"1234")
    dead = GcsNodeInfo(
        node_id=b"9999",
        state=GcsNodeInfo.GcsNodeState.DEAD,
        node_manager_address="127.0.0.1",
        raylet_socket_name="abcd",
        object_store_socket_name="False",
    )
    client.get_all_node_info.return_value = (
        {alive.node_id: alive, dead.node_id: dead},
        0,
    )
    client.get_accelerator_inventory.return_value = _ok_reply()

    result = await _manager(client).list_nodes(
        option=create_api_options(detail=True, limit=10)
    )

    assert len(result.result) == 2
    # Only the alive node triggers a reporter RPC; the dead node is preserved
    # with a null inventory.
    client.get_accelerator_inventory.assert_awaited_once()
    by_id = {entry["node_id"]: entry for entry in result.result}
    assert by_id[b"1234".hex()]["accelerator_inventory"]["status"] == "ok"
    assert by_id[b"9999".hex()]["accelerator_inventory"] is None


async def test_list_nodes_limit_bounds_inventory_queries():
    client = AsyncMock(StateDataSourceClient)
    nodes = {}
    for i in range(3):
        node = generate_node_data(bytes([i + 1]) * 1 + b"node" + str(i).encode())
        nodes[node.node_id] = node
    client.get_all_node_info.return_value = (nodes, 0)
    client.get_accelerator_inventory.return_value = _ok_reply()

    result = await _manager(client).list_nodes(
        option=create_api_options(detail=True, limit=1)
    )

    assert len(result.result) == 1
    client.get_accelerator_inventory.assert_awaited_once()


async def test_list_nodes_unavailable_node_preserved():
    client = AsyncMock(StateDataSourceClient)
    node = generate_node_data(b"1234")
    client.get_all_node_info.return_value = ({node.node_id: node}, 0)
    client.get_accelerator_inventory.side_effect = DataSourceUnavailable("down")

    result = await _manager(client).list_nodes(option=create_api_options(detail=True))

    assert len(result.result) == 1
    inventory = result.result[0]["accelerator_inventory"]
    assert inventory["status"] == "unavailable"
    assert "down" in inventory["status_message"]


async def test_list_nodes_timeout_preserved():
    client = AsyncMock(StateDataSourceClient)
    node = generate_node_data(b"1234")
    client.get_all_node_info.return_value = ({node.node_id: node}, 0)
    client.get_accelerator_inventory.side_effect = asyncio.TimeoutError()

    result = await _manager(client).list_nodes(option=create_api_options(detail=True))

    assert len(result.result) == 1
    assert result.result[0]["accelerator_inventory"]["status"] == "unavailable"


async def test_list_nodes_filter_before_inventory_fanout():
    client = AsyncMock(StateDataSourceClient)
    node1 = generate_node_data(b"1234")
    node2 = generate_node_data(b"5678")
    client.get_all_node_info.return_value = (
        {node1.node_id: node1, node2.node_id: node2},
        0,
    )
    client.get_accelerator_inventory.return_value = _ok_reply()

    result = await _manager(client).list_nodes(
        option=create_api_options(
            detail=True, filters=[("node_id", "=", b"1234".hex())]
        )
    )

    # Only the node surviving the filter triggers the reporter RPC.
    assert len(result.result) == 1
    assert result.result[0]["node_id"] == b"1234".hex()
    client.get_accelerator_inventory.assert_awaited_once()


async def test_list_nodes_unsupported_reply():
    client = AsyncMock(StateDataSourceClient)
    node = generate_node_data(b"1234")
    client.get_all_node_info.return_value = ({node.node_id: node}, 0)
    client.get_accelerator_inventory.return_value = GetAcceleratorInventoryReply(
        status=reporter_pb2.ACCELERATOR_INVENTORY_STATUS_UNSUPPORTED,
        status_message="no implementation",
    )

    result = await _manager(client).list_nodes(option=create_api_options(detail=True))

    inventory = result.result[0]["accelerator_inventory"]
    assert inventory["status"] == "unsupported"
    assert inventory["devices"] == []


async def test_list_nodes_expired_deadline_starts_no_inventory_rpc(monkeypatch):
    """When the GCS query consumes the whole budget, no reporter RPC is
    started; the row is preserved with an explicit unavailable status.

    Uses a fake monotonic clock patched only into ``state_aggregator`` so the
    asyncio event loop's own clock is untouched.
    """
    client = AsyncMock(StateDataSourceClient)
    node = generate_node_data(b"1234")
    client.get_all_node_info.return_value = ({node.node_id: node}, 0)

    clock = {"now": 0.0}
    monkeypatch.setattr(
        state_aggregator, "time", types.SimpleNamespace(monotonic=lambda: clock["now"])
    )

    async def gcs(*args, **kwargs):
        clock["now"] = 100.0  # advance well past the 1s deadline
        return ({node.node_id: node}, 0)

    client.get_all_node_info.side_effect = gcs

    result = await _manager(client).list_nodes(
        option=create_api_options(detail=True, timeout=1)
    )

    client.get_accelerator_inventory.assert_not_awaited()
    assert result.result[0]["accelerator_inventory"]["status"] == "unavailable"


async def test_list_nodes_bounded_concurrency_many_nodes():
    """More nodes than the concurrency ceiling are processed with at most the
    ceiling in flight at once (and genuinely concurrently), then all complete.

    Uses an event barrier and an active counter, not a wall-clock bound, so it
    is deterministic.
    """
    client = AsyncMock(StateDataSourceClient)
    nodes = {}
    for i in range(20):
        node = generate_node_data(bytes([i]) + b"node" + str(i).encode())
        nodes[node.node_id] = node
    client.get_all_node_info.return_value = (nodes, 0)

    active = 0
    max_active = 0
    reached_ceiling = asyncio.Event()
    release = asyncio.Event()

    async def blocking_inventory(node_id, timeout):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        if active == ACCELERATOR_INVENTORY_MAX_CONCURRENCY:
            reached_ceiling.set()
        await release.wait()
        active -= 1
        return _ok_reply()

    client.get_accelerator_inventory.side_effect = blocking_inventory

    task = asyncio.create_task(
        _manager(client).list_nodes(
            option=create_api_options(detail=True, timeout=10, limit=20)
        )
    )
    # Wait until the semaphore saturates at its ceiling.
    await asyncio.wait_for(reached_ceiling.wait(), timeout=5)

    # Concurrency is bounded exactly at the ceiling and is genuinely > 1.
    assert max_active == ACCELERATOR_INVENTORY_MAX_CONCURRENCY

    release.set()
    result = await task

    assert len(result.result) == 20
    assert all(e["accelerator_inventory"]["status"] == "ok" for e in result.result)
    assert client.get_accelerator_inventory.await_count == 20


def test_accelerator_inventory_end_to_end(ray_start_with_dashboard):
    """The head-node State API (list/get/CLI) must report exactly what the
    reporter agent discovers on this node, including the serialized shape."""
    # Ground truth: run the same discovery the reporter agent runs, serialized
    # through the same reply -> dict conversion used by the aggregator.
    expected = _accelerator_inventory_reply_to_dict(
        _accelerator_inventory_to_reply(_discover_accelerator_inventory())
    )

    nodes = list_nodes(detail=True)
    assert len(nodes) == 1
    assert nodes[0].get("accelerator_inventory") == expected

    node = get_node(nodes[0]["node_id"])
    assert node.get("accelerator_inventory") == expected

    runner = CliRunner()
    result = runner.invoke(ray_list, ["nodes", "--detail", "--format=json"])
    assert result.exit_code == 0, result.output
    cli_nodes = json.loads(result.output)
    assert len(cli_nodes) == 1
    assert cli_nodes[0]["accelerator_inventory"] == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
