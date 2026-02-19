from collections import deque
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ray.dashboard.modules.node import node_head
from ray.dashboard.modules.node.datacenter import DataSource


@pytest.mark.asyncio
async def test_update_node_dead_queue_overflow_keeps_incoming_node_key(monkeypatch):
    incoming_node_id = "incoming-node-id"
    evicted_node_id = "evicted-node-id"

    original_nodes = DataSource.nodes.copy()
    try:
        DataSource.nodes.clear()
        DataSource.nodes[incoming_node_id] = {
            "nodeId": incoming_node_id,
            "state": "ALIVE",
        }
        DataSource.nodes[evicted_node_id] = {
            "nodeId": evicted_node_id,
            "state": "DEAD",
        }

        fake_head = SimpleNamespace(
            _registered_head_node_id=None,
            _head_node_registration_time_s=None,
            _module_start_time=0,
            gcs_client=SimpleNamespace(
                async_internal_kv_put=AsyncMock(),
                async_internal_kv_del=AsyncMock(return_value=1),
            ),
            _dead_node_queue=deque([evicted_node_id]),
            _stubs={incoming_node_id: object(), evicted_node_id: object()},
        )

        monkeypatch.setattr(node_head.node_consts, "MAX_DEAD_NODES_TO_CACHE", 1)
        monkeypatch.setattr(
            node_head, "init_grpc_channel", lambda *args, **kwargs: object()
        )
        monkeypatch.setattr(
            node_head.node_manager_pb2_grpc,
            "NodeManagerServiceStub",
            lambda channel: object(),
        )

        incoming_dead_update = {
            "nodeId": incoming_node_id,
            "isHeadNode": False,
            "state": "DEAD",
            "nodeManagerAddress": "127.0.0.1",
            "nodeManagerPort": 12345,
        }

        await node_head.NodeHead._update_node(fake_head, incoming_dead_update)

        # Ensure we preserve the invariant:
        # DataSource.nodes key must match value["nodeId"].
        assert set(DataSource.nodes.keys()) == {incoming_node_id}
        assert DataSource.nodes[incoming_node_id]["nodeId"] == incoming_node_id
        assert DataSource.nodes[incoming_node_id]["state"] == "DEAD"
        assert fake_head.gcs_client.async_internal_kv_del.await_count == 2
    finally:
        DataSource.nodes.clear()
        DataSource.nodes.update(original_nodes)
