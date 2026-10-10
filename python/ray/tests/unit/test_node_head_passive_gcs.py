# Unit tests for how the dashboard's node head reacts to a passive GCS.
import asyncio
import sys

import pytest

import ray._private.ray_constants as ray_constants
from ray.dashboard.modules.node.datacenter import DataSource
from ray.dashboard.modules.node.node_head import NodeHead
from ray.tests.unit.passive_test_utils import (
    count_logged,
    make_dummy_subprocess_config,
    make_mock_gcs_client,
)

pytestmark = pytest.mark.usefixtures("enable_passive_gcs")

NODE_HEAD_LOGGER = "ray.dashboard.modules.node.node_head"

HEAD_NODE_ID = "a" * 56
DEAD_NODE_ID = "c" * 56


def _node(node_id, is_head=False, state="ALIVE"):
    return {
        "nodeId": node_id,
        "isHeadNode": is_head,
        "state": state,
        "nodeManagerAddress": "1.2.3.4",
        "nodeManagerPort": 1234,
    }


@pytest.fixture(autouse=True)
def reset_shared_nodes():
    yield
    # DataSource is class-level state shared by every NodeHead instance.
    DataSource.nodes.clear()


@pytest.fixture
def make_node_head(tmp_path):
    heads = []

    def factory(*, leader):
        head = NodeHead(make_dummy_subprocess_config(tmp_path))
        # The gcs_client property builds a real client on first access.
        head._gcs_client = make_mock_gcs_client(leader=leader)
        heads.append(head)
        return head

    yield factory

    for head in heads:
        for task in head._background_tasks:
            task.cancel()


@pytest.mark.parametrize("update_target", ["nodes", "actors"])
async def test_updates_wait_for_leader_when_passive(
    make_node_head, capture_logger, caplog, update_target
):
    """Subscription loop and actor updates must not proceed until promoted to leader."""
    capture_logger(NODE_HEAD_LOGGER)
    head = make_node_head(leader=False)
    coro = (
        head._subscribe_for_node_updates().__anext__()
        if update_target == "nodes"
        else head._update_actors()
    )

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(coro, timeout=0.05)

    assert count_logged(caplog, "GCS is in passive mode") == 1


async def test_node_updates_proceed_when_active(make_node_head, monkeypatch):
    """When leader, _subscribe_for_node_updates proceeds, yields nodes, and registers head node."""
    head = make_node_head(leader=True)
    nodes = [_node(HEAD_NODE_ID, is_head=True)]

    async def _fake_subscribe():
        for node in nodes:
            yield node

    monkeypatch.setattr(head, "_subscribe_for_node_updates", _fake_subscribe)

    await head._update_nodes()
    assert head._registered_head_node_id == HEAD_NODE_ID
    head.gcs_client.async_internal_kv_put.assert_awaited_once_with(
        ray_constants.KV_HEAD_NODE_ID_KEY,
        HEAD_NODE_ID.encode(),
        overwrite=True,
        namespace=ray_constants.KV_NAMESPACE_JOB,
        timeout=60,
    )


async def test_the_subscription_survives_a_failed_node_update(
    make_node_head, capture_logger, caplog, monkeypatch
):
    """One bad update must not end the loop: it is the only writer of nodes."""
    capture_logger(NODE_HEAD_LOGGER)
    head = make_node_head(leader=True)
    nodes = [_node(HEAD_NODE_ID, is_head=True), _node(DEAD_NODE_ID, state="DEAD")]

    async def _subscribe():
        for node in nodes:
            yield node

    monkeypatch.setattr(head, "_subscribe_for_node_updates", _subscribe)
    original = head._update_node

    async def _fail_on_first(node):
        if node["nodeId"] == HEAD_NODE_ID:
            raise RuntimeError("boom")
        await original(node)

    monkeypatch.setattr(head, "_update_node", _fail_on_first)

    await head._update_nodes()

    # The second node was still processed.
    assert DEAD_NODE_ID in DataSource.nodes
    assert count_logged(caplog, "Failed updating node.") == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
