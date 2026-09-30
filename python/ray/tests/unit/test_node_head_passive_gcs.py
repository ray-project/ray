# Unit tests for how the dashboard's node head reacts to a passive GCS.
import asyncio
import logging
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

import ray._private.ray_constants as ray_constants
import ray.dashboard.modules.node.node_head as node_head_module
import ray.experimental.internal_kv as internal_kv
from ray.dashboard.modules.node.datacenter import DataSource
from ray.dashboard.modules.node.node_head import NodeHead
from ray.dashboard.subprocesses.module import SubprocessModuleConfig
from ray.exceptions import GcsPassiveError

NODE_HEAD_LOGGER = "ray.dashboard.modules.node.node_head"

HEAD_NODE_ID = "a" * 56
DEAD_NODE_ID = "c" * 56


def _passive_gcs_rejection():
    """The error a passive GCS raises, as check_status() translates it."""
    return GcsPassiveError("GCS server is in passive (read-only) mode.")


def _capture(caplog, logger_name):
    """Attach caplog to a module logger.

    The "ray" logger does not propagate to the root logger caplog listens on, so
    caplog.at_level() alone records nothing.
    """
    logger = logging.getLogger(logger_name)
    logger.addHandler(caplog.handler)
    caplog.set_level(logging.INFO, logger=logger_name)
    return logger


def _count_logged(caplog, needle):
    return sum(needle in record.getMessage() for record in caplog.records)


def _alive_head_node(node_id):
    return {
        "nodeId": node_id,
        "isHeadNode": True,
        "state": "ALIVE",
        "nodeManagerAddress": "1.2.3.4",
        "nodeManagerPort": 1234,
    }


def _dead_worker_node(node_id):
    return {
        "nodeId": node_id,
        "isHeadNode": False,
        "state": "DEAD",
        "nodeManagerAddress": "5.6.7.8",
        "nodeManagerPort": 5678,
    }


@pytest.fixture(autouse=True)
def leader_election_on(monkeypatch):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", True)


@pytest.fixture(autouse=True)
def fast_registration_poll(monkeypatch):
    # node_head binds the constant at import time, so patching the consts
    # module would not reach it.
    monkeypatch.setattr(node_head_module, "GCS_REGISTER_RETRY_INTERVAL_S", 0.01)


@pytest.fixture(autouse=True)
def reset_shared_state():
    yield
    internal_kv._internal_kv_reset()
    # DataSource is class-level state shared by every NodeHead instance.
    DataSource.nodes.clear()


@pytest.fixture
def node_head_logs(caplog):
    logger = _capture(caplog, NODE_HEAD_LOGGER)
    yield caplog
    logger.removeHandler(caplog.handler)


def _gcs_client(*, leader):
    gcs_client = MagicMock()
    gcs_client.async_internal_kv_put = AsyncMock(
        side_effect=None if leader else _passive_gcs_rejection()
    )
    gcs_client.async_internal_kv_del = AsyncMock()
    gcs_client.is_gcs_leader = MagicMock(return_value=leader)
    gcs_client.is_gcs_leader_local = MagicMock(return_value=leader)
    gcs_client.async_check_alive = AsyncMock()
    return gcs_client


@pytest.fixture
def make_node_head(tmp_path):
    heads = []

    def factory(*, leader):
        head = NodeHead(
            SubprocessModuleConfig(
                cluster_id_hex="f" * 56,
                gcs_address="127.0.0.1:6379",
                session_name="session",
                temp_dir=str(tmp_path),
                session_dir=str(tmp_path),
                logging_level=logging.INFO,
                logging_format="%(message)s",
                log_dir=str(tmp_path),
                logging_filename="dashboard.log",
                logging_rotate_bytes=1,
                logging_rotate_backup_count=1,
                socket_dir=str(tmp_path),
            )
        )
        # The gcs_client property builds a real client on first access.
        head._gcs_client = _gcs_client(leader=leader)
        heads.append(head)
        return head

    yield factory

    for head in heads:
        for task in head._background_tasks:
            task.cancel()


@pytest.mark.parametrize("update_target", ["nodes", "actors"])
async def test_updates_wait_for_leader_when_passive(
    make_node_head, node_head_logs, update_target
):
    """Subscription loop and actor updates must not proceed until promoted to leader."""
    head = make_node_head(leader=False)
    coro = (
        head._subscribe_for_node_updates().__anext__()
        if update_target == "nodes"
        else head._update_actors()
    )

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(coro, timeout=0.05)

    assert _count_logged(node_head_logs, "GCS is in passive mode") == 1


async def test_node_updates_proceed_when_active(make_node_head, monkeypatch):
    """When leader, _subscribe_for_node_updates proceeds, yields nodes, and registers head node."""
    head = make_node_head(leader=True)

    nodes = [_alive_head_node(HEAD_NODE_ID)]

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
    make_node_head, node_head_logs, monkeypatch
):
    """One bad update must not end the loop: it is the only writer of nodes."""
    head = make_node_head(leader=True)
    nodes = [_alive_head_node(HEAD_NODE_ID), _dead_worker_node(DEAD_NODE_ID)]

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
    assert _count_logged(node_head_logs, f"Failed updating node {HEAD_NODE_ID}") == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
