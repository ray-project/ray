# Unit tests for how the dashboard's node head reacts to a passive GCS.
import asyncio
import logging
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

import ray._private.ray_constants as ray_constants
import ray.dashboard.modules.node.node_head as node_head_module
import ray.experimental.internal_kv as internal_kv
from ray._raylet import GRPC_STATUS_CODE_UNAVAILABLE
from ray.dashboard.modules.node.datacenter import DataSource
from ray.dashboard.modules.node.node_head import NodeHead
from ray.dashboard.subprocesses.module import SubprocessModuleConfig
from ray.exceptions import GcsPassiveError, RpcError

NODE_HEAD_LOGGER = "ray.dashboard.modules.node.node_head"

HEAD_NODE_ID = "a" * 56
NEW_HEAD_NODE_ID = "b" * 56
DEAD_NODE_ID = "c" * 56


def _passive_gcs_rejection():
    """The error a passive GCS raises, as check_status() translates it."""
    return GcsPassiveError(
        "GCS server is in passive (read-only) mode.",
        rpc_code=GRPC_STATUS_CODE_UNAVAILABLE,
    )


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


def _head_node_puts(gcs_client):
    """The value of every head node id write attempted."""
    return [
        call.args[1]
        for call in gcs_client.async_internal_kv_put.call_args_list
        if call.args[0] == ray_constants.KV_HEAD_NODE_ID_KEY
    ]


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
    gcs_client.async_internal_kv_del = AsyncMock(
        side_effect=None if leader else _passive_gcs_rejection()
    )
    gcs_client.is_gcs_leader = MagicMock(return_value=leader)
    gcs_client.is_gcs_leader_local = MagicMock(return_value=leader)
    gcs_client.async_check_alive = AsyncMock()
    return gcs_client


def _set_leader(gcs_client, leader):
    gcs_client.async_internal_kv_put.side_effect = (
        None if leader else _passive_gcs_rejection()
    )
    gcs_client.async_internal_kv_del.side_effect = (
        None if leader else _passive_gcs_rejection()
    )
    gcs_client.is_gcs_leader.return_value = leader
    gcs_client.is_gcs_leader_local.return_value = leader


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


async def _drain_retry(head):
    """Wait for the retry task the refusal spawned."""
    await asyncio.wait_for(asyncio.gather(*head._background_tasks), timeout=10)


async def test_head_node_id_is_registered_when_active(make_node_head, node_head_logs):
    head = make_node_head(leader=True)

    await head._put_head_node_id(HEAD_NODE_ID)

    assert _head_node_puts(head.gcs_client) == [HEAD_NODE_ID.encode()]
    assert head._registered_head_node_id == HEAD_NODE_ID
    assert head._head_node_registration_time_s is not None
    assert head._background_tasks == set()
    assert _count_logged(node_head_logs, "GCS is in passive mode") == 0


async def test_a_refused_head_node_id_is_not_recorded(make_node_head):
    """Recording it would stop every later node update from retrying."""
    head = make_node_head(leader=False)

    await head._put_head_node_id(HEAD_NODE_ID)

    assert head._registered_head_node_id is None
    assert head._head_node_registration_time_s is None


async def test_node_updates_wait_for_leader_when_passive(
    make_node_head, node_head_logs
):
    """Subscription loop must not start or yield until promoted to leader."""
    head = make_node_head(leader=False)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            head._subscribe_for_node_updates().__anext__(), timeout=0.05
        )

    assert _count_logged(node_head_logs, "GCS is in passive mode") == 1


async def test_node_updates_proceed_when_active(make_node_head, monkeypatch):
    """When leader, _subscribe_for_node_updates proceeds and yields nodes."""
    head = make_node_head(leader=True)

    nodes = [_alive_head_node(HEAD_NODE_ID)]

    async def _fake_subscribe():
        for node in nodes:
            yield node

    monkeypatch.setattr(head, "_subscribe_for_node_updates", _fake_subscribe)

    await head._update_nodes()
    assert head._registered_head_node_id == HEAD_NODE_ID
    assert _head_node_puts(head.gcs_client) == [HEAD_NODE_ID.encode()]


async def test_actor_updates_wait_for_leader_when_passive(
    make_node_head, node_head_logs
):
    """Actor updates must not run GetAllActorInfo until promoted to leader."""
    head = make_node_head(leader=False)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(head._update_actors(), timeout=0.05)

    assert _count_logged(node_head_logs, "GCS is in passive mode") == 1


async def test_head_node_id_reraises_passive_rejection_when_flag_off(
    monkeypatch, make_node_head
):
    head = make_node_head(leader=False)
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", False)

    with pytest.raises(GcsPassiveError):
        await head._put_head_node_id(HEAD_NODE_ID)


async def test_head_node_id_reraises_other_rpc_errors(make_node_head):
    head = make_node_head(leader=True)
    head.gcs_client.async_internal_kv_put.side_effect = RpcError(
        "boom", rpc_code=GRPC_STATUS_CODE_UNAVAILABLE
    )

    with pytest.raises(RpcError):
        await head._put_head_node_id(HEAD_NODE_ID)


async def test_a_passive_gcs_does_not_stop_the_node_updates(make_node_head):
    """The refusal used to propagate out and kill the subscription loop."""
    head = make_node_head(leader=False)

    await head._update_node(_alive_head_node(HEAD_NODE_ID))

    assert HEAD_NODE_ID in DataSource.nodes


#
# Dead node cleanup
#


async def test_a_dead_node_is_still_recorded_when_the_cleanup_is_refused(
    make_node_head,
):
    """The bookkeeping after the delete must not be skipped."""
    head = make_node_head(leader=False)

    await head._update_node(_dead_worker_node(DEAD_NODE_ID))

    head.gcs_client.async_internal_kv_del.assert_awaited()
    assert DEAD_NODE_ID in DataSource.nodes
    assert list(head._dead_node_queue) == [DEAD_NODE_ID]


async def test_the_cleanup_reraises_passive_rejection_when_flag_off(
    monkeypatch, make_node_head
):
    head = make_node_head(leader=False)
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", False)

    with pytest.raises(GcsPassiveError):
        await head._delete_agent_addresses(_dead_worker_node(DEAD_NODE_ID))


async def test_the_cleanup_reraises_other_rpc_errors(make_node_head):
    head = make_node_head(leader=True)
    head.gcs_client.async_internal_kv_del.side_effect = RpcError(
        "boom", rpc_code=GRPC_STATUS_CODE_UNAVAILABLE
    )

    with pytest.raises(RpcError):
        await head._delete_agent_addresses(_dead_worker_node(DEAD_NODE_ID))


#
# Subscription containment
#


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
