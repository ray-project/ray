# Unit tests for how the dashboard head and agent react to a passive GCS.
import asyncio
import json
import logging
import sys
from unittest.mock import ANY

import pytest

import ray
import ray._common.usage.usage_constants as usage_constant
import ray._private.ray_constants as ray_constants
import ray.dashboard.agent as agent_module
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.head as head_module
import ray.experimental.internal_kv as internal_kv
from ray._raylet import GRPC_STATUS_CODE_UNAVAILABLE
from ray.dashboard.agent import DashboardAgent
from ray.dashboard.head import DashboardHead
from ray.exceptions import GcsPassiveError, RpcError
from ray.tests.unit.passive_test_utils import (
    count_logged as _count_logged,
    make_mock_gcs_client,
    passive_gcs_rejection as _passive_gcs_rejection,
    set_mock_leader as _set_leader,
)

HEAD_LOGGER = "ray.dashboard.head"
AGENT_LOGGER = "ray.dashboard.agent"

NODE_ID = "a" * 56
NODE_IP = "1.2.3.4"
HTTP_PORT = 8265
SESSION_NAME = "session_2026-01-01_00-00-00_000000_1"
METRICS_ADDRESS = f"{NODE_IP}:{head_module.DASHBOARD_METRIC_PORT}"
DASHBOARD_ADDRESS = f"{NODE_IP}:{HTTP_PORT}"
TRACING_HOOK = "my.module:setup_tracing"


def _puts(gcs_client):
    """The (key, value) of every internal_kv_put the client was asked to do."""
    return [
        (call.args[0], call.args[1])
        for call in gcs_client.async_internal_kv_put.call_args_list
    ]


def _put_args(gcs_client, key):
    """The (value, overwrite, namespace) one key was written with."""
    for call in gcs_client.async_internal_kv_put.call_args_list:
        if call.args[0] == key:
            return call.args[1], call.args[2], call.kwargs["namespace"]
    return None


pytestmark = pytest.mark.usefixtures("enable_passive_gcs")


@pytest.fixture
def head_logs(caplog, capture_logger):
    capture_logger(HEAD_LOGGER)
    return caplog


@pytest.fixture
def agent_logs(caplog, capture_logger):
    capture_logger(AGENT_LOGGER)
    return caplog


@pytest.fixture
def make_head(monkeypatch, tmp_path):
    def factory(*, leader, minimal=False, tracing_startup_hook=None):
        # Otherwise _setup_metrics binds a real metrics HTTP server.
        monkeypatch.setattr(head_module, "prometheus_client", None)
        # DashboardHead derives session_name from the session directory name.
        session_dir = tmp_path / SESSION_NAME
        session_dir.mkdir(exist_ok=True)
        head = DashboardHead(
            http_host=NODE_IP,
            http_port=HTTP_PORT,
            http_port_retries=0,
            gcs_address="127.0.0.1:6379",
            cluster_id_hex="f" * 56,
            node_ip_address=NODE_IP,
            log_dir=str(tmp_path),
            logging_level=logging.INFO,
            logging_format="%(message)s",
            logging_filename="dashboard.log",
            logging_rotate_bytes=1,
            logging_rotate_backup_count=1,
            temp_dir=str(tmp_path),
            session_dir=str(session_dir),
            minimal=minimal,
            serve_frontend=False,
            tracing_startup_hook=tracing_startup_hook,
        )
        # Assigned by run(), which these tests do not go through.
        head.gcs_client = make_mock_gcs_client(leader=leader)
        internal_kv._initialize_internal_kv(head.gcs_client)
        return head

    return factory


@pytest.fixture
def make_agent(monkeypatch):
    def factory(*, leader):
        monkeypatch.setenv("RAY_NODE_ID", NODE_ID)
        gcs_client = make_mock_gcs_client(leader=leader)
        monkeypatch.setattr(agent_module, "GcsClient", lambda **kwargs: gcs_client)
        # Writes a real ports file next to the session dir otherwise.
        monkeypatch.setattr(agent_module, "persist_port", lambda *args: None)
        return DashboardAgent(
            node_ip_address=NODE_IP,
            grpc_port=0,
            gcs_address="127.0.0.1:6379",
            cluster_id_hex="f" * 56,
            minimal=True,
            object_store_name="store",
            raylet_name="raylet",
            log_dir="/tmp",
            temp_dir="/tmp",
            session_dir="/tmp",
            session_name="session",
        )

    return factory


async def _register_dashboard_address(head):
    await head._put_address(
        ray_constants.DASHBOARD_ADDRESS.encode(),
        DASHBOARD_ADDRESS,
        ray_constants.KV_NAMESPACE_DASHBOARD,
    )


async def _await_registration(head):
    """Run _register_addresses_loop to completion, i.e. until it stops waiting."""
    await asyncio.wait_for(head._register_addresses_loop(), timeout=10)


async def _registration_keeps_waiting(head):
    """Let _register_addresses_loop spin a few times without it finishing."""
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(head._register_addresses_loop(), timeout=0.1)


# ==============================================================================
# Dashboard head
# ==============================================================================


async def test_head_registers_both_addresses_when_active(make_head, head_logs):
    """Verifies active Dashboard Head registers metrics and HTTP dashboard addresses immediately."""
    head = make_head(leader=True)

    await head._setup_metrics()
    head._dashboard_address = DASHBOARD_ADDRESS
    await _register_dashboard_address(head)

    assert _puts(head.gcs_client) == [
        (b"DashboardMetricsAddress", METRICS_ADDRESS.encode()),
        (ray_constants.DASHBOARD_ADDRESS.encode(), DASHBOARD_ADDRESS.encode()),
    ]
    assert not head._dashboard_passive_latch.waiting_for_promotion
    assert _count_logged(head_logs, "GCS is in passive mode") == 0


async def test_head_survives_a_passive_rejection(make_head, head_logs):
    """Verifies Dashboard Head catches passive write refusals without crashing."""
    head = make_head(leader=False)

    # Neither write may take the dashboard process down.
    await head._setup_metrics()
    head._dashboard_address = DASHBOARD_ADDRESS
    await _register_dashboard_address(head)

    assert head._dashboard_passive_latch.waiting_for_promotion
    # Once for the window, not once per refused write.
    assert _count_logged(head_logs, "GCS is in passive mode") == 1


@pytest.mark.parametrize(
    "flag_on, exc, expected_exc",
    [
        (False, _passive_gcs_rejection(), GcsPassiveError),
        (
            True,
            RpcError("boom", rpc_code=GRPC_STATUS_CODE_UNAVAILABLE),
            RpcError,
        ),
    ],
)
async def test_head_reraises_errors(monkeypatch, make_head, flag_on, exc, expected_exc):
    head = make_head(leader=True)
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", flag_on)
    head.gcs_client.async_internal_kv_put.side_effect = exc

    with pytest.raises(expected_exc):
        await _register_dashboard_address(head)
    assert not head._dashboard_passive_latch.waiting_for_promotion


async def test_head_replays_every_address_on_promotion(make_head, head_logs):
    """Verifies a Dashboard Head replays both metrics and HTTP dashboard addresses on promotion to leader."""
    head = make_head(leader=False)
    await head._setup_metrics()
    head._dashboard_address = DASHBOARD_ADDRESS
    await _register_dashboard_address(head)
    head.gcs_client.async_internal_kv_put.reset_mock()

    _set_leader(head.gcs_client, True)
    await _await_registration(head)

    assert _puts(head.gcs_client) == [
        (b"DashboardMetricsAddress", METRICS_ADDRESS.encode()),
        (ray_constants.DASHBOARD_ADDRESS.encode(), DASHBOARD_ADDRESS.encode()),
        # Node.start_api_server() skips this one on a passive head and exits.
        (b"webui:url", DASHBOARD_ADDRESS.encode()),
        # Node._write_cluster_info_to_kv() skips these two the same way.
        (b"session_name", SESSION_NAME.encode()),
        (usage_constant.CLUSTER_METADATA_KEY, ANY),
    ]
    assert not head._dashboard_passive_latch.waiting_for_promotion
    assert _count_logged(head_logs, "GCS was promoted to leader") == 1


async def test_head_does_not_overwrite_an_existing_session_name(make_head):
    """session_name is a cluster-lifetime invariant, unlike the addresses."""
    head = make_head(leader=False)
    head._dashboard_address = DASHBOARD_ADDRESS
    await _register_dashboard_address(head)

    _set_leader(head.gcs_client, True)
    await _await_registration(head)

    assert _put_args(head.gcs_client, b"session_name") == (
        SESSION_NAME.encode(),
        False,
        ray_constants.KV_NAMESPACE_SESSION,
    )
    assert _put_args(head.gcs_client, b"webui:url")[1] is True


async def test_head_replaces_the_previous_head_s_cluster_metadata(make_head):
    """`Node.__init__` overwrites it on every head start; a promotion is one.

    Leaving the dead head's copy would version-check joining workers against a
    head that is gone, which breaks a rolling upgrade.
    """
    head = make_head(leader=False)
    head._dashboard_address = DASHBOARD_ADDRESS
    await _register_dashboard_address(head)

    _set_leader(head.gcs_client, True)
    await _await_registration(head)

    value, overwrite, namespace = _put_args(
        head.gcs_client, usage_constant.CLUSTER_METADATA_KEY
    )
    assert overwrite is True
    assert namespace == ray_constants.KV_NAMESPACE_CLUSTER
    metadata = json.loads(value)
    assert metadata["ray_version"] == ray.__version__
    # The dashboard never sees Node's ctor argument, and a standby head is
    # always started by `ray start --head`.
    assert metadata["ray_init_cluster"] is False


@pytest.mark.parametrize(
    "hook, should_replay",
    [(TRACING_HOOK, True), (None, False), ("", False)],
)
async def test_head_tracing_startup_hook_replay(make_head, hook, should_replay):
    """`ray start --tracing-startup-hook` writes it, and that head is gone."""
    head = make_head(leader=False, tracing_startup_hook=hook)
    head._dashboard_address = DASHBOARD_ADDRESS
    await _register_dashboard_address(head)

    _set_leader(head.gcs_client, True)
    await _await_registration(head)

    if should_replay:
        assert _put_args(head.gcs_client, b"tracing_startup_hook") == (
            TRACING_HOOK.encode(),
            True,
            ray_constants.KV_NAMESPACE_TRACING,
        )
    else:
        assert _put_args(head.gcs_client, b"tracing_startup_hook") is None
    assert not head._dashboard_passive_latch.waiting_for_promotion


async def test_head_replays_all_named_replay_keys_on_promotion(make_head):
    """Assert every key in HEAD_PROMOTION_REPLAY_KEYS is replayed on promotion."""
    head = make_head(leader=False, tracing_startup_hook=TRACING_HOOK)
    await head._setup_metrics()
    head._dashboard_address = DASHBOARD_ADDRESS
    await _register_dashboard_address(head)
    head.gcs_client.async_internal_kv_put.reset_mock()

    _set_leader(head.gcs_client, True)
    await _await_registration(head)

    replayed = {
        (call.args[0], call.kwargs.get("namespace"))
        for call in head.gcs_client.async_internal_kv_put.call_args_list
    }
    expected = set(dashboard_consts.HEAD_PROMOTION_REPLAY_KEYS)
    assert replayed == expected


async def test_head_stops_polling_once_registered(make_head):
    head = make_head(leader=False)
    head._dashboard_address = DASHBOARD_ADDRESS
    await _register_dashboard_address(head)

    _set_leader(head.gcs_client, True)
    await _await_registration(head)
    head.gcs_client.is_gcs_leader_local.reset_mock()
    head.gcs_client.async_internal_kv_put.reset_mock()
    await _await_registration(head)

    # A leading head is left with nothing to poll.
    head.gcs_client.is_gcs_leader_local.assert_not_called()
    assert _puts(head.gcs_client) == []


async def test_head_keeps_waiting_while_passive_or_replay_refused(make_head, head_logs):
    """Loop stays waiting when passive, and continues waiting silently if replay writes fail."""
    head = make_head(leader=False)
    head._dashboard_address = DASHBOARD_ADDRESS
    await _register_dashboard_address(head)
    head.gcs_client.async_internal_kv_put.reset_mock()

    # 1. While still passive: no replay writes are attempted.
    await _registration_keeps_waiting(head)
    assert _puts(head.gcs_client) == []
    assert head._dashboard_passive_latch.waiting_for_promotion

    # 2. Local cache flips to leader, but writes are refused (race condition):
    # attempts replay, stays waiting, and does not spam logs.
    head.gcs_client.is_gcs_leader_local.return_value = True
    await _registration_keeps_waiting(head)
    assert len(_puts(head.gcs_client)) > 0
    assert head._dashboard_passive_latch.waiting_for_promotion
    assert _count_logged(head_logs, "GCS is in passive mode") == 1


async def test_head_does_not_poll_when_flag_off(monkeypatch, make_head):
    head = make_head(leader=False)
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", False)

    # Nothing latches the loop, so it never runs an iteration.
    await _await_registration(head)

    head.gcs_client.is_gcs_leader_local.assert_not_called()
    assert _puts(head.gcs_client) == []


# ==============================================================================
# Dashboard agent
# ==============================================================================


def _agent_puts(gcs_client):
    return {key: json.loads(value) for key, value in _puts(gcs_client)}


@pytest.mark.parametrize("leader", [True, False])
async def test_agent_registration(make_agent, agent_logs, leader):
    agent = make_agent(leader=leader)
    retry_task = await agent._register_agent_address(1, 2)

    if leader:
        assert retry_task is None
        assert _agent_puts(agent.gcs_client) == {
            f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{NODE_ID}".encode(): [
                NODE_IP,
                1,
                2,
            ],
            f"{dashboard_consts.DASHBOARD_AGENT_ADDR_IP_PREFIX}{NODE_IP}".encode(): [
                NODE_ID,
                1,
                2,
            ],
        }
        assert _count_logged(agent_logs, "GCS is in passive mode") == 0
    else:
        assert retry_task is not None
        retry_task.cancel()
        assert _count_logged(agent_logs, "GCS is in passive mode") == 1


@pytest.mark.parametrize(
    "flag_on, exc, expected_exc",
    [
        (False, _passive_gcs_rejection(), GcsPassiveError),
        (
            True,
            RpcError("boom", rpc_code=GRPC_STATUS_CODE_UNAVAILABLE),
            RpcError,
        ),
    ],
)
async def test_agent_reraises_errors(
    monkeypatch, make_agent, flag_on, exc, expected_exc
):
    agent = make_agent(leader=True)
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", flag_on)
    agent.gcs_client.async_internal_kv_put.side_effect = exc

    with pytest.raises(expected_exc):
        await agent._register_agent_address(1, 2)


async def test_agent_registers_on_promotion(make_agent, agent_logs):
    agent = make_agent(leader=False)
    retry_task = await agent._register_agent_address(1, 2)
    agent.gcs_client.async_internal_kv_put.reset_mock()

    _set_leader(agent.gcs_client, True)
    await asyncio.wait_for(retry_task, timeout=10)

    assert len(_agent_puts(agent.gcs_client)) == 2
    assert _count_logged(agent_logs, "GCS was promoted to leader") == 1


async def test_agent_retry_loop_reraises_other_errors(make_agent):
    """Only a refusal is retried; anything else still takes the agent down."""
    agent = make_agent(leader=False)
    agent.gcs_client.async_internal_kv_put.side_effect = RpcError(
        "boom", rpc_code=GRPC_STATUS_CODE_UNAVAILABLE
    )

    with pytest.raises(RpcError):
        await asyncio.wait_for(agent._retry_agent_address(1, 2), timeout=10)


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
