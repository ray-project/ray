# Unit tests for ray._private.node.Node that do not need a running Ray instance.
import sys
from unittest.mock import MagicMock

import pytest

import ray._private.ray_constants as ray_constants
from ray._private.node import Node
from ray._raylet import GRPC_STATUS_CODE_UNAVAILABLE
from ray.exceptions import GcsPassiveError, RpcError
from ray.tests.unit.passive_test_utils import (
    passive_gcs_rejection as _passive_gcs_rejection,
)

SESSION_NAME = "session_2026-01-01_00-00-00_000000_1"

pytestmark = pytest.mark.usefixtures("leader_election_on")


def _make_node(*, passive, is_rocksdb=False):
    """Build a Node stub exercising only the cluster-info KV write path."""
    node = Node.__new__(Node)
    node._session_name = SESSION_NAME
    node.ray_init_cluster = False
    node._webui_url = "127.0.0.1:8265"
    node.all_processes = {}

    gcs_client = MagicMock()
    if passive:
        gcs_client.internal_kv_put.side_effect = _passive_gcs_rejection()
    else:
        # A new key is inserted, matching a first-time active head.
        gcs_client.internal_kv_put.return_value = True
    node._gcs_client = gcs_client

    node._ray_params = MagicMock()
    node._ray_params.tracing_startup_hook = None
    node._is_rocksdb_gcs = lambda: is_rocksdb
    node._persist_rocksdb_session_name_file = MagicMock()
    return node


def test_write_cluster_info_survives_a_passive_gcs():
    node = _make_node(passive=True)

    assert node._write_cluster_info_to_kv() is True


@pytest.mark.parametrize(
    "flag_on, exc, expected_exc",
    [
        (False, _passive_gcs_rejection(), GcsPassiveError),
        (
            True,
            RpcError("Unavailable", rpc_code=GRPC_STATUS_CODE_UNAVAILABLE),
            RpcError,
        ),
    ],
)
def test_write_cluster_info_reraises_errors(monkeypatch, flag_on, exc, expected_exc):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", flag_on)
    node = _make_node(passive=False)
    node._gcs_client.internal_kv_put.side_effect = exc

    with pytest.raises(expected_exc):
        node._write_cluster_info_to_kv()


def test_write_cluster_info_writes_everything_when_active():
    node = _make_node(passive=False)
    node._ray_params.tracing_startup_hook = "my.module:hook"

    assert node._write_cluster_info_to_kv() is False

    written_keys = [
        call.args[0] for call in node._gcs_client.internal_kv_put.call_args_list
    ]
    assert b"session_name" in written_keys
    assert b"tracing_startup_hook" in written_keys


def test_write_cluster_info_recovers_the_session_name_on_an_active_restart():
    node = _make_node(passive=False)
    node._gcs_client.internal_kv_put.return_value = False
    node._gcs_client.internal_kv_get.return_value = SESSION_NAME.encode()

    node._write_cluster_info_to_kv()

    node._gcs_client.internal_kv_get.assert_called_once_with(
        b"session_name", ray_constants.KV_NAMESPACE_SESSION
    )


def _prepare_for_api_server(node, monkeypatch):
    """Fill in the attributes Node.start_api_server() reads."""
    node.cluster_id = MagicMock()
    node.temp_dir = "/tmp/ray"
    node._logs_dir = "/tmp/ray/logs"
    node._session_dir = "/tmp/ray/session"
    node._node_ip_address = "127.0.0.1"
    node._gcs_address = "127.0.0.1:6379"
    node.kernel_fate_share = False
    node.max_bytes = 0
    node.backup_count = 0
    node.get_log_file_names = lambda *args, **kwargs: (None, None)
    monkeypatch.setattr(
        "ray._private.services.start_api_server",
        lambda *args, **kwargs: ("127.0.0.1:8265", MagicMock()),
    )
    return node


@pytest.mark.parametrize(
    "flag_on, passive, should_raise",
    [
        (True, True, False),
        (True, False, False),
        (False, True, True),
    ],
)
def test_start_api_server_passive_handling(monkeypatch, flag_on, passive, should_raise):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", flag_on)
    node = _prepare_for_api_server(_make_node(passive=passive), monkeypatch)

    if should_raise:
        with pytest.raises(GcsPassiveError):
            node.start_api_server(include_dashboard=True, raise_on_failure=True)
    else:
        node.start_api_server(include_dashboard=True, raise_on_failure=True)
        attempted_keys = [
            call.args[0] for call in node._gcs_client.internal_kv_put.call_args_list
        ]
        assert attempted_keys == [b"webui:url"]


def test_start_api_server_keeps_the_dashboard_when_passive(monkeypatch):
    node = _prepare_for_api_server(_make_node(passive=True), monkeypatch)
    process_info = MagicMock()
    forwarded = {}

    def fake_start_api_server(*args, **kwargs):
        forwarded.update(kwargs)
        # A passive GCS never accepts the dashboard's address.
        return None, process_info

    monkeypatch.setattr("ray._private.services.start_api_server", fake_start_api_server)

    node.start_api_server(
        include_dashboard=True, raise_on_failure=True, gcs_is_passive=True
    )

    assert forwarded["gcs_is_passive"] is True
    # The dashboard head writes webui:url once this head is promoted.
    node._gcs_client.internal_kv_put.assert_not_called()
    # It is still a child of this node, so it must be shut down with it.
    assert node.all_processes[ray_constants.PROCESS_TYPE_DASHBOARD] == [process_info]


def test_start_api_server_forwards_the_tracing_startup_hook(monkeypatch):
    """The dashboard head needs it to republish the key after a promotion."""
    node = _prepare_for_api_server(_make_node(passive=True), monkeypatch)
    node._ray_params.tracing_startup_hook = "my.module:hook"
    forwarded = {}

    def fake_start_api_server(*args, **kwargs):
        forwarded.update(kwargs)
        return None, MagicMock()

    monkeypatch.setattr("ray._private.services.start_api_server", fake_start_api_server)

    node.start_api_server(include_dashboard=True, raise_on_failure=True)

    assert forwarded["tracing_startup_hook"] == "my.module:hook"


@pytest.mark.parametrize("passive", [True, False])
def test_start_head_processes_forwards_the_passive_gcs_state(monkeypatch, passive):
    node = _prepare_for_api_server(_make_node(passive=passive), monkeypatch)
    node._gcs_address = None
    node._gcs_client = None
    node._ray_params.no_monitor = True
    node._ray_params.ray_client_server_port = None
    node._ray_params.include_dashboard = None
    node.start_gcs_server = MagicMock()
    node.get_gcs_client = lambda: MagicMock()
    node._write_cluster_info_to_kv = MagicMock(return_value=passive)
    node.start_api_server = MagicMock()

    node.start_head_processes()

    node.start_api_server.assert_called_once_with(
        include_dashboard=None,
        raise_on_failure=False,
        gcs_is_passive=passive,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
