# Unit tests for ray._private.node.Node that do not need a running Ray instance.
import sys
from unittest.mock import MagicMock

import pytest

import ray._private.ray_constants as ray_constants
from ray._private.node import Node
from ray._raylet import GRPC_STATUS_CODE_UNAVAILABLE
from ray.exceptions import GcsPassiveError, RpcError

SESSION_NAME = "session_2026-01-01_00-00-00_000000_1"


def _passive_gcs_rejection():
    """The error a passive GCS raises, as check_status() translates it."""
    return GcsPassiveError(
        "GCS server is in passive (read-only) mode.",
        rpc_code=GRPC_STATUS_CODE_UNAVAILABLE,
    )


@pytest.fixture(autouse=True)
def leader_election_on(monkeypatch):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", True)


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

    node._write_cluster_info_to_kv()


def test_write_cluster_info_still_fails_on_an_unreachable_gcs():
    # An unreachable GCS also yields UNAVAILABLE; only the passive one is benign.
    node = _make_node(passive=False)
    node._gcs_client.internal_kv_put.side_effect = RpcError(
        "Unavailable", rpc_code=GRPC_STATUS_CODE_UNAVAILABLE
    )

    with pytest.raises(RpcError):
        node._write_cluster_info_to_kv()


def test_write_cluster_info_still_fails_with_leader_election_off(monkeypatch):
    # No GCS is passive with the feature off, so the rejection is a real error.
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", False)
    node = _make_node(passive=True)

    with pytest.raises(GcsPassiveError):
        node._write_cluster_info_to_kv()


def test_write_cluster_info_leaves_the_rocksdb_marker_alone_when_passive():
    # The marker file has no compare-and-set, so a passive head must not reach it.
    node = _make_node(passive=True, is_rocksdb=True)

    node._write_cluster_info_to_kv()

    node._persist_rocksdb_session_name_file.assert_not_called()


def test_write_cluster_info_writes_everything_when_active():
    node = _make_node(passive=False)
    node._ray_params.tracing_startup_hook = "my.module:hook"

    node._write_cluster_info_to_kv()

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


@pytest.mark.parametrize("passive", [True, False])
def test_start_api_server_survives_a_passive_gcs(monkeypatch, passive):
    node = _prepare_for_api_server(_make_node(passive=passive), monkeypatch)

    node.start_api_server(include_dashboard=True, raise_on_failure=True)

    attempted_keys = [
        call.args[0] for call in node._gcs_client.internal_kv_put.call_args_list
    ]
    assert attempted_keys == [b"webui:url"]


def test_start_api_server_still_fails_with_leader_election_off(monkeypatch):
    monkeypatch.setattr(ray_constants, "RAY_ENABLE_GCS_LEADER_ELECTION", False)
    node = _prepare_for_api_server(_make_node(passive=True), monkeypatch)

    with pytest.raises(GcsPassiveError):
        node.start_api_server(include_dashboard=True, raise_on_failure=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
