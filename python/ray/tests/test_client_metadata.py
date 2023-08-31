import pytest

from ray.util.client.ray_client_helpers import ray_start_client_server
from ray._raylet import NodeID

from ray.runtime_context import RuntimeContext


@pytest.fixture
def short_gcs_client_timeout(monkeypatch):
    monkeypatch.setenv("RAY_gcs_rpc_server_connect_timeout_s", "1")
    monkeypatch.setenv("RAY_gcs_rpc_server_reconnect_timeout_s", "1")
    monkeypatch.setenv("RAY_gcs_grpc_initial_reconnect_backoff_ms", "1")
    monkeypatch.setenv("RAY_gcs_grpc_min_reconnect_backoff_ms", "1")
    monkeypatch.setenv("RAY_gcs_grpc_max_reconnect_backoff_ms", "1")
    yield


def test_get_ray_metadata(short_gcs_client_timeout, ray_start_regular_shared):
    """Test the ClusterInfo client data pathway and API surface"""
    with ray_start_client_server() as ray:
        ip_address = ray_start_regular_shared["node_ip_address"]

        initialized = ray.is_initialized()
        assert initialized

        nodes = ray.nodes()
        assert len(nodes) == 1, nodes
        assert nodes[0]["NodeManagerAddress"] == ip_address

        current_node_id = "node:" + ip_address

        cluster_resources = ray.cluster_resources()
        available_resources = ray.available_resources()

        assert cluster_resources["CPU"] == 1.0
        assert current_node_id in cluster_resources
        assert current_node_id in available_resources


def test_get_runtime_context(short_gcs_client_timeout, ray_start_regular_shared):
    """Test the get_runtime_context data through the metadata API"""
    with ray_start_client_server() as ray:
        rtc = ray.get_runtime_context()
        assert isinstance(rtc, RuntimeContext)
        assert isinstance(rtc.node_id, NodeID)
        assert len(rtc.node_id.hex()) == 56
        assert isinstance(rtc.namespace, str)

        # Ensure this doesn't throw
        ray.get_runtime_context().get()

        with pytest.raises(Exception):
            _ = rtc.task_id


if __name__ == "__main__":
    import os
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
