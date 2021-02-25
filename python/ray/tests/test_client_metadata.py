import pytest

from ray.util.client.ray_client_helpers import ray_start_client_server
from ray._raylet import NodeID

from ray.runtime_context import RuntimeContext


def test_get_ray_metadata(ray_start_regular_shared):
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


def test_get_runtime_context(ray_start_regular_shared):
    """Test the get_runtime_context data through the metadata API"""
    with ray_start_client_server() as ray:
        rtc = ray.get_runtime_context()
        assert isinstance(rtc, RuntimeContext)
        assert isinstance(rtc.node_id, NodeID)
        assert len(rtc.node_id.hex()) == 56

        with pytest.raises(Exception):
            _ = rtc.task_id


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
