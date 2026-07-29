import sys

import pytest

from ray._private.services import get_node


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "gcs_server_port": 0,
            "metrics_export_port": 0,
            "metrics_agent_port": 0,
            "runtime_env_agent_port": 0,
            "dashboard_agent_listen_port": 0,
        }
    ],
    indirect=True,
)
def test_local_port_service_discovery(ray_start_cluster_head):
    """
    Test that when all ports are set to 0 (auto-assign), all components
    self-bind to available ports and the port information is correctly
    reported to GCS.

    """
    cluster = ray_start_cluster_head
    gcs_address = cluster.gcs_address
    node_id = cluster.head_node.node_id

    # We won't be able to get node info if GCS port didn't report correctly
    # So, get_node implicitly validate GCS port reporting.
    node_info = get_node(gcs_address, node_id)

    port_fields = [
        "metrics_export_port",
        "metrics_agent_port",
        "runtime_env_agent_port",
        "dashboard_agent_listen_port",
    ]
    for field in port_fields:
        port = node_info.get(field)
        assert port and port > 0, f"{field} should be > 0, got {port}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
