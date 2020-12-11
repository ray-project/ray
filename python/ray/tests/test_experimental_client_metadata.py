from ray.tests.test_experimental_client import ray_start_client_server


def test_get_ray_metadata(ray_start_regular_shared):
    """
    Test the ClusterInfo client data pathway and API surface
    """
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
