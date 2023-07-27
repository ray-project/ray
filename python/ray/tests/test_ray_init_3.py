import os
import sys
import pytest
import ray
from ray.cluster_utils import Cluster


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="This test is only run on linux CI machines.",
)
def test_ray_init_using_hostname(ray_start_cluster):
    import socket

    hostname = socket.gethostname()
    cluster = Cluster(
        initialize_head=True,
        head_node_args={
            "node_ip_address": hostname,
        },
    )

    # Use `ray.init` to test the connection.
    ray.init(address=cluster.address, _node_ip_address=hostname)

    node_table = cluster.global_state.node_table()
    assert len(node_table) == 1
    assert node_table[0].get("NodeManagerHostname", "") == hostname


if __name__ == "__main__":
    import sys

    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
