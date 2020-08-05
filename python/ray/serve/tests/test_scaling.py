import sys
import socket

import pytest

import ray
from ray import serve
from ray.cluster_utils import Cluster
from ray.serve.constants import SERVE_PROXY_NAME
from ray.serve.utils import block_until_http_ready
from ray.test_utils import wait_for_condition


@pytest.mark.skipif(
    not hasattr(socket, "SO_REUSEPORT"),
    reason=("Port sharing only works on newer verion of Linux. "
            "This test can only be ran when port sharing is supported."))
def test_multiple_routers():
    cluster = Cluster()
    head_node = cluster.add_node()
    cluster.add_node()

    ray.init(head_node.address)
    node_ids = ray.state.node_ids()
    assert len(node_ids) == 2
    serve.init(http_port=8005)

    def actor_name(index):
        return SERVE_PROXY_NAME + "-{}-{}".format(node_ids[0], index)

    # Two actors should be started.
    def get_first_two_actors():
        try:
            ray.get_actor(actor_name(0))
            ray.get_actor(actor_name(1))
            return True
        except ValueError:
            return False

    wait_for_condition(get_first_two_actors)

    # Wait for the actors to come up.
    ray.get(block_until_http_ready.remote("http://127.0.0.1:8005/-/routes"))

    # Kill one of the servers, the HTTP server should still function.
    ray.kill(ray.get_actor(actor_name(0)), no_restart=True)
    ray.get(block_until_http_ready.remote("http://127.0.0.1:8005/-/routes"))

    # Add a new node to the cluster. This should trigger a new router to get
    # started.
    new_node = cluster.add_node()

    def get_third_actor():
        try:
            ray.get_actor(actor_name(2))
            return True
        except ValueError:
            return False

    wait_for_condition(get_third_actor)

    # Remove the newly-added node from the cluster. The corresponding actor
    # should be removed as well.
    cluster.remove_node(new_node)

    def third_actor_removed():
        try:
            ray.get_actor(actor_name(2))
            return False
        except ValueError:
            return True

    # Check that the actor is gone and the HTTP server still functions.
    wait_for_condition(third_actor_removed)
    ray.get(block_until_http_ready.remote("http://127.0.0.1:8005/-/routes"))

    # Clean up the nodes (otherwise Ray will segfault).
    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
