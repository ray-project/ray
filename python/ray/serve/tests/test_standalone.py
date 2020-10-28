"""
The test file for all standalone tests that doesn't
requires a shared Serve instance.
"""
from random import randint
import sys
import socket

import pytest
import requests

import ray
from ray import serve
from ray.cluster_utils import Cluster
from ray.serve.constants import SERVE_PROXY_NAME
from ray.serve.utils import (block_until_http_ready, get_all_node_ids,
                             format_actor_name, get_node_id_for_actor)
from ray.test_utils import wait_for_condition
from ray._private.services import new_port


def test_detached_deployment():
    # https://github.com/ray-project/ray/issues/11437

    cluster = Cluster()
    head_node = cluster.add_node(node_ip_address="127.0.0.1", num_cpus=6)

    # Create first job, check we can run a simple serve endpoint
    ray.init(head_node.address)
    first_job_id = ray.get_runtime_context().job_id
    client = serve.start(detached=True)
    client.create_backend("f", lambda _: "hello")
    client.create_endpoint("f", backend="f")
    assert ray.get(client.get_handle("f").remote()) == "hello"

    ray.shutdown()

    # Create the second job, make sure we can still create new backends.
    ray.init(head_node.address)
    assert ray.get_runtime_context().job_id != first_job_id

    client = serve.connect()
    client.create_backend("g", lambda _: "world")
    client.create_endpoint("g", backend="g")
    assert ray.get(client.get_handle("g").remote()) == "world"

    # Test passed, clean up.
    client.shutdown()
    ray.shutdown()
    cluster.shutdown()


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
    client = serve.start(http_port=8005)  # noqa: F841

    def get_proxy_names():
        proxy_names = []
        for node_id, _ in get_all_node_ids():
            proxy_names.append(
                format_actor_name(SERVE_PROXY_NAME, client._controller_name,
                                  node_id))
        return proxy_names

    wait_for_condition(lambda: len(get_proxy_names()) == 2)
    proxy_names = get_proxy_names()

    # Two actors should be started.
    def get_first_two_actors():
        try:
            ray.get_actor(proxy_names[0])
            ray.get_actor(proxy_names[1])
            return True
        except ValueError:
            return False

    wait_for_condition(get_first_two_actors)

    # Wait for the actors to come up.
    ray.get(block_until_http_ready.remote("http://127.0.0.1:8005/-/routes"))

    # Kill one of the servers, the HTTP server should still function.
    ray.kill(ray.get_actor(get_proxy_names()[0]), no_restart=True)
    ray.get(block_until_http_ready.remote("http://127.0.0.1:8005/-/routes"))

    # Add a new node to the cluster. This should trigger a new router to get
    # started.
    new_node = cluster.add_node()

    wait_for_condition(lambda: len(get_proxy_names()) == 3)
    third_proxy = get_proxy_names()[2]

    def get_third_actor():
        try:
            ray.get_actor(third_proxy)
            return True
        # IndexErrors covers when cluster resources aren't updated yet.
        except (IndexError, ValueError):
            return False

    wait_for_condition(get_third_actor)

    # Remove the newly-added node from the cluster. The corresponding actor
    # should be removed as well.
    cluster.remove_node(new_node)

    def third_actor_removed():
        try:
            ray.get_actor(third_proxy)
            return False
        except ValueError:
            return True

    # Check that the actor is gone and the HTTP server still functions.
    wait_for_condition(third_actor_removed)
    ray.get(block_until_http_ready.remote("http://127.0.0.1:8005/-/routes"))

    # Clean up the nodes (otherwise Ray will segfault).
    ray.shutdown()
    cluster.shutdown()


def test_middleware():
    from starlette.middleware import Middleware
    from starlette.middleware.cors import CORSMiddleware

    port = new_port()
    serve.start(
        http_port=port,
        http_middlewares=[
            Middleware(
                CORSMiddleware, allow_origins=["*"], allow_methods=["*"])
        ])
    ray.get(block_until_http_ready.remote(f"http://127.0.0.1:{port}/-/routes"))

    # Snatched several test cases from Starlette
    # https://github.com/encode/starlette/blob/master/tests/
    # middleware/test_cors.py
    headers = {
        "Origin": "https://example.org",
        "Access-Control-Request-Method": "GET",
    }
    root = f"http://localhost:{port}"
    resp = requests.options(root, headers=headers)
    assert resp.headers["access-control-allow-origin"] == "*"

    resp = requests.get(f"{root}/-/routes", headers=headers)
    assert resp.headers["access-control-allow-origin"] == "*"

    ray.shutdown()


@pytest.mark.skipif(
    not hasattr(socket, "SO_REUSEPORT"),
    reason=("Port sharing only works on newer verion of Linux. "
            "This test can only be ran when port sharing is supported."))
def test_cluster_handle_affinity():
    cluster = Cluster()
    # HACK: using two different ip address so the placement constraint for
    # resource check later will work.
    head_node = cluster.add_node(node_ip_address="127.0.0.1", num_cpus=4)
    cluster.add_node(node_ip_address="0.0.0.0", num_cpus=4)

    ray.init(head_node.address)

    # Make sure we have two nodes.
    node_ids = [n["NodeID"] for n in ray.nodes()]
    assert len(node_ids) == 2

    # Start the backend.
    client = serve.start(http_port=randint(10000, 30000), detached=True)
    client.create_backend("hi:v0", lambda _: "hi")
    client.create_endpoint("hi", backend="hi:v0")

    # Try to retrieve the handle from both head and worker node, check the
    # router's node id.
    @ray.remote
    def check_handle_router_id():
        client = serve.connect()
        handle = client.get_handle("hi")
        return get_node_id_for_actor(handle.router_handle)

    router_node_ids = ray.get([
        check_handle_router_id.options(resources={
            node_id: 0.01
        }).remote() for node_id in ray.state.node_ids()
    ])

    assert set(router_node_ids) == set(node_ids)

    # Clean up the nodes (otherwise Ray will segfault).
    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
