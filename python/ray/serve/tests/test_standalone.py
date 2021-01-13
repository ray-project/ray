"""
The test file for all standalone tests that doesn't
requires a shared Serve instance.
"""
import sys
import socket

import pytest
import requests

import ray
from ray import serve
from ray.cluster_utils import Cluster
from ray.serve.constants import SERVE_PROXY_NAME
from ray.serve.utils import (block_until_http_ready, get_all_node_ids,
                             format_actor_name)
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
    head_node = cluster.add_node(num_cpus=4)
    cluster.add_node(num_cpus=4)

    ray.init(head_node.address)
    node_ids = ray.state.node_ids()
    assert len(node_ids) == 2
    client = serve.start(http_options=dict(port=8005, location="EveryNode"))

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
        http_options=dict(
            port=port,
            middlewares=[
                Middleware(
                    CORSMiddleware, allow_origins=["*"], allow_methods=["*"])
            ]))
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


def test_http_proxy_fail_loudly():
    # Test that if the http server fail to start, serve.start should fail.
    with pytest.raises(socket.gaierror):
        serve.start(http_options={"host": "bad.ip.address"})

    ray.shutdown()


def test_no_http():
    # The following should have the same effect.
    options = [
        {
            "http_host": None
        },
        {
            "http_options": {
                "host": None
            }
        },
        {
            "http_options": {
                "location": None
            }
        },
        {
            "http_options": {
                "location": "NoServer"
            }
        },
    ]

    ray.init()
    for option in options:
        client = serve.start(**option)

        # Only controller actor should exist
        live_actors = [
            actor for actor in ray.actors().values()
            if actor["State"] == ray.gcs_utils.ActorTableData.ALIVE
        ]
        assert len(live_actors) == 1

        client.shutdown()
    ray.shutdown()


def test_http_head_only():
    cluster = Cluster()
    head_node = cluster.add_node(num_cpus=4)
    cluster.add_node(num_cpus=4)

    ray.init(head_node.address)
    node_ids = ray.state.node_ids()
    assert len(node_ids) == 2

    client = serve.start(http_options={
        "port": new_port(),
        "location": "HeadOnly"
    })

    # Only the controller and head node actor should be started
    assert len(ray.actors()) == 2

    # They should all be placed on the head node
    cpu_per_nodes = {
        r["CPU"]
        for r in ray.state.state._available_resources_per_node().values()
    }
    assert cpu_per_nodes == {2, 4}

    client.shutdown()
    ray.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
