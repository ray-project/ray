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
from ray.serve.exceptions import RayServeException
from ray.serve.utils import (block_until_http_ready, get_all_node_ids,
                             format_actor_name)
from ray.test_utils import wait_for_condition
from ray._private.services import new_port


@pytest.fixture
def ray_shutdown():
    yield
    serve.shutdown()
    ray.shutdown()


@pytest.fixture
def ray_cluster():
    cluster = Cluster()
    yield Cluster()
    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


def test_shutdown(ray_shutdown):
    def f():
        pass

    ray.init(num_cpus=16)
    serve.start(http_port=8003)
    serve.create_backend("backend", f)
    serve.create_endpoint("endpoint", backend="backend")

    actor_names = [
        serve.api._global_client._controller_name,
        format_actor_name(SERVE_PROXY_NAME,
                          serve.api._global_client._controller_name,
                          get_all_node_ids()[0][0])
    ]

    def check_alive():
        alive = True
        for actor_name in actor_names:
            try:
                ray.get_actor(actor_name)
            except ValueError:
                alive = False
        return alive

    wait_for_condition(check_alive)

    serve.shutdown()
    with pytest.raises(RayServeException):
        serve.list_backends()

    def check_dead():
        for actor_name in actor_names:
            try:
                ray.get_actor(actor_name)
                return False
            except ValueError:
                pass
        return True

    wait_for_condition(check_dead)


def test_detached_deployment(ray_cluster):
    # https://github.com/ray-project/ray/issues/11437

    cluster = ray_cluster
    head_node = cluster.add_node(node_ip_address="127.0.0.1", num_cpus=6)

    # Create first job, check we can run a simple serve endpoint
    ray.init(head_node.address)
    first_job_id = ray.get_runtime_context().job_id
    serve.start(detached=True)
    serve.create_backend("f", lambda _: "hello")
    serve.create_endpoint("f", backend="f")
    assert ray.get(serve.get_handle("f").remote()) == "hello"

    serve.api._global_client = None
    ray.shutdown()

    # Create the second job, make sure we can still create new backends.
    ray.init(head_node.address)
    assert ray.get_runtime_context().job_id != first_job_id

    serve.create_backend("g", lambda _: "world")
    serve.create_endpoint("g", backend="g")
    assert ray.get(serve.get_handle("g").remote()) == "world"


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows")
@pytest.mark.parametrize("detached", [True, False])
def test_connect(detached, ray_shutdown):
    # Check that you can call serve.connect() from within a backend for both
    # detached and non-detached instances.
    ray.init(num_cpus=16)
    serve.start(detached=detached)

    def connect_in_backend(_):
        serve.create_backend("backend-ception", connect_in_backend)

    serve.create_backend("connect_in_backend", connect_in_backend)
    serve.create_endpoint("endpoint", backend="connect_in_backend")
    ray.get(serve.get_handle("endpoint").remote())
    assert "backend-ception" in serve.list_backends().keys()


@pytest.mark.skipif(
    not hasattr(socket, "SO_REUSEPORT"),
    reason=("Port sharing only works on newer verion of Linux. "
            "This test can only be ran when port sharing is supported."))
def test_multiple_routers(ray_cluster):
    cluster = ray_cluster
    head_node = cluster.add_node(num_cpus=4)
    cluster.add_node(num_cpus=4)

    ray.init(head_node.address)
    node_ids = ray.state.node_ids()
    assert len(node_ids) == 2
    serve.start(http_options=dict(port=8005, location="EveryNode"))

    def get_proxy_names():
        proxy_names = []
        for node_id, _ in get_all_node_ids():
            proxy_names.append(
                format_actor_name(SERVE_PROXY_NAME,
                                  serve.api._global_client._controller_name,
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


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows")
def test_middleware(ray_shutdown):
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


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows")
def test_http_proxy_fail_loudly(ray_shutdown):
    # Test that if the http server fail to start, serve.start should fail.
    with pytest.raises(ValueError):
        serve.start(http_options={"host": "bad.ip.address"})


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows")
def test_no_http(ray_shutdown):
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

    ray.init(num_cpus=16)
    for i, option in enumerate(options):
        print(f"[{i+1}/{len(options)}] Running with {option}")
        serve.start(**option)

        # Only controller actor should exist
        live_actors = [
            actor for actor in ray.actors().values()
            if actor["State"] == ray.gcs_utils.ActorTableData.ALIVE
        ]
        assert len(live_actors) == 1
        controller = serve.api._global_client._controller
        assert len(ray.get(controller.get_http_proxies.remote())) == 0

        # Test that the handle still works.
        def hello(*args):
            return "hello"

        serve.create_backend("backend", hello)
        serve.create_endpoint("endpoint", backend="backend")

        assert ray.get(serve.get_handle("endpoint").remote()) == "hello"
        serve.shutdown()


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows")
def test_http_head_only(ray_cluster):
    cluster = ray_cluster
    head_node = cluster.add_node(num_cpus=4)
    cluster.add_node(num_cpus=4)

    ray.init(head_node.address)
    node_ids = ray.state.node_ids()
    assert len(node_ids) == 2

    serve.start(http_options={"port": new_port(), "location": "HeadOnly"})

    # Only the controller and head node actor should be started
    assert len(ray.actors()) == 2

    # They should all be placed on the head node
    cpu_per_nodes = {
        r["CPU"]
        for r in ray.state.state._available_resources_per_node().values()
    }
    assert cpu_per_nodes == {2, 4}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
