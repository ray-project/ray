"""
The test file for all standalone tests that doesn't
requires a shared Serve instance.
"""
import logging
import os
import random
import socket
import sys
import time

import httpx
import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import (
    run_string_as_driver,
)
from ray._raylet import GcsClient
from ray.cluster_utils import Cluster, cluster_not_supported
from ray.serve._private.constants import (
    SERVE_CONTROLLER_NAME,
    SERVE_DEFAULT_APP_NAME,
    SERVE_NAMESPACE,
    SERVE_PROXY_NAME,
)
from ray.serve._private.default_impl import create_cluster_node_info_cache
from ray.serve._private.http_util import set_socket_reuse_port
from ray.serve._private.utils import block_until_http_ready, format_actor_name
from ray.serve.config import DeploymentMode, HTTPOptions, ProxyLocation
from ray.serve.context import _get_global_client
from ray.serve.schema import ServeApplicationSchema, ServeDeploySchema
from ray.util.state import list_actors


def _get_random_port() -> int:
    return random.randint(10000, 65535)


@pytest.fixture
def ray_cluster():
    if cluster_not_supported:
        pytest.skip("Cluster not supported")
    cluster = Cluster()
    yield Cluster()
    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


@pytest.fixture()
def lower_slow_startup_threshold_and_reset():
    original_slow_startup_warning_s = os.environ.get("SERVE_SLOW_STARTUP_WARNING_S")
    original_slow_startup_warning_period_s = os.environ.get(
        "SERVE_SLOW_STARTUP_WARNING_PERIOD_S"
    )
    # Lower slow startup warning threshold to 1 second to reduce test duration
    os.environ["SERVE_SLOW_STARTUP_WARNING_S"] = "1"
    os.environ["SERVE_SLOW_STARTUP_WARNING_PERIOD_S"] = "1"

    ray.init(num_cpus=2)
    serve.start()
    client = _get_global_client()

    yield client

    serve.shutdown()
    ray.shutdown()

    # Reset slow startup warning threshold to prevent state sharing across unit
    # tests
    if original_slow_startup_warning_s is not None:
        os.environ["SERVE_SLOW_STARTUP_WARNING_S"] = original_slow_startup_warning_s
    if original_slow_startup_warning_period_s is not None:
        os.environ[
            "SERVE_SLOW_STARTUP_WARNING_PERIOD_S"
        ] = original_slow_startup_warning_period_s


def test_shutdown(ray_shutdown):
    ray.init(num_cpus=8)
    serve.start(http_options=dict(port=8003))
    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    cluster_node_info_cache = create_cluster_node_info_cache(gcs_client)
    cluster_node_info_cache.update()

    @serve.deployment
    def f():
        pass

    serve.run(f.bind())

    actor_names = [
        SERVE_CONTROLLER_NAME,
        format_actor_name(
            SERVE_PROXY_NAME,
            cluster_node_info_cache.get_alive_nodes()[0][0],
        ),
    ]

    def check_alive():
        alive = True
        for actor_name in actor_names:
            try:
                ray.get_actor(actor_name, namespace=SERVE_NAMESPACE)
            except ValueError:
                alive = False
        return alive

    wait_for_condition(check_alive)

    serve.shutdown()

    def check_dead():
        for actor_name in actor_names:
            try:
                ray.get_actor(actor_name, namespace=SERVE_NAMESPACE)
                return False
            except ValueError:
                pass
        return True

    wait_for_condition(check_dead)


@pytest.mark.asyncio
async def test_shutdown_async(ray_shutdown):
    ray.init(num_cpus=8)
    serve.start(http_options=dict(port=8003))
    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    cluster_node_info_cache = create_cluster_node_info_cache(gcs_client)
    cluster_node_info_cache.update()

    @serve.deployment
    def f():
        pass

    serve.run(f.bind())

    actor_names = [
        SERVE_CONTROLLER_NAME,
        format_actor_name(
            SERVE_PROXY_NAME,
            cluster_node_info_cache.get_alive_nodes()[0][0],
        ),
    ]

    def check_alive():
        alive = True
        for actor_name in actor_names:
            try:
                ray.get_actor(actor_name, namespace=SERVE_NAMESPACE)
            except ValueError:
                alive = False
        return alive

    wait_for_condition(check_alive)

    await serve.shutdown_async()

    def check_dead():
        for actor_name in actor_names:
            try:
                ray.get_actor(actor_name, namespace=SERVE_NAMESPACE)
                return False
            except ValueError:
                pass
        return True

    wait_for_condition(check_dead)


def test_single_app_shutdown_actors(ray_shutdown):
    """Tests serve.shutdown() works correctly in single-app case

    Ensures that after deploying a (nameless) app using serve.run(), serve.shutdown()
    deletes all actors (controller, http proxy, all replicas) in the "serve" namespace.
    """
    address = ray.init(num_cpus=8)["address"]
    serve.start(http_options=dict(port=8003))

    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app")

    actor_names = {
        "ServeController",
        "ProxyActor",
        "ServeReplica:app:f",
    }

    def check_alive():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return {actor["class_name"] for actor in actors} == actor_names

    def check_dead():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return len(actors) == 0

    wait_for_condition(check_alive)
    serve.shutdown()
    wait_for_condition(check_dead)


@pytest.mark.asyncio
async def test_single_app_shutdown_actors_async(ray_shutdown):
    """Tests serve.shutdown_async() works correctly in single-app case

    Ensures that after deploying a (nameless) app using serve.run(), serve.shutdown_async()
    deletes all actors (controller, http proxy, all replicas) in the "serve" namespace.
    """
    address = ray.init(num_cpus=8)["address"]
    serve.start(http_options=dict(port=8003))

    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app")

    actor_names = {
        "ServeController",
        "ProxyActor",
        "ServeReplica:app:f",
    }

    def check_alive():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return {actor["class_name"] for actor in actors} == actor_names

    def check_dead():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return len(actors) == 0

    wait_for_condition(check_alive)
    await serve.shutdown_async()
    wait_for_condition(check_dead)


def test_multi_app_shutdown_actors(ray_shutdown):
    """Tests serve.shutdown() works correctly in multi-app case.

    Ensures that after deploying multiple distinct applications, serve.shutdown()
    deletes all actors (controller, http proxy, all replicas) in the "serve" namespace.
    """
    address = ray.init(num_cpus=8)["address"]
    serve.start(http_options=dict(port=8003))

    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app1", route_prefix="/app1")
    serve.run(f.bind(), name="app2", route_prefix="/app2")

    actor_names = {
        "ServeController",
        "ProxyActor",
        "ServeReplica:app1:f",
        "ServeReplica:app2:f",
    }

    def check_alive():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return {actor["class_name"] for actor in actors} == actor_names

    def check_dead():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return len(actors) == 0

    wait_for_condition(check_alive)
    serve.shutdown()
    wait_for_condition(check_dead)


@pytest.mark.asyncio
async def test_multi_app_shutdown_actors_async(ray_shutdown):
    """Tests serve.shutdown_async() works correctly in multi-app case.

    Ensures that after deploying multiple distinct applications, serve.shutdown_async()
    deletes all actors (controller, http proxy, all replicas) in the "serve" namespace.
    """
    address = ray.init(num_cpus=8)["address"]
    serve.start(http_options=dict(port=8003))

    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app1", route_prefix="/app1")
    serve.run(f.bind(), name="app2", route_prefix="/app2")

    actor_names = {
        "ServeController",
        "ProxyActor",
        "ServeReplica:app1:f",
        "ServeReplica:app2:f",
    }

    def check_alive():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return {actor["class_name"] for actor in actors} == actor_names

    def check_dead():
        actors = list_actors(
            address=address,
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return len(actors) == 0

    wait_for_condition(check_alive)
    await serve.shutdown_async()
    wait_for_condition(check_dead)


def test_deployment(ray_cluster):
    # https://github.com/ray-project/ray/issues/11437

    cluster = ray_cluster
    head_node = cluster.add_node(num_cpus=6)

    # Create first job, check we can run a simple serve endpoint
    ray.init(head_node.address, namespace=SERVE_NAMESPACE)
    first_job_id = ray.get_runtime_context().get_job_id()
    serve.start()

    @serve.deployment
    def f(*args):
        return "from_f"

    handle = serve.run(f.bind(), name="f", route_prefix="/say_hi_f")
    assert handle.remote().result() == "from_f"
    assert httpx.get("http://localhost:8000/say_hi_f").text == "from_f"

    serve.context._global_client = None
    ray.shutdown()

    # Create the second job, make sure we can still create new deployments.
    ray.init(head_node.address, namespace="serve")
    assert ray.get_runtime_context().get_job_id() != first_job_id

    @serve.deployment
    def g(*args):
        return "from_g"

    handle = serve.run(g.bind(), name="g", route_prefix="/say_hi_g")
    assert handle.remote().result() == "from_g"
    assert httpx.get("http://localhost:8000/say_hi_g").text == "from_g"
    assert httpx.get("http://localhost:8000/say_hi_f").text == "from_f"


def test_connect(ray_shutdown):
    # Check that you can make API calls from within a deployment.
    ray.init(num_cpus=8, namespace="serve")
    serve.start()

    @serve.deployment
    def connect_in_deployment(*args):
        serve.run(
            connect_in_deployment.options(name="deployment-ception").bind(),
            name="app2",
            route_prefix="/app2",
        )

    handle = serve.run(connect_in_deployment.bind())
    handle.remote().result()
    assert "deployment-ception" in serve.status().applications["app2"].deployments


def test_set_socket_reuse_port():
    sock = socket.socket()
    if hasattr(socket, "SO_REUSEPORT"):
        # If the flag exists, we should be able to to use it
        assert set_socket_reuse_port(sock)
    elif sys.platform == "linux":
        # If the flag doesn't exist, but we are only mordern version
        # of linux, we should be able to force set this flag.
        assert set_socket_reuse_port(sock)
    else:
        # Otherwise, it should graceful fail without exception.
        assert not set_socket_reuse_port(sock)


def _reuse_port_is_available():
    sock = socket.socket()
    return set_socket_reuse_port(sock)


@pytest.mark.skipif(
    not _reuse_port_is_available(),
    reason=(
        "Port sharing only works on newer verion of Linux. "
        "This test can only be ran when port sharing is supported."
    ),
)
def test_multiple_routers(ray_cluster):
    cluster = ray_cluster
    head_node = cluster.add_node(num_cpus=4)
    cluster.add_node(num_cpus=4)

    ray.init(head_node.address)
    assert len(ray.nodes()) == 2
    serve.start(http_options=dict(port=8005, location="EveryNode"))

    @serve.deployment(
        num_replicas=2,
        ray_actor_options={"num_cpus": 3},
    )
    class A:
        def __call__(self, *args):
            return "hi"

    serve.run(A.bind())

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    cluster_node_info_cache = create_cluster_node_info_cache(gcs_client)
    cluster_node_info_cache.update()

    def get_proxy_names():
        proxy_names = []
        for node_id in cluster_node_info_cache.get_alive_node_ids():
            proxy_names.append(
                format_actor_name(
                    SERVE_PROXY_NAME,
                    node_id,
                )
            )
        return proxy_names

    wait_for_condition(lambda: len(get_proxy_names()) == 2)
    original_proxy_names = get_proxy_names()

    # Two actors should be started.
    def get_first_two_actors():
        try:
            ray.get_actor(original_proxy_names[0], namespace=SERVE_NAMESPACE)
            ray.get_actor(original_proxy_names[1], namespace=SERVE_NAMESPACE)
            return True
        except ValueError:
            return False

    wait_for_condition(get_first_two_actors)

    # Wait for the actors to come up.
    ray.get(block_until_http_ready.remote("http://127.0.0.1:8005/-/routes"))

    # Kill one of the servers, the HTTP server should still function.
    ray.kill(
        ray.get_actor(get_proxy_names()[0], namespace=SERVE_NAMESPACE), no_restart=True
    )
    ray.get(block_until_http_ready.remote("http://127.0.0.1:8005/-/routes"))

    # Add a new node to the cluster. This should trigger a new router to get
    # started.
    new_node = cluster.add_node(num_cpus=4)
    cluster_node_info_cache.update()

    wait_for_condition(lambda: len(get_proxy_names()) == 3)
    (third_proxy,) = set(get_proxy_names()) - set(original_proxy_names)

    serve.run(A.options(num_replicas=3).bind())

    def get_third_actor():
        try:
            ray.get_actor(third_proxy, namespace=SERVE_NAMESPACE)
            return True
        # IndexErrors covers when cluster resources aren't updated yet.
        except (IndexError, ValueError):
            return False

    wait_for_condition(get_third_actor)

    # Remove the newly-added node from the cluster. The corresponding actor
    # should be removed as well.
    cluster.remove_node(new_node)
    cluster_node_info_cache.update()

    def third_actor_removed():
        try:
            ray.get_actor(third_proxy, namespace=SERVE_NAMESPACE)
            return False
        except ValueError:
            return True

    # Check that the actor is gone and the HTTP server still functions.
    wait_for_condition(third_actor_removed)
    ray.get(block_until_http_ready.remote("http://127.0.0.1:8005/-/routes"))


def test_middleware(ray_shutdown):
    from starlette.middleware import Middleware
    from starlette.middleware.cors import CORSMiddleware

    port = _get_random_port()
    serve.start(
        http_options=dict(
            port=port,
            middlewares=[
                Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"])
            ],
        )
    )

    @serve.deployment
    class Dummy:
        pass

    serve.run(Dummy.bind())
    ray.get(block_until_http_ready.remote(f"http://127.0.0.1:{port}/-/routes"))

    # Snatched several test cases from Starlette
    # https://github.com/encode/starlette/blob/master/tests/
    # middleware/test_cors.py
    headers = {
        "Origin": "https://example.org",
        "Access-Control-Request-Method": "GET",
    }
    root = f"http://localhost:{port}"
    resp = httpx.options(root, headers=headers)
    assert resp.headers["access-control-allow-origin"] == "*"

    resp = httpx.get(f"{root}/-/routes", headers=headers)
    assert resp.headers["access-control-allow-origin"] == "*"


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows")
def test_http_root_path(ray_shutdown):
    @serve.deployment
    def hello():
        return "hello"

    port = _get_random_port()
    root_path = "/serve"
    serve.start(http_options=dict(root_path=root_path, port=port))
    serve.run(hello.bind(), route_prefix="/hello")

    # check routing works as expected
    resp = httpx.get(f"http://127.0.0.1:{port}{root_path}/hello")
    assert resp.status_code == 200
    assert resp.text == "hello"

    # check advertized routes are prefixed correctly
    resp = httpx.get(f"http://127.0.0.1:{port}{root_path}/-/routes")
    assert resp.status_code == 200
    assert resp.json() == {"/hello": "default"}


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows")
def test_http_proxy_fail_loudly(ray_shutdown):
    # Test that if the http server fail to start, serve.start should fail.
    with pytest.raises(RuntimeError):
        serve.start(http_options={"host": "bad.ip.address"})


def test_no_http(ray_shutdown):
    # The following should have the same effect.
    options = [
        {"http_options": {"host": None}},
        {"http_options": {"location": None}},
        {"http_options": {"location": "NoServer"}},
    ]

    address = ray.init(num_cpus=8)["address"]
    for i, option in enumerate(options):
        print(f"[{i+1}/{len(options)}] Running with {option}")
        serve.start(**option)

        # Only controller actor should exist
        live_actors = list_actors(
            address=address,
            filters=[("state", "=", "ALIVE")],
        )
        assert len(live_actors) == 1
        controller = serve.context._global_client._controller
        assert len(ray.get(controller.get_proxies.remote())) == 0

        # Test that the handle still works.
        @serve.deployment
        def hello(*args):
            return "hello"

        handle = serve.run(hello.bind())

        assert handle.remote().result() == "hello"
        serve.shutdown()


def test_http_head_only(ray_cluster):
    cluster = ray_cluster
    head_node = cluster.add_node(num_cpus=4, dashboard_port=_get_random_port())
    cluster.add_node(num_cpus=4)

    ray.init(head_node.address)
    assert len(ray.nodes()) == 2

    serve.start(http_options={"port": _get_random_port(), "location": "HeadOnly"})

    # Only the controller and head node proxy should be started, both on the head node.
    actors = list_actors(address=head_node.address)
    assert len(actors) == 2
    assert all([actor.node_id == head_node.node_id for actor in actors])


def test_serve_shutdown(ray_shutdown):
    ray.init(namespace="serve")
    serve.start()

    @serve.deployment
    class A:
        def __call__(self, *args):
            return "hi"

    serve.run(A.bind())

    assert len(serve.status().applications) == 1

    serve.shutdown()
    serve.start()

    assert len(serve.status().applications) == 0

    serve.run(A.bind())

    assert len(serve.status().applications) == 1


@pytest.mark.asyncio
async def test_serve_shutdown_async(ray_shutdown):
    ray.init(namespace="serve")
    serve.start()

    @serve.deployment
    class A:
        def __call__(self, *args):
            return "hi"

    serve.run(A.bind())

    assert len(serve.status().applications) == 1

    await serve.shutdown_async()
    serve.start()

    assert len(serve.status().applications) == 0

    serve.run(A.bind())

    assert len(serve.status().applications) == 1


def test_instance_in_non_anonymous_namespace(ray_shutdown):
    # Can start instance in non-anonymous namespace.
    ray.init(namespace="foo")
    serve.start()


def test_checkpoint_isolation_namespace(ray_shutdown):
    info = ray.init(namespace="test_namespace1")

    address = info["address"]

    driver_template = """
import ray
from ray import serve

ray.init(address="{address}", namespace="{namespace}")

serve.start(http_options={{"port": {port}}})

@serve.deployment
class A:
    pass

serve.run(A.bind())"""

    run_string_as_driver(
        driver_template.format(address=address, namespace="test_namespace1", port=8000)
    )
    run_string_as_driver(
        driver_template.format(address=address, namespace="test_namespace2", port=8001)
    )


def test_serve_start_different_http_checkpoint_options_warning(
    ray_shutdown, propagate_logs, caplog
):
    logger = logging.getLogger("ray.serve")
    caplog.set_level(logging.WARNING, logger="ray.serve")

    warning_msg = []

    class WarningHandler(logging.Handler):
        def emit(self, record):
            warning_msg.append(self.format(record))

    logger.addHandler(WarningHandler())

    ray.init(namespace="serve-test")
    serve.start()

    # create a different config
    test_http = dict(host="127.1.1.8", port=_get_random_port())

    serve.start(http_options=test_http)

    for test_config, msg in zip([["host", "port"]], warning_msg):
        for test_msg in test_config:
            if "Autoscaling metrics pusher thread" in msg:
                continue
            assert test_msg in msg


def test_recovering_controller_no_redeploy():
    """Ensure controller doesn't redeploy running deployments when recovering."""
    ray_context = ray.init(namespace="x")
    address = ray_context.address_info["address"]
    serve.start()
    client = _get_global_client()

    @serve.deployment
    def f():
        pass

    serve.run(f.bind())

    num_actors = len(list_actors(address, filters=[("state", "=", "ALIVE")]))
    assert num_actors > 0

    pid = ray.get(client._controller.get_pid.remote())

    ray.kill(client._controller, no_restart=False)

    wait_for_condition(lambda: ray.get(client._controller.get_pid.remote()) != pid)

    # Confirm that no new deployment is deployed over the next 5 seconds
    with pytest.raises(RuntimeError):
        wait_for_condition(
            lambda: len(list_actors(address, filters=[("state", "=", "ALIVE")]))
            > num_actors,
            timeout=5,
        )

    serve.shutdown()
    ray.shutdown()


def test_updating_status_message(lower_slow_startup_threshold_and_reset):
    """Check if status message says if a serve deployment has taken a long time"""

    @serve.deployment(
        num_replicas=5,
        ray_actor_options={"num_cpus": 1},
    )
    def f(*args):
        pass

    serve._run(f.bind(), _blocking=False)

    def updating_message():
        deployment_status = (
            serve.status().applications[SERVE_DEFAULT_APP_NAME].deployments["f"]
        )
        message_substring = "more than 1s to be scheduled."
        return (deployment_status.status == "UPDATING") and (
            message_substring in deployment_status.message
        )

    wait_for_condition(updating_message, timeout=20)


def test_unhealthy_override_updating_status(lower_slow_startup_threshold_and_reset):
    """
    Check that if status is UNHEALTHY and there is a resource availability
    issue, the status should not change. The issue that caused the deployment to
    be unhealthy should be prioritized over this resource availability issue.
    """

    @serve.deployment
    class f:
        def __init__(self):
            self.num = 1 / 0

        def __call__(self, request):
            pass

    serve._run(f.bind(), _blocking=False)

    wait_for_condition(
        lambda: serve.status()
        .applications[SERVE_DEFAULT_APP_NAME]
        .deployments["f"]
        .status
        == "DEPLOY_FAILED",
        timeout=20,
    )

    with pytest.raises(RuntimeError):
        wait_for_condition(
            lambda: serve.status()
            .applications[SERVE_DEFAULT_APP_NAME]
            .deployments["f"]
            .status
            == "UPDATING",
            timeout=10,
        )


@serve.deployment(ray_actor_options={"num_cpus": 0})
class Waiter:
    def __init__(self):
        time.sleep(5)

    def __call__(self, *args):
        return "May I take your order?"


WaiterNode = Waiter.bind()


def test_build_app_task_uses_zero_cpus(ray_shutdown):
    """Check that the task to build an app uses zero CPUs."""
    ray.init(num_cpus=0)
    serve.start()

    _get_global_client().deploy_apps(
        ServeDeploySchema(
            applications=[
                ServeApplicationSchema(
                    name="default",
                    route_prefix="/",
                    import_path="ray.serve.tests.test_standalone.WaiterNode",
                )
            ],
        )
    )

    # If the task required any resources, this would fail.
    wait_for_condition(
        lambda: httpx.get("http://localhost:8000/").text == "May I take your order?"
    )

    serve.shutdown()
    ray.shutdown()


@pytest.mark.parametrize(
    "options",
    [
        {
            "proxy_location": None,
            "http_options": None,
            "expected": HTTPOptions(location=DeploymentMode.EveryNode),
        },
        {
            "proxy_location": None,
            "http_options": {"test": "test"},  # location is not specified
            "expected": HTTPOptions(
                location=DeploymentMode.EveryNode
            ),  # using default proxy_location (to align with the case when `http_options` are None)
        },
        {
            "proxy_location": None,
            "http_options": {
                "location": "NoServer"
            },  # `location` is specified, but `proxy_location` is not
            "expected": HTTPOptions(
                location=DeploymentMode.NoServer
            ),  # using `location` value
        },
        {
            "proxy_location": None,
            "http_options": HTTPOptions(location=None),
            "expected": HTTPOptions(location=DeploymentMode.NoServer),
        },
        {
            "proxy_location": None,
            "http_options": HTTPOptions(),
            "expected": HTTPOptions(location=DeploymentMode.HeadOnly),
        },  # using default location from HTTPOptions
        {
            "proxy_location": None,
            "http_options": HTTPOptions(location="NoServer"),
            "expected": HTTPOptions(location=DeploymentMode.NoServer),
        },
        {
            "proxy_location": None,
            "http_options": {"location": "NoServer"},
            "expected": HTTPOptions(location=DeploymentMode.NoServer),
        },
        {
            "proxy_location": "Disabled",
            "http_options": None,
            "expected": HTTPOptions(location=DeploymentMode.NoServer),
        },
        {
            "proxy_location": "Disabled",
            "http_options": {},
            "expected": HTTPOptions(location=DeploymentMode.NoServer),
        },
        {
            "proxy_location": "Disabled",
            "http_options": HTTPOptions(host="foobar"),
            "expected": HTTPOptions(location=DeploymentMode.NoServer, host="foobar"),
        },
        {
            "proxy_location": "Disabled",
            "http_options": {"host": "foobar"},
            "expected": HTTPOptions(location=DeploymentMode.NoServer, host="foobar"),
        },
        {
            "proxy_location": "Disabled",
            "http_options": {"location": "HeadOnly"},
            "expected": HTTPOptions(location=DeploymentMode.NoServer),
        },
        {
            "proxy_location": ProxyLocation.Disabled,
            "http_options": HTTPOptions(location=DeploymentMode.HeadOnly),
            "expected": HTTPOptions(location=DeploymentMode.NoServer),
        },
    ],
)
def test_serve_start_proxy_location(ray_shutdown, options):
    expected_options = options.pop("expected")
    serve.start(**options)
    client = _get_global_client()
    assert ray.get(client._controller.get_http_config.remote()) == expected_options


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
