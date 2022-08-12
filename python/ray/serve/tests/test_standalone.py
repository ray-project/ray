"""
The test file for all standalone tests that doesn't
requires a shared Serve instance.
"""
import logging
import os
import socket
import sys
import time

import pydantic
import pytest
import requests

import ray
import ray._private.gcs_utils as gcs_utils
from ray import serve
from ray._private.services import new_port
from ray._private.test_utils import (
    convert_actor_state,
    run_string_as_driver,
    wait_for_condition,
)
from ray.cluster_utils import Cluster, cluster_not_supported
from ray.serve.config import HTTPOptions
from ray.serve._private.constants import (
    SERVE_NAMESPACE,
    SERVE_PROXY_NAME,
    SERVE_ROOT_URL_ENV_KEY,
)
from ray._private.gcs_utils import GcsClient
from ray.serve.context import get_global_client
from ray.serve.exceptions import RayServeException
from ray.serve.generated.serve_pb2 import ActorNameList
from ray.serve._private.http_util import set_socket_reuse_port
from ray.serve._private.utils import (
    block_until_http_ready,
    format_actor_name,
    get_all_node_ids,
)
from ray.serve.schema import ServeApplicationSchema

# Explicitly importing it here because it is a ray core tests utility (
# not in the tree)
from ray.tests.conftest import maybe_external_redis  # noqa: F401
from ray.tests.conftest import ray_start_with_dashboard  # noqa: F401


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
    client = serve.start(detached=True)

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
    ray.init(num_cpus=16)
    serve.start(http_options=dict(port=8003))
    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    @serve.deployment
    def f():
        pass

    serve.run(f.bind())

    serve_controller_name = serve.context._global_client._controller_name
    actor_names = [
        serve_controller_name,
        format_actor_name(
            SERVE_PROXY_NAME,
            serve.context._global_client._controller_name,
            get_all_node_ids(gcs_client)[0][0],
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
    with pytest.raises(RayServeException):
        serve.list_deployments()

    def check_dead():
        for actor_name in actor_names:
            try:
                ray.get_actor(actor_name, namespace=SERVE_NAMESPACE)
                return False
            except ValueError:
                pass
        return True

    wait_for_condition(check_dead)


def test_detached_deployment(ray_cluster):
    # https://github.com/ray-project/ray/issues/11437

    cluster = ray_cluster
    head_node = cluster.add_node(num_cpus=6)

    # Create first job, check we can run a simple serve endpoint
    ray.init(head_node.address, namespace=SERVE_NAMESPACE)
    first_job_id = ray.get_runtime_context().job_id
    serve.start(detached=True)

    @serve.deployment(route_prefix="/say_hi_f")
    def f(*args):
        return "from_f"

    f.deploy()
    assert ray.get(f.get_handle().remote()) == "from_f"
    assert requests.get("http://localhost:8000/say_hi_f").text == "from_f"

    serve.context._global_client = None
    ray.shutdown()

    # Create the second job, make sure we can still create new deployments.
    ray.init(head_node.address, namespace="serve")
    assert ray.get_runtime_context().job_id != first_job_id

    @serve.deployment(route_prefix="/say_hi_g")
    def g(*args):
        return "from_g"

    g.deploy()
    assert ray.get(g.get_handle().remote()) == "from_g"
    assert requests.get("http://localhost:8000/say_hi_g").text == "from_g"
    assert requests.get("http://localhost:8000/say_hi_f").text == "from_f"


@pytest.mark.parametrize("detached", [True, False])
def test_connect(detached, ray_shutdown):
    # Check that you can make API calls from within a deployment for both
    # detached and non-detached instances.
    ray.init(num_cpus=16, namespace="serve")
    serve.start(detached=detached)

    @serve.deployment
    def connect_in_deployment(*args):
        connect_in_deployment.options(name="deployment-ception").deploy()

    handle = serve.run(connect_in_deployment.bind())
    ray.get(handle.remote())
    assert "deployment-ception" in serve.list_deployments()


@pytest.mark.parametrize("controller_cpu", [True, False])
@pytest.mark.parametrize("num_proxy_cpus", [0, 1, 2])
def test_dedicated_cpu(controller_cpu, num_proxy_cpus, ray_cluster):
    cluster = ray_cluster
    num_cluster_cpus = 8
    head_node = cluster.add_node(num_cpus=num_cluster_cpus)

    ray.init(head_node.address)
    wait_for_condition(lambda: ray.cluster_resources().get("CPU") == num_cluster_cpus)

    num_cpus_used = int(controller_cpu) + num_proxy_cpus

    serve.start(
        dedicated_cpu=controller_cpu, http_options=HTTPOptions(num_cpus=num_proxy_cpus)
    )
    available_cpus = num_cluster_cpus - num_cpus_used
    wait_for_condition(lambda: (ray.available_resources().get("CPU") == available_cpus))
    serve.shutdown()
    ray.shutdown()


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
    node_ids = ray._private.state.node_ids()
    assert len(node_ids) == 2
    serve.start(http_options=dict(port=8005, location="EveryNode"))
    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    def get_proxy_names():
        proxy_names = []
        for node_id, _ in get_all_node_ids(gcs_client):
            proxy_names.append(
                format_actor_name(
                    SERVE_PROXY_NAME,
                    serve.context._global_client._controller_name,
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
    new_node = cluster.add_node()

    wait_for_condition(lambda: len(get_proxy_names()) == 3)
    (third_proxy,) = set(get_proxy_names()) - set(original_proxy_names)

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

    port = new_port()
    serve.start(
        http_options=dict(
            port=port,
            middlewares=[
                Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"])
            ],
        )
    )
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
def test_http_root_url(ray_shutdown):
    @serve.deployment
    def f(_):
        pass

    root_url = "https://my.domain.dev/prefix"

    port = new_port()
    os.environ[SERVE_ROOT_URL_ENV_KEY] = root_url
    serve.start(http_options=dict(port=port))
    serve.run(f.bind())
    assert f.url == root_url + "/f"
    serve.shutdown()
    ray.shutdown()
    del os.environ[SERVE_ROOT_URL_ENV_KEY]

    port = new_port()
    serve.start(http_options=dict(port=port))
    serve.run(f.bind())
    assert f.url != root_url + "/f"
    assert f.url == f"http://127.0.0.1:{port}/f"
    serve.shutdown()
    ray.shutdown()

    ray.init(runtime_env={"env_vars": {SERVE_ROOT_URL_ENV_KEY: root_url}})
    port = new_port()
    serve.start(http_options=dict(port=port))
    serve.run(f.bind())
    assert f.url == root_url + "/f"
    serve.shutdown()
    ray.shutdown()


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows")
def test_http_root_path(ray_shutdown):
    @serve.deployment
    def hello():
        return "hello"

    port = new_port()
    root_path = "/serve"
    serve.start(http_options=dict(root_path=root_path, port=port))
    hello.deploy()

    # check whether url is prefixed correctly
    assert hello.url == f"http://127.0.0.1:{port}{root_path}/hello"

    # check routing works as expected
    resp = requests.get(hello.url)
    assert resp.status_code == 200
    assert resp.text == "hello"

    # check advertized routes are prefixed correctly
    resp = requests.get(f"http://127.0.0.1:{port}{root_path}/-/routes")
    assert resp.status_code == 200
    assert resp.json() == {"/hello": "hello"}


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows")
def test_http_proxy_fail_loudly(ray_shutdown):
    # Test that if the http server fail to start, serve.start should fail.
    with pytest.raises(ValueError):
        serve.start(http_options={"host": "bad.ip.address"})


def test_no_http(ray_shutdown):
    # The following should have the same effect.
    options = [
        {"http_options": {"host": None}},
        {"http_options": {"location": None}},
        {"http_options": {"location": "NoServer"}},
    ]

    ray.init(num_cpus=16)
    for i, option in enumerate(options):
        print(f"[{i+1}/{len(options)}] Running with {option}")
        serve.start(**option)

        # Only controller actor should exist
        live_actors = [
            actor
            for actor in ray._private.state.actors().values()
            if actor["State"] == convert_actor_state(gcs_utils.ActorTableData.ALIVE)
        ]
        assert len(live_actors) == 1
        controller = serve.context._global_client._controller
        assert len(ray.get(controller.get_http_proxies.remote())) == 0

        # Test that the handle still works.
        @serve.deployment
        def hello(*args):
            return "hello"

        handle = serve.run(hello.bind())

        assert ray.get(handle.remote()) == "hello"
        serve.shutdown()


def test_http_head_only(ray_cluster):
    cluster = ray_cluster
    head_node = cluster.add_node(num_cpus=4)
    cluster.add_node(num_cpus=4)

    ray.init(head_node.address)
    node_ids = ray._private.state.node_ids()
    assert len(node_ids) == 2

    serve.start(http_options={"port": new_port(), "location": "HeadOnly"})

    # Only the controller and head node actor should be started
    assert len(ray._private.state.actors()) == 2

    # They should all be placed on the head node
    cpu_per_nodes = {
        r["CPU"]
        for r in ray._private.state.state._available_resources_per_node().values()
    }
    assert cpu_per_nodes == {4, 4}


@pytest.mark.skipif(
    not hasattr(socket, "SO_REUSEPORT"),
    reason=(
        "Port sharing only works on newer verion of Linux. "
        "This test can only be ran when port sharing is supported."
    ),
)
def test_fixed_number_proxies(ray_cluster):
    cluster = ray_cluster
    head_node = cluster.add_node(num_cpus=4)
    cluster.add_node(num_cpus=4)
    cluster.add_node(num_cpus=4)

    ray.init(head_node.address)
    node_ids = ray._private.state.node_ids()
    assert len(node_ids) == 3

    with pytest.raises(
        pydantic.ValidationError,
        match="you must specify the `fixed_number_replicas` parameter.",
    ):
        serve.start(
            http_options={
                "location": "FixedNumber",
            }
        )

    serve.start(
        http_options={
            "port": new_port(),
            "location": "FixedNumber",
            "fixed_number_replicas": 2,
        }
    )

    # Only the controller and two http proxy should be started.
    controller_handle = get_global_client()._controller
    node_to_http_actors = ray.get(controller_handle.get_http_proxies.remote())
    assert len(node_to_http_actors) == 2

    proxy_names_bytes = ray.get(controller_handle.get_http_proxy_names.remote())
    proxy_names = ActorNameList.FromString(proxy_names_bytes)
    assert len(proxy_names.names) == 2

    serve.shutdown()
    ray.shutdown()
    cluster.shutdown()


def test_serve_shutdown(ray_shutdown):
    ray.init(namespace="serve")
    serve.start(detached=True)

    @serve.deployment
    class A:
        def __call__(self, *args):
            return "hi"

    serve.run(A.bind())

    assert len(serve.list_deployments()) == 1

    serve.shutdown()
    serve.start(detached=True)

    assert len(serve.list_deployments()) == 0

    serve.run(A.bind())

    assert len(serve.list_deployments()) == 1


def test_detached_namespace_default_ray_init(ray_shutdown):
    # Can start detached instance when ray is not initialized.
    serve.start(detached=True)


def test_detached_instance_in_non_anonymous_namespace(ray_shutdown):
    # Can start detached instance in non-anonymous namespace.
    ray.init(namespace="foo")
    serve.start(detached=True)


def test_checkpoint_isolation_namespace(ray_shutdown):
    info = ray.init(namespace="test_namespace1")

    address = info["address"]

    driver_template = """
import ray
from ray import serve

ray.init(address="{address}", namespace="{namespace}")

serve.start(detached=True, http_options={{"port": {port}}})

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


@pytest.mark.parametrize("ray_start_with_dashboard", [{"num_cpus": 4}], indirect=True)
def test_snapshot_always_written_to_internal_kv(
    ray_start_with_dashboard, ray_shutdown  # noqa: F811
):
    # https://github.com/ray-project/ray/issues/19752

    @serve.deployment()
    def hello(_):
        return "hello"

    def check():
        try:
            resp = requests.get("http://localhost:8000/hello")
            assert resp.text == "hello"
            return True
        except Exception:
            return False

    serve.start(detached=True)
    serve.run(hello.bind())
    check()

    webui_url = ray_start_with_dashboard["webui_url"]

    def get_deployment_snapshot():
        snapshot = requests.get(f"http://{webui_url}/api/snapshot").json()["data"][
            "snapshot"
        ]
        return snapshot["deployments"]

    # Make sure /api/snapshot return non-empty deployment status.
    def verify_snapshot():
        return get_deployment_snapshot() != {}

    wait_for_condition(verify_snapshot)

    # Sanity check the snapshot is correct
    snapshot = get_deployment_snapshot()
    assert len(snapshot) == 1
    hello_deployment = list(snapshot.values())[0]
    assert hello_deployment["name"] == "hello"
    assert hello_deployment["status"] == "RUNNING"


def test_serve_start_different_http_checkpoint_options_warning(caplog):
    logger = logging.getLogger("ray.serve")
    caplog.set_level(logging.WARNING, logger="ray.serve")

    warning_msg = []

    class WarningHandler(logging.Handler):
        def emit(self, record):
            warning_msg.append(self.format(record))

    logger.addHandler(WarningHandler())

    ray.init(namespace="serve-test")
    serve.start(detached=True)

    # create a different config
    test_http = dict(host="127.1.1.8", port=new_port())

    serve.start(detached=True, http_options=test_http)

    for test_config, msg in zip([["host", "port"]], warning_msg):
        for test_msg in test_config:
            if "Autoscaling metrics pusher thread" in msg:
                continue
            assert test_msg in msg

    serve.shutdown()
    ray.shutdown()


def test_recovering_controller_no_redeploy():
    """Ensure controller doesn't redeploy running deployments when recovering."""
    ray.init(namespace="x")
    client = serve.start(detached=True)

    @serve.deployment
    def f():
        pass

    serve.run(f.bind())

    num_actors = len(ray.util.list_named_actors(all_namespaces=True))
    pid = ray.get(client._controller.get_pid.remote())

    ray.kill(client._controller, no_restart=False)

    wait_for_condition(lambda: ray.get(client._controller.get_pid.remote()) != pid)

    # Confirm that no new deployment is deployed over the next 10 seconds
    with pytest.raises(RuntimeError):
        wait_for_condition(
            lambda: len(ray.util.list_named_actors(all_namespaces=True)) > num_actors,
            timeout=5,
        )

    serve.shutdown()
    ray.shutdown()


def test_updating_status_message(lower_slow_startup_threshold_and_reset):
    """Check if status message says if a serve deployment has taken a long time"""

    client = lower_slow_startup_threshold_and_reset

    @serve.deployment(
        num_replicas=5,
        ray_actor_options={"num_cpus": 1},
    )
    def f(*args):
        pass

    serve.run(f.bind(), _blocking=False)

    def updating_message():
        deployment_status = client.get_serve_status().deployment_statuses[0]
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

    client = lower_slow_startup_threshold_and_reset

    @serve.deployment
    class f:
        def __init__(self):
            self.num = 1 / 0

        def __call__(self, request):
            pass

    serve.run(f.bind(), _blocking=False)

    wait_for_condition(
        lambda: client.get_serve_status().deployment_statuses[0].status == "UNHEALTHY",
        timeout=20,
    )

    with pytest.raises(RuntimeError):
        wait_for_condition(
            lambda: client.get_serve_status().deployment_statuses[0].status
            == "UPDATING",
            timeout=10,
        )


@serve.deployment(ray_actor_options={"num_cpus": 0.1})
class Waiter:
    def __init__(self):
        time.sleep(5)

    def __call__(self, *args):
        return "May I take your order?"


WaiterNode = Waiter.bind()


def test_run_graph_task_uses_zero_cpus():
    """Check that the run_graph() task uses zero CPUs."""

    ray.init(num_cpus=2)
    client = serve.start(detached=True)

    config = {"import_path": "ray.serve.tests.test_standalone.WaiterNode"}
    config = ServeApplicationSchema.parse_obj(config)
    client.deploy_app(config)

    with pytest.raises(RuntimeError):
        wait_for_condition(lambda: ray.available_resources()["CPU"] < 1.9, timeout=5)

    wait_for_condition(
        lambda: requests.get("http://localhost:8000/Waiter").text
        == "May I take your order?"
    )

    serve.shutdown()
    ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
