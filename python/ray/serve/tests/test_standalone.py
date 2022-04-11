"""
The test file for all standalone tests that doesn't
requires a shared Serve instance.
"""
import os
import subprocess
import sys
import socket
from typing import Optional
from tempfile import mkstemp

import pytest
import pydantic
import requests

import ray
from ray import serve
from ray.cluster_utils import Cluster, cluster_not_supported
from ray.serve.constants import SERVE_ROOT_URL_ENV_KEY, SERVE_PROXY_NAME
from ray.serve.exceptions import RayServeException
from ray.serve.generated.serve_pb2 import ActorNameList
from ray.serve.utils import block_until_http_ready, get_all_node_ids, format_actor_name
from ray.serve.config import HTTPOptions
from ray.serve.api import internal_get_global_client
from ray._private.test_utils import (
    run_string_as_driver,
    wait_for_condition,
    convert_actor_state,
)
from ray._private.services import new_port
import ray._private.gcs_utils as gcs_utils

# Explicitly importing it here because it is a ray core tests utility (
# not in the tree)
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


def test_shutdown(ray_shutdown):
    ray.init(num_cpus=16)
    serve.start(http_options=dict(port=8003))

    @serve.deployment
    def f():
        pass

    f.deploy()

    serve_controller_name = serve.api._global_client._controller_name
    actor_names = [
        serve_controller_name,
        format_actor_name(
            SERVE_PROXY_NAME,
            serve.api._global_client._controller_name,
            get_all_node_ids()[0][0],
        ),
    ]

    def check_alive():
        alive = True
        for actor_name in actor_names:
            try:
                if actor_name == serve_controller_name:
                    ray.get_actor(
                        actor_name, namespace=ray.get_runtime_context().namespace
                    )
                else:
                    ray.get_actor(actor_name)
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
                if actor_name == serve_controller_name:
                    ray.get_actor(
                        actor_name, namespace=ray.get_runtime_context().namespace
                    )
                else:
                    ray.get_actor(actor_name)
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
    ray.init(head_node.address, namespace="serve")
    first_job_id = ray.get_runtime_context().job_id
    serve.start(detached=True)

    @serve.deployment(route_prefix="/say_hi_f")
    def f(*args):
        return "from_f"

    f.deploy()
    assert ray.get(f.get_handle().remote()) == "from_f"
    assert requests.get("http://localhost:8000/say_hi_f").text == "from_f"

    serve.api._global_client = None
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

    connect_in_deployment.deploy()
    ray.get(connect_in_deployment.get_handle().remote())
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


@pytest.mark.skipif(
    not hasattr(socket, "SO_REUSEPORT"),
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
    node_ids = ray.state.node_ids()
    assert len(node_ids) == 2
    serve.start(http_options=dict(port=8005, location="EveryNode"))

    def get_proxy_names():
        proxy_names = []
        for node_id, _ in get_all_node_ids():
            proxy_names.append(
                format_actor_name(
                    SERVE_PROXY_NAME, serve.api._global_client._controller_name, node_id
                )
            )
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
    f.deploy()
    assert f.url == root_url + "/f"
    serve.shutdown()
    ray.shutdown()
    del os.environ[SERVE_ROOT_URL_ENV_KEY]

    port = new_port()
    serve.start(http_options=dict(port=port))
    f.deploy()
    assert f.url != root_url + "/f"
    assert f.url == f"http://127.0.0.1:{port}/f"
    serve.shutdown()
    ray.shutdown()

    ray.init(runtime_env={"env_vars": {SERVE_ROOT_URL_ENV_KEY: root_url}})
    port = new_port()
    serve.start(http_options=dict(port=port))
    f.deploy()
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
            for actor in ray.state.actors().values()
            if actor["State"] == convert_actor_state(gcs_utils.ActorTableData.ALIVE)
        ]
        assert len(live_actors) == 1
        controller = serve.api._global_client._controller
        assert len(ray.get(controller.get_http_proxies.remote())) == 0

        # Test that the handle still works.
        @serve.deployment
        def hello(*args):
            return "hello"

        hello.deploy()

        assert ray.get(hello.get_handle().remote()) == "hello"
        serve.shutdown()


def test_http_head_only(ray_cluster):
    cluster = ray_cluster
    head_node = cluster.add_node(num_cpus=4)
    cluster.add_node(num_cpus=4)

    ray.init(head_node.address)
    node_ids = ray.state.node_ids()
    assert len(node_ids) == 2

    serve.start(http_options={"port": new_port(), "location": "HeadOnly"})

    # Only the controller and head node actor should be started
    assert len(ray.state.actors()) == 2

    # They should all be placed on the head node
    cpu_per_nodes = {
        r["CPU"] for r in ray.state.state._available_resources_per_node().values()
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
    node_ids = ray.state.node_ids()
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
    controller_handle = internal_get_global_client()._controller
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

    A.deploy()

    assert len(serve.list_deployments()) == 1

    serve.shutdown()
    serve.start(detached=True)

    assert len(serve.list_deployments()) == 0

    A.deploy()

    assert len(serve.list_deployments()) == 1


def test_detached_namespace_default_ray_init(ray_shutdown):
    # Can start detached instance when ray is not initialized.
    serve.start(detached=True)


def test_detached_instance_in_non_anonymous_namespace(ray_shutdown):
    # Can start detached instance in non-anonymous namespace.
    ray.init(namespace="foo")
    serve.start(detached=True)


@pytest.mark.parametrize("namespace", [None, "test_namespace"])
@pytest.mark.parametrize("detached", [True, False])
def test_serve_controller_namespace(
    ray_shutdown, namespace: Optional[str], detached: bool
):
    """
    Tests the serve controller is started in the current namespace if not
    anonymous or in the "serve" namespace if no namespace is specified.
    When the controller is started in the "serve" namespace, this also tests
    that we can get the serve controller from another namespace.
    """

    ray.init(namespace=namespace)
    serve.start(detached=detached)
    client = serve.api._global_client
    if namespace:
        controller_namespace = namespace
    elif detached:
        controller_namespace = "serve"
    else:
        controller_namespace = ray.get_runtime_context().namespace

    assert ray.get_actor(client._controller_name, namespace=controller_namespace)


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

A.deploy()"""

    run_string_as_driver(
        driver_template.format(address=address, namespace="test_namespace1", port=8000)
    )
    run_string_as_driver(
        driver_template.format(address=address, namespace="test_namespace2", port=8001)
    )


def test_local_store_recovery(ray_shutdown):
    _, tmp_path = mkstemp()

    @serve.deployment
    def hello(_):
        return "hello"

    # https://github.com/ray-project/ray/issues/19987
    @serve.deployment
    def world(_):
        return "world"

    def check(name, raise_error=False):
        try:
            resp = requests.get(f"http://localhost:8000/{name}")
            assert resp.text == name
            return True
        except Exception as e:
            if raise_error:
                raise e
            return False

    # https://github.com/ray-project/ray/issues/20159
    # https://github.com/ray-project/ray/issues/20158
    def clean_up_leaked_processes():
        import psutil

        for proc in psutil.process_iter():
            try:
                cmdline = " ".join(proc.cmdline())
                if "ray::" in cmdline:
                    print(f"Kill {proc} {cmdline}")
                    proc.kill()
            except Exception:
                pass

    def crash():
        subprocess.call(["ray", "stop", "--force"])
        clean_up_leaked_processes()
        ray.shutdown()
        serve.shutdown()

    serve.start(detached=True, _checkpoint_path=f"file://{tmp_path}")
    hello.deploy()
    world.deploy()
    assert check("hello", raise_error=True)
    assert check("world", raise_error=True)
    crash()

    # Simulate a crash

    serve.start(detached=True, _checkpoint_path=f"file://{tmp_path}")
    wait_for_condition(lambda: check("hello"))
    # wait_for_condition(lambda: check("world"))
    crash()


@pytest.mark.parametrize("ray_start_with_dashboard", [{"num_cpus": 4}], indirect=True)
def test_snapshot_always_written_to_internal_kv(
    ray_start_with_dashboard, ray_shutdown  # noqa: F811
):
    # https://github.com/ray-project/ray/issues/19752
    _, tmp_path = mkstemp()

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

    serve.start(detached=True, _checkpoint_path=f"file://{tmp_path}")
    hello.deploy()
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
    import logging
    from tempfile import mkstemp
    from ray.serve.utils import logger
    from ray._private.services import new_port

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
    _, tmp_path = mkstemp()
    test_ckpt = f"file://{tmp_path}"

    serve.start(detached=True, http_options=test_http, _checkpoint_path=test_ckpt)

    for test_config, msg in zip([[test_ckpt], ["host", "port"]], warning_msg):
        for test_msg in test_config:
            assert test_msg in msg

    serve.shutdown()
    ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
