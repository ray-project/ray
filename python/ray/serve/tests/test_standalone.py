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
from ray._common.test_utils import run_string_as_driver, wait_for_condition
from ray._raylet import GcsClient
from ray.cluster_utils import Cluster, cluster_not_supported
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_HA_PROXY,
    SERVE_DEFAULT_APP_NAME,
    SERVE_NAMESPACE,
    SERVE_PROXY_NAME,
)
from ray.serve._private.default_impl import create_cluster_node_info_cache
from ray.serve._private.http_util import set_socket_reuse_port
from ray.serve._private.test_utils import expected_proxy_actors
from ray.serve._private.utils import block_until_http_ready, format_actor_name
from ray.serve.config import (
    ControllerOptions,
    GangSchedulingConfig,
    HTTPOptions,
    ProxyLocation,
)
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


@pytest.mark.skipif(
    RAY_SERVE_ENABLE_HA_PROXY,
    reason="HAProxy ingress: user HTTP middleware runs on the replica, but the "
    "/-/routes endpoint is served by HAProxy, so middleware-injected headers "
    "are absent there.",
)
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
        print(f"[{i + 1}/{len(options)}] Running with {option}")
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

    # Controller and proxy on the head node. Under HAProxy the proxy is the
    # HAProxyManager alongside the fallback ProxyActor, which registers asynchronously.
    expected_classes = {"ServeController", *expected_proxy_actors()}

    def check_head_only_actors():
        actors = list_actors(
            address=head_node.address, filters=[("state", "=", "ALIVE")]
        )
        assert {actor.class_name for actor in actors} == expected_classes
        assert all(actor.node_id == head_node.node_id for actor in actors)
        return True

    wait_for_condition(check_head_only_actors)


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


def test_gang_updating_status_message(lower_slow_startup_threshold_and_reset):
    @serve.deployment(
        num_replicas=4,
        ray_actor_options={"num_cpus": 1},
        gang_scheduling_config=GangSchedulingConfig(gang_size=2),
    )
    def g(*args):
        pass

    serve._run(g.bind(), _blocking=False)

    def updating_message():
        deployment_status = (
            serve.status().applications[SERVE_DEFAULT_APP_NAME].deployments["g"]
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


def _deploy_flaky_app(counter_file, fail_count: int):
    os.environ["FLAKY_BUILD_COUNTER_FILE"] = str(counter_file)
    os.environ["FLAKY_BUILD_FAIL_COUNT"] = str(fail_count)
    counter_file.write_text("0")
    ray.init(num_cpus=1)
    serve.start()
    _get_global_client().deploy_apps(
        ServeDeploySchema(
            applications=[
                ServeApplicationSchema(
                    name="flaky_app",
                    route_prefix="/flaky",
                    import_path="ray.serve.tests.test_config_files.flaky_build.node",
                )
            ]
        )
    )


def test_build_app_retries_until_success(ray_shutdown, tmp_path):
    """A flaky build that succeeds on the 4th attempt deploys cleanly."""
    counter_file = tmp_path / "counter.txt"
    try:
        _deploy_flaky_app(counter_file, fail_count=3)
        wait_for_condition(
            lambda: serve.status().applications["flaky_app"].status == "RUNNING",
            timeout=60,
        )
        assert int(counter_file.read_text()) == 4
    finally:
        os.environ.pop("FLAKY_BUILD_COUNTER_FILE", None)
        os.environ.pop("FLAKY_BUILD_FAIL_COUNT", None)


def test_build_app_fails_after_retries_exhausted(ray_shutdown, tmp_path):
    """If the build keeps failing, the app status surfaces the user error."""
    counter_file = tmp_path / "counter.txt"
    try:
        _deploy_flaky_app(counter_file, fail_count=10)
        wait_for_condition(
            lambda: serve.status().applications["flaky_app"].status == "DEPLOY_FAILED",
            timeout=60,
        )
        assert "flaky build failure" in serve.status().applications["flaky_app"].message
        assert int(counter_file.read_text()) == 4
    finally:
        os.environ.pop("FLAKY_BUILD_COUNTER_FILE", None)
        os.environ.pop("FLAKY_BUILD_FAIL_COUNT", None)


@pytest.mark.parametrize(
    "options",
    [
        {
            "proxy_location": None,
            "http_options": None,
            "expected": HTTPOptions(location=ProxyLocation.EveryNode),
        },
        {
            "proxy_location": None,
            "http_options": {"test": "test"},  # location is not specified
            "expected": HTTPOptions(
                location=ProxyLocation.EveryNode
            ),  # using default proxy_location (to align with the case when `http_options` are None)
        },
        {
            "proxy_location": None,
            "http_options": {
                "location": "NoServer"
            },  # `location` is specified, but `proxy_location` is not
            "expected": HTTPOptions(
                location=ProxyLocation.Disabled
            ),  # using `location` value
        },
        {
            "proxy_location": None,
            "http_options": HTTPOptions(location=None),
            "expected": HTTPOptions(location=ProxyLocation.Disabled),
        },
        {
            "proxy_location": None,
            "http_options": HTTPOptions(),
            "expected": HTTPOptions(location=ProxyLocation.HeadOnly),
        },  # using default location from HTTPOptions
        {
            "proxy_location": None,
            "http_options": HTTPOptions(location="NoServer"),
            "expected": HTTPOptions(location=ProxyLocation.Disabled),
        },
        {
            "proxy_location": None,
            "http_options": {"location": "NoServer"},
            "expected": HTTPOptions(location=ProxyLocation.Disabled),
        },
        {
            "proxy_location": "Disabled",
            "http_options": None,
            "expected": HTTPOptions(location=ProxyLocation.Disabled),
        },
        {
            "proxy_location": "Disabled",
            "http_options": {},
            "expected": HTTPOptions(location=ProxyLocation.Disabled),
        },
        {
            "proxy_location": "Disabled",
            "http_options": HTTPOptions(host="foobar"),
            "expected": HTTPOptions(location=ProxyLocation.Disabled, host="foobar"),
        },
        {
            "proxy_location": "Disabled",
            "http_options": {"host": "foobar"},
            "expected": HTTPOptions(location=ProxyLocation.Disabled, host="foobar"),
        },
        {
            "proxy_location": "Disabled",
            "http_options": {"location": "HeadOnly"},
            "expected": HTTPOptions(location=ProxyLocation.Disabled),
        },
        {
            "proxy_location": ProxyLocation.Disabled,
            "http_options": HTTPOptions(location=ProxyLocation.HeadOnly),
            "expected": HTTPOptions(location=ProxyLocation.Disabled),
        },
    ],
)
def test_serve_start_proxy_location(ray_shutdown, options):
    expected_options = options.pop("expected")
    serve.start(**options)
    client = _get_global_client()
    assert ray.get(client._controller.get_http_config.remote()) == expected_options


@pytest.mark.parametrize(
    "controller_options",
    [
        ControllerOptions(
            runtime_env={
                "env_vars": {
                    "RAY_SERVE_TEST_CONTROLLER_ENV": "from-model",
                    "RAY_SERVE_TEST_CONTROLLER_ENV_2": "second",
                }
            }
        ),
        # Same options passed as a plain dict -- the API coerces it
        # through ``ControllerOptions.model_validate``.
        {
            "runtime_env": {
                "env_vars": {
                    "RAY_SERVE_TEST_CONTROLLER_ENV": "from-model",
                    "RAY_SERVE_TEST_CONTROLLER_ENV_2": "second",
                }
            }
        },
    ],
)
def test_serve_start_controller_options(ray_shutdown, controller_options):
    """``ControllerOptions.runtime_env.env_vars`` lands on the controller actor.

    Uses a custom RAY_SERVE_TEST_CONTROLLER_ENV var (not a real Serve knob)
    so the assertion is decoupled from whatever the Anyscale env hook
    auto-injects. The merge semantics are the env_hook's contract; this
    test only asserts that *our* requested env_vars made it through.
    """
    serve.start(controller_options=controller_options)
    client = _get_global_client()

    # Reach into the controller actor to read its own os.environ; the
    # controller is a singleton named actor on the head node, so a remote
    # task on the same handle runs in the same process.
    def _read_env(self, *, keys):
        import os as _os

        return {k: _os.environ.get(k) for k in keys}

    env_seen = ray.get(
        client._controller.__ray_call__.remote(
            _read_env,
            keys=[
                "RAY_SERVE_TEST_CONTROLLER_ENV",
                "RAY_SERVE_TEST_CONTROLLER_ENV_2",
            ],
        )
    )
    assert env_seen["RAY_SERVE_TEST_CONTROLLER_ENV"] == "from-model"
    assert env_seen["RAY_SERVE_TEST_CONTROLLER_ENV_2"] == "second"


def test_serve_start_controller_options_rejects_disallowed_runtime_env(
    ray_shutdown,
):
    """Bad runtime_env fails at the caller, not from a Ray task."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as exc:
        serve.start(controller_options={"runtime_env": {"pip": ["numpy"]}})
    assert "only supports ['env_vars']" in str(exc.value)


def test_serve_start_does_not_leak_idle_worker(ray_shutdown):
    """Regression test for #63596 / PR #63597.

    Before the fix, ``serve_start_async`` ran ``_start_controller`` as a remote
    Ray task and returned the controller ``ActorHandle`` cross-process to the
    caller (the Dashboard Agent). That transfer inserted the handle's
    ObjectRefs into the executor worker's ``stored_in_objects``, pinning the
    worker IDLE forever (it could never drain). Accumulated across calls in a
    long-lived caller, this eventually OOM'd the head node.

    With the inline fix, controller creation runs in the caller process — there
    is no executor worker to pin. This test drives ``serve_start_async`` (the
    exact #63596 path the Dashboard Agent uses) and asserts the symptom.

    ``ray._private.state.workers()`` reports all WORKER-type processes, which
    includes the actor-hosting workers for the controller and HTTP proxies.
    Those are created on ``serve_start_async`` and reaped on ``serve.shutdown()``
    each cycle, so they do not accumulate. The leaked ``_start_controller``
    executor, by contrast, was pinned IDLE and SURVIVED ``serve.shutdown()``
    (its ``object_id_refs_`` never drained) — so it accumulated across cycles
    and grew the count. The non-growth assertion below catches exactly that.
    """
    import asyncio

    import ray._private.state as state
    from ray.serve._private.api import serve_start_async

    def _idle_worker_count() -> int:
        return len(state.workers())

    # Start and stop Serve several times via the async path. The leaked
    # executor (pre-fix) would survive serve.shutdown() because it was pinned
    # IDLE and never drained, so each cycle would add one to the worker count.
    # Post-fix there is no such executor, so the count must not grow.
    cycles = 3
    counts = []
    for _ in range(cycles):
        asyncio.run(serve_start_async())
        time.sleep(1)
        counts.append(_idle_worker_count())
        serve.shutdown()

    # The count must not grow across cycles — that growth was the leak.
    # Allow equal-or-fewer (reaping may reduce it); forbid growth.
    assert counts[-1] <= counts[0], (
        f"Idle WORKER count grew across {cycles} serve_start_async()/shutdown() "
        f"cycles: {counts}. Growth indicates a pinned (leaked) executor worker "
        f"— the #63596 regression."
    )


def test_serve_start_no_remote_start_controller_task():
    """Structural regression test for PR #63597.

    The leak came from wrapping ``_start_controller`` in a remote Ray task and
    returning its ``ActorHandle`` cross-process. The fix removes that wrapper
    entirely and inlines controller creation. Assert the wrapper symbol is gone
    and neither start path dispatches a remote task for controller creation.
    This guards against a future reintroduction of the leak pattern even if the
    behavioral test above is environment-sensitive.
    """
    import inspect

    from ray.serve._private import api as serve_api

    assert not hasattr(serve_api, "_start_controller"), (
        "_start_controller should be removed; its remote-task wrapping was the "
        "leak source in #63596."
    )

    src_async = inspect.getsource(serve_api.serve_start_async)
    src_sync = inspect.getsource(serve_api.serve_start)
    assert "ray.remote(_start_controller)" not in src_async
    assert "ray.remote(_start_controller)" not in src_sync
    assert "_create_controller_and_proxy_refs" in src_async
    assert "_create_controller_and_proxy_refs" in src_sync


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
