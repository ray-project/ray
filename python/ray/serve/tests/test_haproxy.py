import asyncio
import logging
import subprocess
import sys
import threading
import time
from tempfile import NamedTemporaryFile

import httpx
import pytest
import requests

import ray
from ray import serve
from ray._common.test_utils import (
    SignalActor,
    wait_for_condition,
)
from ray.actor import ActorHandle
from ray.cluster_utils import Cluster
from ray.serve._private.constants import (
    DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
    RAY_SERVE_ENABLE_HA_PROXY,
    SERVE_NAMESPACE,
)
from ray.serve._private.haproxy import HAProxyManager
from ray.serve._private.test_utils import get_application_url
from ray.serve.context import _get_global_client
from ray.serve.schema import (
    ProxyStatus,
    ServeDeploySchema,
    ServeInstanceDetails,
)
from ray.serve.tests.conftest import *  # noqa
from ray.serve.tests.test_cli_2 import ping_endpoint
from ray.tests.conftest import call_ray_stop_only  # noqa: F401
from ray.util.state import list_actors

logger = logging.getLogger(__name__)

# Skip all tests in this module if the HAProxy feature flag is not enabled
pytestmark = pytest.mark.skipif(
    not RAY_SERVE_ENABLE_HA_PROXY,
    reason="RAY_SERVE_ENABLE_HA_PROXY not set.",
)


@pytest.fixture(autouse=True)
def clean_up_haproxy_processes():
    """Clean up haproxy processes before and after each test."""
    subprocess.run(
        ["pkill", "-x", "haproxy"], capture_output=True, text=True, check=False
    )
    yield
    # After test: verify no haproxy processes are running
    result = subprocess.run(
        ["pgrep", "-x", "haproxy"], capture_output=True, text=True, check=False
    )
    assert (
        result.returncode != 0
    ), f"HAProxy processes still running after test: {result.stdout.strip()}"


@pytest.fixture
def shutdown_ray():
    if ray.is_initialized():
        ray.shutdown()
    yield
    if ray.is_initialized():
        ray.shutdown()


def test_deploy_with_no_applications(ray_shutdown):
    """Deploy an empty list of applications, serve should just be started."""
    ray.init(num_cpus=8)
    serve.start(http_options=dict(port=8003))
    client = _get_global_client()
    config = ServeDeploySchema.parse_obj({"applications": []})
    client.deploy_apps(config)

    def serve_running():
        ServeInstanceDetails.parse_obj(
            ray.get(client._controller.get_serve_instance_details.remote())
        )
        actors = list_actors(
            filters=[
                ("ray_namespace", "=", SERVE_NAMESPACE),
                ("state", "=", "ALIVE"),
            ]
        )
        actor_names = [actor["class_name"] for actor in actors]
        return "ServeController" in actor_names and "HAProxyManager" in actor_names

    wait_for_condition(serve_running)
    client.shutdown()


def test_single_app_shutdown_actors(ray_shutdown):
    """Tests serve.shutdown() works correctly in single-app case

    Ensures that after deploying a (nameless) app using serve.run(), serve.shutdown()
    deletes all actors (controller, haproxy, all replicas) in the "serve" namespace.
    """
    address = ray.init(num_cpus=8)["address"]
    serve.start(http_options=dict(port=8003))

    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app")

    actor_names = {
        "ServeController",
        "HAProxyManager",
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
    deletes all actors (controller, haproxy, all replicas) in the "serve" namespace.
    """
    address = ray.init(num_cpus=8)["address"]
    serve.start(http_options=dict(port=8003))

    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app")

    actor_names = {
        "ServeController",
        "HAProxyManager",
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


def test_haproxy_subprocess_killed_on_manager_shutdown(ray_shutdown):
    """Test that the HAProxy subprocess is killed when the HAProxyManager actor is shutdown.

    This ensures proper cleanup of HAProxy processes when the manager is killed,
    preventing orphaned HAProxy processes.
    """

    def get_haproxy_pids():
        """Get all haproxy process PIDs."""
        result = subprocess.run(
            ["pgrep", "-x", "haproxy"], capture_output=True, text=True, timeout=2
        )
        if result.returncode == 0 and result.stdout.strip():
            return [int(pid) for pid in result.stdout.strip().split("\n")]

        return []

    wait_for_condition(
        lambda: len(get_haproxy_pids()) == 0, timeout=5, retry_interval_ms=100
    )

    @serve.deployment
    def hello():
        return "hello"

    serve.run(hello.bind())
    wait_for_condition(
        lambda: len(get_haproxy_pids()) == 1, timeout=10, retry_interval_ms=100
    )

    serve.shutdown()

    wait_for_condition(
        lambda: len(get_haproxy_pids()) == 0, timeout=10, retry_interval_ms=100
    )


# TODO(alexyang): Delete these tests and run test_proxy.py instead once HAProxy is fully supported.
class TestTimeoutKeepAliveConfig:
    """Test setting keep_alive_timeout_s in config and env."""

    def get_proxy_actor(self) -> ActorHandle:
        [proxy_actor] = list_actors(filters=[("class_name", "=", "HAProxyManager")])
        return ray.get_actor(proxy_actor.name, namespace=SERVE_NAMESPACE)

    def test_default_keep_alive_timeout_s(self, ray_shutdown):
        """Test when no keep_alive_timeout_s is set.

        When the keep_alive_timeout_s is not set, the uvicorn keep alive is 5.
        """
        serve.start()
        proxy_actor = self.get_proxy_actor()
        assert (
            ray.get(proxy_actor._get_http_options.remote()).keep_alive_timeout_s
            == DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S
        )

    def test_set_keep_alive_timeout_in_http_configs(self, ray_shutdown):
        """Test when keep_alive_timeout_s is in http configs.

        When the keep_alive_timeout_s is set in http configs, the uvicorn keep alive
        is set correctly.
        """
        keep_alive_timeout_s = 222
        serve.start(http_options={"keep_alive_timeout_s": keep_alive_timeout_s})
        proxy_actor = self.get_proxy_actor()
        assert (
            ray.get(proxy_actor._get_http_options.remote()).keep_alive_timeout_s
            == keep_alive_timeout_s
        )

    @pytest.mark.parametrize(
        "ray_instance",
        [
            {"RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S": "333"},
        ],
        indirect=True,
    )
    def test_set_keep_alive_timeout_in_env(self, ray_instance, ray_shutdown):
        """Test when keep_alive_timeout_s is in env.

        When the keep_alive_timeout_s is set in env, the uvicorn keep alive
        is set correctly.
        """
        serve.start()
        proxy_actor = self.get_proxy_actor()
        assert (
            ray.get(proxy_actor._get_http_options.remote()).keep_alive_timeout_s == 333
        )

    @pytest.mark.parametrize(
        "ray_instance",
        [
            {"RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S": "333"},
        ],
        indirect=True,
    )
    def test_set_timeout_keep_alive_in_both_config_and_env(
        self, ray_instance, ray_shutdown
    ):
        """Test when keep_alive_timeout_s is in both http configs and env.

        When the keep_alive_timeout_s is set in env, the uvicorn keep alive
        is set to the one in env.
        """
        keep_alive_timeout_s = 222
        serve.start(http_options={"keep_alive_timeout_s": keep_alive_timeout_s})
        proxy_actor = self.get_proxy_actor()
        assert (
            ray.get(proxy_actor._get_http_options.remote()).keep_alive_timeout_s == 333
        )


@pytest.mark.asyncio
async def test_drain_and_undrain_haproxy_manager(
    monkeypatch, shutdown_ray, call_ray_stop_only  # noqa: F811
):
    """Test the state transtion of the haproxy manager between
    HEALTHY, DRAINING and DRAINED
    """
    monkeypatch.setenv("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "10")
    monkeypatch.setenv("SERVE_SOCKET_REUSE_PORT_ENABLED", "1")

    cluster = Cluster()
    head_node = cluster.add_node(num_cpus=0)
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    ray.init(address=head_node.address)
    serve.start(http_options={"location": "EveryNode"})

    signal_actor = SignalActor.remote()

    @serve.deployment
    class HelloModel:
        async def __call__(self):
            await signal_actor.wait.remote()
            return "hello"

    serve.run(HelloModel.options(num_replicas=2).bind())

    # 3 proxies, 1 controller, 2 replicas, 1 signal actor
    wait_for_condition(lambda: len(list_actors()) == 7)
    assert len(ray.nodes()) == 3

    client = _get_global_client()
    serve_details = ServeInstanceDetails(
        **ray.get(client._controller.get_serve_instance_details.remote())
    )
    proxy_actor_ids = {proxy.actor_id for _, proxy in serve_details.proxies.items()}

    assert len(proxy_actor_ids) == 3

    # Start a long-running request in background to test draining behavior
    request_result = []

    def make_blocking_request():
        try:
            response = httpx.get("http://localhost:8000/", timeout=5)
            request_result.append(("success", response.status_code))
        except Exception as e:
            request_result.append(("error", str(e)))

    request_thread = threading.Thread(target=make_blocking_request)
    request_thread.start()

    wait_for_condition(
        lambda: ray.get(signal_actor.cur_num_waiters.remote()) >= 1, timeout=10
    )

    serve.run(HelloModel.options(num_replicas=1).bind())

    # 1 proxy should be draining

    def check_proxy_status(proxy_status_to_count):
        serve_details = ServeInstanceDetails(
            **ray.get(client._controller.get_serve_instance_details.remote())
        )
        proxy_status_list = [proxy.status for _, proxy in serve_details.proxies.items()]
        current_status = {
            status: proxy_status_list.count(status) for status in proxy_status_list
        }
        return current_status == proxy_status_to_count, current_status

    wait_for_condition(
        condition_predictor=check_proxy_status,
        proxy_status_to_count={ProxyStatus.HEALTHY: 2, ProxyStatus.DRAINING: 1},
    )

    # should stay in draining status until the signal is sent
    await asyncio.sleep(1)

    assert check_proxy_status(
        proxy_status_to_count={ProxyStatus.HEALTHY: 2, ProxyStatus.DRAINING: 1}
    )

    serve.run(HelloModel.options(num_replicas=2).bind())
    # The proxy should return to healthy status
    wait_for_condition(
        condition_predictor=check_proxy_status,
        proxy_status_to_count={ProxyStatus.HEALTHY: 3},
    )
    serve_details = ServeInstanceDetails(
        **ray.get(client._controller.get_serve_instance_details.remote())
    )

    assert {
        proxy.actor_id for _, proxy in serve_details.proxies.items()
    } == proxy_actor_ids

    serve.run(HelloModel.options(num_replicas=1).bind())
    await signal_actor.send.remote()
    # 1 proxy should be draining and eventually be drained.
    wait_for_condition(
        condition_predictor=check_proxy_status,
        timeout=40,
        proxy_status_to_count={ProxyStatus.HEALTHY: 2},
    )

    # Verify the long-running request completed successfully
    request_thread.join(timeout=5)

    # Clean up serve.
    serve.shutdown()


def test_haproxy_failure(ray_shutdown):
    """Test HAProxyManager is successfully restarted after being killed."""
    ray.init(num_cpus=1)
    serve.start()

    @serve.deployment(name="proxy_failure")
    def function(_):
        return "hello1"

    serve.run(function.bind())

    def check_proxy_alive():
        actors = list_actors(
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return "HAProxyManager" in {actor["class_name"] for actor in actors}

    wait_for_condition(check_proxy_alive)

    [proxy_actor] = list_actors(
        filters=[("class_name", "=", "HAProxyManager"), ("state", "=", "ALIVE")]
    )
    proxy_actor_id = proxy_actor.actor_id

    proxy_actor = ray.get_actor(proxy_actor.name, namespace=SERVE_NAMESPACE)
    ray.kill(proxy_actor, no_restart=False)

    def check_new_proxy():
        proxies = list_actors(
            filters=[("class_name", "=", "HAProxyManager"), ("state", "=", "ALIVE")]
        )
        return len(proxies) == 1 and proxies[0].actor_id != proxy_actor_id

    wait_for_condition(check_new_proxy, timeout=45)
    serve.shutdown()


def test_haproxy_get_target_groups(shutdown_ray):
    """Test that haproxy get_target_groups retrieves the correct target groups."""
    ray.init(num_cpus=4)
    serve.start()

    @serve.deployment
    def function(_):
        return "hello1"

    # Deploy the application
    serve.run(
        function.options(num_replicas=1).bind(), name="test_app", route_prefix="/test"
    )

    def check_proxy_alive():
        actors = list_actors(
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return "HAProxyManager" in {actor["class_name"] for actor in actors}

    wait_for_condition(check_proxy_alive)

    [proxy_actor] = list_actors(
        filters=[("class_name", "=", "HAProxyManager"), ("state", "=", "ALIVE")]
    )
    proxy_actor = ray.get_actor(proxy_actor.name, namespace=SERVE_NAMESPACE)

    def has_n_targets(route_prefix: str, n: int):
        target_groups = ray.get(proxy_actor.get_target_groups.remote())
        for tg in target_groups:
            if (
                tg.route_prefix == route_prefix
                and len(tg.targets) == n
                and tg.fallback_target is not None
            ):
                return True
        return False

    wait_for_condition(has_n_targets, route_prefix="/test", n=1)

    serve.run(
        function.options(num_replicas=2).bind(), name="test_app", route_prefix="/test2"
    )
    wait_for_condition(has_n_targets, route_prefix="/test2", n=2)

    serve.shutdown()


@pytest.mark.asyncio
async def test_haproxy_update_target_groups(ray_shutdown):
    """Test that the haproxy correctly updates the target groups."""
    ray.init(num_cpus=4)
    serve.start(http_options={"host": "0.0.0.0"})

    @serve.deployment
    def function(_):
        return "hello1"

    serve.run(
        function.options(num_replicas=1).bind(), name="app1", route_prefix="/test"
    )
    assert httpx.get("http://localhost:8000/test").text == "hello1"
    assert httpx.get("http://localhost:8000/test2").status_code == 404

    serve.run(
        function.options(num_replicas=1).bind(), name="app2", route_prefix="/test2"
    )
    assert httpx.get("http://localhost:8000/test").text == "hello1"
    assert httpx.get("http://localhost:8000/test2").text == "hello1"

    serve.delete("app1")
    assert httpx.get("http://localhost:8000/test").status_code == 404
    assert httpx.get("http://localhost:8000/test2").text == "hello1"

    serve.run(
        function.options(num_replicas=1).bind(), name="app1", route_prefix="/test"
    )
    assert httpx.get("http://localhost:8000/test").text == "hello1"
    assert httpx.get("http://localhost:8000/test2").text == "hello1"

    serve.shutdown()


@pytest.mark.asyncio
async def test_haproxy_update_draining_health_checks(ray_shutdown):
    """Test that the haproxy update_draining method updates the HAProxy health checks."""
    ray.init(num_cpus=4)
    serve.start()

    signal_actor = SignalActor.remote()

    @serve.deployment
    async def function(_):
        await signal_actor.wait.remote()
        return "hello1"

    serve.run(function.bind())

    def check_proxy_alive():
        actors = list_actors(
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return "HAProxyManager" in {actor["class_name"] for actor in actors}

    wait_for_condition(check_proxy_alive)

    [proxy_actor] = list_actors(
        filters=[("class_name", "=", "HAProxyManager"), ("state", "=", "ALIVE")]
    )
    proxy_actor = ray.get_actor(proxy_actor.name, namespace=SERVE_NAMESPACE)

    assert httpx.get("http://localhost:8000/-/healthz").status_code == 200

    await proxy_actor.update_draining.remote(draining=True)
    wait_for_condition(
        lambda: httpx.get("http://localhost:8000/-/healthz").status_code == 503
    )

    await proxy_actor.update_draining.remote(draining=False)
    wait_for_condition(
        lambda: httpx.get("http://localhost:8000/-/healthz").status_code == 200
    )
    assert not await proxy_actor._is_draining.remote()

    serve.shutdown()


def test_haproxy_http_options(ray_shutdown):
    """Test that the haproxy config file is generated correctly with http options."""
    ray.init(num_cpus=4)
    serve.start(
        http_options={
            "host": "0.0.0.0",
            "port": 8001,
            "keep_alive_timeout_s": 30,
        },
    )

    @serve.deployment
    def function(_):
        return "hello1"

    serve.run(function.bind(), name="test_app", route_prefix="/test")
    url = get_application_url(app_name="test_app", use_localhost=False)
    assert httpx.get(url).text == "hello1"
    with pytest.raises(httpx.ConnectError):
        _ = httpx.get(url.replace(":8001", ":8000")).status_code

    serve.shutdown()


def test_haproxy_metrics(ray_shutdown):
    """Test that the haproxy metrics are exported correctly."""
    ray.init(num_cpus=4)
    serve.start(
        http_options={
            "host": "0.0.0.0",
        },
    )

    @serve.deployment
    def function(_):
        return "hello1"

    serve.run(function.bind())

    assert httpx.get("http://localhost:8000/").text == "hello1"

    metrics_response = httpx.get("http://localhost:9101/metrics")
    assert metrics_response.status_code == 200

    http_backend_metrics = (
        'haproxy_backend_http_responses_total{proxy="http-default",code="2xx"} 1'
    )
    assert http_backend_metrics in metrics_response.text

    serve.shutdown()


def test_haproxy_safe_name():
    """Test that the safe name is generated correctly."""
    assert HAProxyManager.get_safe_name("HTTP-test_foo.bar") == "HTTP-test_foo.bar"
    assert HAProxyManager.get_safe_name("HTTP:test") == "HTTP_test"
    assert HAProxyManager.get_safe_name("HTTP:test/foo") == "HTTP_test.foo"
    assert HAProxyManager.get_safe_name("replica#abc") == "replica-abc"


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_build_multi_app(ray_start_stop):
    with NamedTemporaryFile(mode="w+", suffix=".yaml") as tmp:
        print('Building nodes "TestApp1Node" and "TestApp2Node".')
        # Build an app
        subprocess.check_output(
            [
                "serve",
                "build",
                "ray.serve.tests.test_cli_3.TestApp1Node",
                "ray.serve.tests.test_cli_3.TestApp2Node",
                "-o",
                tmp.name,
            ]
        )
        print("Build succeeded! Deploying node.")

        subprocess.check_output(["serve", "deploy", tmp.name])
        print("Deploy succeeded!")
        wait_for_condition(
            lambda: ping_endpoint("app1") == "wonderful world", timeout=15
        )
        print("App 1 is live and reachable over HTTP.")
        wait_for_condition(
            lambda: ping_endpoint("app2") == "wonderful world", timeout=15
        )
        print("App 2 is live and reachable over HTTP.")

        print("Deleting applications.")
        app_urls = [
            get_application_url("HTTP", app_name=app) for app in ["app1", "app2"]
        ]
        subprocess.check_output(["serve", "shutdown", "-y"])

        def check_no_apps():
            for url in app_urls:
                with pytest.raises(httpx.HTTPError):
                    _ = httpx.get(url).text
            return True

        wait_for_condition(check_no_apps, timeout=15)
        print("Delete succeeded! Node is no longer reachable over HTTP.")


def test_haproxy_manager_ready_with_application(ray_shutdown):
    """Test that HAProxyManager.ready() succeeds when an application is deployed."""
    ray.init(num_cpus=4)
    serve.start()

    @serve.deployment
    def function(_):
        return "hello"

    # Deploy application
    serve.run(function.bind(), name="test_app", route_prefix="/test")

    # Get HAProxyManager actor
    def check_proxy_alive():
        actors = list_actors(
            filters=[("ray_namespace", "=", SERVE_NAMESPACE), ("state", "=", "ALIVE")],
        )
        return "HAProxyManager" in {actor["class_name"] for actor in actors}

    wait_for_condition(check_proxy_alive)

    [proxy_actor] = list_actors(
        filters=[("class_name", "=", "HAProxyManager"), ("state", "=", "ALIVE")]
    )
    proxy_actor = ray.get_actor(proxy_actor.name, namespace=SERVE_NAMESPACE)

    # Call ready() - should succeed with active targets
    ready_result = ray.get(proxy_actor.ready.remote())
    assert ready_result is not None

    wait_for_condition(lambda: httpx.get("http://localhost:8000/test").text == "hello")

    serve.shutdown()


def test_504_error_translated_to_500(ray_shutdown, monkeypatch):
    """Test that HAProxy translates 504 Gateway Timeout errors to 500 Internal Server Error."""
    monkeypatch.setenv("RAY_SERVE_HAPROXY_TIMEOUT_SERVER_S", "2")
    monkeypatch.setenv("RAY_SERVE_HAPROXY_TIMEOUT_CONNECT_S", "1")

    ray.init(num_cpus=8)
    serve.start(http_options=dict(port=8003))

    @serve.deployment
    class TimeoutDeployment:
        def __call__(self, request):
            # Sleep for 3 seconds, longer than HAProxy's 2s timeout
            # Use regular time.sleep (not async) to avoid event loop issues
            time.sleep(3)
            return "This should not be reached"

    serve.run(TimeoutDeployment.bind(), name="timeout_app", route_prefix="/test")

    url = get_application_url("HTTP", app_name="timeout_app")

    # HAProxy should timeout after 2s and return 504->500
    # Client timeout is 10s to ensure HAProxy times out first
    response = requests.get(f"{url}/test", timeout=10)

    # Verify we got 500 (translated from 504), not 504 or 200
    assert (
        response.status_code == 500
    ), f"Expected 500 Internal Server Error (translated from 504), got {response.status_code}"
    assert (
        "Internal Server Error" in response.text
    ), f"Response should contain 'Internal Server Error' message, got: {response.text}"


def test_502_error_translated_to_500(ray_shutdown):
    """Test that HAProxy translates 502 Bad Gateway errors to 500 Internal Server Error."""
    ray.init(num_cpus=8)
    serve.start(http_options=dict(port=8003))

    @serve.deployment
    class BrokenDeployment:
        def __call__(self, request):
            # Always raise an exception to simulate backend failure
            raise RuntimeError("Simulated backend failure for 502 error")

    serve.run(
        BrokenDeployment.bind(), name="broken_app", route_prefix="/test", blocking=False
    )
    url = get_application_url("HTTP", app_name="broken_app")
    response = requests.get(f"{url}/test", timeout=5)

    assert (
        response.status_code == 500
    ), f"Expected 500 Internal Server Error, got {response.status_code}"
    assert (
        "Internal Server Error" in response.text
    ), "Response should contain 'Internal Server Error' message"


def test_haproxy_healthcheck_multiple_apps_and_backends(ray_shutdown):
    """Health check behavior with 3 apps and 2 servers per backend.

    Expectations:
    - With two servers per backend, healthz returns 200 (all backends have a primary UP).
    - Disabling one primary in each backend keeps health at 200 (the other primary is UP).
    - Disabling all servers in each backend results in healthz 503.
    """
    ray.init(num_cpus=8)
    serve.start()

    @serve.deployment
    def f(_):
        return "hello"

    # Helpers
    SOCKET_PATH = "/tmp/haproxy-serve/admin.sock"

    def app_to_backend(app: str) -> str:
        return f"http-{app}"

    def haproxy_show_stat() -> str:
        result = subprocess.run(
            f'echo "show stat" | socat - {SOCKET_PATH}',
            shell=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to query HAProxy stats: {result.stderr}")
        return result.stdout

    def list_primary_servers(backend_name: str) -> list:
        lines = haproxy_show_stat().strip().split("\n")
        servers = []
        for line in lines:
            parts = line.split(",")
            if len(parts) < 2:
                continue
            pxname, svname = parts[0], parts[1]
            if pxname == backend_name and svname not in [
                "FRONTEND",
                "BACKEND",
            ]:
                servers.append(svname)
        return servers

    def set_server_state(backend: str, server: str, state: str) -> None:
        subprocess.run(
            f'echo "set server {backend}/{server} state {state}" | socat - {SOCKET_PATH}',
            shell=True,
            capture_output=True,
            timeout=5,
        )

    def wait_health(expected: int, timeout: float = 15.0) -> None:
        wait_for_condition(
            lambda: httpx.get("http://localhost:8000/-/healthz").status_code
            == expected,
            timeout=timeout,
        )

    # Deploy 3 apps, each with 2 replicas (servers) so each backend has 2 servers + 1 backup
    apps = [
        ("app_a", "/a"),
        ("app_b", "/b"),
        ("app_c", "/c"),
    ]
    for app_name, route in apps:
        serve.run(f.options(num_replicas=2).bind(), name=app_name, route_prefix=route)

    # Wait for all endpoints to be reachable
    for _, route in apps:
        wait_for_condition(
            lambda r=route: httpx.get(f"http://localhost:8000{r}").text == "hello"
        )

    # Wait until each backend shows 2 primary servers in HAProxy stats
    backends = [app_to_backend(app) for app, _ in apps]
    for be in backends:
        wait_for_condition(lambda b=be: len(list_primary_servers(b)) >= 2, timeout=20)

    # Initially healthy
    wait_health(200, timeout=20)

    # Disable one primary per backend, should remain healthy (one primary still UP)
    disabled_servers = []
    for be in backends:
        servers = list_primary_servers(be)
        set_server_state(be, servers[0], "maint")
        disabled_servers.append((be, servers[0]))

    wait_health(200, timeout=20)

    # Disable the remaining primary per backend, should become unhealthy (no servers UP)
    disabled_all = []
    for be in backends:
        servers = list_primary_servers(be)
        # Disable any remaining primary (skip ones already disabled)
        for sv in servers:
            if (be, sv) not in disabled_servers:
                set_server_state(be, sv, "maint")
                disabled_all.append((be, sv))
                break

    wait_health(503, timeout=20)

    # Re-enable all servers and expect health back to 200
    for be, sv in disabled_servers + disabled_all:
        set_server_state(be, sv, "ready")
    wait_health(200, timeout=20)

    # Sanity: all apps still respond
    for _, route in apps:
        resp = httpx.get(f"http://localhost:8000{route}")
        assert resp.status_code == 200 and resp.text == "hello"

    serve.shutdown()


def test_haproxy_empty_backends_for_scaled_down_apps(ray_shutdown):
    """Test that HAProxy has no backend servers for deleted apps.

    Verifies that when RAY_SERVE_ENABLE_HA_PROXY is True and apps are
    deleted, the HAProxy stats show the backend is removed or has no servers.
    """
    ray.init(num_cpus=4)
    serve.start()

    @serve.deployment
    def hello():
        return "hello"

    # Deploy app with 1 replica
    serve.run(
        hello.options(num_replicas=1).bind(), name="test_app", route_prefix="/test"
    )

    r = httpx.get("http://localhost:8000/test")
    assert r.status_code == 200
    assert r.text == "hello"

    # Delete the app - this should remove or empty the backend
    serve.delete("test_app")

    r = httpx.get("http://localhost:8000/test")
    assert r.status_code == 404

    serve.shutdown()


def test_fallback_proxy_starts_with_native_proxy_on_head_node(
    shutdown_ray, call_ray_stop_only  # noqa: F811
):
    """When HAProxy is enabled, verify that two proxy actors run on the head
    node (the native proxy + the fallback Serve proxy) and only the native
    proxy runs on worker nodes."""
    cluster = Cluster()
    head_node = cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.wait_for_nodes()
    ray.init(address=head_node.address)
    serve.start(http_options={"location": "EveryNode"})

    @serve.deployment
    def hello():
        return "hello"

    serve.run(hello.options(num_replicas=2).bind(), name="app1", route_prefix="/app1")

    head_node_id = ray.get_runtime_context().get_node_id()

    def check_proxies():
        actors = list_actors(
            filters=[
                ("ray_namespace", "=", SERVE_NAMESPACE),
                ("state", "=", "ALIVE"),
            ],
        )

        # Count native proxies (HAProxyManager) and Serve proxies (ProxyActor)
        haproxy_actors = [a for a in actors if a["class_name"] == "HAProxyManager"]
        serve_proxy_actors = [a for a in actors if a["class_name"] == "ProxyActor"]

        # There should be one HAProxy manager per node
        if len(haproxy_actors) != 2:
            return False

        # The head node should have a fallback Serve proxy,
        # worker nodes should not.
        if len(serve_proxy_actors) != 1:
            return False

        if serve_proxy_actors[0]["node_id"] != head_node_id:
            return False

        return True

    wait_for_condition(check_proxies, timeout=30)

    serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
