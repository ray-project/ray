import json
import os
import re
import signal
import subprocess
import sys
import time
from tempfile import NamedTemporaryFile
from typing import Pattern

import grpc
import pytest
import requests
import yaml

import ray
from ray import serve
from ray._private.pydantic_compat import BaseModel
from ray._private.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME, SERVE_NAMESPACE
from ray.serve._private.test_utils import (
    ping_fruit_stand,
    ping_grpc_another_method,
    ping_grpc_call_method,
    ping_grpc_healthz,
    ping_grpc_list_applications,
    ping_grpc_model_multiplexing,
    ping_grpc_streaming,
)
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.handle import DeploymentHandle
from ray.serve.tests.common.remote_uris import (
    TEST_DAG_PINNED_URI,
    TEST_DEPLOY_GROUP_PINNED_URI,
)
from ray.serve.tests.conftest import check_ray_stop
from ray.tests.conftest import tmp_working_dir  # noqa: F401, E501
from ray.util.state import list_actors

CONNECTION_ERROR_MSG = "connection error"


def ping_endpoint(endpoint: str, params: str = ""):
    endpoint = endpoint.lstrip("/")

    try:
        return requests.get(f"http://localhost:8000/{endpoint}{params}").text
    except requests.exceptions.ConnectionError:
        return CONNECTION_ERROR_MSG


def check_app_running(app_name: str):
    status_response = subprocess.check_output(["serve", "status"])
    status = yaml.safe_load(status_response)["applications"]
    assert status[app_name]["status"] == "RUNNING"
    return True


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_status_multi_app(ray_start_stop):
    """Deploys a multi-app config file and checks their status."""
    # Check that `serve status` works even if no Serve app is running
    subprocess.check_output(["serve", "status"])
    print("Confirmed `serve status` works when nothing has been deployed.")

    # Deploy config
    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "pizza_world.yaml"
    )
    subprocess.check_output(["serve", "deploy", config_file_name])
    print("Deployed config successfully.")

    def num_live_deployments():
        status_response = subprocess.check_output(["serve", "status"])
        status = yaml.safe_load(status_response)["applications"]
        return len(status["app1"]["deployments"]) and len(status["app2"]["deployments"])

    wait_for_condition(lambda: num_live_deployments() == 3, timeout=15)
    print("All deployments are live.")

    status_response = subprocess.check_output(
        ["serve", "status", "-a", "http://localhost:52365/"]
    )
    statuses = yaml.safe_load(status_response)["applications"]

    expected_deployments_1 = {"f", "BasicDriver"}
    expected_deployments_2 = {
        "Multiplier",
        "Adder",
        "Router",
    }
    for deployment_name, deployment in statuses["app1"]["deployments"].items():
        expected_deployments_1.remove(deployment_name)
        assert deployment["status"] in {"HEALTHY", "UPDATING"}
        assert "message" in deployment
    for deployment_name, deployment in statuses["app2"]["deployments"].items():
        expected_deployments_2.remove(deployment_name)
        assert deployment["status"] in {"HEALTHY", "UPDATING"}
        assert "message" in deployment
    assert len(expected_deployments_1) == 0
    assert len(expected_deployments_2) == 0
    print("All expected deployments are present in the status output.")

    for status in statuses.values():
        assert status["status"] in {"DEPLOYING", "RUNNING"}
        assert time.time() > status["last_deployed_time_s"]
    print("Verified status and deployment timestamp of both apps.")


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_shutdown(ray_start_stop):
    """Deploys a config file and shuts down the Serve application."""

    # Check that `serve shutdown` works even if no Serve app is running
    subprocess.check_output(["serve", "shutdown", "-y"])

    def num_live_deployments():
        status_response = subprocess.check_output(["serve", "status"])
        serve_status = yaml.safe_load(status_response)["applications"][
            SERVE_DEFAULT_APP_NAME
        ]
        return len(serve_status["deployments"])

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "basic_graph.yaml"
    )

    # Check idempotence
    num_iterations = 2
    for iteration in range(1, num_iterations + 1):
        print(f"*** Starting Iteration {iteration}/{num_iterations} ***\n")

        print("Deploying config.")
        subprocess.check_output(["serve", "deploy", config_file_name])
        wait_for_condition(lambda: num_live_deployments() == 2, timeout=15)
        print("Deployment successful. Deployments are live.")

        # `serve config` and `serve status` should print non-empty schemas
        config_response = subprocess.check_output(["serve", "config"])
        yaml.safe_load(config_response)

        status_response = subprocess.check_output(["serve", "status"])
        status = yaml.safe_load(status_response)
        assert len(status["applications"])
        print("`serve config` and `serve status` print non-empty responses.\n")

        print("Deleting Serve app.")
        subprocess.check_output(["serve", "shutdown", "-y"])

        # `serve config` and `serve status` should print messages indicating
        # nothing is deployed
        def serve_config_empty():
            config_response = subprocess.check_output(["serve", "config"])
            return len(config_response) == 0

        def serve_status_empty():
            status_response = subprocess.check_output(["serve", "status"])
            status = yaml.safe_load(status_response)
            return len(status["applications"]) == 0

        wait_for_condition(serve_config_empty)
        wait_for_condition(serve_status_empty)
        print("`serve config` and `serve status` print empty responses.\n")


@serve.deployment
def parrot(request):
    return request.query_params["sound"]


parrot_node = parrot.bind()


@pytest.mark.parametrize("number_of_kill_signals", (1, 2))
@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_application(ray_start_stop, number_of_kill_signals):
    """Deploys valid config file and import path via `serve run`."""

    # Deploy via config file
    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "arithmetic.yaml"
    )

    print('Running config file "arithmetic.yaml".')
    p = subprocess.Popen(["serve", "run", "--address=auto", config_file_name])
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 0]).json() == 1,
        timeout=15,
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["SUB", 5]).json() == 3,
        timeout=15,
    )
    print("Run successful! Deployments are live and reachable over HTTP. Killing run.")

    for _ in range(number_of_kill_signals):
        p.send_signal(signal.SIGINT)  # Equivalent to ctrl-C
    p.wait()
    with pytest.raises(requests.exceptions.ConnectionError):
        requests.post("http://localhost:8000/", json=["ADD", 0]).json()
    print("Kill successful! Deployments are not reachable over HTTP.")

    print('Running node at import path "ray.serve.tests.test_cli_2.parrot_node".')
    # Deploy via import path
    p = subprocess.Popen(
        ["serve", "run", "--address=auto", "ray.serve.tests.test_cli_2.parrot_node"]
    )
    wait_for_condition(lambda: ping_endpoint("/", params="?sound=squawk") == "squawk")
    print("Run successful! Deployment is live and reachable over HTTP. Killing run.")

    p.send_signal(signal.SIGINT)  # Equivalent to ctrl-C
    p.wait()
    assert ping_endpoint("/", params="?sound=squawk") == CONNECTION_ERROR_MSG
    print("Kill successful! Deployment is not reachable over HTTP.")


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_multi_app(ray_start_stop):
    """Deploys valid multi-app config file via `serve run`."""

    # Deploy via config file
    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "pizza_world.yaml"
    )

    print('Running config file "pizza_world.yaml".')
    p = subprocess.Popen(["serve", "run", "--address=auto", config_file_name])
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app1").text == "wonderful world",
        timeout=15,
    )
    print('Application "app1" is reachable over HTTP.')
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).text
        == "12 pizzas please!",
        timeout=15,
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app2", json=["MUL", 2]).text
        == "20 pizzas please!",
        timeout=15,
    )
    print("Run successful! Deployments are live and reachable over HTTP. Killing run.")

    p.send_signal(signal.SIGINT)  # Equivalent to ctrl-C
    p.wait()
    with pytest.raises(requests.exceptions.ConnectionError):
        requests.post("http://localhost:8000/app1")
    with pytest.raises(requests.exceptions.ConnectionError):
        requests.post("http://localhost:8000/app2", json=["ADD", 0])
    print("Kill successful! Deployments are not reachable over HTTP.")


@serve.deployment
class Macaw:
    def __init__(self, color, name="Mulligan", surname=None):
        self.color = color
        self.name = name
        self.surname = surname

    def __call__(self):
        if self.surname is not None:
            return f"{self.name} {self.surname} is {self.color}!"
        else:
            return f"{self.name} is {self.color}!"


molly_macaw = Macaw.bind("green", name="Molly")


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_deployment_node(ray_start_stop):
    """Test `serve run` with bound args and kwargs."""

    # Deploy via import path
    p = subprocess.Popen(
        [
            "serve",
            "run",
            "--address=auto",
            "ray.serve.tests.test_cli_2.molly_macaw",
        ]
    )
    wait_for_condition(lambda: ping_endpoint("/") == "Molly is green!", timeout=10)
    p.send_signal(signal.SIGINT)
    p.wait()
    assert ping_endpoint("/") == CONNECTION_ERROR_MSG


@serve.deployment
class Echo:
    def __init__(self, message: str):
        print("Echo message:", message)
        self._message = message

    def __call__(self, *args):
        return self._message


echo_app = Echo.bind("hello")


def build_echo_app(args):
    return Echo.bind(args.get("message", "DEFAULT"))


class TypedArgs(BaseModel):
    message: str = "DEFAULT"


def build_echo_app_typed(args: TypedArgs):
    return Echo.bind(args.message)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
@pytest.mark.parametrize(
    "import_path",
    [
        "ray.serve.tests.test_cli_2.build_echo_app",
        "ray.serve.tests.test_cli_2.build_echo_app_typed",
    ],
)
def test_run_builder_with_args(ray_start_stop, import_path: str):
    """Test `serve run` with args passed into a builder function.

    Tests both the untyped and typed args cases.
    """
    # First deploy without any arguments, should get default response.
    p = subprocess.Popen(
        [
            "serve",
            "run",
            "--address=auto",
            import_path,
        ]
    )
    wait_for_condition(lambda: ping_endpoint("") == "DEFAULT", timeout=10)
    p.send_signal(signal.SIGINT)
    p.wait()
    assert ping_endpoint("/") == CONNECTION_ERROR_MSG

    # Now deploy passing a message as an argument, should get passed message.
    p = subprocess.Popen(
        [
            "serve",
            "run",
            "--address=auto",
            import_path,
            "message=hello world",
        ]
    )
    wait_for_condition(lambda: ping_endpoint("") == "hello world", timeout=10)

    p.send_signal(signal.SIGINT)
    p.wait()
    assert ping_endpoint("/") == CONNECTION_ERROR_MSG


@serve.deployment
class MetalDetector:
    def __call__(self, *args):
        return os.environ.get("buried_item", "no dice")


metal_detector_node = MetalDetector.bind()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_runtime_env(ray_start_stop):
    """Test `serve run` with runtime_env passed in."""

    # With import path
    p = subprocess.Popen(
        [
            "serve",
            "run",
            "--address=auto",
            "ray.serve.tests.test_cli_2.metal_detector_node",
            "--runtime-env-json",
            ('{"env_vars": {"buried_item": "lucky coin"} }'),
        ]
    )
    wait_for_condition(
        lambda: ping_endpoint("MetalDetector") == "lucky coin", timeout=10
    )
    p.send_signal(signal.SIGINT)
    p.wait()

    # With config
    p = subprocess.Popen(
        [
            "serve",
            "run",
            "--address=auto",
            os.path.join(
                os.path.dirname(__file__),
                "test_config_files",
                "missing_runtime_env.yaml",
            ),
            "--runtime-env-json",
            json.dumps(
                {
                    "py_modules": [TEST_DEPLOY_GROUP_PINNED_URI],
                    "working_dir": "http://nonexistentlink-q490123950ni34t",
                }
            ),
            "--working-dir",
            TEST_DAG_PINNED_URI,
        ]
    )
    wait_for_condition(lambda: ping_endpoint("") == "wonderful world", timeout=15)
    p.send_signal(signal.SIGINT)
    p.wait()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
@pytest.mark.parametrize("config_file", ["basic_graph.yaml", "basic_multi.yaml"])
def test_run_config_port1(ray_start_stop, config_file):
    """Test that `serve run` defaults to port 8000."""
    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", config_file
    )
    p = subprocess.Popen(["serve", "run", config_file_name])
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/").text == "wonderful world",
        timeout=15,
    )
    p.send_signal(signal.SIGINT)
    p.wait()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
@pytest.mark.parametrize(
    "config_file", ["basic_graph_http.yaml", "basic_multi_http.yaml"]
)
def test_run_config_port2(ray_start_stop, config_file):
    """If config file specifies a port, the default port value should not be used."""
    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", config_file
    )
    p = subprocess.Popen(["serve", "run", config_file_name])
    wait_for_condition(
        lambda: requests.post("http://localhost:8005/").text == "wonderful world",
        timeout=15,
    )
    p.send_signal(signal.SIGINT)
    p.wait()


@serve.deployment
class ConstructorFailure:
    def __init__(self):
        raise RuntimeError("Intentionally failing.")


constructor_failure_node = ConstructorFailure.bind()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_teardown(ray_start_stop):
    """Consecutive serve runs should tear down controller so logs can always be seen."""
    logs = subprocess.check_output(
        ["serve", "run", "ray.serve.tests.test_cli_2.constructor_failure_node"],
        stderr=subprocess.STDOUT,
        timeout=30,
    ).decode()
    assert "Intentionally failing." in logs

    logs = subprocess.check_output(
        ["serve", "run", "ray.serve.tests.test_cli_2.constructor_failure_node"],
        stderr=subprocess.STDOUT,
        timeout=30,
    ).decode()
    assert "Intentionally failing." in logs


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_route_prefix_and_name_default(ray_start_stop):
    """Test `serve run` without route_prefix and name options."""

    p = subprocess.Popen(["serve", "run", "ray.serve.tests.test_cli_2.echo_app"])

    wait_for_condition(check_app_running, app_name=SERVE_DEFAULT_APP_NAME)
    assert ping_endpoint("/") == "hello"
    p.send_signal(signal.SIGINT)
    p.wait()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_route_prefix_and_name_override(ray_start_stop):
    """Test `serve run` with route prefix option."""

    p = subprocess.Popen(
        [
            "serve",
            "run",
            "--route-prefix=/hello",
            "--name=hello_app",
            "ray.serve.tests.test_cli_2.echo_app",
        ],
    )

    wait_for_condition(check_app_running, app_name="hello_app")
    assert "Path '/' not found" in ping_endpoint("/")
    assert ping_endpoint("/hello") == "hello"
    p.send_signal(signal.SIGINT)
    p.wait()


@serve.deployment
def global_f(*args):
    return "wonderful world"


@serve.deployment
class NoArgDriver:
    def __init__(self, h: DeploymentHandle):
        self._h = h

    async def __call__(self):
        return await self._h.remote()


TestBuildFNode = global_f.bind()
TestBuildDagNode = NoArgDriver.bind(TestBuildFNode)


TestApp1Node = global_f.options(route_prefix="/app1").bind()
TestApp2Node = NoArgDriver.options(route_prefix="/app2").bind(global_f.bind())


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_build_multi_app(ray_start_stop):
    with NamedTemporaryFile(mode="w+", suffix=".yaml") as tmp:
        print('Building nodes "TestApp1Node" and "TestApp2Node".')
        # Build an app
        grpc_servicer_func_root = "ray.serve.generated.serve_pb2_grpc"
        subprocess.check_output(
            [
                "serve",
                "build",
                "ray.serve.tests.test_cli_2.TestApp1Node",
                "ray.serve.tests.test_cli_2.TestApp2Node",
                "ray.serve.tests.test_config_files.grpc_deployment.g",
                "--grpc-servicer-functions",
                f"{grpc_servicer_func_root}.add_UserDefinedServiceServicer_to_server",
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

        app_name = "app3"
        channel = grpc.insecure_channel("localhost:9000")
        stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
        request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
        metadata = (("application", app_name),)
        response = stub.__call__(request=request, metadata=metadata)
        assert response.greeting == "Hello foo from bar"
        print("App 3 is live and reachable over gRPC.")

        print("Deleting applications.")
        subprocess.check_output(["serve", "shutdown", "-y"])
        wait_for_condition(
            lambda: ping_endpoint("app1") == CONNECTION_ERROR_MSG
            and ping_endpoint("app2") == CONNECTION_ERROR_MSG,
            timeout=15,
        )
        print("Delete succeeded! Node is no longer reachable over HTTP.")


k8sFNode = global_f.options(
    num_replicas=2, ray_actor_options={"num_cpus": 2, "num_gpus": 1}
).bind()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
@pytest.mark.parametrize("use_command", [True, False])
def test_idempotence_after_controller_death(ray_start_stop, use_command: bool):
    """Check that CLI is idempotent even if controller dies."""
    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "basic_graph.yaml"
    )
    success_message_fragment = b"Sent deploy request successfully."
    deploy_response = subprocess.check_output(["serve", "deploy", config_file_name])
    assert success_message_fragment in deploy_response

    ray.init(address="auto", namespace=SERVE_NAMESPACE)
    serve.start()
    wait_for_condition(
        lambda: len(list_actors(filters=[("state", "=", "ALIVE")])) == 4,
        timeout=15,
    )

    # Kill controller
    if use_command:
        subprocess.check_output(["serve", "shutdown", "-y"])
    else:
        serve.shutdown()

    status_response = subprocess.check_output(["serve", "status"])
    status_info = yaml.safe_load(status_response)

    assert len(status_info["applications"]) == 0

    deploy_response = subprocess.check_output(["serve", "deploy", config_file_name])
    assert success_message_fragment in deploy_response

    # Restore testing controller
    serve.start()
    wait_for_condition(
        lambda: len(list_actors(filters=[("state", "=", "ALIVE")])) == 4,
        timeout=15,
    )
    serve.shutdown()
    ray.shutdown()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
class TestRayReinitialization:
    @pytest.fixture
    def import_file_name(self) -> str:
        return "ray.serve.tests.test_config_files.ray_already_initialized:app"

    @pytest.fixture
    def pattern(self) -> Pattern:
        return re.compile(r"Connecting to existing Ray cluster at address: (.*)\.\.\.")

    @pytest.fixture
    def ansi_escape(self) -> Pattern:
        return re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

    def test_run_without_address(self, import_file_name, ray_start_stop):
        """Test serve run with ray already initialized and run without address argument.

        When the imported file already initialized a ray instance and serve doesn't run
        with address argument, then serve does not reinitialize another ray instance and
        cause error.
        """
        p = subprocess.Popen(["serve", "run", import_file_name])
        wait_for_condition(lambda: ping_endpoint("") == "foobar", timeout=10)
        p.send_signal(signal.SIGINT)
        p.wait()

    def test_run_with_address_same_address(self, import_file_name, ray_start_stop):
        """Test serve run with ray already initialized and run with address argument
        that has the same address as existing ray instance.

        When the imported file already initialized a ray instance and serve runs with
        address argument same as the ray instance, then serve does not reinitialize
        another ray instance and cause error.
        """
        p = subprocess.Popen(
            ["serve", "run", "--address=127.0.0.1:6379", import_file_name]
        )
        wait_for_condition(lambda: ping_endpoint("") == "foobar", timeout=10)
        p.send_signal(signal.SIGINT)
        p.wait()

    def test_run_with_address_different_address(
        self, import_file_name, pattern, ansi_escape, ray_start_stop
    ):
        """Test serve run with ray already initialized and run with address argument
        that has the different address as existing ray instance.

        When the imported file already initialized a ray instance and serve runs with
        address argument different as the ray instance, then serve does not reinitialize
        another ray instance and cause error and logs warning to the user.
        """
        p = subprocess.Popen(
            ["serve", "run", "--address=ray://123.45.67.89:50005", import_file_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        wait_for_condition(lambda: ping_endpoint("") == "foobar", timeout=10)
        p.send_signal(signal.SIGINT)
        p.wait()
        process_output, _ = p.communicate()
        logs = process_output.decode("utf-8").strip()
        ray_address = ansi_escape.sub("", pattern.search(logs).group(1))
        expected_warning_message = (
            "An address was passed to `serve run` but the imported module also "
            f"connected to Ray at a different address: '{ray_address}'. You do not "
            "need to call `ray.init` in your code when using `serve run`."
        )
        assert expected_warning_message in logs

    def test_run_with_auto_address(
        self, import_file_name, pattern, ansi_escape, ray_start_stop
    ):
        """Test serve run with ray already initialized and run with "auto" address
        argument.

        When the imported file already initialized a ray instance and serve runs with
        address argument same as the ray instance, then serve does not reinitialize
        another ray instance and cause error.
        """
        p = subprocess.Popen(
            ["serve", "run", "--address=auto", import_file_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        wait_for_condition(lambda: ping_endpoint("") == "foobar", timeout=10)
        p.send_signal(signal.SIGINT)
        p.wait()
        process_output, _ = p.communicate()
        logs = process_output.decode("utf-8").strip()
        ray_address = ansi_escape.sub("", pattern.search(logs).group(1))
        expected_warning_message = (
            "An address was passed to `serve run` but the imported module also "
            f"connected to Ray at a different address: '{ray_address}'. You do not "
            "need to call `ray.init` in your code when using `serve run`."
        )
        assert expected_warning_message not in logs


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_config_request_timeout():
    """Test running serve with request timeout in http_options.

    The config file has 0.1s as the `request_timeout_s` in the `http_options`. First
    case checks that when the query runs longer than the 0.1s, the deployment returns a
    task failed message. The second case checks that when the query takes less than
    0.1s, the deployment returns a success message.
    """

    # Set up ray instance to perform 1 retries
    subprocess.check_output(["ray", "stop", "--force"])
    wait_for_condition(
        check_ray_stop,
        timeout=15,
    )
    subprocess.check_output(
        ["ray", "start", "--head"],
        env=dict(os.environ, RAY_SERVE_HTTP_REQUEST_MAX_RETRIES="1"),
    )
    wait_for_condition(
        lambda: requests.get("http://localhost:52365/api/ray/version").status_code
        == 200,
        timeout=15,
    )

    config_file_name = os.path.join(
        os.path.dirname(__file__),
        "test_config_files",
        "http_option_request_timeout_s.yaml",
    )
    p = subprocess.Popen(["serve", "run", config_file_name])

    # Ensure the http request is killed and failed when the deployment runs longer than
    # the 0.1 request_timeout_s set in in the config yaml
    wait_for_condition(
        lambda: requests.get("http://localhost:8000/app1?sleep_s=0.11").status_code
        == 408,
    )

    # Ensure the http request returned the correct response when the deployment runs
    # shorter than the 0.1 request_timeout_s set up in the config yaml
    wait_for_condition(
        lambda: requests.get("http://localhost:8000/app1?sleep_s=0.09").text
        == "Task Succeeded!",
    )

    p.send_signal(signal.SIGINT)
    p.wait()

    # Stop ray instance
    subprocess.check_output(["ray", "stop", "--force"])
    wait_for_condition(
        check_ray_stop,
        timeout=15,
    )


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deployment_contains_utils(ray_start_stop):
    """Test when deployment contains utils module, it can be deployed successfully.

    When the deployment contains utils module, running serve deploy should successfully
    deployment the application and return the correct response.
    """

    config_file = os.path.join(
        os.path.dirname(__file__),
        "test_config_files",
        "deployment_uses_utils_module.yaml",
    )

    subprocess.check_output(["serve", "deploy", config_file], stderr=subprocess.STDOUT)
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/").text == "hello_from_utils"
    )


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_reload_basic(ray_start_stop, tmp_path):
    """Test `serve run` with reload."""

    code_template = """
from ray import serve

@serve.deployment
class MessageDeployment:
    def __init__(self, msg):
        self.msg = msg

    def __call__(self):
        return self.msg


msg_app = MessageDeployment.bind("Hello {message}!")
    """

    def write_file(message: str):
        with open(os.path.join(tmp_path, "reload_serve.py"), "w") as f:
            code = code_template.format(message=message)
            print(f"Writing updated code:\n{code}")
            f.write(code)
            f.flush()

    write_file("World")

    p = subprocess.Popen(
        [
            "serve",
            "run",
            "--app-dir",
            tmp_path,
            "--reload",
            "reload_serve:msg_app",
        ]
    )
    wait_for_condition(lambda: ping_endpoint("") == "Hello World!", timeout=10)

    # Sleep to ensure the `serve run` command is in the file watching loop when we
    # write the change, else it won't be picked up.
    time.sleep(5)

    # Write the file: an update should be auto-triggered.
    write_file("Updated")
    wait_for_condition(lambda: ping_endpoint("") == "Hello Updated!", timeout=10)

    p.send_signal(signal.SIGINT)
    p.wait()
    assert ping_endpoint("") == CONNECTION_ERROR_MSG


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_serving_request_through_grpc_proxy(ray_start_stop):
    """Test serving request through gRPC proxy

    When Serve runs with a gRPC deployment, the app should be deployed successfully,
    both ListApplications and Healthz methods returning success response, and registered
    gRPC methods are routing to the correct replica and return the correct response.
    """
    config_file = os.path.join(
        os.path.dirname(__file__),
        "test_config_files",
        "deploy_grpc_app.yaml",
    )

    subprocess.check_output(["serve", "deploy", config_file], stderr=subprocess.STDOUT)

    app1 = "app1"
    app_names = [app1]

    channel = grpc.insecure_channel("localhost:9000")

    # Ensures ListApplications method succeeding.
    wait_for_condition(
        ping_grpc_list_applications, channel=channel, app_names=app_names
    )

    # Ensures Healthz method succeeding.
    ping_grpc_healthz(channel)

    # Ensures a custom defined method is responding correctly.
    ping_grpc_call_method(channel, app1)

    # Ensures another custom defined method is responding correctly.
    ping_grpc_another_method(channel, app1)

    # Ensures model multiplexing is responding correctly.
    ping_grpc_model_multiplexing(channel, app1)

    # Ensure Streaming method is responding correctly.
    ping_grpc_streaming(channel, app1)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_grpc_proxy_model_composition(ray_start_stop):
    """Test serving request through gRPC proxy

    When Serve runs with a gRPC deployment, the app should be deployed successfully,
    both ListApplications and Healthz methods returning success response, and model
    composition should work correctly.
    """
    config_file = os.path.join(
        os.path.dirname(__file__),
        "test_config_files",
        "deploy_grpc_model_composition.yaml",
    )

    subprocess.check_output(["serve", "deploy", config_file], stderr=subprocess.STDOUT)

    app = "app1"
    app_names = [app]

    channel = grpc.insecure_channel("localhost:9000")

    # Ensures ListApplications method succeeding.
    wait_for_condition(
        ping_grpc_list_applications, channel=channel, app_names=app_names
    )

    # Ensures Healthz method succeeding.
    ping_grpc_healthz(channel)

    # Ensure model composition is responding correctly.
    ping_fruit_stand(channel, app)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
