import json
import os
import signal
import subprocess
import sys
import time
from typing import Union

import httpx
import pytest
import yaml

from ray import serve
from ray._common.pydantic_compat import BaseModel
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.handle import DeploymentHandle
from ray.serve.tests.common.remote_uris import (
    TEST_DAG_PINNED_URI,
    TEST_DEPLOY_GROUP_PINNED_URI,
)

CONNECTION_ERROR_MSG = "connection error"


def ping_endpoint(endpoint: str, params: str = ""):
    endpoint = endpoint.lstrip("/")

    try:
        return httpx.get(f"http://localhost:8000/{endpoint}{params}").text
    except httpx.HTTPError:
        return CONNECTION_ERROR_MSG


def check_app_status(app_name: str, expected_status: str):
    status_response = subprocess.check_output(["serve", "status"])
    status = yaml.safe_load(status_response)["applications"]
    assert status[app_name]["status"] == expected_status
    return True


def check_app_running(app_name: str):
    return check_app_status(app_name, "RUNNING")


@serve.deployment
def parrot(request):
    return request.query_params["sound"]


parrot_node = parrot.bind()


@serve.deployment
class MetalDetector:
    def __call__(self, *args):
        return os.environ.get("buried_item", "no dice")


metal_detector_node = MetalDetector.bind()


@serve.deployment
class ConstructorFailure:
    def __init__(self):
        raise RuntimeError("Intentionally failing.")


constructor_failure_node = ConstructorFailure.bind()


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


TestApp1Node = global_f.options(name="app1").bind()
TestApp2Node = NoArgDriver.options(name="app2").bind(global_f.bind())


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


k8sFNode = global_f.options(
    num_replicas=2, ray_actor_options={"num_cpus": 2, "num_gpus": 1}
).bind()


class TestRun:
    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    @pytest.mark.parametrize(
        "proxy_location,expected",
        [
            (
                None,
                "EveryNode",
            ),  # default ProxyLocation `EveryNode` is used as http_options.location is not specified
            ("EveryNode", "EveryNode"),
            ("HeadOnly", "HeadOnly"),
            ("Disabled", "Disabled"),
        ],
    )
    def test_proxy_location(self, ray_start_stop, tmp_path, proxy_location, expected):
        # when the `serve run` cli command is executed
        # without serve already running (for the first time)
        # `proxy_location` should be set from the config file if specified
        def is_proxy_location_correct(expected_proxy_location: str) -> bool:
            try:
                response = httpx.get(
                    "http://localhost:8265/api/serve/applications/"
                ).text
                response_json = json.loads(response)
                print("response_json")
                print(response_json)
                return response_json["proxy_location"] == expected_proxy_location
            except httpx.HTTPError:
                return False

        def arithmetic_config(with_proxy_location: Union[str, None]) -> str:
            config_file_name = os.path.join(
                os.path.dirname(__file__), "test_config_files", "arithmetic.yaml"
            )
            with open(config_file_name, "r") as config_file:
                arithmetic_config_dict = yaml.safe_load(config_file)

            config_path = tmp_path / "config.yaml"
            if with_proxy_location:
                arithmetic_config_dict["proxy_location"] = with_proxy_location
            with open(config_path, "w") as f:
                yaml.dump(arithmetic_config_dict, f)
            return str(config_path)

        config_path = arithmetic_config(with_proxy_location=proxy_location)
        p = subprocess.Popen(["serve", "run", config_path])
        wait_for_condition(
            lambda: is_proxy_location_correct(expected_proxy_location=expected),
            timeout=10,
        )
        p.send_signal(signal.SIGINT)
        p.wait()

    @pytest.mark.parametrize("number_of_kill_signals", (1, 2))
    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    def test_run_application(self, ray_start_stop, number_of_kill_signals):
        """Deploys valid config file and import path via `serve run`."""

        # Deploy via config file
        config_file_name = os.path.join(
            os.path.dirname(__file__), "test_config_files", "arithmetic.yaml"
        )

        print('Running config file "arithmetic.yaml".')
        p = subprocess.Popen(["serve", "run", "--address=auto", config_file_name])
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/", json=["ADD", 0]).json() == 1,
            timeout=15,
        )
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/", json=["SUB", 5]).json() == 3,
            timeout=15,
        )
        print(
            "Run successful! Deployments are live and reachable over HTTP. Killing run."
        )

        for _ in range(number_of_kill_signals):
            p.send_signal(signal.SIGINT)  # Equivalent to ctrl-C
        p.wait()
        with pytest.raises(httpx.HTTPError):
            httpx.post("http://localhost:8000/", json=["ADD", 0]).json()
        print("Kill successful! Deployments are not reachable over HTTP.")

        print('Running node at import path "ray.serve.tests.test_cli_3.parrot_node".')
        # Deploy via import path
        p = subprocess.Popen(
            ["serve", "run", "--address=auto", "ray.serve.tests.test_cli_3.parrot_node"]
        )
        wait_for_condition(
            lambda: ping_endpoint("/", params="?sound=squawk") == "squawk"
        )
        print(
            "Run successful! Deployment is live and reachable over HTTP. Killing run."
        )

        p.send_signal(signal.SIGINT)  # Equivalent to ctrl-C
        p.wait()
        assert ping_endpoint("/", params="?sound=squawk") == CONNECTION_ERROR_MSG
        print("Kill successful! Deployment is not reachable over HTTP.")

    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    def test_run_multi_app(self, ray_start_stop):
        """Deploys valid multi-app config file via `serve run`."""

        # Deploy via config file
        config_file_name = os.path.join(
            os.path.dirname(__file__), "test_config_files", "pizza_world.yaml"
        )

        print('Running config file "pizza_world.yaml".')
        p = subprocess.Popen(["serve", "run", "--address=auto", config_file_name])
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").text == "wonderful world",
            timeout=15,
        )
        print('Application "app1" is reachable over HTTP.')
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app2", json=["ADD", 2]).text
            == "12 pizzas please!",
            timeout=15,
        )
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app2", json=["MUL", 2]).text
            == "20 pizzas please!",
            timeout=15,
        )
        print(
            "Run successful! Deployments are live and reachable over HTTP. Killing run."
        )

        p.send_signal(signal.SIGINT)  # Equivalent to ctrl-C
        p.wait()
        with pytest.raises(httpx.HTTPError):
            _ = httpx.post("http://localhost:8000/app1").text
        with pytest.raises(httpx.HTTPError):
            _ = httpx.post("http://localhost:8000/app2", json=["ADD", 0]).text
        print("Kill successful! Deployments are not reachable over HTTP.")

    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    def test_run_deployment_node(self, ray_start_stop):
        """Test `serve run` with bound args and kwargs."""

        # Deploy via import path
        p = subprocess.Popen(
            [
                "serve",
                "run",
                "--address=auto",
                "ray.serve.tests.test_cli_3.molly_macaw",
            ]
        )
        wait_for_condition(lambda: ping_endpoint("/") == "Molly is green!", timeout=10)
        p.send_signal(signal.SIGINT)
        p.wait()
        assert ping_endpoint("/") == CONNECTION_ERROR_MSG

    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    @pytest.mark.parametrize(
        "import_path",
        [
            "ray.serve.tests.test_cli_3.build_echo_app",
            "ray.serve.tests.test_cli_3.build_echo_app_typed",
        ],
    )
    def test_run_builder_with_args(self, ray_start_stop, import_path: str):
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

    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    def test_run_runtime_env(self, ray_start_stop):
        """Test `serve run` with runtime_env passed in."""

        # With import path
        p = subprocess.Popen(
            [
                "serve",
                "run",
                "--address=auto",
                "ray.serve.tests.test_cli_3.metal_detector_node",
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

    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    @pytest.mark.parametrize("config_file", ["basic_graph.yaml", "basic_multi.yaml"])
    def test_run_config_port1(self, ray_start_stop, config_file):
        """Test that `serve run` defaults to port 8000."""
        config_file_name = os.path.join(
            os.path.dirname(__file__), "test_config_files", config_file
        )
        p = subprocess.Popen(["serve", "run", config_file_name])
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/").text == "wonderful world",
            timeout=15,
        )
        p.send_signal(signal.SIGINT)
        p.wait()

    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    @pytest.mark.parametrize(
        "config_file", ["basic_graph_http.yaml", "basic_multi_http.yaml"]
    )
    def test_run_config_port2(self, ray_start_stop, config_file):
        """If config file specifies a port, the default port value should not be used."""
        config_file_name = os.path.join(
            os.path.dirname(__file__), "test_config_files", config_file
        )
        p = subprocess.Popen(["serve", "run", config_file_name])
        wait_for_condition(
            lambda: httpx.post("http://localhost:8005/").text == "wonderful world",
            timeout=15,
        )
        p.send_signal(signal.SIGINT)
        p.wait()

    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    def test_run_teardown(self, ray_start_stop):
        """Consecutive serve runs should tear down controller so logs can always be seen."""
        logs = subprocess.check_output(
            ["serve", "run", "ray.serve.tests.test_cli_3.constructor_failure_node"],
            stderr=subprocess.STDOUT,
            timeout=30,
        ).decode()
        assert "Intentionally failing." in logs

        logs = subprocess.check_output(
            ["serve", "run", "ray.serve.tests.test_cli_3.constructor_failure_node"],
            stderr=subprocess.STDOUT,
            timeout=30,
        ).decode()
        assert "Intentionally failing." in logs

    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    def test_run_route_prefix_and_name_default(self, ray_start_stop):
        """Test `serve run` without route_prefix and name options."""

        p = subprocess.Popen(
            [
                "serve",
                "run",
                "--address=auto",
                "ray.serve.tests.test_cli_3.echo_app",
            ]
        )

        wait_for_condition(check_app_running, app_name=SERVE_DEFAULT_APP_NAME)
        assert ping_endpoint("/") == "hello"
        p.send_signal(signal.SIGINT)
        p.wait()

    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    def test_run_route_prefix_and_name_override(self, ray_start_stop):
        """Test `serve run` with route prefix option."""

        p = subprocess.Popen(
            [
                "serve",
                "run",
                "--address=auto",
                "--route-prefix=/hello",
                "--name=hello_app",
                "ray.serve.tests.test_cli_3.echo_app",
            ],
        )

        wait_for_condition(check_app_running, app_name="hello_app")
        assert "Path '/' not found" in ping_endpoint("/")
        assert ping_endpoint("/hello") == "hello"
        p.send_signal(signal.SIGINT)
        p.wait()

    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    def test_run_config_request_timeout(self, ray_start_stop):
        """Test running serve with request timeout in http_options.

        The config file has 0.1s as the `request_timeout_s` in the `http_options`. First
        case checks that when the query runs longer than the 0.1s, the deployment returns a
        task failed message. The second case checks that when the query takes less than
        0.1s, the deployment returns a success message.
        """

        config_file_name = os.path.join(
            os.path.dirname(__file__),
            "test_config_files",
            "http_option_request_timeout_s.yaml",
        )
        p = subprocess.Popen(["serve", "run", config_file_name])

        # Ensure the http request is killed and failed when the deployment runs longer than
        # the 0.1 request_timeout_s set in in the config yaml
        wait_for_condition(
            lambda: httpx.get("http://localhost:8000/app1?sleep_s=0.11").status_code
            == 408,
        )

        # Ensure the http request returned the correct response when the deployment runs
        # shorter than the 0.1 request_timeout_s set up in the config yaml
        wait_for_condition(
            lambda: httpx.get("http://localhost:8000/app1?sleep_s=0.09").text
            == "Task Succeeded!",
        )

        p.send_signal(signal.SIGINT)
        p.wait()

    @pytest.mark.skipif(
        sys.platform == "win32", reason="File path incorrect on Windows."
    )
    def test_run_reload_basic(self, ray_start_stop, tmp_path):
        """Test `serve run` with reload."""

        code_template = """
from ray import serve

@serve.deployment
class MessageDeployment:
    def __init__(self, msg):
        {invalid_suffix}
        self.msg = msg

    def __call__(self):
        return self.msg


msg_app = MessageDeployment.bind("Hello {message}!")
        """

        def write_file(message: str, invalid_suffix: str = ""):
            with open(os.path.join(tmp_path, "reload_serve.py"), "w") as f:
                code = code_template.format(
                    invalid_suffix=invalid_suffix, message=message
                )
                print(f"Writing updated code:\n{code}")
                f.write(code)
                f.flush()

        write_file("World")

        p = subprocess.Popen(
            [
                "serve",
                "run",
                "--address=auto",
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

        # Ensure a bad change doesn't shut down serve and serve reports deploy failed.
        write_file(message="update1", invalid_suffix="foobar")
        wait_for_condition(
            condition_predictor=check_app_status,
            app_name="default",
            expected_status="DEPLOY_FAILED",
        )

        # Ensure the following reload happens as expected.
        write_file("Updated2")
        wait_for_condition(lambda: ping_endpoint("") == "Hello Updated2!", timeout=10)

        p.send_signal(signal.SIGINT)
        p.wait()
        assert ping_endpoint("") == CONNECTION_ERROR_MSG


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
