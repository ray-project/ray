import json
import os
import re
import signal
import subprocess
import sys
from tempfile import NamedTemporaryFile
from typing import Dict, Optional, Pattern

import grpc
import httpx
import pytest
import yaml

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve._private.test_utils import (
    get_application_url,
    ping_fruit_stand,
    ping_grpc_another_method,
    ping_grpc_call_method,
    ping_grpc_healthz,
    ping_grpc_list_applications,
    ping_grpc_model_multiplexing,
    ping_grpc_streaming,
)
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.util.state import list_actors

CONNECTION_ERROR_MSG = "connection error"


def ping_endpoint(app_name: str = SERVE_DEFAULT_APP_NAME, params: str = ""):
    try:
        url = get_application_url("HTTP", app_name=app_name)
        return httpx.get(f"{url}/{params}").text
    except httpx.HTTPError:
        return CONNECTION_ERROR_MSG


def check_app_status(app_name: str, expected_status: str):
    status_response = subprocess.check_output(["serve", "status"])
    status = yaml.safe_load(status_response)["applications"]
    assert status[app_name]["status"] == expected_status
    return True


def check_app_running(app_name: str):
    return check_app_status(app_name, "RUNNING")


def check_http_response(expected_text: str, json: Optional[Dict] = None):
    url = get_application_url("HTTP")
    resp = httpx.post(url, json=json)
    assert resp.text == expected_text
    return True


def test_start_shutdown(ray_start_stop):
    subprocess.check_output(["serve", "start"])
    # deploy a simple app
    import_path = "ray.serve.tests.test_config_files.arg_builders.build_echo_app"

    deploy_response = subprocess.check_output(["serve", "deploy", import_path])
    assert b"Sent deploy request successfully." in deploy_response

    wait_for_condition(
        check_http_response,
        expected_text="DEFAULT",
        timeout=15,
    )

    ret = subprocess.check_output(["serve", "shutdown", "-y"])
    assert b"Sent shutdown request; applications will be deleted asynchronously" in ret

    def check_no_apps():
        status = subprocess.check_output(["serve", "status"])
        return b"applications: {}" in status

    wait_for_condition(check_no_apps, timeout=15)

    # Test shutdown when no Serve instance is running
    ret = subprocess.check_output(["serve", "shutdown", "-y"], stderr=subprocess.STDOUT)
    assert b"No Serve instance found running" in ret


def test_start_shutdown_without_serve_running(ray_start_stop):
    # Test shutdown when no Serve instance is running
    ret = subprocess.check_output(["serve", "shutdown", "-y"], stderr=subprocess.STDOUT)
    assert b"No Serve instance found running" in ret


# def test_start_shutdown_without_ray_running():
#     # Test shutdown when Ray is not running
#     ret = subprocess.check_output(["serve", "shutdown", "-y"], stderr=subprocess.STDOUT)
#     assert b"Unable to shutdown Serve on the cluster" in ret


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
        def serve_config_empty_warning():
            config_response = subprocess.check_output(["serve", "config"]).decode(
                "utf-8"
            )
            return config_response == "No configuration was found.\n"

        def serve_status_empty():
            status_response = subprocess.check_output(["serve", "status"])
            status = yaml.safe_load(status_response)
            return len(status["applications"]) == 0

        wait_for_condition(serve_config_empty_warning)
        wait_for_condition(serve_status_empty)
        print("`serve config` and `serve status` print empty responses.\n")


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy_with_http_options(ray_start_stop):
    """Deploys config with host and port options specified"""

    f1 = os.path.join(
        os.path.dirname(__file__), "test_config_files", "basic_graph_http.yaml"
    )
    success_message_fragment = b"Sent deploy request successfully."

    with open(f1, "r") as config_file:
        config = yaml.safe_load(config_file)

    deploy_response = subprocess.check_output(["serve", "deploy", f1])
    assert success_message_fragment in deploy_response

    wait_for_condition(
        lambda: httpx.post("http://localhost:8005/", json=None).text
        == "wonderful world",
        timeout=15,
    )

    # Config should contain matching host and port options
    info_response = subprocess.check_output(["serve", "config"])
    info = yaml.safe_load(info_response)

    # TODO(zcin): the assertion should just be `info == config` here but the output
    # formatting removes a lot of info.
    assert info == config["applications"][0]


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
                "ray.serve.tests.test_cli_3.TestApp1Node",
                "ray.serve.tests.test_cli_3.TestApp2Node",
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
        channel = grpc.insecure_channel(get_application_url("gRPC", app_name=app_name))
        stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
        request = serve_pb2.UserDefinedMessage(name="foo", num=30, foo="bar")
        metadata = (("application", app_name),)
        response = stub.__call__(request=request, metadata=metadata)
        assert response.greeting == "Hello foo from bar"
        print("App 3 is live and reachable over gRPC.")

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

    channel = grpc.insecure_channel(get_application_url("gRPC", app_name=app1))

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

    channel = grpc.insecure_channel(get_application_url("gRPC", app_name=app))

    # Ensures ListApplications method succeeding.
    wait_for_condition(
        ping_grpc_list_applications, channel=channel, app_names=app_names
    )

    # Ensures Healthz method succeeding.
    ping_grpc_healthz(channel)

    # Ensure model composition is responding correctly.
    ping_fruit_stand(channel, app)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_control_c_shutdown_serve_components(ray_start_stop):
    """Test ctrl+c after `serve run` shuts down serve components."""

    p = subprocess.Popen(["serve", "run", "ray.serve.tests.test_cli_3.echo_app"])

    # Make sure Serve components are up and running
    wait_for_condition(check_app_running, app_name=SERVE_DEFAULT_APP_NAME)
    assert httpx.get("http://localhost:8000/-/healthz").text == "success"
    assert json.loads(httpx.get("http://localhost:8000/-/routes").text) == {
        "/": "default"
    }
    assert httpx.get("http://localhost:8000/").text == "hello"

    # Send ctrl+c to shutdown Serve components
    p.send_signal(signal.SIGINT)
    p.wait()

    # Make sure Serve components are shutdown
    status_response = subprocess.check_output(["serve", "status"])
    status = yaml.safe_load(status_response)
    assert status == {"applications": {}, "proxies": {}, "target_capacity": None}


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
@pytest.mark.parametrize(
    "ray_start_stop_in_specific_directory",
    [
        os.path.join(os.path.dirname(__file__), "test_config_files"),
    ],
    indirect=True,
)
def test_deploy_with_access_to_current_directory(ray_start_stop_in_specific_directory):
    """Test serve deploy using modules in the current directory succeeds.

    There was an issue where dashboard client doesn't add the current directory to
    the sys.path and failed to deploy a Serve app defined in the directory. This
    test ensures that files in the current directory can be accessed and deployed.

    See: https://github.com/ray-project/ray/issues/43889
    """
    # Deploy Serve application with a config in the current directory.
    subprocess.check_output(["serve", "deploy", "use_current_working_directory.yaml"])

    # Ensure serve deploy eventually succeeds.
    def check_deploy_successfully():
        status_response = subprocess.check_output(["serve", "status"])
        assert b"RUNNING" in status_response
        return True

    wait_for_condition(check_deploy_successfully, timeout=5)


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
        wait_for_condition(lambda: ping_endpoint() == "foobar", timeout=10)
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
        wait_for_condition(lambda: ping_endpoint() == "foobar", timeout=10)
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
        wait_for_condition(lambda: ping_endpoint() == "foobar", timeout=10)
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
        wait_for_condition(lambda: ping_endpoint() == "foobar", timeout=10)
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
