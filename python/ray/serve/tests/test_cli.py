import os
import signal
import subprocess
import sys
import time
import json
from tempfile import NamedTemporaryFile
from typing import List

import click
from pydantic import BaseModel
import pytest
import requests
import yaml

import ray
from ray import serve
from ray.experimental.state.api import list_actors
from ray._private.test_utils import wait_for_condition
from ray.serve.schema import ServeApplicationSchema
from ray.serve._private.constants import SERVE_NAMESPACE, MULTI_APP_MIGRATION_MESSAGE
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.tests.conftest import tmp_working_dir  # noqa: F401, E501
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient
from ray.serve.scripts import convert_args_to_dict, remove_ansi_escape_sequences
from ray.serve._private.constants import (
    SERVE_DEFAULT_APP_NAME,
    DEPLOYMENT_NAME_PREFIX_SEPARATOR,
)

CONNECTION_ERROR_MSG = "connection error"


def ping_endpoint(endpoint: str, params: str = ""):
    try:
        return requests.get(f"http://localhost:8000/{endpoint}{params}").text
    except requests.exceptions.ConnectionError:
        return CONNECTION_ERROR_MSG


def assert_deployments_live(names: List[str]):
    """Checks if all deployments named in names have at least 1 living replica."""

    running_actor_names = [actor["name"] for actor in list_actors()]

    all_deployments_live, nonliving_deployment = True, ""
    for deployment_name in names:
        for actor_name in running_actor_names:
            if deployment_name in actor_name:
                break
        else:
            all_deployments_live, nonliving_deployment = False, deployment_name
    assert all_deployments_live, f'"{nonliving_deployment}" deployment is not live.'


def test_convert_args_to_dict():
    assert convert_args_to_dict(tuple()) == {}

    with pytest.raises(
        click.ClickException, match="Invalid application argument 'bad_arg'"
    ):
        convert_args_to_dict(("bad_arg",))

    assert convert_args_to_dict(("key1=val1", "key2=val2")) == {
        "key1": "val1",
        "key2": "val2",
    }


def test_start_shutdown(ray_start_stop):
    subprocess.check_output(["serve", "start"])
    subprocess.check_output(["serve", "shutdown", "-y"])


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy(ray_start_stop):
    """Deploys some valid config files and checks that the deployments work."""
    # Initialize serve in test to enable calling serve.list_deployments()
    ray.init(address="auto", namespace=SERVE_NAMESPACE)

    # Create absolute file names to YAML config files
    pizza_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "pizza.yaml"
    )
    arithmetic_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "arithmetic.yaml"
    )

    success_message_fragment = b"Sent deploy request successfully."

    # Ensure the CLI is idempotent
    num_iterations = 2
    for iteration in range(1, num_iterations + 1):
        print(f"*** Starting Iteration {iteration}/{num_iterations} ***\n")

        print("Deploying pizza config.")
        deploy_response = subprocess.check_output(["serve", "deploy", pizza_file_name])
        assert success_message_fragment in deploy_response
        print("Deploy request sent successfully.")

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "3 pizzas please!",
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["MUL", 2]).json()
            == "-4 pizzas please!",
            timeout=15,
        )
        print("Deployments are reachable over HTTP.")

        deployment_names = [
            f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}DAGDriver",
            f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}create_order",
            f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}Router",
            f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}Multiplier",
            f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}Adder",
        ]
        assert_deployments_live(deployment_names)
        print("All deployments are live.\n")

        print("Deploying arithmetic config.")
        deploy_response = subprocess.check_output(
            ["serve", "deploy", arithmetic_file_name, "-a", "http://localhost:52365/"]
        )
        assert success_message_fragment in deploy_response
        print("Deploy request sent successfully.")

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 0]).json()
            == 1,
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["SUB", 5]).json()
            == 3,
            timeout=15,
        )
        print("Deployments are reachable over HTTP.")

        deployment_names = [
            f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}DAGDriver",
            f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}Router",
            f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}Add",
            f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}Subtract",
        ]
        assert_deployments_live(deployment_names)
        print("All deployments are live.\n")

    ray.shutdown()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy_with_http_options(ray_start_stop):
    """Deploys config with host and port options specified"""

    f1 = os.path.join(
        os.path.dirname(__file__), "test_config_files", "basic_graph_http.yaml"
    )
    f2 = os.path.join(
        os.path.dirname(__file__), "test_config_files", "basic_graph.yaml"
    )
    success_message_fragment = b"Sent deploy request successfully."

    with open(f1, "r") as config_file:
        config = yaml.safe_load(config_file)

    deploy_response = subprocess.check_output(["serve", "deploy", f1])
    assert success_message_fragment in deploy_response

    wait_for_condition(
        lambda: requests.post("http://localhost:8005/").text == "wonderful world",
        timeout=15,
    )

    # Config should contain matching host and port options
    info_response = subprocess.check_output(["serve", "config"])
    info = yaml.safe_load(info_response)

    assert config == info

    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_output(["serve", "deploy", f2], stderr=subprocess.STDOUT)

    assert requests.post("http://localhost:8005/").text == "wonderful world"

    deploy_response = subprocess.check_output(["serve", "deploy", f1])
    assert success_message_fragment in deploy_response

    wait_for_condition(
        lambda: requests.post("http://localhost:8005/").text == "wonderful world",
        timeout=15,
    )


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy_multi_app(ray_start_stop):
    """Deploys some valid config files and checks that the deployments work."""
    # Initialize serve in test to enable calling serve.list_deployments()
    ray.init(address="auto", namespace=SERVE_NAMESPACE)

    # Create absolute file names to YAML config files
    two_pizzas = os.path.join(
        os.path.dirname(__file__), "test_config_files", "two_pizzas.yaml"
    )
    pizza_world = os.path.join(
        os.path.dirname(__file__), "test_config_files", "pizza_world.yaml"
    )

    success_message_fragment = b"Sent deploy request successfully."

    # Ensure the CLI is idempotent
    num_iterations = 2
    for iteration in range(1, num_iterations + 1):
        print(f"*** Starting Iteration {iteration}/{num_iterations} ***\n")

        print("Deploying two pizzas config.")
        deploy_response = subprocess.check_output(["serve", "deploy", two_pizzas])
        assert success_message_fragment in deploy_response
        print("Deploy request sent successfully.")

        # Test add and mul for each of the two apps
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "3 pizzas please!",
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["MUL", 2]).json()
            == "2 pizzas please!",
            timeout=15,
        )
        print('Application "app1" is reachable over HTTP.')
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
            == "5 pizzas please!",
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["MUL", 2]).json()
            == "4 pizzas please!",
            timeout=15,
        )
        print('Application "app2" is reachable over HTTP.')

        deployment_names = [
            "app1_DAGDriver",
            "app1_create_order",
            "app1_Router",
            "app1_Multiplier",
            "app1_Adder",
            "app2_DAGDriver",
            "app2_create_order",
            "app2_Router",
            "app2_Multiplier",
            "app2_Adder",
        ]
        assert_deployments_live(deployment_names)
        print("All deployments are live.\n")

        print("Deploying pizza world config.")
        deploy_response = subprocess.check_output(["serve", "deploy", pizza_world])
        assert success_message_fragment in deploy_response
        print("Deploy request sent successfully.")

        # Test app1 (simple wonderful world) and app2 (add + mul)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1").text
            == "wonderful world",
            timeout=15,
        )
        print('Application "app1" is reachable over HTTP.')
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
            == "12 pizzas please!",
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["MUL", 2]).json()
            == "20 pizzas please!",
            timeout=15,
        )
        print('Application "app2" is reachable over HTTP.')

        deployment_names = [
            "app1_BasicDriver",
            "app1_f",
            "app2_DAGDriver",
            "app2_create_order",
            "app2_Router",
            "app2_Multiplier",
            "app2_Adder",
        ]
        assert_deployments_live(deployment_names)
        print("All deployments are live.\n")

    ray.shutdown()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy_duplicate_apps(ray_start_stop):
    """If a config with duplicate app names is deployed, `serve deploy` should fail.
    The response should clearly indicate a validation error.
    """

    config_file = os.path.join(
        os.path.dirname(__file__), "test_config_files", "duplicate_app_names.yaml"
    )

    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.check_output(
            ["serve", "deploy", config_file], stderr=subprocess.STDOUT
        )
    assert "ValidationError" in e.value.output.decode("utf-8")


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy_duplicate_routes(ray_start_stop):
    """If a config with duplicate routes is deployed, the PUT request should fail.
    The response should clearly indicate a validation error.
    """

    config_file = os.path.join(
        os.path.dirname(__file__), "test_config_files", "duplicate_app_routes.yaml"
    )

    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.check_output(
            ["serve", "deploy", config_file], stderr=subprocess.STDOUT
        )
    assert "ValidationError" in e.value.output.decode("utf-8")


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy_bad_config1(ray_start_stop):
    """Deploy a bad config with field applications, should try to parse as v2 config."""

    config_file = os.path.join(
        os.path.dirname(__file__), "test_config_files", "bad_multi_config.yaml"
    )

    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.check_output(
            ["serve", "deploy", config_file], stderr=subprocess.STDOUT
        )
    assert "ValidationError" in e.value.output.decode("utf-8")
    assert "ServeDeploySchema" in e.value.output.decode("utf-8")


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy_bad_config2(ray_start_stop):
    """
    Deploy a bad config without field applications, should try to parse as v1 config.
    """

    config_file = os.path.join(
        os.path.dirname(__file__), "test_config_files", "bad_single_config.yaml"
    )

    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.check_output(
            ["serve", "deploy", config_file], stderr=subprocess.STDOUT
        )
    assert "ValidationError" in e.value.output.decode("utf-8")
    assert "ServeApplicationSchema" in e.value.output.decode("utf-8")


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy_single_with_name(ray_start_stop):
    config_file = os.path.join(
        os.path.dirname(__file__), "test_config_files", "single_config_with_name.yaml"
    )

    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.check_output(
            ["serve", "deploy", config_file], stderr=subprocess.STDOUT
        )
    assert "name" in e.value.output.decode("utf-8")
    assert MULTI_APP_MIGRATION_MESSAGE in e.value.output.decode("utf-8")


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy_multi_app_builder_with_args(ray_start_stop):
    """Deploys a config file containing multiple applications that take arguments."""
    # Create absolute file names to YAML config file.
    apps_with_args = os.path.join(
        os.path.dirname(__file__), "test_config_files", "apps_with_args.yaml"
    )

    subprocess.check_output(["serve", "deploy", apps_with_args])

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/untyped_default").text
        == "DEFAULT",
        timeout=10,
    )

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/untyped_hello").text == "hello",
        timeout=10,
    )

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/typed_default").text == "DEFAULT",
        timeout=10,
    )

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/typed_hello").text == "hello",
        timeout=10,
    )


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_config(ray_start_stop):
    """Deploys config and checks that `serve config` returns correct response."""

    # Check that `serve config` works even if no Serve app is running
    info_response = subprocess.check_output(["serve", "config"])
    info = yaml.safe_load(info_response)

    assert ServeApplicationSchema.get_empty_schema_dict() == info

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "basic_graph.yaml"
    )
    success_message_fragment = b"Sent deploy request successfully."

    with open(config_file_name, "r") as config_file:
        config = yaml.safe_load(config_file)

    deploy_response = subprocess.check_output(["serve", "deploy", config_file_name])
    assert success_message_fragment in deploy_response

    # Config should be immediately ready
    info_response = subprocess.check_output(["serve", "config"])
    info = yaml.safe_load(info_response)

    assert config == info


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_config_multi_app(ray_start_stop):
    """Deploys multi-app config and checks output of `serve config`."""

    # Check that `serve config` works even if no Serve app is running
    subprocess.check_output(["serve", "config"])

    # Deploy config
    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "pizza_world.yaml"
    )
    with open(config_file_name, "r") as config_file:
        config = yaml.safe_load(config_file)
    subprocess.check_output(["serve", "deploy", config_file_name])

    # Config should be immediately ready
    info_response = subprocess.check_output(["serve", "config"])
    fetched_configs = list(yaml.safe_load_all(info_response))

    assert config["applications"][0] == fetched_configs[0]
    assert config["applications"][1] == fetched_configs[1]


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_status(ray_start_stop):
    """Deploys a config file and checks its status."""

    # Check that `serve status` works even if no Serve app is running
    subprocess.check_output(["serve", "status"])

    # Deploy config
    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "pizza.yaml"
    )
    subprocess.check_output(["serve", "deploy", config_file_name])

    def num_live_deployments():
        status_response = subprocess.check_output(["serve", "status"])
        serve_status = yaml.safe_load(status_response)
        return len(serve_status["deployment_statuses"])

    wait_for_condition(lambda: num_live_deployments() == 5, timeout=15)
    status_response = subprocess.check_output(
        ["serve", "status", "-a", "http://localhost:52365/"]
    )
    serve_status = yaml.safe_load(status_response)

    expected_deployments = {
        f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}DAGDriver",
        f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}Multiplier",
        f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}Adder",
        f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}Router",
        f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}create_order",
    }
    for status in serve_status["deployment_statuses"]:
        expected_deployments.remove(status["name"])
        assert status["status"] in {"HEALTHY", "UPDATING"}
        assert "message" in status
    assert len(expected_deployments) == 0

    assert serve_status["app_status"]["status"] in {"DEPLOYING", "RUNNING"}
    wait_for_condition(
        lambda: time.time() > serve_status["app_status"]["deployment_timestamp"],
        timeout=2,
    )


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_status_error_msg_format(ray_start_stop):
    """Deploys a faulty config file and checks its status."""

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "deployment_fail.yaml"
    )

    subprocess.check_output(["serve", "deploy", config_file_name])

    def check_for_failed_deployment():
        serve_status = yaml.safe_load(
            subprocess.check_output(
                ["serve", "status", "-a", "http://localhost:52365/"]
            )
        )
        app_status = ServeSubmissionClient("http://localhost:52365").get_status()
        return (
            serve_status["app_status"]["status"] == "DEPLOY_FAILED"
            and remove_ansi_escape_sequences(app_status["app_status"]["message"])
            in serve_status["app_status"]["message"]
        )

    wait_for_condition(check_for_failed_deployment)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_status_invalid_runtime_env(ray_start_stop):
    """Deploys a config file with invalid runtime env and checks status.

    get_status() should not throw error (meaning REST API returned 200 status code) and
    the status be deploy failed."""

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "bad_runtime_env.yaml"
    )

    subprocess.check_output(["serve", "deploy", config_file_name])

    def check_for_failed_deployment():
        app_status = ServeSubmissionClient("http://localhost:52365").get_status()
        return (
            app_status["app_status"]["status"] == "DEPLOY_FAILED"
            and "Failed to set up runtime environment"
            in app_status["app_status"]["message"]
        )

    wait_for_condition(check_for_failed_deployment)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_status_syntax_error(ray_start_stop):
    """Deploys Serve app with syntax error, checks the error message is descriptive."""

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "syntax_error.yaml"
    )

    subprocess.check_output(["serve", "deploy", config_file_name])

    def check_for_failed_deployment():
        app_status = ServeSubmissionClient("http://localhost:52365").get_status()
        return (
            app_status["app_status"]["status"] == "DEPLOY_FAILED"
            and "x = (1 + 2" in app_status["app_status"]["message"]
        )

    wait_for_condition(check_for_failed_deployment)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_status_constructor_error(ray_start_stop):
    """Deploys Serve deployment that errors out in constructor, checks that the
    traceback is surfaced.
    """

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "deployment_fail.yaml"
    )

    subprocess.check_output(["serve", "deploy", config_file_name])

    def check_for_failed_deployment():
        status_response = subprocess.check_output(
            ["serve", "status", "-a", "http://localhost:52365/"]
        )
        serve_status = yaml.safe_load(status_response)
        return (
            serve_status["app_status"]["status"] == "DEPLOY_FAILED"
            and "ZeroDivisionError" in serve_status["deployment_statuses"][0]["message"]
        )

    wait_for_condition(check_for_failed_deployment)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_status_package_unavailable_in_controller(ray_start_stop):
    """Test that exceptions raised from packages that are installed on deployment actors
    but not on controller is serialized and surfaced properly.
    """

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "sqlalchemy.yaml"
    )

    subprocess.check_output(["serve", "deploy", config_file_name])

    def check_for_failed_deployment():
        status_response = subprocess.check_output(
            ["serve", "status", "-a", "http://localhost:52365/"]
        )
        serve_status = yaml.safe_load(status_response)
        return (
            serve_status["app_status"]["status"] == "DEPLOY_FAILED"
            and "some_wrong_url" in serve_status["deployment_statuses"][0]["message"]
        )

    wait_for_condition(check_for_failed_deployment, timeout=15)


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
        serve_status = list(yaml.safe_load_all(status_response))
        return len(serve_status[0]["deployment_statuses"]) and len(
            serve_status[1]["deployment_statuses"]
        )

    wait_for_condition(lambda: num_live_deployments() == 5, timeout=15)
    print("All deployments are live.")

    status_response = subprocess.check_output(
        ["serve", "status", "-a", "http://localhost:52365/"]
    )
    serve_statuses = yaml.safe_load_all(status_response)

    expected_deployments = {
        "app1_f",
        "app1_BasicDriver",
        "app2_DAGDriver",
        "app2_Multiplier",
        "app2_Adder",
        "app2_Router",
        "app2_create_order",
    }
    for status in serve_statuses:
        for deployment in status["deployment_statuses"]:
            expected_deployments.remove(deployment["name"])
            assert deployment["status"] in {"HEALTHY", "UPDATING"}
            assert "message" in deployment
    assert len(expected_deployments) == 0
    print("All expected deployments are present in the status output.")

    for status in serve_statuses:
        assert status["app_status"]["status"] in {"DEPLOYING", "RUNNING"}
        assert time.time() > status["app_status"]["deployment_timestamp"]
    print("Verified status and deployment timestamp of both apps.")


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_shutdown(ray_start_stop):
    """Deploys a config file and shuts down the Serve application."""

    # Check that `serve shutdown` works even if no Serve app is running
    subprocess.check_output(["serve", "shutdown", "-y"])

    def num_live_deployments():
        status_response = subprocess.check_output(["serve", "status"])
        serve_status = yaml.safe_load(status_response)
        return len(serve_status["deployment_statuses"])

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
        config = yaml.safe_load(config_response)
        assert ServeApplicationSchema.get_empty_schema_dict() != config

        status_response = subprocess.check_output(["serve", "status"])
        status = yaml.safe_load(status_response)
        assert "There are no applications running on this cluster." != status
        print("`serve config` and `serve status` print non-empty responses.\n")

        print("Deleting Serve app.")
        subprocess.check_output(["serve", "shutdown", "-y"])

        # `serve config` and `serve status` should print empty schemas
        def serve_config_empty():
            config_response = subprocess.check_output(["serve", "config"])
            config = yaml.safe_load(config_response)
            return ServeApplicationSchema.get_empty_schema_dict() == config

        def serve_status_empty():
            status_response = subprocess.check_output(["serve", "status"])
            status = yaml.safe_load(status_response)
            return "There are no applications running on this cluster." == status

        wait_for_condition(serve_config_empty)
        wait_for_condition(serve_status_empty)
        print("`serve config` and `serve status` print empty responses.\n")


@serve.deployment
def parrot(request):
    return request.query_params["sound"]


parrot_node = parrot.bind()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_application(ray_start_stop):
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

    p.send_signal(signal.SIGINT)  # Equivalent to ctrl-C
    p.wait()
    with pytest.raises(requests.exceptions.ConnectionError):
        requests.post("http://localhost:8000/", json=["ADD", 0]).json()
    print("Kill successful! Deployments are not reachable over HTTP.")

    print('Running node at import path "ray.serve.tests.test_cli.parrot_node".')
    # Deploy via import path
    p = subprocess.Popen(
        ["serve", "run", "--address=auto", "ray.serve.tests.test_cli.parrot_node"]
    )
    wait_for_condition(
        lambda: ping_endpoint("parrot", params="?sound=squawk") == "squawk"
    )
    print("Run successful! Deployment is live and reachable over HTTP. Killing run.")

    p.send_signal(signal.SIGINT)  # Equivalent to ctrl-C
    p.wait()
    assert ping_endpoint("parrot", params="?sound=squawk") == CONNECTION_ERROR_MSG
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
        lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
        == "12 pizzas please!",
        timeout=15,
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app2", json=["MUL", 2]).json()
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
            "ray.serve.tests.test_cli.molly_macaw",
        ]
    )
    wait_for_condition(lambda: ping_endpoint("Macaw") == "Molly is green!", timeout=10)
    p.send_signal(signal.SIGINT)
    p.wait()
    assert ping_endpoint("Macaw") == CONNECTION_ERROR_MSG


@serve.deployment
class Echo:
    def __init__(self, message: str):
        print("Echo message:", message)
        self._message = message

    def __call__(self, *args):
        return self._message


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
        "ray.serve.tests.test_cli.build_echo_app",
        "ray.serve.tests.test_cli.build_echo_app_typed",
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
    assert ping_endpoint("") == CONNECTION_ERROR_MSG

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
    assert ping_endpoint("") == CONNECTION_ERROR_MSG


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
            "ray.serve.tests.test_cli.metal_detector_node",
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
            (
                '{"py_modules": ["https://github.com/ray-project/test_deploy_group'
                '/archive/67971777e225600720f91f618cdfe71fc47f60ee.zip"],'
                '"working_dir": "http://nonexistentlink-q490123950ni34t"}'
            ),
            "--working-dir",
            (
                "https://github.com/ray-project/test_dag/archive/"
                "40d61c141b9c37853a7014b8659fc7f23c1d04f6.zip"
            ),
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


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
@pytest.mark.parametrize(
    "config_file", ["basic_graph_http.yaml", "basic_multi_http.yaml"]
)
def test_run_config_port3(ray_start_stop, config_file):
    """If port is specified as argument to `serve run`, it should override config."""
    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", config_file
    )
    p = subprocess.Popen(["serve", "run", "--port=8010", config_file_name])
    wait_for_condition(
        lambda: requests.post("http://localhost:8010/").text == "wonderful world",
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
        ["serve", "run", "ray.serve.tests.test_cli.constructor_failure_node"],
        stderr=subprocess.STDOUT,
        timeout=30,
    ).decode()
    assert "Intentionally failing." in logs

    logs = subprocess.check_output(
        ["serve", "run", "ray.serve.tests.test_cli.constructor_failure_node"],
        stderr=subprocess.STDOUT,
        timeout=30,
    ).decode()
    assert "Intentionally failing." in logs


@serve.deployment
def global_f(*args):
    return "wonderful world"


@serve.deployment
class NoArgDriver:
    def __init__(self, dag: RayServeDAGHandle):
        self.dag = dag

    async def __call__(self):
        return await (await self.dag.remote())


TestBuildFNode = global_f.bind()
TestBuildDagNode = NoArgDriver.bind(TestBuildFNode)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
@pytest.mark.parametrize("node", ["TestBuildFNode", "TestBuildDagNode"])
def test_build(ray_start_stop, node):
    with NamedTemporaryFile(mode="w+", suffix=".yaml") as tmp:

        print(f'Building node "{node}".')
        # Build an app
        subprocess.check_output(
            [
                "serve",
                "build",
                f"ray.serve.tests.test_cli.{node}",
                "-o",
                tmp.name,
            ]
        )
        print("Build succeeded! Deploying node.")

        subprocess.check_output(["serve", "deploy", tmp.name])
        wait_for_condition(lambda: ping_endpoint("") == "wonderful world", timeout=15)
        print("Deploy succeeded! Node is live and reachable over HTTP. Deleting node.")

        subprocess.check_output(["serve", "shutdown", "-y"])
        wait_for_condition(
            lambda: ping_endpoint("") == CONNECTION_ERROR_MSG, timeout=15
        )
        print("Delete succeeded! Node is not reachable over HTTP.")


TestApp1Node = global_f.options(route_prefix="/app1").bind()
TestApp2Node = NoArgDriver.options(route_prefix="/app2").bind(global_f.bind())


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_build_multi_app(ray_start_stop):
    with NamedTemporaryFile(mode="w+", suffix=".yaml") as tmp:

        print('Building nodes "TestApp1Node" and "TestApp2Node".')
        # Build an app
        subprocess.check_output(
            [
                "serve",
                "build",
                "--multi-app",
                "ray.serve.tests.test_cli.TestApp1Node",
                "ray.serve.tests.test_cli.TestApp2Node",
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
def test_build_kubernetes_flag():
    with NamedTemporaryFile(mode="w+", suffix=".yaml") as tmp:
        print("Building k8sFNode.")
        subprocess.check_output(
            [
                "serve",
                "build",
                "ray.serve.tests.test_cli.k8sFNode",
                "-o",
                tmp.name,
                "-k",
            ]
        )
        print("Build succeeded!")

        tmp.seek(0)
        config = yaml.safe_load(tmp.read())
        assert config == {
            "importPath": "ray.serve.tests.test_cli.k8sFNode",
            "runtimeEnv": json.dumps({}),
            "host": "0.0.0.0",
            "port": 8000,
            "deployments": [
                {
                    "name": "global_f",
                    "numReplicas": 2,
                    "rayActorOptions": {
                        "numCpus": 2.0,
                        "numGpus": 1.0,
                    },
                },
            ],
        }


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
    serve.start(detached=True)
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

    assert status_info == "There are no applications running on this cluster."

    deploy_response = subprocess.check_output(["serve", "deploy", config_file_name])
    assert success_message_fragment in deploy_response

    # Restore testing controller
    serve.start(detached=True)
    wait_for_condition(
        lambda: len(list_actors(filters=[("state", "=", "ALIVE")])) == 4,
        timeout=15,
    )
    serve.shutdown()
    ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
