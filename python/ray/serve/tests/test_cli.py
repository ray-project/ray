import os
import subprocess
import sys
import time
from copy import deepcopy
from tempfile import NamedTemporaryFile
from typing import Dict, List, Optional

import pytest
import requests
import yaml

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME, SERVE_NAMESPACE
from ray.serve.scripts import remove_ansi_escape_sequences
from ray.util.state import list_actors


def assert_deployments_live(ids: List[DeploymentID]):
    """Checks if all deployments named in names have at least 1 living replica."""

    running_actor_names = [actor["name"] for actor in list_actors()]

    for deployment_id in ids:
        prefix = f"{deployment_id.app_name}#{deployment_id.name}"
        msg = f"Deployment {deployment_id} is not live"
        assert any(prefix in actor_name for actor_name in running_actor_names), msg


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


def test_start_shutdown_without_ray_running():
    # Test shutdown when Ray is not running
    ret = subprocess.check_output(["serve", "shutdown", "-y"], stderr=subprocess.STDOUT)
    assert b"Unable to shutdown Serve on the cluster" in ret


def check_http_response(expected_text: str, json: Optional[Dict] = None):
    resp = requests.post("http://localhost:8000/", json=json)
    assert resp.text == expected_text
    return True


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy_basic(ray_start_stop):
    """Deploys some valid config files and checks that the deployments work."""
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
            check_http_response,
            json=["ADD", 2],
            expected_text="3 pizzas please!",
            timeout=15,
        )
        wait_for_condition(
            check_http_response,
            json=["MUL", 2],
            expected_text="-4 pizzas please!",
            timeout=15,
        )
        print("Deployments are reachable over HTTP.")

        deployments = [
            DeploymentID(name="Router"),
            DeploymentID(name="Multiplier"),
            DeploymentID(name="Adder"),
        ]
        assert_deployments_live(deployments)
        print("All deployments are live.\n")

        print("Deploying arithmetic config.")
        deploy_response = subprocess.check_output(
            ["serve", "deploy", arithmetic_file_name, "-a", "http://localhost:52365/"]
        )
        assert success_message_fragment in deploy_response
        print("Deploy request sent successfully.")

        wait_for_condition(
            check_http_response,
            json=["ADD", 0],
            expected_text="1",
            timeout=15,
        )
        wait_for_condition(
            check_http_response,
            json=["SUB", 5],
            expected_text="3",
            timeout=15,
        )
        print("Deployments are reachable over HTTP.")

        deployments = [
            DeploymentID(name="Router"),
            DeploymentID(name="Add"),
            DeploymentID(name="Subtract"),
        ]
        assert_deployments_live(deployments)
        print("All deployments are live.\n")

    ray.shutdown()


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
        lambda: requests.post("http://localhost:8005/").text == "wonderful world",
        timeout=15,
    )

    # Config should contain matching host and port options
    info_response = subprocess.check_output(["serve", "config"])
    info = yaml.safe_load(info_response)

    # TODO(zcin): the assertion should just be `info == config` here but the output
    # formatting removes a lot of info.
    assert info == config["applications"][0]


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy_multi_app_basic(ray_start_stop):
    """Deploys some valid config files and checks that the deployments work."""
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
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).text
            == "3 pizzas please!",
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["MUL", 2]).text
            == "2 pizzas please!",
            timeout=15,
        )
        print('Application "app1" is reachable over HTTP.')
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).text
            == "5 pizzas please!",
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["MUL", 2]).text
            == "4 pizzas please!",
            timeout=15,
        )
        print('Application "app2" is reachable over HTTP.')

        deployment_names = [
            DeploymentID(name="Router", app_name="app1"),
            DeploymentID(name="Multiplier", app_name="app1"),
            DeploymentID(name="Adder", app_name="app1"),
            DeploymentID(name="Router", app_name="app2"),
            DeploymentID(name="Multiplier", app_name="app2"),
            DeploymentID(name="Adder", app_name="app2"),
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
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).text
            == "12 pizzas please!",
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["MUL", 2]).text
            == "20 pizzas please!",
            timeout=15,
        )
        print('Application "app2" is reachable over HTTP.')

        deployment_names = [
            DeploymentID(name="BasicDriver", app_name="app1"),
            DeploymentID(name="f", app_name="app1"),
            DeploymentID(name="Router", app_name="app2"),
            DeploymentID(name="Multiplier", app_name="app2"),
            DeploymentID(name="Adder", app_name="app2"),
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
def test_deploy_bad_v2_config(ray_start_stop):
    """Deploy a bad config with field applications, should try to parse as v2 config."""

    config_file = os.path.join(
        os.path.dirname(__file__), "test_config_files", "bad_multi_config.yaml"
    )

    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.check_output(
            ["serve", "deploy", config_file], stderr=subprocess.STDOUT
        )

    output = e.value.output.decode("utf-8")

    assert "ValidationError" in output, output
    assert "ServeDeploySchema" in output, output
    assert "Please ensure each application's route_prefix is unique" in output


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
def test_cli_without_config_deploy(ray_start_stop):
    """Deploys application with serve.run instead of a config, and check that cli
    still works as expected.
    """

    @serve.deployment
    def fn():
        return "hi"

    serve.run(fn.bind())

    def check_cli():
        info_response = subprocess.check_output(["serve", "config"])
        status_response = subprocess.check_output(["serve", "status"])
        fetched_status = yaml.safe_load(status_response)["applications"][
            SERVE_DEFAULT_APP_NAME
        ]

        assert len(info_response) == 0
        assert fetched_status["status"] == "RUNNING"
        assert fetched_status["deployments"]["fn"]["status"] == "HEALTHY"
        return True

    wait_for_condition(check_cli)
    serve.shutdown()
    ray.shutdown()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_config_with_deleting_app(ray_start_stop):
    """Test that even if one or more apps is deleting, serve config still works"""

    config_json1 = {
        "applications": [
            {
                "name": "app1",
                "route_prefix": "/app1",
                "import_path": "ray.serve.tests.test_config_files.world.DagNode",
            },
            {
                "name": "app2",
                "route_prefix": "/app2",
                "import_path": "ray.serve.tests.test_config_files.delete_blocked.app",
            },
        ]
    }
    config_json2 = deepcopy(config_json1)
    del config_json2["applications"][1]

    def check_cli(expected_configs: List, expected_statuses: int):
        info_response = subprocess.check_output(["serve", "config"])
        status_response = subprocess.check_output(["serve", "status"])
        fetched_configs = list(yaml.safe_load_all(info_response))
        statuses = yaml.safe_load(status_response)

        return (
            len(
                [
                    s
                    for s in statuses["applications"].values()
                    if s["status"] == "RUNNING"
                ]
            )
            == expected_statuses
            and fetched_configs == expected_configs
        )

    with NamedTemporaryFile(mode="w+", suffix=".yaml") as tmp:
        tmp.write(yaml.safe_dump(config_json1))
        tmp.flush()
        subprocess.check_output(["serve", "deploy", tmp.name])
        print("Deployed config with app1 and app2.")

    wait_for_condition(
        check_cli, expected_configs=config_json1["applications"], expected_statuses=2
    )
    print("`serve status` and `serve config` are returning expected responses.")

    with NamedTemporaryFile(mode="w+", suffix=".yaml") as tmp:
        tmp.write(yaml.safe_dump(config_json2))
        tmp.flush()
        subprocess.check_output(["serve", "deploy", tmp.name])
        print("Redeployed config with app2 removed.")

    wait_for_condition(
        check_cli, expected_configs=config_json2["applications"], expected_statuses=1
    )
    print("`serve status` and `serve config` are returning expected responses.")


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_status_basic(ray_start_stop):
    """Deploys a config file and checks its status."""

    # Check that `serve status` works even if no Serve app is running
    subprocess.check_output(["serve", "status"])

    # Deploy config
    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "pizza.yaml"
    )
    subprocess.check_output(["serve", "deploy", config_file_name])

    def num_live_deployments(app_name):
        status_response = subprocess.check_output(["serve", "status"])
        serve_status = yaml.safe_load(status_response)
        return len(serve_status["applications"][app_name]["deployments"])

    wait_for_condition(
        lambda: num_live_deployments(SERVE_DEFAULT_APP_NAME) == 3, timeout=15
    )
    status_response = subprocess.check_output(
        ["serve", "status", "-a", "http://localhost:52365/"]
    )
    serve_status = yaml.safe_load(status_response)
    default_app = serve_status["applications"][SERVE_DEFAULT_APP_NAME]

    expected_deployments = {
        "Multiplier",
        "Adder",
        "Router",
    }
    for name, status in default_app["deployments"].items():
        expected_deployments.remove(name)
        assert status["status"] in {"HEALTHY", "UPDATING"}
        assert status["status_trigger"] == "CONFIG_UPDATE_COMPLETED"
        assert status["replica_states"]["RUNNING"] in {0, 1}
        assert "message" in status
    assert len(expected_deployments) == 0

    assert default_app["status"] in {"DEPLOYING", "RUNNING"}
    wait_for_condition(
        lambda: time.time() > default_app["last_deployed_time_s"],
        timeout=2,
    )

    def proxy_healthy():
        status_response = subprocess.check_output(
            ["serve", "status", "-a", "http://localhost:52365/"]
        )
        proxy_status = yaml.safe_load(status_response)["proxies"]
        return len(proxy_status) and all(p == "HEALTHY" for p in proxy_status.values())

    wait_for_condition(proxy_healthy)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_status_error_msg_format(ray_start_stop):
    """Deploys a faulty config file and checks its status."""

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "deployment_fail.yaml"
    )

    subprocess.check_output(["serve", "deploy", config_file_name])

    def check_for_failed_deployment():
        cli_output = subprocess.check_output(
            ["serve", "status", "-a", "http://localhost:52365/"]
        )
        cli_status = yaml.safe_load(cli_output)["applications"][SERVE_DEFAULT_APP_NAME]
        api_status = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        assert cli_status["status"] == "DEPLOY_FAILED"
        assert remove_ansi_escape_sequences(cli_status["message"]) in api_status.message

        deployment_status = cli_status["deployments"]["A"]
        assert deployment_status["status"] == "DEPLOY_FAILED"
        assert deployment_status["status_trigger"] == "REPLICA_STARTUP_FAILED"
        return True

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
        cli_output = subprocess.check_output(
            ["serve", "status", "-a", "http://localhost:52365/"]
        )
        cli_status = yaml.safe_load(cli_output)["applications"][SERVE_DEFAULT_APP_NAME]
        assert cli_status["status"] == "DEPLOY_FAILED"
        assert "Failed to set up runtime environment" in cli_status["message"]
        return True

    wait_for_condition(check_for_failed_deployment, timeout=15)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_status_syntax_error(ray_start_stop):
    """Deploys Serve app with syntax error, checks error message has traceback."""

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "syntax_error.yaml"
    )

    subprocess.check_output(["serve", "deploy", config_file_name])

    def check_for_failed_deployment():
        cli_output = subprocess.check_output(
            ["serve", "status", "-a", "http://localhost:52365/"]
        )
        status = yaml.safe_load(cli_output)["applications"][SERVE_DEFAULT_APP_NAME]
        assert status["status"] == "DEPLOY_FAILED"
        assert "Traceback (most recent call last)" in status["message"]
        assert "x = (1 + 2" in status["message"]
        return True

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
        cli_output = subprocess.check_output(
            ["serve", "status", "-a", "http://localhost:52365/"]
        )
        status = yaml.safe_load(cli_output)["applications"][SERVE_DEFAULT_APP_NAME]
        assert status["status"] == "DEPLOY_FAILED"

        deployment_status = status["deployments"]["A"]
        assert deployment_status["status"] == "DEPLOY_FAILED"
        assert deployment_status["status_trigger"] == "REPLICA_STARTUP_FAILED"
        assert "ZeroDivisionError" in deployment_status["message"]
        return True

    wait_for_condition(check_for_failed_deployment)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_status_constructor_retry_error(ray_start_stop):
    """Deploys Serve deployment that errors out in constructor, checks that the
    retry message is surfaced.
    """

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "deployment_fail_2.yaml"
    )

    subprocess.check_output(["serve", "deploy", config_file_name])

    def check_for_failed_deployment():
        cli_output = subprocess.check_output(
            ["serve", "status", "-a", "http://localhost:52365/"]
        )
        status = yaml.safe_load(cli_output)["applications"][SERVE_DEFAULT_APP_NAME]
        assert status["status"] == "DEPLOYING"

        deployment_status = status["deployments"]["A"]
        assert deployment_status["status"] == "UPDATING"
        assert deployment_status["status_trigger"] == "CONFIG_UPDATE_STARTED"
        assert "ZeroDivisionError" in deployment_status["message"]
        return True

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
        cli_output = subprocess.check_output(
            ["serve", "status", "-a", "http://localhost:52365/"]
        )
        status = yaml.safe_load(cli_output)["applications"][SERVE_DEFAULT_APP_NAME]
        assert status["status"] == "DEPLOY_FAILED"
        assert "some_wrong_url" in status["deployments"]["TestDeployment"]["message"]
        return True

    wait_for_condition(check_for_failed_deployment, timeout=20)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_max_replicas_per_node(ray_start_stop):
    """Test that max_replicas_per_node can be set via config file."""

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "max_replicas_per_node.yaml"
    )

    subprocess.check_output(["serve", "deploy", config_file_name])

    def check_application_status():
        cli_output = subprocess.check_output(
            ["serve", "status", "-a", "http://localhost:52365/"]
        )
        status = yaml.safe_load(cli_output)["applications"]
        assert (
            status["valid"]["status"] == "RUNNING"
            and status["invalid"]["status"] == "DEPLOY_FAILED"
        )
        return True

    wait_for_condition(check_application_status, timeout=15)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_replica_placement_group_options(ray_start_stop):
    """Test that placement group options can be set via config file."""

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "replica_placement_groups.yaml"
    )

    subprocess.check_output(["serve", "deploy", config_file_name])

    def check_application_status():
        cli_output = subprocess.check_output(
            ["serve", "status", "-a", "http://localhost:52365/"]
        )
        status = yaml.safe_load(cli_output)["applications"]
        assert (
            status["valid"]["status"] == "RUNNING"
            and status["invalid_bundles"]["status"] == "DEPLOY_FAILED"
            and status["invalid_strategy"]["status"] == "DEPLOY_FAILED"
        )
        return True

    wait_for_condition(check_application_status, timeout=15)


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy_from_import_path(ray_start_stop):
    """Test that `deploy` works from an import path."""

    import_path = "ray.serve.tests.test_config_files.arg_builders.build_echo_app"

    subprocess.check_output(["serve", "deploy", import_path])
    wait_for_condition(
        check_http_response,
        expected_text="DEFAULT",
        timeout=15,
    )

    subprocess.check_output(["serve", "deploy", import_path, "message=redeployed!"])
    wait_for_condition(
        check_http_response,
        expected_text="redeployed!",
        timeout=15,
    )


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
