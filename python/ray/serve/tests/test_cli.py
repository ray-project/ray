import yaml
import json
import os
import subprocess
import sys
import signal
import pytest
import requests

import ray
from ray import serve
from ray.tests.conftest import tmp_working_dir  # noqa: F401, E501
from ray._private.test_utils import wait_for_condition
from ray.serve.api import Application


def ping_endpoint(endpoint: str, params: str = ""):
    try:
        return requests.get(f"http://localhost:8000/{endpoint}{params}").text
    except requests.exceptions.ConnectionError:
        return "connection error"


@pytest.fixture
def ray_start_stop():
    subprocess.check_output(["ray", "start", "--head"])
    yield
    subprocess.check_output(["ray", "stop", "--force"])


def test_start_shutdown(ray_start_stop):
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_output(["serve", "shutdown"])

    subprocess.check_output(["serve", "start"])
    subprocess.check_output(["serve", "shutdown"])


def test_start_shutdown_in_namespace(ray_start_stop):
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_output(["serve", "shutdown", "-n", "test"])

    subprocess.check_output(["serve", "start", "-n", "test"])
    subprocess.check_output(["serve", "shutdown", "-n", "test"])


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy(ray_start_stop):
    # Deploys some valid config files and checks that the deployments work

    # Initialize serve in test to enable calling serve.list_deployments()
    ray.init(address="auto", namespace="serve")
    serve.start(detached=True)

    # Create absolute file names to YAML config files
    three_deployments = os.path.join(
        os.path.dirname(__file__), "test_config_files", "three_deployments.yaml"
    )
    two_deployments = os.path.join(
        os.path.dirname(__file__), "test_config_files", "two_deployments.yaml"
    )
    deny_deployment = os.path.join(
        os.path.dirname(__file__), "test_config_files", "deny_access.yaml"
    )

    # Dictionary mapping test config file names to expected deployment names
    # and configurations. These should match the values specified in the YAML
    # files.
    configs = {
        three_deployments: {
            "shallow": {
                "num_replicas": 1,
                "response": "Hello shallow world!",
            },
            "deep": {
                "num_replicas": 1,
                "response": "Hello deep world!",
            },
            "one": {
                "num_replicas": 2,
                "response": "2",
            },
        },
        two_deployments: {
            "shallow": {
                "num_replicas": 3,
                "response": "Hello shallow world!",
            },
            "one": {
                "num_replicas": 2,
                "response": "2",
            },
        },
    }

    request_url = "http://localhost:8000/"
    success_message_fragment = b"Sent deploy request successfully!"

    # Check idempotence:
    for _ in range(2):
        for config_file_name, expected_deployments in configs.items():
            deploy_response = subprocess.check_output(
                ["serve", "deploy", config_file_name]
            )
            assert success_message_fragment in deploy_response

            for name, deployment_config in expected_deployments.items():
                wait_for_condition(
                    lambda: (
                        requests.get(f"{request_url}{name}").text
                        == deployment_config["response"]
                    ),
                    timeout=15,
                )

            running_deployments = serve.list_deployments()

            # Check that running deployment names match expected deployment names
            assert set(running_deployments.keys()) == expected_deployments.keys()

            for name, deployment in running_deployments.items():
                assert (
                    deployment.num_replicas
                    == expected_deployments[name]["num_replicas"]
                )

        # Deploy a deployment without HTTP access
        deploy_response = subprocess.check_output(["serve", "deploy", deny_deployment])
        assert success_message_fragment in deploy_response

        wait_for_condition(
            lambda: requests.get(f"{request_url}shallow").status_code == 404, timeout=15
        )
        assert (
            ray.get(serve.get_deployment("shallow").get_handle().remote())
            == "Hello shallow world!"
        )

    serve.shutdown()
    ray.shutdown()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_config(ray_start_stop):
    # Deploys valid config file and checks that serve info returns correct
    # response

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "two_deployments.yaml"
    )
    success_message_fragment = b"Sent deploy request successfully!"
    deploy_response = subprocess.check_output(["serve", "deploy", config_file_name])
    assert success_message_fragment in deploy_response

    info_response = subprocess.check_output(["serve", "config"])
    info = yaml.safe_load(info_response)

    assert "deployments" in info
    assert len(info["deployments"]) == 2

    # Validate non-default information about shallow deployment
    shallow_info = None
    for deployment_info in info["deployments"]:
        if deployment_info["name"] == "shallow":
            shallow_info = deployment_info

    assert shallow_info is not None
    assert shallow_info["import_path"] == "test_env.shallow_import.ShallowClass"
    assert shallow_info["num_replicas"] == 3
    assert shallow_info["route_prefix"] == "/shallow"
    assert (
        "https://github.com/shrekris-anyscale/test_deploy_group/archive/HEAD.zip"
        in shallow_info["ray_actor_options"]["runtime_env"]["py_modules"]
    )
    assert (
        "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"
        in shallow_info["ray_actor_options"]["runtime_env"]["py_modules"]
    )

    # Validate non-default information about one deployment
    one_info = None
    for deployment_info in info["deployments"]:
        if deployment_info["name"] == "one":
            one_info = deployment_info

    assert one_info is not None
    assert one_info["import_path"] == "test_module.test.one"
    assert one_info["num_replicas"] == 2
    assert one_info["route_prefix"] == "/one"
    assert (
        "https://github.com/shrekris-anyscale/test_deploy_group/archive/HEAD.zip"
        in one_info["ray_actor_options"]["runtime_env"]["py_modules"]
    )
    assert (
        "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"
        in one_info["ray_actor_options"]["runtime_env"]["py_modules"]
    )


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_status(ray_start_stop):
    # Deploys a config file and checks its status

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "three_deployments.yaml"
    )

    subprocess.check_output(["serve", "deploy", config_file_name])
    status_response = subprocess.check_output(["serve", "status"])
    statuses = json.loads(status_response)["statuses"]

    expected_deployments = {"shallow", "deep", "one"}
    for status in statuses:
        expected_deployments.remove(status["name"])
        assert status["status"] in {"HEALTHY", "UPDATING"}
        assert "message" in status
    assert len(expected_deployments) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_delete(ray_start_stop):
    # Deploys a config file and deletes it

    def get_num_deployments():
        info_response = subprocess.check_output(["serve", "config"])
        info = yaml.safe_load(info_response)
        return len(info["deployments"])

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "two_deployments.yaml"
    )

    # Check idempotence
    for _ in range(2):
        subprocess.check_output(["serve", "deploy", config_file_name])
        wait_for_condition(lambda: get_num_deployments() == 2, timeout=35)

        subprocess.check_output(["serve", "delete", "-y"])
        wait_for_condition(lambda: get_num_deployments() == 0, timeout=35)


@serve.deployment
def parrot(request):
    return request.query_params["sound"]


parrot_app = Application([parrot])


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_application(ray_start_stop):
    # Deploys valid config file and import path via serve run

    # Deploy via config file
    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "two_deployments.yaml"
    )

    p = subprocess.Popen(["serve", "run", "--address=auto", config_file_name])
    wait_for_condition(lambda: ping_endpoint("one") == "2", timeout=10)
    wait_for_condition(
        lambda: ping_endpoint("shallow") == "Hello shallow world!", timeout=10
    )

    p.send_signal(signal.SIGINT)  # Equivalent to ctrl-C
    p.wait()
    assert ping_endpoint("one") == "connection error"
    assert ping_endpoint("shallow") == "connection error"

    # Deploy via import path
    p = subprocess.Popen(
        ["serve", "run", "--address=auto", "ray.serve.tests.test_cli.parrot_app"]
    )
    wait_for_condition(
        lambda: ping_endpoint("parrot", params="?sound=squawk") == "squawk", timeout=10
    )

    p.send_signal(signal.SIGINT)  # Equivalent to ctrl-C
    p.wait()
    assert ping_endpoint("parrot", params="?sound=squawk") == "connection error"


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
    # Tests serve run with specified args and kwargs

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
    assert ping_endpoint("Macaw") == "connection error"


@serve.deployment
class MetalDetector:
    def __call__(self, *args):
        return os.environ.get("buried_item", "no dice")


metal_detector_node = MetalDetector.bind()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_runtime_env(ray_start_stop):
    # Test serve run with runtime_env passed in

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
                '{"py_modules": ["https://github.com/shrekris-anyscale/'
                'test_deploy_group/archive/HEAD.zip"],'
                '"working_dir": "http://nonexistentlink-q490123950ni34t"}'
            ),
            "--working-dir",
            "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip",
        ]
    )
    wait_for_condition(lambda: ping_endpoint("one") == "2", timeout=10)
    p.send_signal(signal.SIGINT)
    p.wait()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
@pytest.mark.parametrize("use_command", [True, False])
def test_idempotence_after_controller_death(ray_start_stop, use_command: bool):
    """Check that CLI is idempotent even if controller dies."""

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "two_deployments.yaml"
    )
    success_message_fragment = b"Sent deploy request successfully!"
    deploy_response = subprocess.check_output(["serve", "deploy", config_file_name])
    assert success_message_fragment in deploy_response

    ray.init(address="auto", namespace="serve")
    serve.start(detached=True)
    assert len(serve.list_deployments()) == 2

    # Kill controller
    if use_command:
        subprocess.check_output(["serve", "shutdown"])
    else:
        serve.shutdown()

    info_response = subprocess.check_output(["serve", "config"])
    info = yaml.safe_load(info_response)

    assert "deployments" in info
    assert len(info["deployments"]) == 0

    deploy_response = subprocess.check_output(["serve", "deploy", config_file_name])
    assert success_message_fragment in deploy_response

    # Restore testing controller
    serve.start(detached=True)
    assert len(serve.list_deployments()) == 2
    serve.shutdown()
    ray.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
