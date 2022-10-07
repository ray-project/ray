import os
import signal
import subprocess
import sys
import time
import json
from tempfile import NamedTemporaryFile
from typing import List

import pytest
import requests
import yaml

import ray
from ray import serve
from ray.experimental.state.api import list_actors
from ray._private.test_utils import wait_for_condition
from ray.serve.schema import ServeApplicationSchema, ServeStatusSchema
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.tests.conftest import tmp_working_dir  # noqa: F401, E501
from ray.dashboard.modules.serve.sdk import ServeSubmissionClient
from ray.serve.scripts import remove_ansi_escape_sequences

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

    success_message_fragment = b"Sent deploy request successfully!"

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
            "DAGDriver",
            "create_order",
            "Router",
            "Multiplier",
            "Adder",
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

        deployment_names = ["DAGDriver", "Router", "Add", "Subtract"]
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
    success_message_fragment = b"Sent deploy request successfully!"

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
        subprocess.check_output(["serve", "deploy", f2])

    assert requests.post("http://localhost:8005/").text == "wonderful world"

    deploy_response = subprocess.check_output(["serve", "deploy", f1])
    assert success_message_fragment in deploy_response

    wait_for_condition(
        lambda: requests.post("http://localhost:8005/").text == "wonderful world",
        timeout=15,
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
    success_message_fragment = b"Sent deploy request successfully!"

    with open(config_file_name, "r") as config_file:
        config = yaml.safe_load(config_file)

    deploy_response = subprocess.check_output(["serve", "deploy", config_file_name])
    assert success_message_fragment in deploy_response

    # Config should be immediately ready
    info_response = subprocess.check_output(["serve", "config"])
    info = yaml.safe_load(info_response)

    assert config == info


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_status(ray_start_stop):
    """Deploys a config file and checks its status."""

    # Check that `serve status` works even if no Serve app is running
    status_response = subprocess.check_output(["serve", "status"])
    status = yaml.safe_load(status_response)

    assert ServeStatusSchema.get_empty_schema_dict() == status

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
        "DAGDriver",
        "Multiplier",
        "Adder",
        "Router",
        "create_order",
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

    status_response = subprocess.check_output(
        ["serve", "status", "-a", "http://localhost:52365/"]
    )
    serve_status = yaml.safe_load(status_response)
    print("serve_status", serve_status)

    def check_for_failed_deployment():
        app_status = ServeSubmissionClient("http://localhost:52365").get_status()
        return (
            len(serve_status["deployment_statuses"]) == 0
            and serve_status["app_status"]["status"] == "DEPLOY_FAILED"
            and remove_ansi_escape_sequences(app_status["app_status"]["message"])
            in serve_status["app_status"]["message"]
        )

    wait_for_condition(check_for_failed_deployment, timeout=2)


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
        assert ServeStatusSchema.get_empty_schema_dict() != status
        print("`serve config` and `serve status` print non-empty responses.\n")

        print("Deleting Serve app.")
        subprocess.check_output(["serve", "shutdown", "-y"])
        wait_for_condition(lambda: num_live_deployments() == 0, timeout=15)
        print("Deletion successful. All deployments have shut down.")

        # `serve config` and `serve status` should print empty schemas
        config_response = subprocess.check_output(["serve", "config"])
        config = yaml.safe_load(config_response)
        assert ServeApplicationSchema.get_empty_schema_dict() == config

        status_response = subprocess.check_output(["serve", "status"])
        status = yaml.safe_load(status_response)
        assert ServeStatusSchema.get_empty_schema_dict() == status
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
    success_message_fragment = b"Sent deploy request successfully!"
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

    assert len(status_info["deployment_statuses"]) == 0

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
