import copy
import sys
from typing import Dict

import pytest
import requests

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
import ray._private.ray_constants as ray_constants
from ray.serve.tests.conftest import *  # noqa: F401 F403

GET_OR_PUT_URL = "http://localhost:52365/api/serve/deployments/"
STATUS_URL = "http://localhost:52365/api/serve/deployments/status"


def deploy_and_check_config(config: Dict):
    put_response = requests.put(GET_OR_PUT_URL, json=config, timeout=30)
    assert put_response.status_code == 200
    print("PUT request sent successfully.")

    # Config should be immediately retrievable
    get_response = requests.get(GET_OR_PUT_URL, timeout=15)
    assert get_response.status_code == 200
    assert get_response.json() == config
    print("GET request returned correct config.")


@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on OSX.")
def test_put_get(ray_start_stop):
    config1 = {
        "import_path": (
            "ray.serve.tests.test_config_files.test_dag.conditional_dag.serve_dag"
        ),
        "runtime_env": {},
        "deployments": [
            {
                "name": "Multiplier",
                "user_config": {"factor": 1},
            },
            {
                "name": "Adder",
                "ray_actor_options": {
                    "runtime_env": {"env_vars": {"override_increment": "1"}}
                },
            },
        ],
    }

    # Use empty dictionary for Adder's ray_actor_options.
    config2 = copy.deepcopy(config1)
    config2["deployments"][1]["ray_actor_options"] = {}

    config3 = {
        "import_path": "ray.serve.tests.test_config_files.world.DagNode",
    }

    # Ensure the REST API is idempotent
    num_iterations = 2
    for iteration in range(1, num_iterations + 1):
        print(f"*** Starting Iteration {iteration}/{num_iterations} ***\n")

        print("Sending PUT request for config1.")
        deploy_and_check_config(config1)
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
        print("Deployments are live and reachable over HTTP.\n")

        print("Sending PUT request for config2.")
        deploy_and_check_config(config2)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "4 pizzas please!",
            timeout=15,
        )
        print("Adder deployment updated correctly.\n")

        print("Sending PUT request for config3.")
        deploy_and_check_config(config3)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/").text == "wonderful world",
            timeout=15,
        )
        print("Deployments are live and reachable over HTTP.\n")


@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on OSX.")
def test_put_bad_schema(ray_start_stop):
    config = {"not_a_real_field": "value"}

    put_response = requests.put(GET_OR_PUT_URL, json=config, timeout=5)
    assert put_response.status_code == 400


@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on OSX.")
def test_delete(ray_start_stop):
    config = {
        "import_path": "dir.subdir.a.add_and_sub.serve_dag",
        "runtime_env": {
            "working_dir": (
                "https://github.com/ray-project/test_dag/archive/"
                "40d61c141b9c37853a7014b8659fc7f23c1d04f6.zip"
            )
        },
        "deployments": [
            {
                "name": "Subtract",
                "ray_actor_options": {
                    "runtime_env": {
                        "py_modules": [
                            (
                                "https://github.com/ray-project/test_module/archive/"
                                "aa6f366f7daa78c98408c27d917a983caa9f888b.zip"
                            )
                        ]
                    }
                },
            }
        ],
    }

    # Ensure the REST API is idempotent
    num_iterations = 2
    for iteration in range(1, num_iterations + 1):
        print(f"*** Starting Iteration {iteration}/{num_iterations} ***\n")

        print("Sending PUT request for config.")
        deploy_and_check_config(config)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 1]).json()
            == 2,
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["SUB", 1]).json()
            == -1,
            timeout=15,
        )
        print("Deployments are live and reachable over HTTP.\n")

        print("Sending DELETE request for config.")
        delete_response = requests.delete(GET_OR_PUT_URL, timeout=15)
        assert delete_response.status_code == 200
        print("DELETE request sent successfully.")

        # Make sure all deployments are deleted
        wait_for_condition(
            lambda: len(requests.get(GET_OR_PUT_URL, timeout=15).json()["deployments"])
            == 0,
            timeout=15,
        )
        print("GET request returned empty config successfully.")

        with pytest.raises(requests.exceptions.ConnectionError):
            requests.post("http://localhost:8000/", json=["ADD", 1]).raise_for_status()
        with pytest.raises(requests.exceptions.ConnectionError):
            requests.post("http://localhost:8000/", json=["SUB", 1]).raise_for_status()
        print("Deployments have been deleted and are not reachable.\n")


@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on OSX.")
def test_get_status(ray_start_stop):
    print("Checking status info before any deployments.")

    status_response = requests.get(STATUS_URL, timeout=30)
    assert status_response.status_code == 200
    print("Retrieved status info successfully.")

    serve_status = status_response.json()
    assert len(serve_status["deployment_statuses"]) == 0
    assert serve_status["app_status"]["status"] == "NOT_STARTED"
    assert serve_status["app_status"]["deployment_timestamp"] == 0
    assert serve_status["app_status"]["message"] == ""
    print("Status info on fresh Serve application is correct.\n")

    config = {
        "import_path": "ray.serve.tests.test_config_files.world.DagNode",
    }

    print("Deploying config.")
    deploy_and_check_config(config)
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/").text == "wonderful world",
        timeout=15,
    )
    print("Deployments are live and reachable over HTTP.\n")

    status_response = requests.get(STATUS_URL, timeout=30)
    assert status_response.status_code == 200
    print("Retrieved status info successfully.")

    serve_status = status_response.json()

    deployment_statuses = serve_status["deployment_statuses"]
    assert len(deployment_statuses) == 2
    expected_deployment_names = {"f", "BasicDriver"}
    for deployment_status in deployment_statuses:
        assert deployment_status["name"] in expected_deployment_names
        expected_deployment_names.remove(deployment_status["name"])
        assert deployment_status["status"] in {"UPDATING", "HEALTHY"}
        assert deployment_status["message"] == ""
    assert len(expected_deployment_names) == 0
    print("Deployments' statuses are correct.")

    assert serve_status["app_status"]["status"] in {"DEPLOYING", "RUNNING"}
    assert serve_status["app_status"]["deployment_timestamp"] > 0
    assert serve_status["app_status"]["message"] == ""
    print("Serve app status is correct.")


@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on OSX.")
def test_serve_namespace(ray_start_stop):
    """
    Check that the Dashboard's Serve can interact with the Python API
    when they both start in the "serve" namespace.
    """

    config = {
        "import_path": "ray.serve.tests.test_config_files.world.DagNode",
    }

    print("Deploying config.")
    deploy_and_check_config(config)
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/").text == "wonderful world",
        timeout=15,
    )
    print("Deployments are live and reachable over HTTP.\n")

    ray.init(address="auto", namespace="serve")
    client = serve.start()
    print("Connected to Serve with Python API.")
    serve_status = client.get_serve_status()
    assert (
        len(serve_status.deployment_statuses) == 2
        and serve_status.get_deployment_status("f") is not None
    )
    print("Successfully retrieved deployment statuses with Python API.")
    print("Shutting down Python API.")
    serve.shutdown()


def test_default_dashboard_agent_listen_port():
    """
    Defaults in the code and the documentation assume
    the dashboard agent listens to HTTP on port 52365.
    """
    assert ray_constants.DEFAULT_DASHBOARD_AGENT_LISTEN_PORT == 52365


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
