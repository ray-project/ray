import copy
import sys
from typing import Dict

import pytest
import requests

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
import ray._private.ray_constants as ray_constants
from ray.experimental.state.api import list_actors
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve.tests.conftest import *  # noqa: F401 F403
from ray.serve.schema import ServeInstanceDetails
from ray.serve._private.common import ApplicationStatus, DeploymentStatus

GET_OR_PUT_URL = "http://localhost:52365/api/serve/deployments/"
STATUS_URL = "http://localhost:52365/api/serve/deployments/status"

GET_OR_PUT_URL_V2 = "http://localhost:52365/api/serve/applications/"


def deploy_and_check_config(config: Dict):
    put_response = requests.put(GET_OR_PUT_URL, json=config, timeout=30)
    assert put_response.status_code == 200
    print("PUT request sent successfully.")

    # Config should be immediately retrievable
    get_response = requests.get(GET_OR_PUT_URL, timeout=15)
    assert get_response.status_code == 200
    assert get_response.json() == config
    print("GET request returned correct config.")


def deploy_config_multi_app(config: Dict):
    put_response = requests.put(GET_OR_PUT_URL_V2, json=config, timeout=30)
    assert put_response.status_code == 200
    print("PUT request sent successfully.")


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
def test_put_get_multi_app(ray_start_stop):
    pizza_import_path = (
        "ray.serve.tests.test_config_files.test_dag.conditional_dag.serve_dag"
    )
    world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
    config1 = {
        "host": "127.0.0.1",
        "port": 8000,
        "applications": [
            {
                "name": "app1",
                "route_prefix": "/app1",
                "import_path": pizza_import_path,
                "deployments": [
                    {
                        "name": "Adder",
                        "ray_actor_options": {
                            "runtime_env": {"env_vars": {"override_increment": "3"}}
                        },
                    },
                    {
                        "name": "Multiplier",
                        "ray_actor_options": {
                            "runtime_env": {"env_vars": {"override_factor": "4"}}
                        },
                    },
                ],
            },
            {
                "name": "app2",
                "route_prefix": "/app2",
                "import_path": world_import_path,
            },
        ],
    }

    # Use empty dictionary for app1 Adder's ray_actor_options.
    config2 = copy.deepcopy(config1)
    config2["applications"][0]["deployments"][0]["ray_actor_options"] = {}

    config3 = copy.deepcopy(config1)
    config3["applications"][0] = {
        "name": "app1",
        "route_prefix": "/app1",
        "import_path": world_import_path,
    }

    # Ensure the REST API is idempotent
    num_iterations = 3
    for iteration in range(num_iterations):
        print(f"*** Starting Iteration {iteration + 1}/{num_iterations} ***\n")

        # APPLY CONFIG 1
        print("Sending PUT request for config1.")
        deploy_config_multi_app(config1)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "5 pizzas please!",
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["MUL", 2]).json()
            == "8 pizzas please!",
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2").text
            == "wonderful world",
            timeout=15,
        )
        print("Deployments are live and reachable over HTTP.\n")

        # APPLY CONFIG 2: App #1 Adder should add 2 to input.
        print("Sending PUT request for config2.")
        deploy_config_multi_app(config2)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "4 pizzas please!",
            timeout=15,
        )
        print("Adder deployment updated correctly.\n")

        # APPLY CONFIG 3: App #1 should be overwritten to world:DagNode
        print("Sending PUT request for config3.")
        deploy_config_multi_app(config3)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1").text
            == "wonderful world",
            timeout=15,
        )
        print("Deployments are live and reachable over HTTP.\n")


@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on OSX.")
@pytest.mark.parametrize("put_url", (GET_OR_PUT_URL, GET_OR_PUT_URL_V2))
def test_put_bad_schema(ray_start_stop, put_url: str):
    config = {"not_a_real_field": "value"}

    put_response = requests.put(put_url, json=config, timeout=5)
    assert put_response.status_code == 400


@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on OSX.")
def test_put_duplicate_apps(ray_start_stop):
    """If a config with duplicate app names is deployed, the PUT request should fail.
    The response should clearly indicate a validation error.
    """

    config = {
        "host": "127.0.0.1",
        "port": 8000,
        "applications": [
            {
                "name": "app1",
                "route_prefix": "/a",
                "import_path": "module.graph",
            },
            {
                "name": "app1",
                "route_prefix": "/b",
                "import_path": "module.graph",
            },
        ],
    }
    put_response = requests.put(GET_OR_PUT_URL_V2, json=config, timeout=5)
    assert put_response.status_code == 400 and "ValidationError" in put_response.text


@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on OSX.")
def test_put_duplicate_routes(ray_start_stop):
    """If a config with duplicate routes is deployed, the PUT request should fail.
    The response should clearly indicate a validation error.
    """

    config = {
        "host": "127.0.0.1",
        "port": 8000,
        "applications": [
            {
                "name": "app1",
                "route_prefix": "/alice",
                "import_path": "module.graph",
            },
            {
                "name": "app2",
                "route_prefix": "/alice",
                "import_path": "module.graph",
            },
        ],
    }
    put_response = requests.put(GET_OR_PUT_URL_V2, json=config, timeout=5)
    assert put_response.status_code == 400 and "ValidationError" in put_response.text


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
def test_delete_multi_app(ray_start_stop):
    py_module = (
        "https://github.com/ray-project/test_module/archive/"
        "aa6f366f7daa78c98408c27d917a983caa9f888b.zip"
    )
    config = {
        "host": "127.0.0.1",
        "port": 8000,
        "applications": [
            {
                "name": "app1",
                "route_prefix": "/app1",
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
                            "runtime_env": {"py_modules": [py_module]}
                        },
                    }
                ],
            },
            {
                "name": "app2",
                "route_prefix": "/app2",
                "import_path": "ray.serve.tests.test_config_files.world.DagNode",
            },
        ],
    }

    # Ensure the REST API is idempotent
    num_iterations = 2
    for iteration in range(1, num_iterations + 1):
        print(f"*** Starting Iteration {iteration}/{num_iterations} ***\n")

        print("Sending PUT request for config.")
        deploy_config_multi_app(config)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 1]).json()
            == 2,
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["SUB", 1]).json()
            == -1,
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2").text
            == "wonderful world",
            timeout=15,
        )
        print("Deployments are live and reachable over HTTP.\n")

        print("Sending DELETE request for config.")
        delete_response = requests.delete(GET_OR_PUT_URL_V2, timeout=15)
        assert delete_response.status_code == 200
        print("DELETE request sent successfully.")

        wait_for_condition(
            lambda: len(
                list_actors(
                    filters=[
                        ("ray_namespace", "=", SERVE_NAMESPACE),
                        ("state", "=", "ALIVE"),
                    ]
                )
            )
            == 0
        )

        with pytest.raises(requests.exceptions.ConnectionError):
            requests.post(
                "http://localhost:8000/app1", json=["ADD", 1]
            ).raise_for_status()
        with pytest.raises(requests.exceptions.ConnectionError):
            requests.post("http://localhost:8000/app2").raise_for_status()
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
def test_get_serve_instance_details(ray_start_stop):
    world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
    fastapi_import_path = "ray.serve.tests.test_config_files.fastapi_deployment.node"
    config1 = {
        "host": "127.0.0.1",
        "port": 8000,
        "applications": [
            {
                "name": "app1",
                "route_prefix": "/app1",
                "import_path": world_import_path,
                "deployments": [
                    {
                        "name": "f",
                        "ray_actor_options": {"num_cpus": 0.2},
                    },
                ],
            },
            {
                "name": "app2",
                "route_prefix": "/app2",
                "import_path": fastapi_import_path,
            },
        ],
    }

    deploy_config_multi_app(config1)

    def applications_running():
        response = requests.get(GET_OR_PUT_URL_V2, timeout=15)
        assert response.status_code == 200

        serve_details = ServeInstanceDetails(**response.json())
        return (
            serve_details.applications["app1"].status == ApplicationStatus.RUNNING
            and serve_details.applications["app2"].status == ApplicationStatus.RUNNING
        )

    wait_for_condition(applications_running, timeout=15)
    print("All applications are in a RUNNING state.")

    serve_details = ServeInstanceDetails(**requests.get(GET_OR_PUT_URL_V2).json())
    # CHECK: host and port
    assert serve_details.host == "127.0.0.1"
    assert serve_details.port == 8000
    print('Confirmed fetched host and port metadata are "127.0.0.1" and "8000".')

    app_details = serve_details.applications

    # CHECK: app configs are equal
    assert (
        app_details["app1"].deployed_app_config.dict(exclude_unset=True)
        == config1["applications"][0]
    )
    assert (
        app_details["app2"].deployed_app_config.dict(exclude_unset=True)
        == config1["applications"][1]
    )
    print("Confirmed the deployed app configs from the fetched metadata is correct.")

    # CHECK: deployment timestamp
    assert app_details["app1"].last_deployed_time_s > 0
    assert app_details["app2"].last_deployed_time_s > 0
    print("Confirmed deployment timestamps are nonzero.")

    # CHECK: docs path
    assert app_details["app1"].docs_path is None
    assert app_details["app2"].docs_path == "/my_docs"
    print("Confirmed docs paths are correct.")

    # CHECK: all deployments are present
    assert app_details["app1"].deployments.keys() == {
        "app1_f",
        "app1_BasicDriver",
    }
    assert app_details["app2"].deployments.keys() == {
        "app2_FastAPIDeployment",
    }
    print("Metadata for all deployed deployments are present.")

    # CHECK: application details
    for app in ["app1", "app2"]:
        assert app_details[app].route_prefix == f"/{app}"
        for dep_details in app_details[app].deployments.values():
            assert dep_details.status == DeploymentStatus.HEALTHY

            # Route prefix should be app level options eventually
            assert "route_prefix" not in dep_details.deployment_config.dict(
                exclude_unset=True
            )
    print("Finished checking application details.")


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
