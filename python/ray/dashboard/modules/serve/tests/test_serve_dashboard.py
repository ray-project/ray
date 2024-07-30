import copy
import os
import sys
from typing import Dict

import pytest
import requests

from ray._private.test_utils import wait_for_condition
from ray.serve._private.common import (
    ApplicationStatus,
    DeploymentStatus,
    DeploymentStatusTrigger,
    ProxyStatus,
    ReplicaState,
)
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve.schema import ServeInstanceDetails
from ray.serve.tests.conftest import *  # noqa: F401 F403
from ray.tests.conftest import *  # noqa: F401 F403
from ray.util.state import list_actors

# For local testing on a Macbook, set `export TEST_ON_DARWIN=1`.
TEST_ON_DARWIN = os.environ.get("TEST_ON_DARWIN", "0") == "1"


SERVE_AGENT_URL = "http://localhost:52365/api/serve/applications/"
SERVE_HEAD_URL = "http://localhost:8265/api/serve/applications/"


def deploy_config_multi_app(config: Dict, url: str):
    put_response = requests.put(url, json=config, timeout=30)
    assert put_response.status_code == 200
    print("PUT request sent successfully.")


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
@pytest.mark.parametrize("url", [SERVE_AGENT_URL, SERVE_HEAD_URL])
def test_put_get_multi_app(ray_start_stop, url):
    pizza_import_path = (
        "ray.serve.tests.test_config_files.test_dag.conditional_dag.serve_dag"
    )
    world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
    config1 = {
        "http_options": {
            "host": "127.0.0.1",
            "port": 8000,
        },
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
    num_iterations = 2
    for iteration in range(num_iterations):
        print(f"*** Starting Iteration {iteration + 1}/{num_iterations} ***\n")

        # APPLY CONFIG 1
        print("Sending PUT request for config1.")
        deploy_config_multi_app(config1, url)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).text
            == "5 pizzas please!",
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["MUL", 2]).text
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
        deploy_config_multi_app(config2, url)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).text
            == "4 pizzas please!",
            timeout=15,
        )
        print("Adder deployment updated correctly.\n")

        # APPLY CONFIG 3: App #1 should be overwritten to world:DagNode
        print("Sending PUT request for config3.")
        deploy_config_multi_app(config3, url)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1").text
            == "wonderful world",
            timeout=15,
        )
        print("Deployments are live and reachable over HTTP.\n")


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
@pytest.mark.parametrize(
    "put_url",
    [
        SERVE_AGENT_URL,
        SERVE_HEAD_URL,
    ],
)
def test_put_bad_schema(ray_start_stop, put_url: str):
    config = {"not_a_real_field": "value"}

    put_response = requests.put(put_url, json=config, timeout=5)
    assert put_response.status_code == 400


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
@pytest.mark.parametrize("url", [SERVE_AGENT_URL, SERVE_HEAD_URL])
def test_put_duplicate_apps(ray_start_stop, url):
    """If a config with duplicate app names is deployed, the PUT request should fail.
    The response should clearly indicate a validation error.
    """

    config = {
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
    put_response = requests.put(url, json=config, timeout=5)
    assert put_response.status_code == 400 and "ValidationError" in put_response.text


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
@pytest.mark.parametrize("url", [SERVE_AGENT_URL, SERVE_HEAD_URL])
def test_put_duplicate_routes(ray_start_stop, url):
    """If a config with duplicate routes is deployed, the PUT request should fail.
    The response should clearly indicate a validation error.
    """

    config = {
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
    put_response = requests.put(url, json=config, timeout=5)
    assert put_response.status_code == 400 and "ValidationError" in put_response.text


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
@pytest.mark.parametrize("url", [SERVE_AGENT_URL, SERVE_HEAD_URL])
def test_delete_multi_app(ray_start_stop, url):
    py_module = (
        "https://github.com/ray-project/test_module/archive/"
        "aa6f366f7daa78c98408c27d917a983caa9f888b.zip"
    )
    config = {
        "applications": [
            {
                "name": "app1",
                "route_prefix": "/app1",
                "import_path": "dir.subdir.a.add_and_sub.serve_dag",
                "runtime_env": {
                    "working_dir": (
                        "https://github.com/ray-project/test_dag/archive/"
                        "78b4a5da38796123d9f9ffff59bab2792a043e95.zip"
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
        deploy_config_multi_app(config, url)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 1]).text
            == "2",
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["SUB", 1]).text
            == "-1",
            timeout=15,
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2").text
            == "wonderful world",
            timeout=15,
        )
        print("Deployments are live and reachable over HTTP.\n")

        print("Sending DELETE request for config.")
        delete_response = requests.delete(url, timeout=15)
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


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
@pytest.mark.parametrize("url", [SERVE_AGENT_URL, SERVE_HEAD_URL])
def test_get_serve_instance_details_not_started(ray_start_stop, url):
    """Test REST API when Serve hasn't started yet."""
    # Parse the response to ensure it's formatted correctly.
    ServeInstanceDetails(**requests.get(url).json())


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
@pytest.mark.parametrize(
    "f_deployment_options",
    [
        {"name": "f", "ray_actor_options": {"num_cpus": 0.2}},
        {
            "name": "f",
            "autoscaling_config": {
                "min_replicas": 1,
                "initial_replicas": 3,
                "max_replicas": 10,
            },
        },
    ],
)
@pytest.mark.parametrize("url", [SERVE_AGENT_URL, SERVE_HEAD_URL])
def test_get_serve_instance_details(ray_start_stop, f_deployment_options, url):
    grpc_port = 9001
    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
        "ray.serve.generated.serve_pb2_grpc.add_FruitServiceServicer_to_server",
    ]
    world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
    fastapi_import_path = "ray.serve.tests.test_config_files.fastapi_deployment.node"
    config = {
        "proxy_location": "HeadOnly",
        "http_options": {
            "host": "127.0.0.1",
            "port": 8005,
        },
        "grpc_options": {
            "port": grpc_port,
            "grpc_servicer_functions": grpc_servicer_functions,
        },
        "applications": [
            {
                "name": "app1",
                "route_prefix": "/apple",
                "import_path": world_import_path,
                "deployments": [f_deployment_options],
            },
            {
                "name": "app2",
                "route_prefix": "/banana",
                "import_path": fastapi_import_path,
            },
        ],
    }
    expected_values = {
        "app1": {
            "route_prefix": "/apple",
            "docs_path": None,
            "deployments": {"f", "BasicDriver"},
        },
        "app2": {
            "route_prefix": "/banana",
            "docs_path": "/my_docs",
            "deployments": {"FastAPIDeployment"},
        },
    }

    deploy_config_multi_app(config, url)

    def applications_running():
        response = requests.get(url, timeout=15)
        assert response.status_code == 200

        serve_details = ServeInstanceDetails(**response.json())
        return (
            serve_details.applications["app1"].status == ApplicationStatus.RUNNING
            and serve_details.applications["app2"].status == ApplicationStatus.RUNNING
        )

    wait_for_condition(applications_running, timeout=15)
    print("All applications are in a RUNNING state.")

    serve_details = ServeInstanceDetails(**requests.get(url).json())
    # CHECK: proxy location, HTTP host, and HTTP port
    assert serve_details.proxy_location == "HeadOnly"
    assert serve_details.http_options.host == "127.0.0.1"
    assert serve_details.http_options.port == 8005

    # CHECK: gRPC port and grpc_servicer_functions
    assert serve_details.grpc_options.port == grpc_port
    assert serve_details.grpc_options.grpc_servicer_functions == grpc_servicer_functions
    print(
        "Confirmed fetched proxy location, HTTP host, HTTP port, gRPC port, and grpc_"
        "servicer_functions metadata correct."
    )

    # Check HTTP Proxy statuses
    for proxy in serve_details.proxies.values():
        assert proxy.status == ProxyStatus.HEALTHY
        assert os.path.exists("/tmp/ray/session_latest/logs" + proxy.log_file_path)
    print("Checked HTTP Proxy details.")
    # Check controller info
    assert serve_details.controller_info.actor_id
    assert serve_details.controller_info.actor_name
    assert serve_details.controller_info.node_id
    assert serve_details.controller_info.node_ip
    assert os.path.exists(
        "/tmp/ray/session_latest/logs" + serve_details.controller_info.log_file_path
    )

    app_details = serve_details.applications
    # CHECK: application details
    for i, app in enumerate(["app1", "app2"]):
        assert (
            app_details[app].deployed_app_config.dict(exclude_unset=True)
            == config["applications"][i]
        )
        assert app_details[app].last_deployed_time_s > 0
        assert app_details[app].route_prefix == expected_values[app]["route_prefix"]
        assert app_details[app].docs_path == expected_values[app]["docs_path"]

        # CHECK: all deployments are present
        assert (
            app_details[app].deployments.keys() == expected_values[app]["deployments"]
        )

        for deployment in app_details[app].deployments.values():
            assert deployment.status == DeploymentStatus.HEALTHY
            assert (
                deployment.status_trigger
                == DeploymentStatusTrigger.CONFIG_UPDATE_COMPLETED
            )
            # Route prefix should be app level options eventually
            assert "route_prefix" not in deployment.deployment_config.dict(
                exclude_unset=True
            )
            if isinstance(deployment.deployment_config.num_replicas, int):
                assert (
                    len(deployment.replicas)
                    == deployment.deployment_config.num_replicas
                )
                assert len(deployment.replicas) == deployment.target_num_replicas

            for replica in deployment.replicas:
                assert replica.replica_id
                assert replica.state == ReplicaState.RUNNING
                assert deployment.name in replica.actor_name
                assert replica.actor_id and replica.node_id and replica.node_ip
                assert replica.start_time_s > app_details[app].last_deployed_time_s
                file_path = "/tmp/ray/session_latest/logs" + replica.log_file_path
                assert os.path.exists(file_path)

    print("Finished checking application details.")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
