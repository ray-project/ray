import copy
import importlib
import os
import re
import sys
from typing import Dict

import grpc
import pytest
import requests

import ray
import ray._private.ray_constants as ray_constants
from ray import serve
from ray._private.test_utils import generate_system_config_map, wait_for_condition
from ray.serve._private.common import (
    ApplicationStatus,
    DeploymentStatus,
    DeploymentStatusTrigger,
    ProxyStatus,
    ReplicaState,
)
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.schema import HTTPOptionsSchema, ServeInstanceDetails
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


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_serve_namespace(ray_start_stop):
    """Check that the driver can interact with Serve using the Python API."""

    config = {
        "applications": [
            {
                "name": "my_app",
                "import_path": "ray.serve.tests.test_config_files.world.DagNode",
            }
        ],
    }

    print("Deploying config.")
    deploy_config_multi_app(config, SERVE_HEAD_URL)
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/").text == "wonderful world",
        timeout=15,
    )
    print("Deployments are live and reachable over HTTP.\n")

    ray.init(address="auto", namespace="serve")
    my_app_status = serve.status().applications["my_app"]
    assert (
        len(my_app_status.deployments) == 2
        and my_app_status.deployments["f"] is not None
    )
    print("Successfully retrieved deployment statuses with Python API.")
    print("Shutting down Python API.")
    serve.shutdown()
    ray.shutdown()


@pytest.mark.parametrize(
    "option,override",
    [
        ("proxy_location", "HeadOnly"),
        ("http_options", {"host": "127.0.0.2"}),
        ("http_options", {"port": 8000}),
        ("http_options", {"root_path": "/serve_updated"}),
    ],
)
def test_put_with_http_options(ray_start_stop, option, override):
    """Submits a config with HTTP options specified.

    Trying to submit a config to the serve agent with the HTTP options modified should
    NOT fail:
      - If Serve is NOT running, HTTP options will be honored when starting Serve
      - If Serve is running, HTTP options will be ignored, and warning will be logged
      urging users to restart Serve if they want their options to take effect
    """

    pizza_import_path = "ray.serve.tests.test_config_files.pizza.serve_dag"
    world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
    original_http_options_json = {
        "host": "127.0.0.1",
        "port": 8000,
        "root_path": "/serve",
    }
    original_serve_config_json = {
        "proxy_location": "EveryNode",
        "http_options": original_http_options_json,
        "applications": [
            {
                "name": "app1",
                "route_prefix": "/app1",
                "import_path": pizza_import_path,
            },
            {
                "name": "app2",
                "route_prefix": "/app2",
                "import_path": world_import_path,
            },
        ],
    }
    deploy_config_multi_app(original_serve_config_json, SERVE_HEAD_URL)

    # Wait for deployments to be up
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/serve/app1", json=["ADD", 2]).text
        == "4 pizzas please!",
        timeout=15,
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/serve/app2").text
        == "wonderful world",
        timeout=15,
    )

    updated_serve_config_json = copy.deepcopy(original_serve_config_json)
    updated_serve_config_json[option] = override

    put_response = requests.put(
        SERVE_HEAD_URL, json=updated_serve_config_json, timeout=5
    )
    assert put_response.status_code == 200

    # Fetch Serve status and confirm that HTTP options are unchanged
    get_response = requests.get(SERVE_HEAD_URL, timeout=5)
    serve_details = ServeInstanceDetails.parse_obj(get_response.json())

    original_http_options = HTTPOptionsSchema.parse_obj(original_http_options_json)

    assert original_http_options == serve_details.http_options.dict(exclude_unset=True)

    # Deployments should still be up
    assert (
        requests.post("http://localhost:8000/serve/app1", json=["ADD", 2]).text
        == "4 pizzas please!"
    )
    assert requests.post("http://localhost:8000/serve/app2").text == "wonderful world"


def test_put_with_grpc_options(ray_start_stop):
    """Submits a config with gRPC options specified.

    Ensure gRPC options can be accepted by the api. HTTP deployment continue to
    accept requests. gRPC deployment is also able to accept requests.
    """

    grpc_servicer_functions = [
        "ray.serve.generated.serve_pb2_grpc.add_UserDefinedServiceServicer_to_server",
    ]
    test_files_import_path = "ray.serve.tests.test_config_files."
    grpc_import_path = f"{test_files_import_path}grpc_deployment:g"
    world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
    original_config = {
        "proxy_location": "EveryNode",
        "http_options": {
            "host": "127.0.0.1",
            "port": 8000,
            "root_path": "/serve",
        },
        "grpc_options": {
            "port": 9000,
            "grpc_servicer_functions": grpc_servicer_functions,
        },
        "applications": [
            {
                "name": "app1",
                "route_prefix": "/app1",
                "import_path": grpc_import_path,
            },
            {
                "name": "app2",
                "route_prefix": "/app2",
                "import_path": world_import_path,
            },
        ],
    }
    # Ensure api can accept config with gRPC options
    deploy_config_multi_app(original_config, SERVE_HEAD_URL)

    # Ensure HTTP requests are still working
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/serve/app2").text
        == "wonderful world",
        timeout=15,
    )

    # Ensure gRPC requests also work
    channel = grpc.insecure_channel("localhost:9000")
    stub = serve_pb2_grpc.UserDefinedServiceStub(channel)
    test_in = serve_pb2.UserDefinedMessage(name="foo", num=30)
    metadata = (("application", "app1"),)
    response = stub.Method1(request=test_in, metadata=metadata)
    assert response.greeting == "Hello foo from method1"


def test_default_dashboard_agent_listen_port():
    """
    Defaults in the code and the documentation assume
    the dashboard agent listens to HTTP on port 52365.
    """
    assert ray_constants.DEFAULT_DASHBOARD_AGENT_LISTEN_PORT == 52365


@pytest.fixture
def short_serve_kv_timeout(monkeypatch):
    monkeypatch.setenv("RAY_SERVE_KV_TIMEOUT_S", "3")
    importlib.reload(ray.serve._private.constants)  # to reload the constants set above
    yield


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        {
            **generate_system_config_map(
                gcs_rpc_server_reconnect_timeout_s=3600,
                gcs_server_request_timeout_seconds=3,
            ),
        }
    ],
    indirect=True,
)
@pytest.mark.parametrize("url", [SERVE_AGENT_URL, SERVE_HEAD_URL])
def test_get_applications_while_gcs_down(
    short_serve_kv_timeout, ray_start_regular_with_external_redis, url
):
    # Test serve REST API availability when the GCS is down.
    serve.start(detached=True)

    get_response = requests.get(url, timeout=15)
    assert get_response.status_code == 200
    ray._private.worker._global_node.kill_gcs_server()

    for _ in range(10):
        assert requests.get(url, timeout=30).status_code == 200

    ray._private.worker._global_node.start_gcs_server()

    for _ in range(10):
        assert requests.get(url, timeout=30).status_code == 200

    serve.shutdown()


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
@pytest.mark.parametrize("url", [SERVE_AGENT_URL, SERVE_HEAD_URL])
def test_target_capacity_field(ray_start_stop, url: str):
    """Test that the `target_capacity` field is always populated as expected."""

    raw_json = requests.get(url).json()

    # `target_capacity` should be present in the response before deploying anything.
    assert raw_json["target_capacity"] is None

    config = {
        "http_options": {
            "host": "127.0.0.1",
            "port": 8000,
        },
        "applications": [],
    }
    deploy_config_multi_app(config, url)

    # `target_capacity` should be present in the response even if not set.
    raw_json = requests.get(url).json()
    assert raw_json["target_capacity"] is None
    details = ServeInstanceDetails(**raw_json)
    assert details.target_capacity is None
    assert details.http_options.host == "127.0.0.1"
    assert details.http_options.port == 8000
    assert details.applications == {}

    # Set `target_capacity`, ensure it is returned properly.
    config["target_capacity"] = 20
    deploy_config_multi_app(config, url)
    raw_json = requests.get(url).json()
    assert raw_json["target_capacity"] == 20
    details = ServeInstanceDetails(**raw_json)
    assert details.target_capacity == 20
    assert details.http_options.host == "127.0.0.1"
    assert details.http_options.port == 8000
    assert details.applications == {}

    # Update `target_capacity`, ensure it is returned properly.
    config["target_capacity"] = 40
    deploy_config_multi_app(config, url)
    raw_json = requests.get(url).json()
    assert raw_json["target_capacity"] == 40
    details = ServeInstanceDetails(**raw_json)
    assert details.target_capacity == 40
    assert details.http_options.host == "127.0.0.1"
    assert details.http_options.port == 8000
    assert details.applications == {}

    # Reset `target_capacity` by omitting it, ensure it is returned properly.
    del config["target_capacity"]
    deploy_config_multi_app(config, url)
    raw_json = requests.get(url).json()
    assert raw_json["target_capacity"] is None
    details = ServeInstanceDetails(**raw_json)
    assert details.target_capacity is None
    assert details.http_options.host == "127.0.0.1"
    assert details.http_options.port == 8000
    assert details.applications == {}

    # Try to set an invalid `target_capacity`, ensure a `400` status is returned.
    config["target_capacity"] = 101
    assert requests.put(url, json=config, timeout=30).status_code == 400


def check_log_file(log_file: str, expected_regex: list):
    with open(log_file, "r") as f:
        s = f.read()
        for regex in expected_regex:
            assert re.findall(regex, s) != []
    return True


def test_put_with_logging_config(ray_start_stop):
    """Test serve component logging config can be updated via REST API."""

    url = "http://localhost:8265/api/serve/applications/"
    import_path = "ray.serve.tests.test_config_files.logging_config_test.model"
    config1 = {
        "http_options": {
            "host": "127.0.0.1",
            "port": 8000,
        },
        "logging_config": {
            "encoding": "JSON",
        },
        "applications": [
            {
                "name": "app",
                "route_prefix": "/app",
                "import_path": import_path,
            },
        ],
    }

    deploy_config_multi_app(config1, url)
    wait_for_condition(
        lambda: requests.get("http://localhost:8000/app").status_code == 200,
        timeout=15,
    )
    print("Deployments are live and reachable over HTTP.\n")

    # Make sure deployment & controller both log in json format.
    resp = requests.post("http://localhost:8000/app").json()
    replica_id = resp["replica"].split("#")[-1]
    expected_log_regex = [f'"replica": "{replica_id}", ']
    check_log_file(resp["log_file"], expected_log_regex)
    expected_log_regex = ['.*"component_name": "controller".*']
    check_log_file(resp["controller_log_file"], expected_log_regex)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
