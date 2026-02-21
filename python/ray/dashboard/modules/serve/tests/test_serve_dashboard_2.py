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
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import generate_system_config_map
from ray.serve.generated import serve_pb2, serve_pb2_grpc
from ray.serve.schema import HTTPOptionsSchema, ServeInstanceDetails
from ray.serve.tests.conftest import *  # noqa: F401 F403
from ray.tests.conftest import *  # noqa: F401 F403

# For local testing on a Macbook, set `export TEST_ON_DARWIN=1`.
TEST_ON_DARWIN = os.environ.get("TEST_ON_DARWIN", "0") == "1"


SERVE_HEAD_URL = "http://localhost:8265/api/serve/applications/"


def deploy_config_multi_app(config: Dict, url: str):
    put_response = requests.put(url, json=config, timeout=30)
    assert put_response.status_code == 200
    print("PUT request sent successfully.")


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

    my_app_status = serve.status().applications["my_app"]
    assert (
        len(my_app_status.deployments) == 2
        and my_app_status.deployments["f"] is not None
    )
    print("Successfully retrieved deployment statuses with Python API.")
    print("Shutting down Python API.")


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
def test_get_applications_while_gcs_down(
    short_serve_kv_timeout, ray_start_regular_with_external_redis
):
    # Test serve REST API availability when the GCS is down.
    serve.start(detached=True)

    get_response = requests.get(SERVE_HEAD_URL, timeout=15)
    assert get_response.status_code == 200
    ray._private.worker._global_node.kill_gcs_server()

    for _ in range(10):
        assert requests.get(SERVE_HEAD_URL, timeout=30).status_code == 200

    ray._private.worker._global_node.start_gcs_server()

    for _ in range(10):
        assert requests.get(SERVE_HEAD_URL, timeout=30).status_code == 200

    serve.shutdown()


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_target_capacity_field(ray_start_stop):
    """Test that the `target_capacity` field is always populated as expected."""

    raw_json = requests.get(SERVE_HEAD_URL).json()

    # `target_capacity` should be present in the response before deploying anything.
    assert raw_json["target_capacity"] is None

    config = {
        "http_options": {
            "host": "127.0.0.1",
            "port": 8000,
        },
        "applications": [],
    }
    deploy_config_multi_app(config, SERVE_HEAD_URL)

    # `target_capacity` should be present in the response even if not set.
    raw_json = requests.get(SERVE_HEAD_URL).json()
    assert raw_json["target_capacity"] is None
    details = ServeInstanceDetails(**raw_json)
    assert details.target_capacity is None
    assert details.http_options.host == "127.0.0.1"
    assert details.http_options.port == 8000
    assert details.applications == {}

    # Set `target_capacity`, ensure it is returned properly.
    config["target_capacity"] = 20
    deploy_config_multi_app(config, SERVE_HEAD_URL)
    raw_json = requests.get(SERVE_HEAD_URL).json()
    assert raw_json["target_capacity"] == 20
    details = ServeInstanceDetails(**raw_json)
    assert details.target_capacity == 20
    assert details.http_options.host == "127.0.0.1"
    assert details.http_options.port == 8000
    assert details.applications == {}

    # Update `target_capacity`, ensure it is returned properly.
    config["target_capacity"] = 40
    deploy_config_multi_app(config, SERVE_HEAD_URL)
    raw_json = requests.get(SERVE_HEAD_URL).json()
    assert raw_json["target_capacity"] == 40
    details = ServeInstanceDetails(**raw_json)
    assert details.target_capacity == 40
    assert details.http_options.host == "127.0.0.1"
    assert details.http_options.port == 8000
    assert details.applications == {}

    # Reset `target_capacity` by omitting it, ensure it is returned properly.
    del config["target_capacity"]
    deploy_config_multi_app(config, SERVE_HEAD_URL)
    raw_json = requests.get(SERVE_HEAD_URL).json()
    assert raw_json["target_capacity"] is None
    details = ServeInstanceDetails(**raw_json)
    assert details.target_capacity is None
    assert details.http_options.host == "127.0.0.1"
    assert details.http_options.port == 8000
    assert details.applications == {}

    # Try to set an invalid `target_capacity`, ensure a `400` status is returned.
    config["target_capacity"] = 101
    assert requests.put(SERVE_HEAD_URL, json=config, timeout=30).status_code == 400


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
