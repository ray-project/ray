import copy
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict

import pytest
import requests

import ray
from ray import serve
from ray._common.test_utils import Semaphore, SignalActor, wait_for_condition
from ray.serve._private.common import (
    DeploymentStatus,
    DeploymentStatusTrigger,
    ReplicaState,
)
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.test_utils import get_num_alive_replicas
from ray.serve.schema import ApplicationStatus, ProxyStatus, ServeInstanceDetails
from ray.serve.tests.conftest import *  # noqa: F401 F403
from ray.tests.conftest import *  # noqa: F401 F403
from ray.util.state import list_actors

# For local testing on a Macbook, set `export TEST_ON_DARWIN=1`.
TEST_ON_DARWIN = os.environ.get("TEST_ON_DARWIN", "0") == "1"


SERVE_HEAD_URL = "http://localhost:8265/api/serve/applications/"
SERVE_HEAD_DEPLOYMENT_SCALE_URL = "http://localhost:8265/api/v1/applications/{app_name}/deployments/{deployment_name}/scale"
CONFIG_FILE_TEXT = """
applications:
  - name: test_app
    route_prefix: /
    import_path: ray.dashboard.modules.serve.tests.test_serve_dashboard.deployment_app
    deployments:
      - name: hello_world
        num_replicas: 1
"""


def deploy_config_multi_app(config: Dict, url: str):
    put_response = requests.put(url, json=config, timeout=30)
    assert put_response.status_code == 200
    print("PUT request sent successfully.")


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_put_get_multi_app(ray_start_stop):
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
        deploy_config_multi_app(config1, SERVE_HEAD_URL)
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
        deploy_config_multi_app(config2, SERVE_HEAD_URL)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).text
            == "4 pizzas please!",
            timeout=15,
        )
        print("Adder deployment updated correctly.\n")

        # APPLY CONFIG 3: App #1 should be overwritten to world:DagNode
        print("Sending PUT request for config3.")
        deploy_config_multi_app(config3, SERVE_HEAD_URL)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1").text
            == "wonderful world",
            timeout=15,
        )
        print("Deployments are live and reachable over HTTP.\n")


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_put_bad_schema(ray_start_stop):
    config = {"not_a_real_field": "value"}

    put_response = requests.put(SERVE_HEAD_URL, json=config, timeout=5)
    assert put_response.status_code == 400


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_put_duplicate_apps(ray_start_stop):
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
    put_response = requests.put(SERVE_HEAD_URL, json=config, timeout=5)
    assert put_response.status_code == 400 and "ValidationError" in put_response.text


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_put_duplicate_routes(ray_start_stop):
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
    put_response = requests.put(SERVE_HEAD_URL, json=config, timeout=5)
    assert put_response.status_code == 400 and "ValidationError" in put_response.text


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_delete_multi_app(ray_start_stop):
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
        deploy_config_multi_app(config, SERVE_HEAD_URL)
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
        delete_response = requests.delete(SERVE_HEAD_URL, timeout=15)
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
def test_get_serve_instance_details_not_started(ray_start_stop):
    """Test REST API when Serve hasn't started yet."""
    # Parse the response to ensure it's formatted correctly.
    serve_details = ServeInstanceDetails(**requests.get(SERVE_HEAD_URL).json())
    assert serve_details.target_groups == []


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
def test_get_serve_instance_details(ray_start_stop, f_deployment_options):
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
            "source": "declarative",
            "required_resources": {
                "f": {
                    "CPU": f_deployment_options.get("ray_actor_options", {}).get(
                        "num_cpus", 0.1
                    )
                },
                "BasicDriver": {"CPU": 0.1},
            },
        },
        "app2": {
            "route_prefix": "/banana",
            "docs_path": "/my_docs",
            "deployments": {"FastAPIDeployment"},
            "source": "declarative",
            "required_resources": {"FastAPIDeployment": {"CPU": 1}},
        },
    }

    deploy_config_multi_app(config, SERVE_HEAD_URL)

    def applications_running():
        response = requests.get(SERVE_HEAD_URL, timeout=15)
        assert response.status_code == 200

        serve_details = ServeInstanceDetails(**response.json())
        return (
            serve_details.applications["app1"].status == ApplicationStatus.RUNNING
            and serve_details.applications["app2"].status == ApplicationStatus.RUNNING
        )

    wait_for_condition(applications_running, timeout=15)
    print("All applications are in a RUNNING state.")

    serve_details = ServeInstanceDetails(**requests.get(SERVE_HEAD_URL).json())
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
    proxy_ips = [proxy.node_ip for proxy in serve_details.proxies.values()]
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
        assert app_details[app].source == expected_values[app]["source"]

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
            assert (
                deployment.required_resources
                == expected_values[app]["required_resources"][deployment.name]
            )

            for replica in deployment.replicas:
                assert replica.replica_id
                assert replica.state == ReplicaState.RUNNING
                assert deployment.name in replica.actor_name
                assert replica.actor_id and replica.node_id and replica.node_ip
                assert replica.start_time_s > app_details[app].last_deployed_time_s
                file_path = "/tmp/ray/session_latest/logs" + replica.log_file_path
                assert os.path.exists(file_path)

    print("Finished checking application details.")

    # Check target details
    target_groups = serve_details.target_groups
    assert len(target_groups) == 2
    # sort target_groups by protocol
    target_groups.sort(key=lambda x: x.protocol.lower())
    assert len(target_groups[0].targets) == 1
    assert target_groups[0].protocol == "gRPC"
    assert target_groups[0].route_prefix == "/"
    assert target_groups[1].protocol == "HTTP"
    assert target_groups[1].route_prefix == "/"
    for target in target_groups[0].targets:
        assert target.ip in proxy_ips
        assert target.port == 9001
        assert target.instance_id == ""
    for target in target_groups[1].targets:
        assert target.ip in proxy_ips
        assert target.port == 8005
        assert target.instance_id == ""


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_get_serve_instance_details_for_imperative_apps(ray_start_stop):
    """
    Most behavior is checked by test_get_serve_instance_details.
    This test mostly checks for the different behavior of
    imperatively-deployed apps, with some crossover.
    """
    # Submit the apps in a subprocess, since doing it from the main process
    # seems to make Serve stop unexpectedly
    # https://github.com/ray-project/ray/pull/45522#discussion_r1720479757
    deploy = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).parent / "deploy_imperative_serve_apps.py"),
        ],
        capture_output=True,
        universal_newlines=True,
    )
    print(deploy.stdout)
    assert deploy.returncode == 0

    def applications_running():
        response = requests.get(SERVE_HEAD_URL, timeout=15)
        assert response.status_code == 200

        serve_details = ServeInstanceDetails(**response.json())
        return (
            serve_details.applications["app1"].status == ApplicationStatus.RUNNING
            and serve_details.applications["app2"].status == ApplicationStatus.RUNNING
        )

    wait_for_condition(applications_running, timeout=15)
    print("All applications are in a RUNNING state.")

    expected_values = {
        "app1": {
            "route_prefix": "/apple",
            "docs_path": None,
            "deployments": {"f", "BasicDriver"},
            "source": "imperative",
        },
        "app2": {
            "route_prefix": "/banana",
            "docs_path": None,
            "deployments": {"f", "BasicDriver"},
            "source": "imperative",
        },
    }

    serve_details = ServeInstanceDetails(**requests.get(SERVE_HEAD_URL).json())

    app_details = serve_details.applications
    # CHECK: application details
    for i, app in enumerate(["app1", "app2"]):
        assert app_details[app].deployed_app_config is None
        assert app_details[app].last_deployed_time_s > 0
        assert app_details[app].route_prefix == expected_values[app]["route_prefix"]
        assert app_details[app].docs_path == expected_values[app]["docs_path"]
        assert app_details[app].source == expected_values[app]["source"]

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


@serve.deployment(name="hello_world", num_replicas=1)
class DeploymentClass:
    def __init__(self):
        pass

    def __call__(self):
        return "test"


deployment_app = DeploymentClass.bind()


@serve.deployment(name="hello_world", num_replicas=2, version="v2")
class DeploymentClassWithBlockingInit:
    def __init__(self, semaphore_handle):
        ray.get(semaphore_handle.acquire.remote())
        ray.get(semaphore_handle.release.remote())

    def __call__(self):
        return "test"


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
class TestScaleDeploymentEndpoint:
    def _run_serve_deploy(self, config_path: Path):
        proc = subprocess.run(
            [
                "serve",
                "deploy",
                "-a",
                "http://localhost:8265",
                str(config_path),
            ],
            capture_output=True,
        )

        assert proc.returncode == 0, proc.stderr.decode("utf-8")

    def _get_deployment_details(
        self, app_name="test_app", deployment_name="hello_world"
    ):
        """Get deployment details from serve instance."""
        serve_details = ServeInstanceDetails(**requests.get(SERVE_HEAD_URL).json())
        app_details = serve_details.applications[app_name]

        return app_details.deployments[deployment_name]

    def _scale_and_verify_deployment(
        self,
        num_replicas,
        app_name="test_app",
        deployment_name="hello_world",
        verify_actual_replicas=True,
    ):
        """Scale a deployment and verify both target and actual replica counts."""
        response = requests.post(
            SERVE_HEAD_DEPLOYMENT_SCALE_URL.format(
                app_name=app_name, deployment_name=deployment_name
            ),
            json={"target_num_replicas": num_replicas},
            timeout=30,
        )

        response_data = response.json()

        assert response.status_code == 200
        assert "message" in response_data
        assert (
            "Scaling request received. Deployment will get scaled asynchronously."
            in response_data["message"]
        )

        self._verify_deployment_details(
            app_name=app_name,
            deployment_name=deployment_name,
            target_num_replicas=num_replicas,
            verify_actual_replicas=verify_actual_replicas,
        )

    def _verify_deployment_details(
        self,
        app_name="test_app",
        deployment_name="hello_world",
        target_num_replicas=None,
        deployment_status=None,
        verify_actual_replicas=True,
    ):
        deployment_details = self._get_deployment_details(app_name, deployment_name)

        if target_num_replicas is not None:
            assert deployment_details.target_num_replicas == target_num_replicas

        if deployment_status is not None:
            assert deployment_details.status == deployment_status

        if verify_actual_replicas:
            wait_for_condition(
                lambda: get_num_alive_replicas(deployment_name, app_name)
                == target_num_replicas,
                timeout=30,
            )

        return True

    def test_scale_deployment_endpoint_comprehensive(self, ray_start_stop):
        serve.run(DeploymentClass.bind(), name="test_app")

        wait_for_condition(
            lambda: self._get_deployment_details().status == DeploymentStatus.HEALTHY
        )  # Wait for deployment to be healthy

        self._scale_and_verify_deployment(
            3
        )  # Test 1: Basic scaling up and down with actual replica verification

        self._scale_and_verify_deployment(1)

        self._scale_and_verify_deployment(0)  # Test 2: Scale to zero replicas

        self._scale_and_verify_deployment(2)  # Test 3: Scale from zero replicas

    def test_scale_deployment_during_application_startup(self, ray_start_stop):
        semaphore = Semaphore.remote(value=0)

        serve._run(
            DeploymentClassWithBlockingInit.bind(semaphore),
            name="test_app",
            _blocking=False,
        )

        wait_for_condition(
            self._verify_deployment_details,
            target_num_replicas=2,
            deployment_status=DeploymentStatus.UPDATING,
            verify_actual_replicas=False,
            timeout=30,
        )

        self._scale_and_verify_deployment(4, verify_actual_replicas=False)

        wait_for_condition(
            self._verify_deployment_details,
            target_num_replicas=4,
            deployment_status=DeploymentStatus.UPDATING,
            verify_actual_replicas=False,
            timeout=30,
        )

        ray.get(semaphore.release.remote())

        wait_for_condition(
            self._verify_deployment_details,
            target_num_replicas=4,
            deployment_status=DeploymentStatus.HEALTHY,
            verify_actual_replicas=True,
            timeout=30,
        )

    def test_scale_deployment_during_application_upgrade(self, ray_start_stop):
        semaphore = Semaphore.remote(value=1)

        serve._run(DeploymentClass.bind(), name="test_app", _blocking=False)

        wait_for_condition(
            self._verify_deployment_details,
            target_num_replicas=1,
            deployment_status=DeploymentStatus.HEALTHY,
            verify_actual_replicas=True,
            timeout=30,
        )

        serve._run(
            DeploymentClassWithBlockingInit.bind(semaphore),
            name="test_app",
            _blocking=False,
        )

        wait_for_condition(
            self._verify_deployment_details,
            target_num_replicas=2,
            deployment_status=DeploymentStatus.UPDATING,
            verify_actual_replicas=False,
            timeout=30,
        )

        assert (
            get_num_alive_replicas(deployment_name="hello_world", app_name="test_app")
            == 1
        )

        self._scale_and_verify_deployment(3, verify_actual_replicas=False)

        wait_for_condition(
            self._verify_deployment_details,
            target_num_replicas=3,
            deployment_status=DeploymentStatus.UPDATING,
            verify_actual_replicas=False,
            timeout=30,
        )

        ray.get(
            semaphore.release.remote()
        )  # Release the semaphore to allow the second and third replica to start

        wait_for_condition(
            self._verify_deployment_details,
            target_num_replicas=3,
            deployment_status=DeploymentStatus.HEALTHY,
            verify_actual_replicas=True,
            timeout=30,
        )

    def test_scale_deployment_during_application_deletion(self, ray_start_stop):
        signal_actor = SignalActor.remote()

        @serve.deployment(name="hello_world", num_replicas=1)
        class DeploymentClassWithBlockingDel:
            def __init__(self, signal_actor_handle):
                self.signal_actor_handle = signal_actor_handle

            def __del__(self):
                ray.get(self.signal_actor_handle.wait.remote())

            def __call__(self):
                return "test"

        serve._run(
            DeploymentClassWithBlockingDel.bind(signal_actor),
            name="test_app",
            _blocking=False,
        )

        wait_for_condition(
            lambda: self._get_deployment_details().status == DeploymentStatus.HEALTHY
        )  # Wait for deployment to be healthy

        serve.delete("test_app", _blocking=False)

        wait_for_condition(lambda: ray.get(signal_actor.cur_num_waiters.remote()) == 1)

        response = requests.post(
            SERVE_HEAD_DEPLOYMENT_SCALE_URL.format(
                app_name="test_app", deployment_name="hello_world"
            ),
            json={"target_num_replicas": 5},
            timeout=30,
        )

        assert response.status_code == 412
        assert "Deployment is deleted" in response.json()["error"]

        ray.get(signal_actor.send.remote())

    def test_scale_deployment_retention_across_application_upgrade(
        self, ray_start_stop
    ):
        """Test that replica counts set via /scale are retained across application upgrade."""

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            config_v1_file = tmp_path / "config_v1.yaml"
            config_v1_file.write_text(CONFIG_FILE_TEXT)

            self._run_serve_deploy(config_v1_file)

            wait_for_condition(
                self._verify_deployment_details,
                deployment_status=DeploymentStatus.HEALTHY,
                target_num_replicas=1,
                timeout=30,
            )

            self._scale_and_verify_deployment(
                3, verify_actual_replicas=False
            )  # Scale to 3 replicas

            wait_for_condition(
                self._verify_deployment_details,
                target_num_replicas=3,
                deployment_status=DeploymentStatus.HEALTHY,
                verify_actual_replicas=True,
                timeout=30,
            )

            self._run_serve_deploy(config_v1_file)  # Redeploy the application

            wait_for_condition(
                self._verify_deployment_details,
                target_num_replicas=3,
                deployment_status=DeploymentStatus.HEALTHY,
                verify_actual_replicas=True,
                timeout=30,
            )

    def test_scale_deployment_retention_during_serve_controller_restart(
        self, ray_start_stop
    ):
        """Test that replica counts set via /scale are retained after serve controller restart."""
        serve.start()

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            config_v1_file = tmp_path / "config_v1.yaml"
            config_v1_file.write_text(CONFIG_FILE_TEXT)

            self._run_serve_deploy(config_v1_file)

            wait_for_condition(
                self._verify_deployment_details,
                deployment_status=DeploymentStatus.HEALTHY,
                target_num_replicas=1,
                timeout=30,
            )

            self._scale_and_verify_deployment(
                3, verify_actual_replicas=False
            )  # Scale to 3 replicas

            wait_for_condition(
                self._verify_deployment_details,
                target_num_replicas=3,
                deployment_status=DeploymentStatus.HEALTHY,
                verify_actual_replicas=True,
                timeout=30,
            )

            ray.kill(serve.context._get_global_client()._controller, no_restart=False)

            wait_for_condition(
                self._verify_deployment_details,
                target_num_replicas=3,
                deployment_status=DeploymentStatus.HEALTHY,
                verify_actual_replicas=True,
                timeout=30,
            )

    def test_error_case(self, ray_start_stop):
        serve.start()

        error_response = requests.post(
            SERVE_HEAD_DEPLOYMENT_SCALE_URL.format(
                app_name="nonexistent", deployment_name="hello_world"
            ),
            json={"target_num_replicas": 2},
            timeout=30,
        )
        assert error_response.status_code == 400
        assert "not found" in error_response.json()["error"].lower()

        error_response = requests.post(
            SERVE_HEAD_DEPLOYMENT_SCALE_URL.format(
                app_name="test_app", deployment_name="nonexistent"
            ),
            json={"target_num_replicas": 2},
            timeout=30,
        )
        assert error_response.status_code == 400
        assert "not found" in error_response.json()["error"].lower()

        error_response = requests.post(
            SERVE_HEAD_DEPLOYMENT_SCALE_URL.format(
                app_name="test_app", deployment_name="hello_world"
            ),
            json={"invalid_field": 2},
            timeout=30,
        )
        assert error_response.status_code == 400
        assert "invalid request body" in error_response.json()["error"].lower()


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_get_serve_instance_details_api_type_filtering(ray_start_stop):
    """
    Test the api_type query parameter for filtering applications by API type.
    Tests both declarative and imperative applications.
    """
    # First, deploy declarative applications
    world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
    declarative_config = {
        "applications": [
            {
                "name": "declarative_app1",
                "route_prefix": "/declarative1",
                "import_path": world_import_path,
            },
            {
                "name": "declarative_app2",
                "route_prefix": "/declarative2",
                "import_path": world_import_path,
            },
        ],
    }

    deploy_config_multi_app(declarative_config, SERVE_HEAD_URL)

    # Wait for declarative apps to be running
    def declarative_apps_running():
        response = requests.get(SERVE_HEAD_URL, timeout=15)
        assert response.status_code == 200
        serve_details = ServeInstanceDetails(**response.json())
        return len(serve_details.applications) == 2 and all(
            app.status == ApplicationStatus.RUNNING
            for app in serve_details.applications.values()
        )

    wait_for_condition(declarative_apps_running, timeout=15)
    print("Declarative applications are running.")

    # Deploy imperative applications using subprocess
    deploy = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).parent / "deploy_imperative_serve_apps.py"),
        ],
        capture_output=True,
        universal_newlines=True,
    )
    assert deploy.returncode == 0

    # Wait for imperative apps to be running
    def all_apps_running():
        response = requests.get(SERVE_HEAD_URL, timeout=15)
        assert response.status_code == 200
        serve_details = ServeInstanceDetails(**response.json())
        return len(
            serve_details.applications
        ) == 4 and all(  # 2 declarative + 2 imperative
            app.status == ApplicationStatus.RUNNING
            for app in serve_details.applications.values()
        )

    wait_for_condition(all_apps_running, timeout=15)
    print("All applications (declarative + imperative) are running.")

    # Test 1: No api_type parameter - should return all applications
    response = requests.get(SERVE_HEAD_URL, timeout=15)
    assert response.status_code == 200
    serve_details = ServeInstanceDetails(**response.json())
    assert len(serve_details.applications) == 4
    app_names = set(serve_details.applications.keys())
    assert app_names == {"declarative_app1", "declarative_app2", "app1", "app2"}

    # Test 2: Filter by declarative applications
    response = requests.get(SERVE_HEAD_URL + "?api_type=declarative", timeout=15)
    assert response.status_code == 200
    serve_details = ServeInstanceDetails(**response.json())
    assert len(serve_details.applications) == 2
    app_names = set(serve_details.applications.keys())
    assert app_names == {"declarative_app1", "declarative_app2"}
    for app in serve_details.applications.values():
        assert app.source == "declarative"

    # Test 3: Filter by imperative applications
    response = requests.get(SERVE_HEAD_URL + "?api_type=imperative", timeout=15)
    assert response.status_code == 200
    serve_details = ServeInstanceDetails(**response.json())
    assert len(serve_details.applications) == 2
    app_names = set(serve_details.applications.keys())
    assert app_names == {"app1", "app2"}
    for app in serve_details.applications.values():
        assert app.source == "imperative"

    # Test 4: Filter by unknown - should return 400 error (unknown is not a valid user input)
    response = requests.get(SERVE_HEAD_URL + "?api_type=unknown", timeout=15)
    assert response.status_code == 400
    assert "Invalid 'api_type' value" in response.text
    assert "Must be one of: imperative, declarative" in response.text


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_get_serve_instance_details_invalid_api_type(ray_start_stop):
    """
    Test that invalid api_type values return appropriate error responses.
    """
    # Test with invalid api_type value
    response = requests.get(SERVE_HEAD_URL + "?api_type=invalid_type", timeout=15)
    assert response.status_code == 400
    assert "Invalid 'api_type' value" in response.text
    assert "Must be one of: imperative, declarative" in response.text

    # Test with another invalid value
    response = requests.get(SERVE_HEAD_URL + "?api_type=python", timeout=15)
    assert response.status_code == 400
    assert "Invalid 'api_type' value" in response.text


@pytest.mark.skipif(
    sys.platform == "darwin" and not TEST_ON_DARWIN, reason="Flaky on OSX."
)
def test_get_serve_instance_details_api_type_case_insensitive(ray_start_stop):
    """
    Test that api_type parameter is case insensitive.
    """
    # Deploy a declarative application
    world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
    config = {
        "applications": [
            {
                "name": "test_app",
                "route_prefix": "/test",
                "import_path": world_import_path,
            }
        ],
    }

    deploy_config_multi_app(config, SERVE_HEAD_URL)

    def app_running():
        response = requests.get(SERVE_HEAD_URL, timeout=15)
        assert response.status_code == 200
        serve_details = ServeInstanceDetails(**response.json())
        return (
            len(serve_details.applications) == 1
            and serve_details.applications["test_app"].status
            == ApplicationStatus.RUNNING
        )

    wait_for_condition(app_running, timeout=15)

    # Test case insensitive filtering
    test_cases = ["DECLARATIVE", "Declarative", "declarative", "DeClArAtIvE"]

    for api_type_value in test_cases:
        response = requests.get(
            f"{SERVE_HEAD_URL}?api_type={api_type_value}", timeout=15
        )
        assert response.status_code == 200
        serve_details = ServeInstanceDetails(**response.json())
        assert len(serve_details.applications) == 1
        assert "test_app" in serve_details.applications


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
