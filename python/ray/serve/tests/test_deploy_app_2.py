import subprocess
import sys
import time
from contextlib import contextmanager
from typing import Dict
from functools import partial

import pytest
import requests

import ray
import ray.actor
import ray._private.state
from ray.util.state import list_actors

from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve.exceptions import RayServeException
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.common import ApplicationStatus, DeploymentStatus, ReplicaState
from ray.serve._private.constants import (
    SERVE_NAMESPACE,
    SERVE_DEFAULT_APP_NAME,
    DEPLOYMENT_NAME_PREFIX_SEPARATOR,
)
from ray.serve.schema import (
    ServeApplicationSchema,
    ServeDeploySchema,
    ServeInstanceDetails,
)


@pytest.fixture
def shutdown_ray_and_serve():
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()
    yield
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()


@contextmanager
def start_and_shutdown_ray_cli():
    subprocess.check_output(["ray", "stop", "--force"])
    wait_for_condition(_check_ray_stop, timeout=15)
    subprocess.check_output(["ray", "start", "--head"])

    yield

    subprocess.check_output(["ray", "stop", "--force"])
    wait_for_condition(_check_ray_stop, timeout=15)


@pytest.fixture(scope="class")
def start_and_shutdown_ray_cli_class():
    with start_and_shutdown_ray_cli():
        yield


def _check_ray_stop():
    try:
        requests.get("http://localhost:52365/api/ray/version")
        return False
    except Exception:
        return True


class TestDeployApp:
    @pytest.fixture(scope="function")
    def client(self, start_and_shutdown_ray_cli_class, shutdown_ray_and_serve):
        wait_for_condition(
            lambda: requests.get("http://localhost:52365/api/ray/version").status_code
            == 200,
            timeout=15,
        )
        ray.init(address="auto", namespace=SERVE_NAMESPACE)
        yield serve.start(detached=True)

    def check_running(self, client: ServeControllerClient):
        serve_status = client.get_serve_status()
        return serve_status.app_status.status == ApplicationStatus.RUNNING

    def check_deployments_dead(self, deployment_names):
        actor_names = [
            actor["class_name"]
            for actor in list_actors(
                filters=[("state", "=", "ALIVE")],
            )
        ]
        return all(
            f"ServeReplica:{name}" not in actor_names for name in deployment_names
        )

    def get_num_replicas(self, client: ServeControllerClient, deployment_name: str):
        replicas = ray.get(
            client._controller._dump_replica_states_for_testing.remote(deployment_name)
        )
        running_replicas = replicas.get([ReplicaState.RUNNING])
        return len(running_replicas)

    def get_test_config(self) -> Dict:
        return {"import_path": "ray.serve.tests.test_config_files.pizza.serve_dag"}

    def check_single_app(self):
        """Checks the application deployed through the config from get_test_config()"""
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["MUL", 3]).json()
            == "9 pizzas please!"
        )

    def get_test_deploy_config(self) -> Dict:
        return {
            "applications": [
                {
                    "name": "app1",
                    "route_prefix": "/app1",
                    "import_path": "ray.serve.tests.test_config_files.pizza.serve_dag",
                },
                {
                    "name": "app2",
                    "route_prefix": "/app2",
                    "import_path": "ray.serve.tests.test_config_files.pizza.serve_dag",
                    "deployments": [
                        {
                            "name": "Adder",
                            "user_config": {
                                "increment": 3,
                            },
                        },
                        {
                            "name": "Multiplier",
                            "user_config": {
                                "factor": 4,
                            },
                        },
                    ],
                },
            ],
        }

    def check_multi_app(self):
        """
        Checks the applications deployed through the config from
        get_test_deploy_config().
        """

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["MUL", 3]).json()
            == "9 pizzas please!"
        )

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
            == "5 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["MUL", 3]).json()
            == "12 pizzas please!"
        )

    def test_update_config_user_config(self, client: ServeControllerClient):
        """Check that replicas stay alive when user config is updated."""

        config_template = {
            "import_path": "ray.serve.tests.test_config_files.pid.node",
            "deployments": [{"name": "f", "user_config": {"name": "alice"}}],
        }

        # Deploy first time
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(partial(self.check_running, client), timeout=15)

        # Query
        pid1, res = requests.get("http://localhost:8000/f").json()
        assert res == "alice"

        # Redeploy with updated option
        config_template["deployments"][0]["user_config"] = {"name": "bob"}
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))

        # Query
        def check():
            pids = []
            for _ in range(4):
                pid, res = requests.get("http://localhost:8000/f").json()
                assert res == "bob"
                pids.append(pid)
            assert pid1 in pids
            return True

        wait_for_condition(check)

    def test_update_config_graceful_shutdown_timeout(
        self, client: ServeControllerClient
    ):
        """Check that replicas stay alive when graceful_shutdown_timeout_s is updated"""
        name = f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}f"
        config_template = {
            "import_path": "ray.serve.tests.test_config_files.pid.node",
            "deployments": [{"name": "f", "graceful_shutdown_timeout_s": 1000}],
        }

        # Deploy first time
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(partial(self.check_running, client), timeout=15)
        handle = client.get_handle(name)

        # Start off with signal ready, and send query
        ray.get(handle.send.remote())
        pid1 = ray.get(handle.remote())[0]
        print("PID of replica after first deployment:", pid1)

        # Redeploy with shutdown timeout set to 5 seconds
        config_template["deployments"][0]["graceful_shutdown_timeout_s"] = 5
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(partial(self.check_running, client), timeout=15)

        pid2 = ray.get(handle.remote())[0]
        assert pid1 == pid2
        print("PID of replica after redeployment:", pid2)

        # Send blocking query
        handle.send.remote(clear=True)
        handle.remote()
        # Try to delete deployment, should be blocked until the timeout at 5 seconds
        client.delete_apps([SERVE_DEFAULT_APP_NAME], blocking=False)
        # Replica should be dead within 10 second timeout, which means
        # graceful_shutdown_timeout_s was successfully updated lightweightly
        wait_for_condition(partial(self.check_deployments_dead, ["f"]))

    def test_update_config_max_concurrent_queries(self, client: ServeControllerClient):
        """Check that replicas stay alive when max_concurrent_queries is updated."""

        name = f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}f"
        config_template = {
            "import_path": "ray.serve.tests.test_config_files.pid.node",
            "deployments": [{"name": "f", "max_concurrent_queries": 1000}],
        }

        # Deploy first time, max_concurent_queries set to 1000.
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(partial(self.check_running, client), timeout=15)

        all_replicas = ray.get(client._controller._all_running_replicas.remote())
        assert len(all_replicas) == 1
        assert (
            all_replicas[list(all_replicas.keys())[0]][0].max_concurrent_queries == 1000
        )

        handle = client.get_handle(name)

        responses = ray.get([handle.remote() for _ in range(10)])
        pids1 = {response[0] for response in responses}
        assert len(pids1) == 1

        # Redeploy with max concurrent queries set to 2.
        config_template["deployments"][0]["max_concurrent_queries"] = 2
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(partial(self.check_running, client), timeout=15)

        # Verify that the PID of the replica didn't change.
        responses = ray.get([handle.remote() for _ in range(10)])
        pids2 = {response[0] for response in responses}
        assert pids2 == pids1

    def test_update_config_health_check_period(self, client: ServeControllerClient):
        """Check that replicas stay alive when max_concurrent_queries is updated."""

        name = f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}f"
        config_template = {
            "import_path": "ray.serve.tests.test_config_files.pid.async_node",
            "deployments": [{"name": "f", "health_check_period_s": 100}],
        }

        # Deploy first time, wait for replica running and deployment healthy
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(partial(self.check_running, client), timeout=15)

        handle = client.get_handle(name)
        pid1 = ray.get(handle.remote())[0]

        # The health check counter shouldn't increase beyond any initial health checks
        # done as part of the replica startup sequence.
        initial_counter = ray.get(handle.get_counter.remote(health_check=True))
        time.sleep(5)
        assert (
            ray.get(handle.get_counter.remote(health_check=True)) <= initial_counter + 1
        )

        # Update the deployment's health check period to 0.1 seconds.
        config_template["deployments"][0]["health_check_period_s"] = 0.1
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(partial(self.check_running, client), timeout=15)

        # Health check counter should now quickly increase due to the shorter period.
        wait_for_condition(
            lambda: ray.get(handle.get_counter.remote(health_check=True)) >= 30,
            retry_interval_ms=1000,
            timeout=10,
        )

        # Check that it's the same replica (it wasn't torn down to update the config).
        pid2 = ray.get(handle.remote())[0]
        assert pid1 == pid2

    def test_update_config_health_check_timeout(self, client: ServeControllerClient):
        """Check that replicas stay alive when max_concurrent_queries is updated."""

        name = f"{SERVE_DEFAULT_APP_NAME}{DEPLOYMENT_NAME_PREFIX_SEPARATOR}f"
        # Deploy with a very long initial health_check_timeout_s
        # Also set small health_check_period_s to make test run faster
        config_template = {
            "import_path": "ray.serve.tests.test_config_files.pid.async_node",
            "deployments": [
                {
                    "name": "f",
                    "health_check_period_s": 1,
                    "health_check_timeout_s": 1000,
                }
            ],
        }

        # Deploy first time, wait for replica running and deployment healthy
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(partial(self.check_running, client), timeout=15)

        handle = client.get_handle(name)
        pid1 = ray.get(handle.remote())[0]

        # Redeploy with health check timeout reduced to 1 second
        config_template["deployments"][0]["health_check_timeout_s"] = 1
        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(partial(self.check_running, client), timeout=15)

        # Check that it's the same replica, it didn't get teared down
        # (needs to be done before the tests below because the replica will be marked
        # unhealthy then stopped and restarted)
        pid2 = ray.get(handle.remote())[0]
        assert pid1 == pid2

        # Block in health check
        ray.get(handle.send.remote(clear=True, health_check=True))
        wait_for_condition(
            lambda: client.get_serve_status().get_deployment_status(name).status
            == DeploymentStatus.UNHEALTHY
        )

    def test_deploy_separate_runtime_envs(self, client: ServeControllerClient):
        """Deploy two applications with separate runtime envs."""

        config_template = {
            "applications": [
                {
                    "name": "app1",
                    "route_prefix": "/app1",
                    "import_path": "conditional_dag.serve_dag",
                    "runtime_env": {
                        "working_dir": (
                            "https://github.com/ray-project/test_dag/archive/"
                            "41d09119cbdf8450599f993f51318e9e27c59098.zip"
                        )
                    },
                },
                {
                    "name": "app2",
                    "route_prefix": "/app2",
                    "import_path": "hello_world.app",
                    "runtime_env": {
                        "working_dir": (
                            "https://github.com/zcin/test_runtime_env/archive/"
                            "c96019b6049cd9a2997db5ea0f10432bfeffb844.zip"
                        )
                    },
                },
            ],
        }

        client.deploy_apps(ServeDeploySchema(**config_template))

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "0 pizzas please!"
        )

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2").text == "Hello world!"
        )

    def test_deploy_one_app_failed(self, client: ServeControllerClient):
        """Deploy two applications with separate runtime envs."""

        world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
        fail_import_path = "ray.serve.tests.test_config_files.fail.node"
        config_template = {
            "applications": [
                {
                    "name": "app1",
                    "route_prefix": "/app1",
                    "import_path": world_import_path,
                },
                {
                    "name": "app2",
                    "route_prefix": "/app2",
                    "import_path": fail_import_path,
                },
            ],
        }

        client.deploy_apps(ServeDeploySchema(**config_template))

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1").text
            == "wonderful world"
        )

        wait_for_condition(
            lambda: client.get_serve_status("app1").app_status.status
            == ApplicationStatus.RUNNING
            and client.get_serve_status("app2").app_status.status
            == ApplicationStatus.DEPLOY_FAILED
        )

    def test_deploy_with_route_prefix_conflict(self, client: ServeControllerClient):
        world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
        pizza_import_path = "ray.serve.tests.test_config_files.pizza.serve_dag"
        test_config = {
            "applications": [
                {
                    "name": "app1",
                    "route_prefix": "/app1",
                    "import_path": world_import_path,
                },
                {
                    "name": "app2",
                    "route_prefix": "/app2",
                    "import_path": pizza_import_path,
                },
            ],
        }

        client.deploy_apps(ServeDeploySchema(**test_config))

        wait_for_condition(
            lambda: requests.get("http://localhost:8000/app1").text == "wonderful world"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )

        # Buffer time
        time.sleep(1)

        test_config["applications"][1] = {
            "name": "app3",
            "route_prefix": "/app2",
            "import_path": world_import_path,
        }

        client.deploy_apps(ServeDeploySchema(**test_config))

        def check():
            serve_details = ServeInstanceDetails(
                **ray.get(client._controller.get_serve_instance_details.remote())
            )
            app1_running = (
                "app1" in serve_details.applications
                and serve_details.applications["app1"].status == "RUNNING"
            )
            app3_running = (
                "app3" in serve_details.applications
                and serve_details.applications["app3"].status == "RUNNING"
            )
            app2_gone = "app2" not in serve_details.applications
            return app1_running and app3_running and app2_gone

        wait_for_condition(check)

        # app1 and app3 should be up and running
        wait_for_condition(
            lambda: requests.get("http://localhost:8000/app1").text == "wonderful world"
        )
        wait_for_condition(
            lambda: requests.get("http://localhost:8000/app2").text == "wonderful world"
        )

    def test_deploy_single_then_multi(self, client: ServeControllerClient):
        """Deploying single-app then multi-app config should fail."""

        single_app_config = ServeApplicationSchema.parse_obj(self.get_test_config())
        multi_app_config = ServeDeploySchema.parse_obj(self.get_test_deploy_config())

        # Deploy single app config
        client.deploy_apps(single_app_config)
        self.check_single_app()

        # Deploying multi app config afterwards should fail
        with pytest.raises(RayServeException):
            client.deploy_apps(multi_app_config)
        # The original application should still be up and running
        self.check_single_app()

    def test_deploy_multi_then_single(self, client: ServeControllerClient):
        """Deploying multi-app then single-app config should fail."""

        single_app_config = ServeApplicationSchema.parse_obj(self.get_test_config())
        multi_app_config = ServeDeploySchema.parse_obj(self.get_test_deploy_config())

        # Deploy multi app config
        client.deploy_apps(multi_app_config)
        self.check_multi_app()

        # Deploying single app config afterwards should fail
        with pytest.raises(RayServeException):
            client.deploy_apps(single_app_config)
        # The original applications should still be up and running
        self.check_multi_app()

    def test_deploy_multi_app_deleting(self, client: ServeControllerClient):
        """Test deleting an application by removing from config."""

        config = ServeDeploySchema.parse_obj(self.get_test_deploy_config())
        client.deploy_apps(config)
        self.check_multi_app()

        # Delete app2
        del config.applications[1]
        client.deploy_apps(config)

        # Fetch details immediately afterwards, should parse correctly
        details = ray.get(client._controller.get_serve_instance_details.remote())
        ServeInstanceDetails(**details)
        # We don't enforce that the state is deleting here because that could cause
        # flaky test performance. The app could have been deleted by the time of query
        assert (
            "app2" not in details["applications"]
            or details["applications"]["app2"]["status"] == ApplicationStatus.DELETING
        )

        info_valid = True

        def check_app_status():
            global info_valid
            try:
                # Fetch details, should always parse correctly
                details = ray.get(
                    client._controller.get_serve_instance_details.remote()
                )
                ServeInstanceDetails(**details)
                return (
                    details["applications"]["app1"]["status"]
                    == ApplicationStatus.RUNNING
                    and "app2" not in details["applications"]
                )
            except Exception:
                info_valid = False

        wait_for_condition(check_app_status)
        # Check that all all details fetched from controller parsed correctly
        assert info_valid

    def test_deploy_nonexistent_deployment(self, client: ServeControllerClient):
        """Apply a config that lists a deployment that doesn't exist in the application.
        The error message should be descriptive.
        """

        config = ServeDeploySchema.parse_obj(self.get_test_deploy_config())
        # Change names to invalid names that don't contain "deployment" or "application"
        config.applications[1].name = "random1"
        config.applications[1].deployments[0].name = "random2"
        client.deploy_apps(config)

        def check_app_message():
            details = ray.get(client._controller.get_serve_instance_details.remote())
            # The error message should be descriptive
            # e.g. no deployment "x" in application "y"
            return (
                "application" in details["applications"]["random1"]["message"]
                and "deployment" in details["applications"]["random1"]["message"]
            )

        wait_for_condition(check_app_message)

    def test_deploy_with_no_applications(self, client: ServeControllerClient):
        """Deploy an empty list of applications, serve should just be started."""

        config = ServeDeploySchema.parse_obj({"applications": []})
        client.deploy_apps(config)

        def serve_running():
            ServeInstanceDetails.parse_obj(
                ray.get(client._controller.get_serve_instance_details.remote())
            )
            actors = list_actors(
                filters=[
                    ("ray_namespace", "=", SERVE_NAMESPACE),
                    ("state", "=", "ALIVE"),
                ]
            )
            actor_names = [actor["class_name"] for actor in actors]
            return "ServeController" in actor_names and "HTTPProxyActor" in actor_names

        wait_for_condition(serve_running)

    def test_deployments_not_listed_in_config(self, client: ServeControllerClient):
        """Apply a config without the app's deployments listed. The deployments should
        not redeploy.
        """

        config = {"import_path": "ray.serve.tests.test_config_files.pid.node"}
        client.deploy_apps(ServeApplicationSchema(**config))
        wait_for_condition(partial(self.check_running, client), timeout=15)
        pid1, _ = requests.get("http://localhost:8000/f").json()

        # Redeploy the same config (with no deployments listed)
        client.deploy_apps(ServeApplicationSchema(**config))
        wait_for_condition(partial(self.check_running, client), timeout=15)

        # It should be the same replica actor
        pids = []
        for _ in range(4):
            pids.append(requests.get("http://localhost:8000/f").json()[0])
        assert all(pid == pid1 for pid in pids)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
