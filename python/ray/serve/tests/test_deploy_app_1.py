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
from ray.util.state import list_actors, list_tasks

from ray import serve
from ray._private.test_utils import (
    wait_for_condition,
    SignalActor,
)
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.common import ApplicationStatus, ReplicaState
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve.schema import ServeApplicationSchema, ServeDeploySchema


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


def _check_ray_stop():
    try:
        requests.get("http://localhost:52365/api/ray/version")
        return False
    except Exception:
        return True


class TestDeployApp:
    @pytest.fixture(scope="function")
    def client(self, shutdown_ray_and_serve):
        with start_and_shutdown_ray_cli():
            wait_for_condition(
                lambda: requests.get(
                    "http://localhost:52365/api/ray/version"
                ).status_code
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

    def test_deploy_app_basic(self, client: ServeControllerClient):
        config = ServeApplicationSchema.parse_obj(self.get_test_config())
        client.deploy_apps(config)
        self.check_single_app()

    def test_deploy_multi_app_basic(self, client: ServeControllerClient):
        config = ServeDeploySchema.parse_obj(self.get_test_deploy_config())
        client.deploy_apps(config)
        self.check_multi_app()

    def test_deploy_app_with_overriden_config(self, client: ServeControllerClient):
        config = self.get_test_config()
        config["deployments"] = [
            {
                "name": "Multiplier",
                "user_config": {
                    "factor": 4,
                },
            },
            {
                "name": "Adder",
                "user_config": {
                    "increment": 5,
                },
            },
        ]

        client.deploy_apps(ServeApplicationSchema.parse_obj(config))

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 0]).json()
            == "5 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["MUL", 2]).json()
            == "8 pizzas please!"
        )

    def test_deploy_app_update_config(self, client: ServeControllerClient):
        config = ServeApplicationSchema.parse_obj(self.get_test_config())
        client.deploy_apps(config)
        self.check_single_app()

        config = self.get_test_config()
        config["deployments"] = [
            {
                "name": "Adder",
                "user_config": {
                    "increment": -1,
                },
            },
        ]

        client.deploy_apps(ServeApplicationSchema.parse_obj(config))

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "1 pizzas please!"
        )

    def test_deploy_multi_app_update_config(self, client: ServeControllerClient):
        config = self.get_test_deploy_config()
        client.deploy_apps(ServeDeploySchema.parse_obj(config))
        self.check_multi_app()

        config["applications"][0]["deployments"] = [
            {
                "name": "Adder",
                "user_config": {
                    "increment": -1,
                },
            },
        ]

        config["applications"][1]["deployments"] = [
            {
                "name": "Adder",
                "user_config": {
                    "increment": 10,
                },
            },
        ]

        client.deploy_apps(ServeDeploySchema.parse_obj(config))
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "1 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
            == "12 pizzas please!"
        )

    def test_deploy_app_update_num_replicas(self, client: ServeControllerClient):
        config = ServeApplicationSchema.parse_obj(self.get_test_config())
        client.deploy_apps(config)
        self.check_single_app()

        actors = list_actors(filters=[("state", "=", "ALIVE")])

        config = self.get_test_config()
        config["deployments"] = [
            {
                "name": "Adder",
                "num_replicas": 2,
                "user_config": {
                    "increment": 0,
                },
                "ray_actor_options": {"num_cpus": 0.1},
            },
            {
                "name": "Multiplier",
                "num_replicas": 3,
                "user_config": {
                    "factor": 0,
                },
                "ray_actor_options": {"num_cpus": 0.1},
            },
        ]

        client.deploy_apps(ServeApplicationSchema.parse_obj(config))

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "2 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["MUL", 3]).json()
            == "0 pizzas please!"
        )

        wait_for_condition(
            lambda: client.get_serve_status().app_status.status
            == ApplicationStatus.RUNNING,
            timeout=15,
        )

        updated_actors = list_actors(filters=[("state", "=", "ALIVE")])
        assert len(updated_actors) == len(actors) + 3

    def test_deploy_multi_app_update_num_replicas(self, client: ServeControllerClient):
        config = self.get_test_deploy_config()
        client.deploy_apps(ServeDeploySchema.parse_obj(config))
        self.check_multi_app()

        actors = list_actors(filters=[("state", "=", "ALIVE")])

        # app1
        config["applications"][0]["deployments"] = [
            {
                "name": "Adder",
                "num_replicas": 2,  # +1
                "user_config": {
                    "increment": 0,
                },
                "ray_actor_options": {"num_cpus": 0.1},
            },
            {
                "name": "Multiplier",
                "num_replicas": 3,  # +2
                "user_config": {
                    "factor": 0,
                },
                "ray_actor_options": {"num_cpus": 0.1},
            },
        ]

        # app2
        config["applications"][1]["deployments"] = [
            {
                "name": "Adder",
                "num_replicas": 3,  # +2
                "user_config": {
                    "increment": 100,
                },
                "ray_actor_options": {"num_cpus": 0.1},
            },
            {
                "name": "Multiplier",
                "num_replicas": 4,  # +3
                "user_config": {
                    "factor": 0,
                },
                "ray_actor_options": {"num_cpus": 0.1},
            },
        ]

        client.deploy_apps(ServeDeploySchema.parse_obj(config))
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "2 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
            == "102 pizzas please!"
        )

        wait_for_condition(
            lambda: client.get_serve_status("app1").app_status.status
            == ApplicationStatus.RUNNING,
            timeout=15,
        )
        wait_for_condition(
            lambda: client.get_serve_status("app2").app_status.status
            == ApplicationStatus.RUNNING,
            timeout=15,
        )

        updated_actors = list_actors(filters=[("state", "=", "ALIVE")])
        assert len(updated_actors) == len(actors) + 8

    def test_deploy_app_update_timestamp(self, client: ServeControllerClient):
        assert client.get_serve_status().app_status.deployment_timestamp == 0

        config = ServeApplicationSchema.parse_obj(self.get_test_config())
        client.deploy_apps(config)

        assert client.get_serve_status().app_status.deployment_timestamp > 0

        first_deploy_time = client.get_serve_status().app_status.deployment_timestamp
        time.sleep(0.1)

        config = self.get_test_config()
        config["deployments"] = [
            {
                "name": "Adder",
                "num_replicas": 2,
            },
        ]
        client.deploy_apps(ServeApplicationSchema.parse_obj(config))

        assert (
            client.get_serve_status().app_status.deployment_timestamp
            > first_deploy_time
        )
        assert client.get_serve_status().app_status.status in {
            ApplicationStatus.DEPLOYING,
            ApplicationStatus.RUNNING,
        }
        self.check_single_app()

    def test_deploy_multi_app_update_timestamp(self, client: ServeControllerClient):
        assert client.get_serve_status("app1").app_status.deployment_timestamp == 0
        assert client.get_serve_status("app2").app_status.deployment_timestamp == 0

        config = self.get_test_deploy_config()
        client.deploy_apps(ServeDeploySchema.parse_obj(config))

        first_deploy_time_app1 = client.get_serve_status(
            "app1"
        ).app_status.deployment_timestamp
        first_deploy_time_app2 = client.get_serve_status(
            "app2"
        ).app_status.deployment_timestamp

        assert first_deploy_time_app1 > 0 and first_deploy_time_app2 > 0
        time.sleep(0.1)

        # app1
        config["applications"][0]["deployments"] = [
            {
                "name": "Adder",
                "num_replicas": 2,
            },
        ]
        # app2
        config["applications"][1]["deployments"] = [
            {
                "name": "Adder",
                "num_replicas": 3,
            },
        ]
        client.deploy_apps(ServeDeploySchema.parse_obj(config))

        assert (
            client.get_serve_status("app1").app_status.deployment_timestamp
            > first_deploy_time_app1
            and client.get_serve_status("app2").app_status.deployment_timestamp
            > first_deploy_time_app2
        )
        assert {
            client.get_serve_status("app1").app_status.status,
            client.get_serve_status("app2").app_status.status,
        } <= {
            ApplicationStatus.DEPLOYING,
            ApplicationStatus.RUNNING,
        }
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )

    def test_deploy_app_overwrite_apps(self, client: ServeControllerClient):
        """Check that overwriting a live app with a new one works."""

        # Launch first graph. Its driver's route_prefix should be "/".
        test_config_1 = ServeApplicationSchema.parse_obj(
            {
                "import_path": "ray.serve.tests.test_config_files.world.DagNode",
            }
        )
        client.deploy_apps(test_config_1)

        wait_for_condition(
            lambda: requests.get("http://localhost:8000/").text == "wonderful world"
        )

        # Launch second graph. Its driver's route_prefix should also be "/".
        # "/" should lead to the new driver.
        test_config_2 = ServeApplicationSchema.parse_obj(
            {
                "import_path": "ray.serve.tests.test_config_files.pizza.serve_dag",
            }
        )
        client.deploy_apps(test_config_2)

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )

    def test_deploy_multi_app_overwrite_apps(self, client: ServeControllerClient):
        """Check that redeploying different apps with same names works as expected."""

        world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
        pizza_import_path = "ray.serve.tests.test_config_files.pizza.serve_dag"
        test_config = ServeDeploySchema.parse_obj(
            {
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
        )
        client.deploy_apps(test_config)

        wait_for_condition(
            lambda: requests.get("http://localhost:8000/app1").text == "wonderful world"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )

        # Switch the two application import paths
        test_config.applications[0].import_path = pizza_import_path
        test_config.applications[1].import_path = world_import_path
        client.deploy_apps(test_config)

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )
        wait_for_condition(
            lambda: requests.get("http://localhost:8000/app2").text == "wonderful world"
        )

    def test_deploy_multi_app_overwrite_apps2(self, client: ServeControllerClient):
        """Check that deploying a new set of applications removes old ones."""

        world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
        pizza_import_path = "ray.serve.tests.test_config_files.pizza.serve_dag"
        test_config = ServeDeploySchema.parse_obj(
            {
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
        )
        # Deploy app1 and app2
        client.deploy_apps(test_config)

        wait_for_condition(
            lambda: requests.get("http://localhost:8000/app1").text == "wonderful world"
        )
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )

        # Deploy app3
        new_config = ServeDeploySchema.parse_obj(
            {
                "applications": [
                    {
                        "name": "app3",
                        "route_prefix": "/app3",
                        "import_path": pizza_import_path,
                        "deployments": [
                            {
                                "name": "Adder",
                                "user_config": {
                                    "increment": 3,
                                },
                            },
                        ],
                    },
                ],
            }
        )
        client.deploy_apps(new_config)

        def check_dead():
            actors = list_actors(
                filters=[
                    ("ray_namespace", "=", SERVE_NAMESPACE),
                    ("state", "=", "ALIVE"),
                ]
            )
            for actor in actors:
                assert (
                    "app1" not in actor["class_name"]
                    and "app2" not in actor["class_name"]
                )
            return True

        # Deployments from app1 and app2 should be deleted
        wait_for_condition(check_dead)

        # App1 and App2 should be gone
        assert requests.get("http://localhost:8000/app1").status_code != 200
        assert (
            requests.post("http://localhost:8000/app2", json=["ADD", 2]).status_code
            != 200
        )

        # App3 should be up and running
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app3", json=["ADD", 2]).json()
            == "5 pizzas please!"
        )

    def test_deploy_app_runtime_env(self, client: ServeControllerClient):
        config_template = {
            "import_path": "conditional_dag.serve_dag",
            "runtime_env": {
                "working_dir": (
                    "https://github.com/ray-project/test_dag/archive/"
                    "41d09119cbdf8450599f993f51318e9e27c59098.zip"
                )
            },
        }

        config1 = ServeApplicationSchema.parse_obj(config_template)
        client.deploy_apps(config1)

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "0 pizzas please!"
        )

        # Override the configuration
        config_template["deployments"] = [
            {
                "name": "Adder",
                "ray_actor_options": {
                    "runtime_env": {"env_vars": {"override_increment": "1"}}
                },
            }
        ]
        config2 = ServeApplicationSchema.parse_obj(config_template)
        client.deploy_apps(config2)

        wait_for_condition(
            lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
            == "3 pizzas please!"
        )

    def test_deploy_multi_app_deployments_removed(self, client: ServeControllerClient):
        """Test redeploying applications will remove old deployments."""

        world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
        world_deployments = ["f", "BasicDriver"]
        pizza_import_path = "ray.serve.tests.test_config_files.pizza.serve_dag"
        pizza_deployments = [
            "Adder",
            "Multiplier",
            "Router",
            "create_order",
            "DAGDriver",
        ]
        test_config = ServeDeploySchema.parse_obj(
            {
                "applications": [
                    {
                        "name": "app1",
                        "route_prefix": "/app1",
                        "import_path": pizza_import_path,
                    },
                ],
            }
        )
        # Deploy with pizza graph first
        client.deploy_apps(test_config)

        def check_pizza():
            # Check that the live deployments and actors are what we expect: exactly the
            # set of deployments in the pizza graph
            actor_class_names = {
                actor["class_name"]
                for actor in list_actors(filters=[("state", "=", "ALIVE")])
            }
            deployment_replicas = set(
                ray.get(client._controller._all_running_replicas.remote()).keys()
            )
            assert {
                f"app1_{deployment}" for deployment in pizza_deployments
            } == deployment_replicas
            assert {"HTTPProxyActor", "ServeController"}.union(
                {f"ServeReplica:app1_{deployment}" for deployment in pizza_deployments}
            ) == actor_class_names
            return True

        wait_for_condition(check_pizza)
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
            == "4 pizzas please!"
        )

        # Redeploy with world graph
        test_config.applications[0].import_path = world_import_path
        client.deploy_apps(test_config)

        def check_world():
            # Check that the live deployments and actors are what we expect: exactly the
            # set of deployments in the world graph
            actor_class_names = {
                actor["class_name"]
                for actor in list_actors(filters=[("state", "=", "ALIVE")])
            }
            deployment_replicas = set(
                ray.get(client._controller._all_running_replicas.remote()).keys()
            )
            assert {
                f"app1_{deployment}" for deployment in world_deployments
            } == deployment_replicas
            assert {"HTTPProxyActor", "ServeController"}.union(
                {f"ServeReplica:app1_{deployment}" for deployment in world_deployments}
            ) == actor_class_names
            return True

        wait_for_condition(check_world)
        wait_for_condition(
            lambda: requests.get("http://localhost:8000/app1").text == "wonderful world"
        )

    def test_controller_recover_and_deploy(self, client: ServeControllerClient):
        """Ensure that in-progress deploy can finish even after controller dies."""

        signal = SignalActor.options(name="signal123").remote()

        config_json = {
            "applications": [
                {
                    "name": "default",
                    "import_path": "ray.serve.tests.test_config_files.hangs.app",
                }
            ]
        }
        config = ServeDeploySchema.parse_obj(config_json)
        client.deploy_apps(config)

        # Wait for deploy_serve_application task to start->config has been checkpointed
        wait_for_condition(
            lambda: len(
                list_tasks(
                    filters=[("func_or_class_name", "=", "build_serve_application")],
                )
            )
            > 0
        )
        ray.kill(client._controller, no_restart=False)

        signal.send.remote()

        # When controller restarts, it should redeploy config automatically
        wait_for_condition(
            lambda: requests.post("http://localhost:8000/").text == "hello world"
        )

        serve.shutdown()
        client = serve.start(detached=True)

        # Ensure config checkpoint has been deleted
        assert client.get_serve_status().app_status.deployment_timestamp == 0

    @pytest.mark.parametrize(
        "field_to_update",
        ["import_path", "runtime_env", "ray_actor_options"],
    )
    def test_deploy_config_update_heavyweight(
        self, client: ServeControllerClient, field_to_update: str
    ):
        """Check that replicas are torn down when code updates are made."""
        config_template = {
            "import_path": "ray.serve.tests.test_config_files.pid.node",
            "deployments": [
                {
                    "name": "f",
                    "autoscaling_config": None,
                    "user_config": {"name": "alice"},
                    "ray_actor_options": {"num_cpus": 0.1},
                },
            ],
        }

        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(partial(self.check_running, client), timeout=15)
        pid1, _ = requests.get("http://localhost:8000/f").json()

        if field_to_update == "import_path":
            config_template[
                "import_path"
            ] = "ray.serve.tests.test_config_files.pid.dup_node"
        elif field_to_update == "runtime_env":
            config_template["runtime_env"] = {"env_vars": {"test_var": "test_val"}}
        elif field_to_update == "ray_actor_options":
            config_template["deployments"][0]["ray_actor_options"] = {"num_cpus": 0.2}

        client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
        wait_for_condition(partial(self.check_running, client), timeout=15)

        pids = []
        for _ in range(4):
            pids.append(requests.get("http://localhost:8000/f").json()[0])
        assert pid1 not in pids


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
