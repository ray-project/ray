import subprocess
import sys
import time
from contextlib import contextmanager
from typing import Dict

import pytest
import requests

import ray
import ray.actor
import ray._private.state
from ray.experimental.state.api import list_actors

from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.common import ApplicationStatus, DeploymentStatus
from ray.serve.schema import ServeApplicationSchema, ServeDeploySchema


def get_test_config() -> Dict:
    return {"import_path": "ray.serve.tests.test_config_files.pizza.serve_dag"}


def get_test_deploy_config() -> Dict:
    return {
        "host": "127.0.0.1",
        "port": 8000,
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


def check_ray_stop():
    try:
        requests.get("http://localhost:52365/api/ray/version")
        return False
    except Exception:
        return True

@pytest.fixture(scope="function")
def serve_client():
    subprocess.check_output(["ray", "stop", "--force"])
    wait_for_condition(
        check_ray_stop,
        timeout=15,
    )
    subprocess.check_output(["ray", "start", "--head"])
    wait_for_condition(
        lambda: requests.get("http://localhost:52365/api/ray/version").status_code
        == 200,
        timeout=15,
    )
    ray.init(address="auto", namespace=SERVE_NAMESPACE)
    yield serve.start(detached=True)
    serve.shutdown()
    ray.shutdown()
    subprocess.check_output(["ray", "stop", "--force"])
    wait_for_condition(
        check_ray_stop,
        timeout=15,
    )


def test_deploy_app_basic(serve_client: ServeControllerClient):
    config = ServeApplicationSchema.parse_obj(get_test_config())
    serve_client.deploy_apps(config)

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
        == "4 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["MUL", 3]).json()
        == "9 pizzas please!"
    )


def test_deploy_multi_app(serve_client: ServeControllerClient):
    config = ServeDeploySchema.parse_obj(get_test_deploy_config())
    serve_client.deploy_apps(config)

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


def test_deploy_app_with_overriden_config(serve_client: ServeControllerClient):

    config = get_test_config()
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

    serve_client.deploy_apps(ServeApplicationSchema.parse_obj(config))

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 0]).json()
        == "5 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["MUL", 2]).json()
        == "8 pizzas please!"
    )


def test_deploy_app_update_config(serve_client: ServeControllerClient):
    config = ServeApplicationSchema.parse_obj(get_test_config())
    serve_client.deploy_apps(config)

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
        == "4 pizzas please!"
    )

    config = get_test_config()
    config["deployments"] = [
        {
            "name": "Adder",
            "user_config": {
                "increment": -1,
            },
        },
    ]

    serve_client.deploy_apps(ServeApplicationSchema.parse_obj(config))

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
        == "1 pizzas please!"
    )


def test_deploy_multi_app_update_config(serve_client: ServeControllerClient):
    config = get_test_deploy_config()
    serve_client.deploy_apps(ServeDeploySchema.parse_obj(config))

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
        == "4 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
        == "5 pizzas please!"
    )

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

    serve_client.deploy_apps(ServeDeploySchema.parse_obj(config))
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
        == "1 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
        == "12 pizzas please!"
    )


def test_deploy_app_update_num_replicas(serve_client: ServeControllerClient):
    config = ServeApplicationSchema.parse_obj(get_test_config())
    serve_client.deploy_apps(config)

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
        == "4 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["MUL", 3]).json()
        == "9 pizzas please!"
    )

    actors = list_actors(filters=[("state", "=", "ALIVE")])

    config = get_test_config()
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

    serve_client.deploy_apps(ServeApplicationSchema.parse_obj(config))

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
        == "2 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["MUL", 3]).json()
        == "0 pizzas please!"
    )

    wait_for_condition(
        lambda: serve_client.get_serve_status().app_status.status
        == ApplicationStatus.RUNNING,
        timeout=15,
    )

    updated_actors = list_actors(filters=[("state", "=", "ALIVE")])
    assert len(updated_actors) == len(actors) + 3


def test_deploy_multi_app_update_num_replicas(serve_client: ServeControllerClient):
    config = get_test_deploy_config()
    serve_client.deploy_apps(ServeDeploySchema.parse_obj(config))

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
        == "4 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
        == "5 pizzas please!"
    )

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

    serve_client.deploy_apps(ServeDeploySchema.parse_obj(config))
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
        == "2 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).json()
        == "102 pizzas please!"
    )

    wait_for_condition(
        lambda: serve_client.get_serve_status("app1").app_status.status
        == ApplicationStatus.RUNNING,
        timeout=15,
    )
    wait_for_condition(
        lambda: serve_client.get_serve_status("app2").app_status.status
        == ApplicationStatus.RUNNING,
        timeout=15,
    )

    updated_actors = list_actors(filters=[("state", "=", "ALIVE")])
    assert len(updated_actors) == len(actors) + 8


def test_deploy_app_update_timestamp(serve_client: ServeControllerClient):
    assert serve_client.get_serve_status().app_status.deployment_timestamp == 0

    config = ServeApplicationSchema.parse_obj(get_test_config())
    serve_client.deploy_apps(config)

    assert serve_client.get_serve_status().app_status.deployment_timestamp > 0

    first_deploy_time = serve_client.get_serve_status().app_status.deployment_timestamp
    time.sleep(0.1)

    config = get_test_config()
    config["deployments"] = [
        {
            "name": "Adder",
            "num_replicas": 2,
        },
    ]
    serve_client.deploy_apps(ServeApplicationSchema.parse_obj(config))

    assert serve_client.get_serve_status().app_status.deployment_timestamp > first_deploy_time
    assert serve_client.get_serve_status().app_status.status in {
        ApplicationStatus.DEPLOYING,
        ApplicationStatus.RUNNING,
    }


def test_deploy_multi_app_update_timestamp(serve_client: ServeControllerClient):
    assert serve_client.get_serve_status("app1").app_status.deployment_timestamp == 0
    assert serve_client.get_serve_status("app2").app_status.deployment_timestamp == 0

    config = get_test_deploy_config()
    serve_client.deploy_apps(ServeDeploySchema.parse_obj(config))

    first_deploy_time_app1 = serve_client.get_serve_status(
        "app1"
    ).app_status.deployment_timestamp
    first_deploy_time_app2 = serve_client.get_serve_status(
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
    serve_client.deploy_apps(ServeDeploySchema.parse_obj(config))

    assert (
        serve_client.get_serve_status("app1").app_status.deployment_timestamp
        > first_deploy_time_app1
        and serve_client.get_serve_status("app2").app_status.deployment_timestamp
        > first_deploy_time_app2
    )
    assert {
        serve_client.get_serve_status("app1").app_status.status,
        serve_client.get_serve_status("app2").app_status.status,
    } <= {
        ApplicationStatus.DEPLOYING,
        ApplicationStatus.RUNNING,
    }


def test_deploy_app_overwrite_apps(serve_client: ServeControllerClient):
    """Check that overwriting a live app with a new one works."""

    # Launch first graph. Its driver's route_prefix should be "/".
    test_config_1 = ServeApplicationSchema.parse_obj(
        {
            "import_path": "ray.serve.tests.test_config_files.world.DagNode",
        }
    )
    serve_client.deploy_apps(test_config_1)

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
    serve_client.deploy_apps(test_config_2)

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
        == "4 pizzas please!"
    )


def test_deploy_multi_app_overwrite_apps(serve_client: ServeControllerClient):
    """Check that overwriting multiple applications works as expected."""

    world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
    pizza_import_path = "ray.serve.tests.test_config_files.pizza.serve_dag"
    test_config = ServeDeploySchema.parse_obj(
        {
            "host": "127.0.0.1",
            "port": 8000,
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
    serve_client.deploy_apps(test_config)

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
    serve_client.deploy_apps(test_config)

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).json()
        == "4 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.get("http://localhost:8000/app2").text == "wonderful world"
    )


def test_deploy_app_runtime_env(serve_client: ServeControllerClient):
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
    serve_client.deploy_apps(config1)

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
    serve_client.deploy_apps(config2)

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
        == "3 pizzas please!"
    )


def test_controller_recover_and_deploy(serve_client: ServeControllerClient):
    """Ensure that in-progress deploy can finish even after controller dies."""

    config = ServeApplicationSchema.parse_obj(get_test_config())
    serve_client.deploy_apps(config)

    # Wait for app to deploy
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
        == "4 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["MUL", 3]).json()
        == "9 pizzas please!"
    )
    deployment_timestamp = serve_client.get_serve_status().app_status.deployment_timestamp

    # Delete all deployments, but don't update config
    serve_client.delete_deployments(
        ["Router", "Multiplier", "Adder", "create_order", "DAGDriver"]
    )

    ray.kill(serve_client._controller, no_restart=False)

    # When controller restarts, it should redeploy config automatically
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).json()
        == "4 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["MUL", 3]).json()
        == "9 pizzas please!"
    )
    assert (
        deployment_timestamp
        == serve_client.get_serve_status().app_status.deployment_timestamp
    )

    serve.shutdown()
    serve_client = serve.start(detached=True)

    # Ensure config checkpoint has been deleted
    assert serve_client.get_serve_status().app_status.deployment_timestamp == 0


@pytest.mark.parametrize(
    "field_to_update,option_to_update,config_update",
    [
        ("import_path", "", False),
        ("runtime_env", "", False),
        ("deployments", "num_replicas", True),
        ("deployments", "autoscaling_config", True),
        ("deployments", "user_config", True),
        ("deployments", "ray_actor_options", False),
    ],
)
def test_deploy_config_update(
    serve_client,
    field_to_update: str,
    option_to_update: str,
    config_update: bool,
):
    """
    Check that replicas stay alive when lightweight config updates are made and
    replicas are torn down when code updates are made.
    """

    def deployment_running():
        serve_status = serve_client.get_serve_status()
        return (
            serve_status.get_deployment_status("f") is not None
            and serve_status.app_status.status == ApplicationStatus.RUNNING
            and serve_status.get_deployment_status("f").status
            == DeploymentStatus.HEALTHY
        )

    config_template = {
        "import_path": "ray.serve.tests.test_config_files.pid.node",
        "deployments": [
            {
                "name": "f",
                "autoscaling_config": None,
                "user_config": None,
                "ray_actor_options": {"num_cpus": 0.1},
            },
        ],
    }

    serve_client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
    wait_for_condition(deployment_running, timeout=15)
    pid1 = requests.get("http://localhost:8000/f").text

    if field_to_update == "import_path":
        config_template["import_path"] = "ray.serve.tests.test_config_files.pid.bnode"
    elif field_to_update == "runtime_env":
        config_template["runtime_env"] = {"env_vars": {"test_var": "test_val"}}
    elif field_to_update == "deployments":
        updated_options = {
            "num_replicas": 2,
            "autoscaling_config": {"max_replicas": 2},
            "user_config": {"name": "bob"},
            "ray_actor_options": {"num_cpus": 0.2},
        }
        config_template["deployments"][0][option_to_update] = updated_options[
            option_to_update
        ]

    serve_client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
    wait_for_condition(deployment_running, timeout=15)

    # This assumes that Serve implements round-robin routing for its replicas. As
    # long as that doesn't change, this test shouldn't be flaky; however if that
    # routing ever changes, this test could become mysteriously flaky
    pids = []
    for _ in range(4):
        pids.append(requests.get("http://localhost:8000/f").text)
    assert (pid1 in pids) == config_update


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
