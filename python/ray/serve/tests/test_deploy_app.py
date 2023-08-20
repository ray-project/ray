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
from ray.tests.conftest import call_ray_stop_only  # noqa: F401


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


@pytest.fixture(scope="module")
def start_and_shutdown_ray_cli_module():
    with start_and_shutdown_ray_cli():
        yield


def _check_ray_stop():
    try:
        requests.get("http://localhost:52365/api/ray/version")
        return False
    except Exception:
        return True


@pytest.fixture(scope="function")
def client(start_and_shutdown_ray_cli_module, shutdown_ray_and_serve):
    wait_for_condition(
        lambda: requests.get("http://localhost:52365/api/ray/version").status_code
        == 200,
        timeout=15,
    )
    ray.init(address="auto", namespace=SERVE_NAMESPACE)
    yield serve.start(detached=True)


def check_running(_client: ServeControllerClient):
    assert (
        serve.status().applications[SERVE_DEFAULT_APP_NAME].status
        == ApplicationStatus.RUNNING
    )
    return True


def check_deployments_dead(deployment_names):
    actor_names = [
        actor["class_name"]
        for actor in list_actors(
            filters=[("state", "=", "ALIVE")],
        )
    ]
    return all(f"ServeReplica:{name}" not in actor_names for name in deployment_names)


def get_num_replicas(_client: ServeControllerClient, deployment_name: str):
    replicas = ray.get(
        _client._controller._dump_replica_states_for_testing.remote(deployment_name)
    )
    running_replicas = replicas.get([ReplicaState.RUNNING])
    return len(running_replicas)


def get_test_config() -> Dict:
    return {"import_path": "ray.serve.tests.test_config_files.pizza.serve_dag"}


def check_single_app():
    """Checks the application deployed through the config from get_test_config()"""
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).text
        == "4 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["MUL", 3]).text
        == "9 pizzas please!"
    )


def get_test_deploy_config() -> Dict:
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


def check_multi_app():
    """
    Checks the applications deployed through the config from
    get_test_deploy_config().
    """

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).text
        == "4 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app1", json=["MUL", 3]).text
        == "9 pizzas please!"
    )

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).text
        == "5 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app2", json=["MUL", 3]).text
        == "12 pizzas please!"
    )


def test_deploy_app_basic(client: ServeControllerClient):
    config = ServeApplicationSchema.parse_obj(get_test_config())
    client.deploy_apps(config)
    check_single_app()


def test_deploy_multi_app_basic(client: ServeControllerClient):
    config = ServeDeploySchema.parse_obj(get_test_deploy_config())
    client.deploy_apps(config)
    check_multi_app()


def test_deploy_app_with_overriden_config(client: ServeControllerClient):
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

    client.deploy_apps(ServeApplicationSchema.parse_obj(config))

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 0]).text
        == "5 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["MUL", 2]).text
        == "8 pizzas please!"
    )


def test_deploy_app_update_config(client: ServeControllerClient):
    config = ServeApplicationSchema.parse_obj(get_test_config())
    client.deploy_apps(config)
    check_single_app()

    config = get_test_config()
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
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).text
        == "1 pizzas please!"
    )


def test_deploy_multi_app_update_config(client: ServeControllerClient):
    config = get_test_deploy_config()
    client.deploy_apps(ServeDeploySchema.parse_obj(config))
    check_multi_app()

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
        lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).text
        == "1 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).text
        == "12 pizzas please!"
    )


def test_deploy_app_update_num_replicas(client: ServeControllerClient):
    config = ServeApplicationSchema.parse_obj(get_test_config())
    client.deploy_apps(config)
    check_single_app()

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

    client.deploy_apps(ServeApplicationSchema.parse_obj(config))

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).text
        == "2 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/", json=["MUL", 3]).text
        == "0 pizzas please!"
    )

    wait_for_condition(
        lambda: serve.status().applications[SERVE_DEFAULT_APP_NAME].status
        == ApplicationStatus.RUNNING,
        timeout=15,
    )

    updated_actors = list_actors(filters=[("state", "=", "ALIVE")])
    assert len(updated_actors) == len(actors) + 3


def test_deploy_multi_app_update_num_replicas(client: ServeControllerClient):
    config = get_test_deploy_config()
    client.deploy_apps(ServeDeploySchema.parse_obj(config))
    check_multi_app()

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
        lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).text
        == "2 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).text
        == "102 pizzas please!"
    )

    wait_for_condition(
        lambda: serve.status().applications["app1"].status == ApplicationStatus.RUNNING,
        timeout=15,
    )
    wait_for_condition(
        lambda: serve.status().applications["app2"].status == ApplicationStatus.RUNNING,
        timeout=15,
    )

    updated_actors = list_actors(filters=[("state", "=", "ALIVE")])
    assert len(updated_actors) == len(actors) + 8


def test_deploy_app_update_timestamp(client: ServeControllerClient):
    assert SERVE_DEFAULT_APP_NAME not in serve.status().applications

    config = ServeApplicationSchema.parse_obj(get_test_config())
    client.deploy_apps(config)

    first_deploy_time = (
        serve.status().applications[SERVE_DEFAULT_APP_NAME].last_deployed_time_s
    )
    assert first_deploy_time > 0

    time.sleep(0.1)

    config = get_test_config()
    config["deployments"] = [
        {
            "name": "Adder",
            "num_replicas": 2,
        },
    ]
    client.deploy_apps(ServeApplicationSchema.parse_obj(config))

    assert (
        serve.status().applications[SERVE_DEFAULT_APP_NAME].last_deployed_time_s
        > first_deploy_time
    )
    assert serve.status().applications[SERVE_DEFAULT_APP_NAME].status in {
        ApplicationStatus.DEPLOYING,
        ApplicationStatus.RUNNING,
    }
    check_single_app()


def test_deploy_multi_app_update_timestamp(client: ServeControllerClient):
    assert "app1" not in serve.status().applications
    assert "app2" not in serve.status().applications

    config = get_test_deploy_config()
    client.deploy_apps(ServeDeploySchema.parse_obj(config))

    first_deploy_time_app1 = serve.status().applications["app1"].last_deployed_time_s
    first_deploy_time_app2 = serve.status().applications["app2"].last_deployed_time_s

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
        serve.status().applications["app1"].last_deployed_time_s
        > first_deploy_time_app1
        and serve.status().applications["app2"].last_deployed_time_s
        > first_deploy_time_app2
    )
    assert {
        serve.status().applications["app1"].status,
        serve.status().applications["app1"].status,
    } <= {
        ApplicationStatus.DEPLOYING,
        ApplicationStatus.RUNNING,
    }
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).text
        == "4 pizzas please!"
    )


def test_deploy_app_overwrite_apps(client: ServeControllerClient):
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
        lambda: requests.post("http://localhost:8000/", json=["ADD", 2]).text
        == "4 pizzas please!"
    )


def test_deploy_multi_app_overwrite_apps(client: ServeControllerClient):
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
        lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).text
        == "4 pizzas please!"
    )

    # Switch the two application import paths
    test_config.applications[0].import_path = pizza_import_path
    test_config.applications[1].import_path = world_import_path
    client.deploy_apps(test_config)

    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).text
        == "4 pizzas please!"
    )
    wait_for_condition(
        lambda: requests.get("http://localhost:8000/app2").text == "wonderful world"
    )


def test_deploy_multi_app_overwrite_apps2(client: ServeControllerClient):
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
        lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).text
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
            assert "app1" not in actor["name"] and "app2" not in actor["name"]
        return True

    # Deployments from app1 and app2 should be deleted
    wait_for_condition(check_dead)

    # App1 and App2 should be gone
    assert requests.get("http://localhost:8000/app1").status_code != 200
    assert (
        requests.post("http://localhost:8000/app2", json=["ADD", 2]).status_code != 200
    )

    # App3 should be up and running
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app3", json=["ADD", 2]).text
        == "5 pizzas please!"
    )


def test_deploy_app_runtime_env(client: ServeControllerClient):
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


def test_deploy_multi_app_deployments_removed(client: ServeControllerClient):
    """Test redeploying applications will remove old deployments."""

    world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
    world_deployments = ["f", "BasicDriver"]
    pizza_import_path = "ray.serve.tests.test_config_files.pizza.serve_dag"
    pizza_deployments = [
        "Adder",
        "Multiplier",
        "Router",
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

    def check_app(deployments):
        # Check that the live deployments and actors are what we expect: exactly the
        # set of deployments in the pizza graph
        actor_names = {
            actor["name"] for actor in list_actors(filters=[("state", "=", "ALIVE")])
        }
        expected_actor_name_prefixes = {
            "SERVE_CONTROLLER_ACTOR:SERVE_PROXY_ACTOR",
            "SERVE_CONTROLLER_ACTOR",
        }.union({f"SERVE_REPLICA::app1_{deployment}" for deployment in deployments})
        for prefix in expected_actor_name_prefixes:
            assert any(name.startswith(prefix) for name in actor_names)

        deployment_replicas = set(
            ray.get(client._controller._all_running_replicas.remote()).keys()
        )
        assert {
            f"app1_{deployment}" for deployment in deployments
        } == deployment_replicas
        return True

    wait_for_condition(check_app, deployments=pizza_deployments)
    wait_for_condition(
        lambda: requests.post("http://localhost:8000/app1", json=["ADD", 2]).text
        == "4 pizzas please!"
    )

    # Redeploy with world graph
    test_config.applications[0].import_path = world_import_path
    client.deploy_apps(test_config)

    wait_for_condition(check_app, deployments=world_deployments)
    wait_for_condition(
        lambda: requests.get("http://localhost:8000/app1").text == "wonderful world"
    )


def test_controller_recover_and_deploy(client: ServeControllerClient):
    """Ensure that in-progress deploy can finish even after controller dies."""

    signal = SignalActor.options(name="signal123").remote()

    config_json = {
        "applications": [
            {
                "name": SERVE_DEFAULT_APP_NAME,
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
    assert SERVE_DEFAULT_APP_NAME not in serve.status().applications


@pytest.mark.parametrize(
    "field_to_update",
    ["import_path", "runtime_env", "ray_actor_options"],
)
def test_deploy_config_update_heavyweight(
    client: ServeControllerClient, field_to_update: str
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
    wait_for_condition(partial(check_running, client), timeout=15)
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
    wait_for_condition(partial(check_running, client), timeout=15)

    pids = []
    for _ in range(4):
        pids.append(requests.get("http://localhost:8000/f").json()[0])
    assert pid1 not in pids


def test_update_config_user_config(client: ServeControllerClient):
    """Check that replicas stay alive when user config is updated."""

    config_template = {
        "import_path": "ray.serve.tests.test_config_files.pid.node",
        "deployments": [{"name": "f", "user_config": {"name": "alice"}}],
    }

    # Deploy first time
    client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
    wait_for_condition(partial(check_running, client), timeout=15)

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


def test_update_config_graceful_shutdown_timeout(client: ServeControllerClient):
    """Check that replicas stay alive when graceful_shutdown_timeout_s is updated"""
    config_template = {
        "import_path": "ray.serve.tests.test_config_files.pid.node",
        "deployments": [{"name": "f", "graceful_shutdown_timeout_s": 1000}],
    }

    # Deploy first time
    client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
    wait_for_condition(partial(check_running, client), timeout=15)
    handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)

    # Start off with signal ready, and send query
    ray.get(handle.send.remote())
    pid1 = ray.get(handle.remote())[0]
    print("PID of replica after first deployment:", pid1)

    # Redeploy with shutdown timeout set to 5 seconds
    config_template["deployments"][0]["graceful_shutdown_timeout_s"] = 5
    client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
    wait_for_condition(partial(check_running, client), timeout=15)

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
    wait_for_condition(partial(check_deployments_dead, ["f"]))


def test_update_config_max_concurrent_queries(client: ServeControllerClient):
    """Check that replicas stay alive when max_concurrent_queries is updated."""

    config_template = {
        "import_path": "ray.serve.tests.test_config_files.pid.node",
        "deployments": [{"name": "f", "max_concurrent_queries": 1000}],
    }

    # Deploy first time, max_concurent_queries set to 1000.
    client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
    wait_for_condition(partial(check_running, client), timeout=15)

    all_replicas = ray.get(client._controller._all_running_replicas.remote())
    assert len(all_replicas) == 1
    assert all_replicas[list(all_replicas.keys())[0]][0].max_concurrent_queries == 1000

    handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)

    responses = ray.get([handle.remote() for _ in range(10)])
    pids1 = {response[0] for response in responses}
    assert len(pids1) == 1

    # Redeploy with max concurrent queries set to 2.
    config_template["deployments"][0]["max_concurrent_queries"] = 2
    client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
    wait_for_condition(partial(check_running, client), timeout=15)

    # Verify that the PID of the replica didn't change.
    responses = ray.get([handle.remote() for _ in range(10)])
    pids2 = {response[0] for response in responses}
    assert pids2 == pids1


def test_update_config_health_check_period(client: ServeControllerClient):
    """Check that replicas stay alive when max_concurrent_queries is updated."""

    config_template = {
        "import_path": "ray.serve.tests.test_config_files.pid.async_node",
        "deployments": [{"name": "f", "health_check_period_s": 100}],
    }

    # Deploy first time, wait for replica running and deployment healthy
    client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
    wait_for_condition(partial(check_running, client), timeout=15)

    handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
    pid1 = ray.get(handle.remote())[0]

    # The health check counter shouldn't increase beyond any initial health checks
    # done as part of the replica startup sequence.
    initial_counter = ray.get(handle.get_counter.remote(health_check=True))
    time.sleep(5)
    assert ray.get(handle.get_counter.remote(health_check=True)) <= initial_counter + 1

    # Update the deployment's health check period to 0.1 seconds.
    config_template["deployments"][0]["health_check_period_s"] = 0.1
    client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
    wait_for_condition(partial(check_running, client), timeout=15)

    # Health check counter should now quickly increase due to the shorter period.
    wait_for_condition(
        lambda: ray.get(handle.get_counter.remote(health_check=True)) >= 30,
        retry_interval_ms=1000,
        timeout=10,
    )

    # Check that it's the same replica (it wasn't torn down to update the config).
    pid2 = ray.get(handle.remote())[0]
    assert pid1 == pid2


def test_update_config_health_check_timeout(client: ServeControllerClient):
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
    wait_for_condition(partial(check_running, client), timeout=15)

    handle = serve.get_deployment_handle("f", SERVE_DEFAULT_APP_NAME)
    pid1 = ray.get(handle.remote())[0]

    # Redeploy with health check timeout reduced to 1 second
    config_template["deployments"][0]["health_check_timeout_s"] = 1
    client.deploy_apps(ServeApplicationSchema.parse_obj(config_template))
    wait_for_condition(partial(check_running, client), timeout=15)

    # Check that it's the same replica, it didn't get teared down
    # (needs to be done before the tests below because the replica will be marked
    # unhealthy then stopped and restarted)
    pid2 = ray.get(handle.remote())[0]
    assert pid1 == pid2

    # Block in health check
    ray.get(handle.send.remote(clear=True, health_check=True))
    wait_for_condition(
        lambda: serve.status()
        .applications[SERVE_DEFAULT_APP_NAME]
        .deployments[name]
        .status
        == DeploymentStatus.UNHEALTHY
    )


def test_deploy_separate_runtime_envs(client: ServeControllerClient):
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


def test_deploy_one_app_failed(client: ServeControllerClient):
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
        lambda: requests.post("http://localhost:8000/app1").text == "wonderful world"
    )

    wait_for_condition(
        lambda: serve.status().applications["app1"].status == ApplicationStatus.RUNNING
        and serve.status().applications["app2"].status
        == ApplicationStatus.DEPLOY_FAILED
    )


def test_deploy_with_route_prefix_conflict(client: ServeControllerClient):
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
        lambda: requests.post("http://localhost:8000/app2", json=["ADD", 2]).text
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


def test_deploy_single_then_multi(client: ServeControllerClient):
    """Deploying single-app then multi-app config should fail."""

    single_app_config = ServeApplicationSchema.parse_obj(get_test_config())
    multi_app_config = ServeDeploySchema.parse_obj(get_test_deploy_config())

    # Deploy single app config
    client.deploy_apps(single_app_config)
    check_single_app()

    # Deploying multi app config afterwards should fail
    with pytest.raises(RayServeException):
        client.deploy_apps(multi_app_config)
    # The original application should still be up and running
    check_single_app()


def test_deploy_multi_then_single(client: ServeControllerClient):
    """Deploying multi-app then single-app config should fail."""

    single_app_config = ServeApplicationSchema.parse_obj(get_test_config())
    multi_app_config = ServeDeploySchema.parse_obj(get_test_deploy_config())

    # Deploy multi app config
    client.deploy_apps(multi_app_config)
    check_multi_app()

    # Deploying single app config afterwards should fail
    with pytest.raises(RayServeException):
        client.deploy_apps(single_app_config)
    # The original applications should still be up and running
    check_multi_app()


def test_deploy_multi_app_deleting(client: ServeControllerClient):
    """Test deleting an application by removing from config."""

    config = ServeDeploySchema.parse_obj(get_test_deploy_config())
    client.deploy_apps(config)
    check_multi_app()

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
            details = ray.get(client._controller.get_serve_instance_details.remote())
            ServeInstanceDetails(**details)
            return (
                details["applications"]["app1"]["status"] == ApplicationStatus.RUNNING
                and "app2" not in details["applications"]
            )
        except Exception:
            info_valid = False

    wait_for_condition(check_app_status)
    # Check that all all details fetched from controller parsed correctly
    assert info_valid


def test_deploy_nonexistent_deployment(client: ServeControllerClient):
    """Apply a config that lists a deployment that doesn't exist in the application.
    The error message should be descriptive.
    """

    config = ServeDeploySchema.parse_obj(get_test_deploy_config())
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


def test_deploy_with_no_applications(client: ServeControllerClient):
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


def test_deployments_not_listed_in_config(client: ServeControllerClient):
    """Apply a config without the app's deployments listed. The deployments should
    not redeploy.
    """

    config = {"import_path": "ray.serve.tests.test_config_files.pid.node"}
    client.deploy_apps(ServeApplicationSchema(**config))
    wait_for_condition(partial(check_running, client), timeout=15)
    pid1, _ = requests.get("http://localhost:8000/f").json()

    # Redeploy the same config (with no deployments listed)
    client.deploy_apps(ServeApplicationSchema(**config))
    wait_for_condition(partial(check_running, client), timeout=15)

    # It should be the same replica actor
    pids = []
    for _ in range(4):
        pids.append(requests.get("http://localhost:8000/f").json()[0])
    assert all(pid == pid1 for pid in pids)


def test_get_app_handle(client: ServeControllerClient):
    config = ServeDeploySchema.parse_obj(get_test_deploy_config())
    client.deploy_apps(config)
    check_multi_app()

    handle_1 = serve.get_app_handle("app1")
    handle_2 = serve.get_app_handle("app2")
    assert ray.get(handle_1.route.remote("ADD", 2)) == "4 pizzas please!"
    assert ray.get(handle_2.route.remote("ADD", 2)) == "5 pizzas please!"


@pytest.mark.parametrize("heavyweight", [True, False])
def test_deploy_lightweight_multiple_route_prefix(
    client: ServeControllerClient, heavyweight: bool
):
    """If user deploys a config that sets route prefix for a non-ingress deployment,
    the deploy should fail.
    """

    config = {
        "applications": [
            {
                "name": "default",
                "import_path": "ray.serve.tests.test_config_files.world.DagNode",
            }
        ]
    }
    client.deploy_apps(ServeDeploySchema(**config))

    def check():
        assert requests.post("http://localhost:8000/").text == "wonderful world"
        return True

    wait_for_condition(check)

    # Add route prefix for non-ingress deployment
    config["applications"][0]["deployments"] = [{"name": "f", "route_prefix": "/"}]
    if heavyweight:
        # Trigger re-build of the application
        config["applications"][0]["runtime_env"] = {"env_vars": {"test": "3"}}
    client.deploy_apps(ServeDeploySchema(**config))

    def check_failed():
        s = serve.status().applications["default"]
        assert s.status == ApplicationStatus.DEPLOY_FAILED
        assert "Found multiple route prefixes" in s.message
        return True

    wait_for_condition(check_failed)

    # Check 10 more times to make sure the status doesn't oscillate
    for _ in range(10):
        s = serve.status().applications["default"]
        assert s.status == ApplicationStatus.DEPLOY_FAILED
        assert "Found multiple route prefixes" in s.message
        time.sleep(0.1)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
