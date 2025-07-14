import sys
import time
from typing import Dict, List, Union

import httpx
import pytest

import ray
import ray.actor
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.common import DeploymentID, DeploymentStatus
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME, SERVE_NAMESPACE
from ray.serve._private.test_utils import (
    check_num_replicas_gte,
    check_num_replicas_lte,
    get_application_url,
)
from ray.serve.schema import (
    ApplicationStatus,
    ServeDeploySchema,
    ServeInstanceDetails,
)
from ray.serve.tests.common.remote_uris import (
    TEST_DAG_PINNED_URI,
    TEST_RUNTIME_ENV_PINNED_URI,
)
from ray.tests.conftest import call_ray_stop_only  # noqa: F401
from ray.util.state import list_actors


def check_running(app_name: str = SERVE_DEFAULT_APP_NAME):
    assert serve.status().applications[app_name].status == ApplicationStatus.RUNNING
    return True


def check_endpoint(json: Union[List, Dict], expected: str, app_name: str = "default"):
    url = get_application_url("HTTP", app_name=app_name)
    resp = httpx.post(url, json=json)
    assert resp.text == expected
    return True


def check_deploy_failed(app_name: str, message: str):
    status = serve.status().applications[app_name]
    assert status.status == "DEPLOY_FAILED"
    assert message in status.message
    return True


def get_test_config() -> Dict:
    return {"import_path": "ray.serve.tests.test_config_files.pizza.serve_dag"}


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
        check_endpoint,
        json=["ADD", 2],
        expected="4 pizzas please!",
        app_name="app1",
    )
    wait_for_condition(
        check_endpoint,
        json=["MUL", 3],
        expected="9 pizzas please!",
        app_name="app1",
    )

    wait_for_condition(
        check_endpoint,
        json=["ADD", 2],
        expected="5 pizzas please!",
        app_name="app2",
    )
    wait_for_condition(
        check_endpoint,
        json=["MUL", 3],
        expected="12 pizzas please!",
        app_name="app2",
    )


def test_deploy_multi_app_basic(serve_instance):
    client = serve_instance

    config = ServeDeploySchema.parse_obj(get_test_deploy_config())
    client.deploy_apps(config)
    check_multi_app()


def test_two_fastapi_in_one_application(serve_instance):
    client = serve_instance
    config = {
        "applications": [
            {
                "name": "app1",
                "route_prefix": "/app1",
                "import_path": "ray.serve.tests.test_config_files.multi_fastapi.invalid_model",
            }
        ],
    }
    client.deploy_apps(ServeDeploySchema.parse_obj(config))
    wait_for_condition(check_deploy_failed, app_name="app1", message="FastAPI")


def test_deploy_multi_app_update_config(serve_instance):
    client = serve_instance
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
    url = get_application_url("HTTP", app_name="app1")
    wait_for_condition(
        lambda: httpx.post(f"{url}", json=["ADD", 2]).text == "1 pizzas please!"
    )
    url = get_application_url("HTTP", app_name="app2")
    wait_for_condition(
        lambda: httpx.post(f"{url}", json=["ADD", 2]).text == "12 pizzas please!"
    )


def test_deploy_multi_app_update_num_replicas(serve_instance):
    client = serve_instance

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
    url = get_application_url("HTTP", app_name="app1")
    wait_for_condition(
        lambda: httpx.post(f"{url}", json=["ADD", 2]).text == "2 pizzas please!",
    )
    url = get_application_url("HTTP", app_name="app2")
    wait_for_condition(
        lambda: httpx.post(f"{url}", json=["ADD", 2]).text == "102 pizzas please!"
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


def test_deploy_multi_app_update_timestamp(serve_instance):
    client = serve_instance

    assert "app1" not in serve.status().applications
    assert "app2" not in serve.status().applications

    config = get_test_deploy_config()
    client.deploy_apps(ServeDeploySchema.parse_obj(config))
    wait_for_condition(check_running, app_name="app1", timeout=15)
    wait_for_condition(check_running, app_name="app2", timeout=15)

    first_deploy_time_app1 = serve.status().applications["app1"].last_deployed_time_s
    url = get_application_url("HTTP", app_name="app1")
    first_deploy_time_app2 = serve.status().applications["app2"].last_deployed_time_s

    assert first_deploy_time_app1 > 0 and first_deploy_time_app2 > 0

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
    wait_for_condition(check_running, app_name="app1", timeout=15)
    wait_for_condition(check_running, app_name="app2", timeout=15)
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
    url = get_application_url("HTTP", app_name="app1")
    wait_for_condition(
        lambda: httpx.post(f"{url}", json=["ADD", 2]).text == "4 pizzas please!"
    )


def test_deploy_multi_app_overwrite_apps(serve_instance):
    client = serve_instance

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
    wait_for_condition(check_running, app_name="app1", timeout=15)
    wait_for_condition(check_running, app_name="app2", timeout=15)
    url = get_application_url("HTTP", app_name="app1")
    wait_for_condition(lambda: httpx.get(f"{url}").text == "wonderful world")
    url = get_application_url("HTTP", app_name="app2")
    wait_for_condition(
        lambda: httpx.post(f"{url}", json=["ADD", 2]).text == "4 pizzas please!"
    )

    # Switch the two application import paths
    test_config.applications[0].import_path = pizza_import_path
    test_config.applications[1].import_path = world_import_path
    client.deploy_apps(test_config)
    wait_for_condition(check_running, app_name="app1", timeout=15)
    wait_for_condition(check_running, app_name="app2", timeout=15)

    url = get_application_url("HTTP", app_name="app1")
    wait_for_condition(
        lambda: httpx.post(f"{url}", json=["ADD", 2]).text == "4 pizzas please!"
    )
    url = get_application_url("HTTP", app_name="app2")
    wait_for_condition(lambda: httpx.get(f"{url}").text == "wonderful world")


def test_deploy_multi_app_overwrite_apps2(serve_instance):
    """Check that deploying a new set of applications removes old ones."""
    client = serve_instance

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
    wait_for_condition(check_running, app_name="app1", timeout=15)
    wait_for_condition(check_running, app_name="app2", timeout=15)
    url1 = get_application_url("HTTP", app_name="app1")
    wait_for_condition(lambda: httpx.get(f"{url1}").text == "wonderful world")
    url2 = get_application_url("HTTP", app_name="app2")
    wait_for_condition(
        lambda: httpx.post(f"{url2}", json=["ADD", 2]).text == "4 pizzas please!"
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
    wait_for_condition(check_running, app_name="app3", timeout=15)

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
    url1 = get_application_url("HTTP", app_name="app1")
    assert httpx.get(f"{url1}").status_code != 200
    url2 = get_application_url("HTTP", app_name="app2")
    assert httpx.post(f"{url2}", json=["ADD", 2]).status_code != 200

    # App3 should be up and running
    url3 = get_application_url("HTTP", app_name="app3")
    wait_for_condition(
        lambda: httpx.post(f"{url3}", json=["ADD", 2]).text == "5 pizzas please!"
    )


def test_deploy_multi_app_deployments_removed(serve_instance):
    """Test redeploying applications will remove old deployments."""
    client = serve_instance

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
    wait_for_condition(check_running, app_name="app1", timeout=15)
    url = get_application_url("HTTP", app_name="app1")

    def check_app(deployments):
        # Check that the live deployments and actors are what we expect: exactly the
        # set of deployments in the pizza graph
        actor_names = {
            actor["name"] for actor in list_actors(filters=[("state", "=", "ALIVE")])
        }
        expected_actor_name_prefixes = {
            "SERVE_PROXY_ACTOR",
            "SERVE_CONTROLLER_ACTOR",
        }.union({f"SERVE_REPLICA::app1#{deployment}" for deployment in deployments})
        for prefix in expected_actor_name_prefixes:
            assert any(name.startswith(prefix) for name in actor_names)

        assert {
            DeploymentID(name=deployment, app_name="app1") for deployment in deployments
        } == set(ray.get(client._controller._all_running_replicas.remote()).keys())
        return True

    wait_for_condition(check_app, deployments=pizza_deployments)
    wait_for_condition(
        lambda: httpx.post(f"{url}", json=["ADD", 2]).text == "4 pizzas please!"
    )

    # Redeploy with world graph
    test_config.applications[0].import_path = world_import_path
    client.deploy_apps(test_config)
    wait_for_condition(check_running, app_name="app1", timeout=15)
    url = get_application_url("HTTP", app_name="app1")

    wait_for_condition(check_app, deployments=world_deployments)
    wait_for_condition(lambda: httpx.post(f"{url}").text == "wonderful world")


@pytest.mark.parametrize(
    "field_to_update",
    ["import_path", "runtime_env", "ray_actor_options"],
)
def test_deploy_config_update_heavyweight(serve_instance, field_to_update: str):
    """Check that replicas are torn down when code updates are made."""
    client = serve_instance
    config_template = {
        "applications": [
            {
                "name": "default",
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
        ]
    }

    client.deploy_apps(ServeDeploySchema.parse_obj(config_template))
    wait_for_condition(check_running, timeout=15)
    url = get_application_url("HTTP", app_name=SERVE_DEFAULT_APP_NAME)
    pid1, _ = httpx.get(f"{url}").json()

    if field_to_update == "import_path":
        config_template["applications"][0][
            "import_path"
        ] = "ray.serve.tests.test_config_files.pid.dup_node"
    elif field_to_update == "runtime_env":
        config_template["applications"][0]["runtime_env"] = {
            "env_vars": {"test_var": "test_val"}
        }
    elif field_to_update == "ray_actor_options":
        config_template["applications"][0]["deployments"][0]["ray_actor_options"] = {
            "num_cpus": 0.2
        }

    client.deploy_apps(ServeDeploySchema.parse_obj(config_template))
    wait_for_condition(check_running, timeout=15)
    url = get_application_url("HTTP", app_name=SERVE_DEFAULT_APP_NAME)

    pids = []
    for _ in range(4):
        pids.append(httpx.get(f"{url}").json()[0])
    assert pid1 not in pids


def test_update_config_user_config(serve_instance):
    """Check that replicas stay alive when user config is updated."""
    client = serve_instance

    config_template = {
        "import_path": "ray.serve.tests.test_config_files.pid.node",
        "deployments": [{"name": "f", "user_config": {"name": "alice"}}],
    }

    # Deploy first time
    client.deploy_apps(ServeDeploySchema.parse_obj({"applications": [config_template]}))
    wait_for_condition(check_running, timeout=15)
    # Query
    url = get_application_url("HTTP")
    pid1, res = httpx.get(f"{url}/f").json()
    assert res == "alice"

    # Redeploy with updated option
    config_template["deployments"][0]["user_config"] = {"name": "bob"}
    client.deploy_apps(ServeDeploySchema.parse_obj({"applications": [config_template]}))

    # Query
    def check():
        pids = []
        for _ in range(4):
            pid, res = httpx.get(f"{url}/f").json()
            assert res == "bob"
            pids.append(pid)
        assert pid1 in pids
        return True

    wait_for_condition(check)


def test_update_config_max_ongoing_requests(serve_instance):
    """Check that replicas stay alive when max_ongoing_requests is updated."""
    client = serve_instance

    signal = SignalActor.options(name="signal123").remote()

    config_template = {
        "import_path": "ray.serve.tests.test_config_files.get_signal.app",
        "deployments": [{"name": "A"}],
    }
    config_template["deployments"][0]["max_ongoing_requests"] = 1000

    # Deploy first time, max_ongoing_requests set to 1000.
    client.deploy_apps(ServeDeploySchema.parse_obj({"applications": [config_template]}))
    wait_for_condition(check_running, timeout=15)
    handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)

    # Send 10 requests. All of them should be sent to the replica immediately,
    # but the requests should be blocked waiting for the signal
    refs = [handle.remote() for _ in range(10)]
    wait_for_condition(
        lambda: ray.get(signal.cur_num_waiters.remote()) == 10, timeout=2
    )

    signal.send.remote()
    pids = {ref.result() for ref in refs}
    assert len(pids) == 1
    pid1 = pids.pop()

    # Reset for redeployment
    signal.send.remote(clear=True)
    # Redeploy with max concurrent queries set to 5
    config_template["deployments"][0]["max_ongoing_requests"] = 5
    client.deploy_apps(ServeDeploySchema.parse_obj({"applications": [config_template]}))
    wait_for_condition(check_running, timeout=2)

    # Send 10 requests. Only 5 of them should be sent to the replica
    # immediately, and the remaining 5 should queue at the handle.
    refs = [handle.remote() for _ in range(10)]
    with pytest.raises(RuntimeError):
        wait_for_condition(
            lambda: ray.get(signal.cur_num_waiters.remote()) > 5, timeout=2
        )

    signal.send.remote()
    pids = {ref.result() for ref in refs}
    assert pids == {pid1}


def test_update_config_health_check_period(serve_instance):
    """Check that replicas stay alive when max_ongoing_requests is updated."""
    client = serve_instance

    config_template = {
        "import_path": "ray.serve.tests.test_config_files.pid.async_node",
        "deployments": [{"name": "f", "health_check_period_s": 100}],
    }

    # Deploy first time, wait for replica running and deployment healthy
    client.deploy_apps(ServeDeploySchema.parse_obj({"applications": [config_template]}))
    wait_for_condition(check_running, timeout=15)

    handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
    pid1 = handle.remote().result()[0]

    # The health check counter shouldn't increase beyond any initial health checks
    # done as part of the replica startup sequence.
    initial_counter = handle.get_counter.remote(health_check=True).result()
    time.sleep(5)
    assert handle.get_counter.remote(health_check=True).result() <= initial_counter + 1

    # Update the deployment's health check period to 0.1 seconds.
    config_template["deployments"][0]["health_check_period_s"] = 0.1
    client.deploy_apps(ServeDeploySchema.parse_obj({"applications": [config_template]}))
    wait_for_condition(check_running, timeout=15)

    # Health check counter should now quickly increase due to the shorter period.
    wait_for_condition(
        lambda: handle.get_counter.remote(health_check=True).result() >= 30,
        retry_interval_ms=1000,
        timeout=10,
    )

    # Check that it's the same replica (it wasn't torn down to update the config).
    pid2 = handle.remote().result()[0]
    assert pid1 == pid2


def test_update_config_health_check_timeout(serve_instance):
    """Check that replicas stay alive when max_ongoing_requests is updated."""
    client = serve_instance

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
    client.deploy_apps(ServeDeploySchema.parse_obj({"applications": [config_template]}))
    wait_for_condition(check_running, timeout=15)

    handle = serve.get_deployment_handle("f", SERVE_DEFAULT_APP_NAME)
    pid1 = handle.remote().result()[0]

    # Redeploy with health check timeout reduced to 1 second
    config_template["deployments"][0]["health_check_timeout_s"] = 1
    client.deploy_apps(ServeDeploySchema.parse_obj({"applications": [config_template]}))
    wait_for_condition(check_running, timeout=15)

    # Check that it's the same replica, it didn't get teared down
    # (needs to be done before the tests below because the replica will be marked
    # unhealthy then stopped and restarted)
    pid2 = handle.remote().result()[0]
    assert pid1 == pid2

    # Block in health check
    handle.send.remote(clear=True, health_check=True).result()
    wait_for_condition(
        lambda: serve.status()
        .applications[SERVE_DEFAULT_APP_NAME]
        .deployments["f"]
        .status
        == DeploymentStatus.UNHEALTHY
    )


def test_update_autoscaling_config(serve_instance):
    client = serve_instance
    signal = SignalActor.options(name="signal123").remote()

    config_template = {
        "import_path": "ray.serve.tests.test_config_files.get_signal.app",
        "deployments": [
            {
                "name": "A",
                "autoscaling_config": {
                    "target_ongoing_requests": 1,
                    "min_replicas": 1,
                    "max_replicas": 10,
                    "metrics_interval_s": 15,
                    "upscale_delay_s": 0.5,
                    "downscale_delay_s": 0.5,
                    "look_back_period_s": 2,
                },
                "graceful_shutdown_timeout_s": 1,
            }
        ],
    }

    print(time.ctime(), "Deploying pid application.")
    client.deploy_apps(ServeDeploySchema.parse_obj({"applications": [config_template]}))
    wait_for_condition(check_running, timeout=15)
    print(time.ctime(), "Application is RUNNING.")

    print(time.ctime(), "Sending 1 initial unblocked request.")
    h = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
    signal.send.remote()
    h.remote().result()

    print(time.ctime(), "Sending 5 blocked requests. Deployment should NOT scale up.")
    signal.send.remote(clear=True)
    [h.remote() for _ in range(5)]
    with pytest.raises(RuntimeError, match="timeout"):
        wait_for_condition(check_num_replicas_gte, name="A", target=2)

    print(time.ctime(), "Redeploying with `metrics_interval_s` updated to 0.5s.")
    config_template["deployments"][0]["autoscaling_config"]["metrics_interval_s"] = 0.5
    client.deploy_apps(ServeDeploySchema.parse_obj({"applications": [config_template]}))

    wait_for_condition(check_num_replicas_gte, name="A", target=2)
    print(time.ctime(), "Number of replicas scaled up. Unblocking signal.")
    signal.send.remote()

    wait_for_condition(check_num_replicas_lte, name="A", target=1)
    print(time.ctime(), "Number of replicas dropped back down to 1.")


def test_deploy_separate_runtime_envs(serve_instance):
    """Deploy two applications with separate runtime envs."""
    client = serve_instance

    config_template = {
        "applications": [
            {
                "name": "app1",
                "route_prefix": "/app1",
                "import_path": "conditional_dag.serve_dag",
                "runtime_env": {
                    "working_dir": TEST_DAG_PINNED_URI,
                },
            },
            {
                "name": "app2",
                "route_prefix": "/app2",
                "import_path": "hello_world.app",
                "runtime_env": {
                    "working_dir": TEST_RUNTIME_ENV_PINNED_URI,
                },
            },
        ],
    }

    client.deploy_apps(ServeDeploySchema(**config_template))
    wait_for_condition(check_running, app_name="app1", timeout=15)
    wait_for_condition(check_running, app_name="app2", timeout=15)
    wait_for_condition(
        check_endpoint,
        json=["ADD", 2],
        expected="0 pizzas please!",
        app_name="app1",
        timeout=90,
    )
    url = get_application_url("HTTP", app_name="app2")
    wait_for_condition(lambda: httpx.post(f"{url}").text == "Hello world!")


def test_deploy_multi_app_deleting(serve_instance):
    """Test deleting an application by removing from config."""
    client = serve_instance

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


def test_deploy_nonexistent_deployment(serve_instance):
    """Apply a config that lists a deployment that doesn't exist in the application.
    The error message should be descriptive.
    """
    client = serve_instance

    config = ServeDeploySchema.parse_obj(get_test_deploy_config())
    # Change names to invalid names that don't contain "deployment" or "application"
    config.applications[1].name = "random1"
    config.applications[1].deployments[0].name = "random2"
    client.deploy_apps(config)

    def check_app_message():
        details = ray.get(client._controller.get_serve_instance_details.remote())
        # The error message should be descriptive
        # e.g. no deployment "x" in application "y", available deployments: "z"
        message = details["applications"]["random1"]["message"]
        return (
            "Deployment" in message
            and "Available" in message
            and "application" in message
        )

    wait_for_condition(check_app_message)


def test_get_app_handle(serve_instance):
    client = serve_instance
    config = ServeDeploySchema.parse_obj(get_test_deploy_config())
    client.deploy_apps(config)
    check_multi_app()

    handle_1 = serve.get_app_handle("app1")
    handle_2 = serve.get_app_handle("app2")
    assert handle_1.route.remote("ADD", 2).result() == "4 pizzas please!"
    assert handle_2.route.remote("ADD", 2).result() == "5 pizzas please!"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
