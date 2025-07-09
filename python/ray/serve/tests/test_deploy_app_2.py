import logging
import re
import sys
import time
from copy import copy
from functools import partial
from typing import List

import httpx
import pytest

import ray
import ray.actor
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME, SERVE_NAMESPACE
from ray.serve._private.test_utils import (
    check_num_replicas_eq,
)
from ray.serve.schema import (
    ApplicationStatus,
    ServeApplicationSchema,
    ServeDeploySchema,
    ServeInstanceDetails,
)
from ray.serve.tests.test_deploy_app import check_running
from ray.tests.conftest import call_ray_stop_only  # noqa: F401
from ray.util.state import list_actors


def check_log_file(log_file: str, expected_regex: list):
    with open(log_file, "r") as f:
        s = f.read()
        print(s)
        for regex in expected_regex:
            assert re.findall(regex, s) != [], f"Did not find pattern '{regex}' in {s}"
    return True


def check_deployments_dead(deployment_ids: List[DeploymentID]):
    prefixes = [f"{id.app_name}#{id.name}" for id in deployment_ids]
    actor_names = [
        actor["name"] for actor in list_actors(filters=[("state", "=", "ALIVE")])
    ]
    return all(f"ServeReplica::{p}" not in actor_names for p in prefixes)


class TestDeploywithLoggingConfig:
    def get_deploy_config(self, model_within_logging_config: bool = False):
        if model_within_logging_config:
            path = "ray.serve.tests.test_config_files.logging_config_test.model2"
        else:
            path = "ray.serve.tests.test_config_files.logging_config_test.model"
        return {
            "applications": [
                {
                    "name": "app1",
                    "route_prefix": "/app1",
                    "import_path": path,
                },
            ],
        }

    @pytest.mark.parametrize("encoding_type", ["TEXT", "JSON"])
    def test_deploy_app_with_application_logging_config(
        self, serve_instance, encoding_type: str
    ):
        """Deploy application with application logging config"""
        client = serve_instance
        config_dict = self.get_deploy_config()

        config_dict["applications"][0]["logging_config"] = {
            "encoding": encoding_type,
        }
        config = ServeDeploySchema.parse_obj(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200
        )

        resp = httpx.post("http://localhost:8000/app1").json()

        replica_id = resp["replica"].split("#")[-1]
        if encoding_type == "JSON":
            expected_log_regex = [f'"replica": "{replica_id}", ']
        else:
            expected_log_regex = [f".*{replica_id}.*"]
        check_log_file(resp["log_file"], expected_log_regex)

    @pytest.mark.parametrize("encoding_type", ["TEXT", "JSON"])
    def test_deploy_app_with_deployment_logging_config(
        self, serve_instance, encoding_type: str
    ):
        client = serve_instance
        """Deploy application with deployment logging config inside the yaml"""
        config_dict = self.get_deploy_config()

        config_dict["applications"][0]["deployments"] = [
            {
                "name": "Model",
                "logging_config": {
                    "encoding": encoding_type,
                },
            },
        ]
        config = ServeDeploySchema.parse_obj(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200
        )

        resp = httpx.post("http://localhost:8000/app1").json()

        replica_id = resp["replica"].split("#")[-1]
        if encoding_type == "JSON":
            expected_log_regex = [f'"replica": "{replica_id}", ']
        else:
            expected_log_regex = [f".*{replica_id}.*"]
        check_log_file(resp["log_file"], expected_log_regex)

    def test_deployment_logging_config_in_code(self, serve_instance):
        """Deploy application with deployment logging config inside the code"""
        client = serve_instance
        config_dict = self.get_deploy_config(model_within_logging_config=True)
        config = ServeDeploySchema.parse_obj(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200
        )
        resp = httpx.post("http://localhost:8000/app1").json()
        check_log_file(resp["log_file"], [".*this_is_debug_info.*"])

    def test_overwritting_logging_config(self, serve_instance):
        """Overwrite the default logging config with application logging config"""
        client = serve_instance
        config_dict = self.get_deploy_config()
        config = ServeDeploySchema.parse_obj(config_dict)
        client.deploy_apps(config)

        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200
        )

        def get_replica_info_format(replica_id: ReplicaID) -> str:
            app_name = replica_id.deployment_id.app_name
            deployment_name = replica_id.deployment_id.name
            return f"{app_name}_{deployment_name} {replica_id.unique_id}"

        # By default, log level is "INFO"
        r = httpx.post("http://localhost:8000/app1")
        r.raise_for_status()
        request_id = r.headers["X-Request-Id"]
        replica_id = ReplicaID.from_full_id_str(r.json()["replica"])

        # Make sure 'model_debug_level' log content does not exist.
        with pytest.raises(AssertionError):
            check_log_file(r.json()["log_file"], [".*this_is_debug_info.*"])

        # Check the log formatting.
        check_log_file(
            r.json()["log_file"],
            f" {get_replica_info_format(replica_id)} {request_id} ",
        )

        # Set log level to "DEBUG"
        config_dict["applications"][0]["logging_config"] = {
            "log_level": "DEBUG",
        }
        config = ServeDeploySchema.parse_obj(config_dict)
        client.deploy_apps(config)

        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200
            and httpx.post("http://localhost:8000/app1").json()["log_level"]
            == logging.DEBUG,
        )
        r = httpx.post("http://localhost:8000/app1")
        r.raise_for_status()
        request_id = r.headers["X-Request-Id"]
        replica_id = ReplicaID.from_full_id_str(r.json()["replica"])
        check_log_file(
            r.json()["log_file"],
            [
                # Check for DEBUG-level log statement.
                ".*this_is_debug_info.*",
                # Check that the log formatting has remained the same.
                f" {get_replica_info_format(replica_id)} {request_id} ",
            ],
        )

    def test_not_overwritting_logging_config_in_yaml(self, serve_instance):
        """Deployment logging config in yaml should not be overwritten
        by application logging config.
        """
        client = serve_instance
        config_dict = self.get_deploy_config()
        config_dict["applications"][0]["deployments"] = [
            {
                "name": "Model",
                "logging_config": {
                    "log_level": "DEBUG",
                },
            },
        ]
        config_dict["applications"][0]["logging_config"] = {
            "log_level": "INFO",
        }

        config = ServeDeploySchema.parse_obj(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200
        )
        resp = httpx.post("http://localhost:8000/app1").json()
        check_log_file(resp["log_file"], [".*this_is_debug_info.*"])

    def test_not_overwritting_logging_config_in_code(self, serve_instance):
        """Deployment logging config in code should not be overwritten
        by application logging config.
        """
        client = serve_instance
        config_dict = self.get_deploy_config(model_within_logging_config=True)
        config_dict["applications"][0]["logging_config"] = {
            "log_level": "INFO",
        }

        config = ServeDeploySchema.parse_obj(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200
        )
        resp = httpx.post("http://localhost:8000/app1").json()
        check_log_file(resp["log_file"], [".*this_is_debug_info.*"])

    def test_logs_dir(self, serve_instance):
        client = serve_instance
        config_dict = self.get_deploy_config()
        config_dict["applications"][0]["logging_config"] = {
            "log_level": "DEBUG",
        }
        config = ServeDeploySchema.parse_obj(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200
        )
        resp = httpx.get("http://127.0.0.1:8000/app1").json()

        # Construct a new path
        # "/tmp/ray/session_xxx/logs/serve/new_dir"
        paths = resp["log_file"].split("/")
        paths[-1] = "new_dir"
        new_log_dir = "/".join(paths)

        config_dict["applications"][0]["logging_config"] = {
            "log_level": "DEBUG",
            "logs_dir": new_log_dir,
        }
        config = ServeDeploySchema.parse_obj(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200
            and "new_dir" in httpx.get("http://127.0.0.1:8000/app1").json()["log_file"]
        )
        resp = httpx.get("http://127.0.0.1:8000/app1").json()
        # log content should be redirected to new file
        check_log_file(resp["log_file"], [".*this_is_debug_info.*"])

    @pytest.mark.parametrize("enable_access_log", [True, False])
    def test_access_log(self, serve_instance, enable_access_log: bool):
        client = serve_instance
        config_dict = self.get_deploy_config()
        config_dict["applications"][0]["logging_config"] = {
            "enable_access_log": enable_access_log,
        }
        config = ServeDeploySchema.parse_obj(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200
        )
        resp = httpx.get("http://127.0.0.1:8000/app1")
        assert resp.status_code == 200
        resp = resp.json()
        if enable_access_log:
            check_log_file(resp["log_file"], [".*this_is_access_log.*"])
        else:
            with pytest.raises(AssertionError):
                check_log_file(resp["log_file"], [".*this_is_access_log.*"])


def test_deploy_with_no_applications(serve_instance):
    """Deploy an empty list of applications, serve should just be started."""
    client = serve_instance
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
        return "ServeController" in actor_names and "ProxyActor" in actor_names

    wait_for_condition(serve_running)


def test_deployments_not_listed_in_config(serve_instance):
    """Apply a config without the app's deployments listed. The deployments should
    not redeploy.
    """
    client = serve_instance
    config = {
        "applications": [{"import_path": "ray.serve.tests.test_config_files.pid.node"}]
    }
    client.deploy_apps(ServeDeploySchema(**config))
    wait_for_condition(check_running, timeout=15)
    pid1, _ = httpx.get("http://localhost:8000/").json()

    # Redeploy the same config (with no deployments listed)
    client.deploy_apps(ServeDeploySchema(**config))
    wait_for_condition(check_running, timeout=15)

    # It should be the same replica actor
    pids = []
    for _ in range(4):
        pids.append(httpx.get("http://localhost:8000/").json()[0])
    assert all(pid == pid1 for pid in pids)


@pytest.mark.parametrize("rebuild", [True, False])
def test_redeploy_old_config_after_failed_deployment(serve_instance, rebuild):
    """
    1. Deploy application which succeeds.
    2. Redeploy application with an import path that fails.
    3. Redeploy the exact same config from step 1.

    Verify that step 3 succeeds and the application returns to running state.
    """
    client = serve_instance
    app_config = {
        "name": "default",
        "import_path": "ray.serve.tests.test_config_files.world.DagNode",
    }
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))

    def check_application_running():
        status = serve.status().applications["default"]
        assert status.status == "RUNNING"
        assert httpx.post("http://localhost:8000/").text == "wonderful world"
        return True

    wait_for_condition(check_application_running)

    # Change config so that redeploy will error
    new_app_config = copy(app_config)
    if rebuild:
        # New import path will cause an error upon importing app
        new_app_config[
            "import_path"
        ] = "ray.serve.tests.test_config_files.import_error.app"
        err_msg = "ZeroDivisionError"
    else:
        # Set config for a nonexistent deployment
        new_app_config["deployments"] = [{"name": "nonexistent", "num_replicas": 1}]
        err_msg = "Deployment 'nonexistent' does not exist."
    client.deploy_apps(ServeDeploySchema(**{"applications": [new_app_config]}))

    def check_deploy_failed(message):
        status = serve.status().applications["default"]
        assert status.status == "DEPLOY_FAILED"
        assert message in status.message
        return True

    wait_for_condition(check_deploy_failed, message=err_msg)

    # Redeploy old config
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))

    wait_for_condition(check_application_running)


def test_deploy_does_not_affect_dynamic_apps(serve_instance):
    """
    Deploy a set of apps via the declarative API (REST API) and then a dynamic
    app via the imperative API (`serve.run`).

    Check that applying a new config via the declarative API does not affect
    the app deployed using the imperative API.
    """
    client = serve_instance
    config = ServeDeploySchema(
        applications=[
            ServeApplicationSchema(
                name="declarative-app-1",
                route_prefix="/app-1",
                import_path="ray.serve.tests.test_config_files.world.DagNode",
            ),
        ],
    )
    client.deploy_apps(config)

    def check_application_running(
        name: str, route_prefix: str, *, msg: str = "wonderful world"
    ):
        status = serve.status().applications[name]
        assert status.status == "RUNNING"
        assert httpx.post(f"http://localhost:8000{route_prefix}/").text == msg
        return True

    wait_for_condition(
        check_application_running, name="declarative-app-1", route_prefix="/app-1"
    )

    # Now `serve.run` a dynamic app.
    @serve.deployment
    class D:
        def __call__(self, *args) -> str:
            return "Hello!"

    serve.run(D.bind(), name="dynamic-app", route_prefix="/dynamic")
    wait_for_condition(
        check_application_running,
        name="dynamic-app",
        route_prefix="/dynamic",
        msg="Hello!",
    )

    # Add a new app via declarative API.
    # Existing declarative app and dynamic app should not be affected.
    config.applications.append(
        ServeApplicationSchema(
            name="declarative-app-2",
            route_prefix="/app-2",
            import_path="ray.serve.tests.test_config_files.world.DagNode",
        ),
    )
    client.deploy_apps(config)

    wait_for_condition(
        check_application_running, name="declarative-app-2", route_prefix="/app-2"
    )
    wait_for_condition(
        check_application_running, name="declarative-app-1", route_prefix="/app-1"
    )
    wait_for_condition(
        check_application_running,
        name="dynamic-app",
        route_prefix="/dynamic",
        msg="Hello!",
    )

    # Delete one of the apps via declarative API.
    # Other declarative app and dynamic app should not be affected.
    config.applications.pop(0)
    client.deploy_apps(config)

    wait_for_condition(
        check_application_running, name="declarative-app-2", route_prefix="/app-2"
    )
    wait_for_condition(
        check_application_running,
        name="dynamic-app",
        route_prefix="/dynamic",
        msg="Hello!",
    )

    wait_for_condition(lambda: "declarative-app-1" not in serve.status().applications)

    # Now overwrite the declarative app with a dynamic app with the same name.
    # On subsequent declarative apply, that app should not be affected.
    serve.run(D.bind(), name="declarative-app-2", route_prefix="/app-2")
    wait_for_condition(
        check_application_running,
        name="declarative-app-2",
        route_prefix="/app-2",
        msg="Hello!",
    )

    config.applications = [
        ServeApplicationSchema(
            name="declarative-app-1",
            route_prefix="/app-1",
            import_path="ray.serve.tests.test_config_files.world.DagNode",
        ),
    ]
    client.deploy_apps(config)

    wait_for_condition(
        check_application_running,
        name="declarative-app-1",
        route_prefix="/app-1",
    )
    wait_for_condition(
        check_application_running,
        name="dynamic-app",
        route_prefix="/dynamic",
        msg="Hello!",
    )
    wait_for_condition(
        check_application_running,
        name="declarative-app-2",
        route_prefix="/app-2",
        msg="Hello!",
    )

    # Verify that the controller does not delete the dynamic apps on recovery.
    ray.kill(client._controller, no_restart=False)
    wait_for_condition(
        check_application_running,
        name="dynamic-app",
        route_prefix="/dynamic",
        msg="Hello!",
    )
    wait_for_condition(
        check_application_running,
        name="declarative-app-2",
        route_prefix="/app-2",
        msg="Hello!",
    )

    # Now overwrite the dynamic app with a declarative one and check that it gets
    # deleted upon another apply that doesn't include it.
    config.applications = [
        ServeApplicationSchema(
            name="declarative-app-2",
            route_prefix="/app-2",
            import_path="ray.serve.tests.test_config_files.world.DagNode",
        ),
    ]
    client.deploy_apps(config)
    wait_for_condition(
        check_application_running,
        name="declarative-app-2",
        route_prefix="/app-2",
    )

    config.applications = []
    client.deploy_apps(config)

    wait_for_condition(lambda: "declarative-app-2" not in serve.status().applications)


def test_change_route_prefix(serve_instance):
    # Deploy application with route prefix /old
    client = serve_instance
    app_config = {
        "name": "default",
        "route_prefix": "/old",
        "import_path": "ray.serve.tests.test_config_files.pid.node",
    }
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))

    wait_for_condition(check_running)
    pid1 = httpx.get("http://localhost:8000/old").json()[0]

    # Redeploy application with route prefix /new.
    app_config["route_prefix"] = "/new"
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))

    # Check that the old route is gone and the response from the new route
    # has the same PID (replica wasn't restarted).
    def check_switched():
        # Old route should be gone
        resp = httpx.get("http://localhost:8000/old")
        assert "Path '/old' not found." in resp.text

        # Response from new route should be same PID
        pid2 = httpx.get("http://localhost:8000/new").json()[0]
        assert pid2 == pid1
        return True

    wait_for_condition(check_switched)


def test_num_replicas_auto_api(serve_instance):
    """Test setting only `num_replicas="auto"`."""
    client = serve_instance
    config_template = {
        "import_path": "ray.serve.tests.test_config_files.pid.node",
        "deployments": [{"name": "f", "num_replicas": "auto"}],
    }

    client.deploy_apps(ServeDeploySchema.parse_obj({"applications": [config_template]}))
    wait_for_condition(check_running, timeout=15)
    print("Application is RUNNING.")
    check_num_replicas_eq("f", 1)

    app_details = client.get_serve_details()["applications"][SERVE_DEFAULT_APP_NAME]
    deployment_config = app_details["deployments"]["f"]["deployment_config"]
    assert "num_replicas" not in deployment_config
    assert deployment_config["max_ongoing_requests"] == 5
    assert deployment_config["autoscaling_config"] == {
        # Set by `num_replicas="auto"`
        "target_ongoing_requests": 2.0,
        "min_replicas": 1,
        "max_replicas": 100,
        # Untouched defaults
        "look_back_period_s": 30.0,
        "metrics_interval_s": 10.0,
        "upscale_delay_s": 30.0,
        "downscale_delay_s": 600.0,
        "upscale_smoothing_factor": None,
        "downscale_smoothing_factor": None,
        "upscaling_factor": None,
        "downscaling_factor": None,
        "smoothing_factor": 1.0,
        "initial_replicas": None,
    }


def test_num_replicas_auto_basic(serve_instance):
    """Test `num_replicas="auto"` and the default values are used in autoscaling."""
    client = serve_instance
    signal = SignalActor.options(name="signal123").remote()

    config_template = {
        "import_path": "ray.serve.tests.test_config_files.get_signal.app",
        "deployments": [
            {
                "name": "A",
                "num_replicas": "auto",
                "autoscaling_config": {
                    "look_back_period_s": 2.0,
                    "metrics_interval_s": 1.0,
                    "upscale_delay_s": 1.0,
                },
                "graceful_shutdown_timeout_s": 1,
            }
        ],
    }

    print(time.ctime(), "Deploying pid application.")
    client.deploy_apps(ServeDeploySchema.parse_obj({"applications": [config_template]}))
    wait_for_condition(check_running, timeout=15)
    print(time.ctime(), "Application is RUNNING.")
    check_num_replicas_eq("A", 1)

    app_details = client.get_serve_details()["applications"][SERVE_DEFAULT_APP_NAME]
    deployment_config = app_details["deployments"]["A"]["deployment_config"]
    # Set by `num_replicas="auto"`
    assert "num_replicas" not in deployment_config
    assert deployment_config["max_ongoing_requests"] == 5
    assert deployment_config["autoscaling_config"] == {
        # Set by `num_replicas="auto"`
        "target_ongoing_requests": 2.0,
        "min_replicas": 1,
        "max_replicas": 100,
        # Overrided by `autoscaling_config`
        "look_back_period_s": 2.0,
        "metrics_interval_s": 1.0,
        "upscale_delay_s": 1.0,
        # Untouched defaults
        "downscale_delay_s": 600.0,
        "upscale_smoothing_factor": None,
        "downscale_smoothing_factor": None,
        "upscaling_factor": None,
        "downscaling_factor": None,
        "smoothing_factor": 1.0,
        "initial_replicas": None,
    }

    h = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
    for i in range(3):
        [h.remote() for _ in range(2)]

        def check_num_waiters(target: int):
            assert ray.get(signal.cur_num_waiters.remote()) == target
            return True

        wait_for_condition(check_num_waiters, target=2 * (i + 1))
        print(time.time(), f"Number of waiters on signal reached {2*(i+1)}.")
        wait_for_condition(check_num_replicas_eq, name="A", target=i + 1)
        print(time.time(), f"Confirmed number of replicas are at {i+1}.")

    signal.send.remote()


def test_deploy_one_app_failed(serve_instance):
    """Deploy two applications with separate runtime envs."""
    client = serve_instance
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
        lambda: httpx.post("http://localhost:8000/app1").text == "wonderful world"
    )

    wait_for_condition(
        lambda: serve.status().applications["app1"].status == ApplicationStatus.RUNNING
        and serve.status().applications["app2"].status
        == ApplicationStatus.DEPLOY_FAILED
    )

    # Ensure the request doesn't hang and actually returns a 503 error.
    # The timeout is there to prevent the test from hanging and blocking
    # the test suite if it does fail.
    r = httpx.post("http://localhost:8000/app2", timeout=10)
    assert r.status_code == 503 and "unavailable" in r.text


def test_deploy_with_route_prefix_conflict(serve_instance):
    world_import_path = "ray.serve.tests.test_config_files.world.DagNode"
    pizza_import_path = "ray.serve.tests.test_config_files.pizza.serve_dag"
    client = serve_instance
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
        lambda: httpx.get("http://localhost:8000/app1").text == "wonderful world"
    )
    wait_for_condition(
        lambda: httpx.post("http://localhost:8000/app2", json=["ADD", 2]).text
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
        lambda: httpx.get("http://localhost:8000/app1").text == "wonderful world"
    )
    wait_for_condition(
        lambda: httpx.get("http://localhost:8000/app2").text == "wonderful world"
    )


def test_update_config_graceful_shutdown_timeout(serve_instance):
    """Check that replicas stay alive when graceful_shutdown_timeout_s is updated"""
    client = serve_instance

    config_template = {
        "import_path": "ray.serve.tests.test_config_files.pid.node",
        "deployments": [{"name": "f", "graceful_shutdown_timeout_s": 1000}],
    }

    # Deploy first time
    client.deploy_apps(ServeDeploySchema.parse_obj({"applications": [config_template]}))
    wait_for_condition(check_running, timeout=15)
    handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)

    # Start off with signal ready, and send query
    handle.send.remote().result()
    pid1 = handle.remote().result()[0]
    print("PID of replica after first deployment:", pid1)

    # Redeploy with shutdown timeout set to 5 seconds
    config_template["deployments"][0]["graceful_shutdown_timeout_s"] = 5
    client.deploy_apps(ServeDeploySchema.parse_obj({"applications": [config_template]}))
    wait_for_condition(check_running, timeout=15)

    pid2 = handle.remote().result()[0]
    assert pid1 == pid2
    print("PID of replica after redeployment:", pid2)

    # Send blocking query
    handle.send.remote(clear=True)
    handle.remote()
    # Try to delete deployment, should be blocked until the timeout at 5 seconds
    client.delete_apps([SERVE_DEFAULT_APP_NAME], blocking=False)
    # Replica should be dead within 10 second timeout, which means
    # graceful_shutdown_timeout_s was successfully updated lightweightly
    wait_for_condition(partial(check_deployments_dead, [DeploymentID(name="f")]))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
