import logging
import re
import sys
import threading
import time
from copy import copy
from functools import partial
from typing import List, Tuple

import httpx
import pytest

import ray
import ray.actor
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.constants import (
    RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE,
    SERVE_DEFAULT_APP_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.test_utils import (
    Accumulator,
    check_num_replicas_eq,
    check_num_replicas_gte,
    check_running,
    check_target_groups_ready,
    get_application_url,
)
from ray.serve._private.utils import DEFAULT
from ray.serve.exceptions import DeploymentUnavailableError
from ray.serve.schema import (
    ApplicationStatus,
    ServeApplicationSchema,
    ServeDeploySchema,
    ServeInstanceDetails,
)
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
        config = ServeDeploySchema.model_validate(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200,
            timeout=20,
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
        config = ServeDeploySchema.model_validate(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200,
            timeout=20,
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
        config = ServeDeploySchema.model_validate(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200,
            timeout=20,
        )
        resp = httpx.post("http://localhost:8000/app1").json()
        check_log_file(resp["log_file"], [".*this_is_debug_info.*"])

    def test_overwritting_logging_config(self, serve_instance):
        """Overwrite the default logging config with application logging config"""
        client = serve_instance
        config_dict = self.get_deploy_config()
        config = ServeDeploySchema.model_validate(config_dict)
        client.deploy_apps(config)

        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200,
            timeout=20,
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
        config = ServeDeploySchema.model_validate(config_dict)
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

        config = ServeDeploySchema.model_validate(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200,
            timeout=20,
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

        config = ServeDeploySchema.model_validate(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200,
            timeout=20,
        )
        resp = httpx.post("http://localhost:8000/app1").json()
        check_log_file(resp["log_file"], [".*this_is_debug_info.*"])

    def test_logs_dir(self, serve_instance):
        client = serve_instance
        config_dict = self.get_deploy_config()
        config_dict["applications"][0]["logging_config"] = {
            "log_level": "DEBUG",
        }
        config = ServeDeploySchema.model_validate(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200,
            timeout=20,
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
        config = ServeDeploySchema.model_validate(config_dict)
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
        config = ServeDeploySchema.model_validate(config_dict)
        client.deploy_apps(config)
        wait_for_condition(
            lambda: httpx.post("http://localhost:8000/app1").status_code == 200,
            timeout=20,
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
    config = ServeDeploySchema.model_validate({"applications": []})
    client.deploy_apps(config)

    def serve_running():
        ServeInstanceDetails.model_validate(
            ray.get(client._controller.get_serve_instance_details.remote())
        )
        actors = list_actors(
            filters=[
                ("ray_namespace", "=", SERVE_NAMESPACE),
                ("state", "=", "ALIVE"),
            ]
        )
        actor_names = [actor["class_name"] for actor in actors]
        has_proxy = any("Proxy" in name for name in actor_names)
        return "ServeController" in actor_names and has_proxy

    wait_for_condition(serve_running)


def test_deployments_not_listed_in_config(serve_instance):
    """Apply a config without the app's deployments listed. The deployments should
    not redeploy.
    """
    client = serve_instance
    config = {
        "applications": [{"import_path": "ray.serve.tests.test_config_files.pid.node"}]
    }
    client.deploy_apps(ServeDeploySchema(**config), _blocking=True)
    check_running()
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

    wait_for_condition(check_application_running, timeout=20)

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

    wait_for_condition(check_deploy_failed, message=err_msg, timeout=20)

    # Redeploy old config
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))

    wait_for_condition(check_application_running, timeout=20)


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
    client.deploy_apps(config, _blocking=True)
    check_running(app_name="declarative-app-1")
    url = get_application_url(app_name="declarative-app-1")
    assert httpx.post(url).text == "wonderful world"

    # Now `serve.run` a dynamic app.
    @serve.deployment
    class D:
        def __call__(self, *args) -> str:
            return "Hello!"

    serve.run(D.bind(), name="dynamic-app", route_prefix="/dynamic")
    wait_for_condition(check_running, app_name="dynamic-app")
    url = get_application_url(app_name="dynamic-app")
    assert httpx.post(url).text == "Hello!"

    # Add a new app via declarative API.
    # Existing declarative app and dynamic app should not be affected.
    config.applications.append(
        ServeApplicationSchema(
            name="declarative-app-2",
            route_prefix="/app-2",
            import_path="ray.serve.tests.test_config_files.world.DagNode",
        ),
    )
    client.deploy_apps(config, _blocking=True)
    check_running(app_name="declarative-app-2")
    url = get_application_url(app_name="declarative-app-2")
    assert httpx.post(url).text == "wonderful world"

    url = get_application_url(app_name="declarative-app-1")
    assert httpx.post(url).text == "wonderful world"

    url = get_application_url(app_name="dynamic-app")
    assert httpx.post(url).text == "Hello!"

    # Delete one of the apps via declarative API.
    # Other declarative app and dynamic app should not be affected.
    config.applications.pop(0)
    client.deploy_apps(config)
    wait_for_condition(check_running, app_name="declarative-app-2")
    url = get_application_url(app_name="declarative-app-2")
    assert httpx.post(url).text == "wonderful world"

    url = get_application_url(app_name="dynamic-app")
    assert httpx.post(url).text == "Hello!"

    wait_for_condition(lambda: "declarative-app-1" not in serve.status().applications)

    # Now overwrite the declarative app with a dynamic app with the same name.
    # On subsequent declarative apply, that app should not be affected.
    serve.run(D.bind(), name="declarative-app-2", route_prefix="/app-2")
    wait_for_condition(check_running, app_name="declarative-app-2")
    url = get_application_url(app_name="declarative-app-2")
    assert httpx.post(url).text == "Hello!"

    config.applications = [
        ServeApplicationSchema(
            name="declarative-app-1",
            route_prefix="/app-1",
            import_path="ray.serve.tests.test_config_files.world.DagNode",
        ),
    ]
    client.deploy_apps(config, _blocking=True)
    check_running(app_name="declarative-app-1")
    url = get_application_url(app_name="declarative-app-1")
    assert httpx.post(url).text == "wonderful world"

    wait_for_condition(check_running, app_name="dynamic-app")
    url = get_application_url(app_name="dynamic-app")
    assert httpx.post(url).text == "Hello!"

    wait_for_condition(check_running, app_name="declarative-app-2")
    url = get_application_url(app_name="declarative-app-2")
    assert httpx.post(url).text == "Hello!"

    # Verify that the controller does not delete the dynamic apps on recovery.
    ray.kill(client._controller, no_restart=False)

    wait_for_condition(check_running, app_name="declarative-app-1")
    # It takes some time for the target groups to be ready after controller recovery.
    # So we make sure the target groups are ready before obtaining the URL.
    wait_for_condition(
        check_target_groups_ready, client=client, app_name="declarative-app-1"
    )
    url = get_application_url(app_name="declarative-app-1")
    assert httpx.post(url).text == "wonderful world"

    wait_for_condition(check_running, app_name="dynamic-app")
    wait_for_condition(check_target_groups_ready, client=client, app_name="dynamic-app")
    url = get_application_url(app_name="dynamic-app")
    assert httpx.post(url).text == "Hello!"

    wait_for_condition(check_running, app_name="declarative-app-2")
    wait_for_condition(
        check_target_groups_ready, client=client, app_name="declarative-app-2"
    )
    url = get_application_url(app_name="declarative-app-2")
    assert httpx.post(url).text == "Hello!"

    # Now overwrite the dynamic app with a declarative one and check that it gets
    # deleted upon another apply that doesn't include it.
    config.applications = [
        ServeApplicationSchema(
            name="declarative-app-2",
            route_prefix="/app-2",
            import_path="ray.serve.tests.test_config_files.world.DagNode",
        ),
    ]
    client.deploy_apps(config, _blocking=True)
    check_running(app_name="declarative-app-2")
    url = get_application_url(app_name="declarative-app-2")
    assert httpx.post(url).text == "wonderful world"

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
    client.deploy_apps(
        ServeDeploySchema(**{"applications": [app_config]}), _blocking=True
    )
    check_running()
    url = get_application_url()
    pid1 = httpx.get(url).json()[0]
    # Redeploy application with route prefix /new.
    app_config["route_prefix"] = "/new"
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    wait_for_condition(check_running)
    # Check that the old route is gone and the response from the new route
    # has the same PID (replica wasn't restarted).
    def check_switched():
        # Old route should be gone
        url = get_application_url(exclude_route_prefix=True)
        resp = httpx.get(f"{url}/old")
        assert "Path '/old' not found." in resp.text

        # Response from new route should be same PID
        url = get_application_url(exclude_route_prefix=True)
        pid2 = httpx.get(f"{url}/new").json()[0]
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

    client.deploy_apps(
        ServeDeploySchema.model_validate({"applications": [config_template]})
    )
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
        "downscale_to_zero_delay_s": None,
        "upscale_smoothing_factor": None,
        "downscale_smoothing_factor": None,
        "upscaling_factor": None,
        "downscaling_factor": None,
        "smoothing_factor": 1.0,
        "initial_replicas": None,
        "aggregation_function": "mean",
        "policy": {
            "policy_function": "ray.serve.autoscaling_policy:default_autoscaling_policy",
            "policy_kwargs": {},
        },
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
    client.deploy_apps(
        ServeDeploySchema.model_validate({"applications": [config_template]})
    )
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
        "downscale_to_zero_delay_s": None,
        "upscale_smoothing_factor": None,
        "downscale_smoothing_factor": None,
        "upscaling_factor": None,
        "downscaling_factor": None,
        "smoothing_factor": 1.0,
        "initial_replicas": None,
        "aggregation_function": "mean",
        "policy": {
            "policy_function": "ray.serve.autoscaling_policy:default_autoscaling_policy",
            "policy_kwargs": {},
        },
    }

    h = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
    for i in range(3):
        [h.remote() for _ in range(2)]

        def check_num_waiters(target: int):
            assert ray.get(signal.cur_num_waiters.remote()) == target
            return True

        wait_for_condition(check_num_waiters, target=2 * (i + 1), timeout=30)
        print(time.time(), f"Number of waiters on signal reached {2*(i+1)}.")
        if RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE is False:
            # When merging timeseries from replicas and handles with LOCF, the same request can appear in
            # both because they report at different times (e.g. replica: 4 running, handle: 2 queued).
            # That double-counts requests and inflates the total, biasing aggregations
            # (especially mean) upward and causing over-scaling.
            wait_for_condition(
                check_num_replicas_gte, name="A", target=i + 1, timeout=30
            )
        else:
            wait_for_condition(
                check_num_replicas_eq, name="A", target=i + 1, timeout=30
            )
        print(time.time(), f"Confirmed number of replicas are at {i+1}.")

    ray.get(signal.send.remote())


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
        lambda: httpx.post("http://localhost:8000/app1").text == "wonderful world",
        timeout=20,
    )

    wait_for_condition(
        lambda: serve.status().applications["app1"].status == ApplicationStatus.RUNNING
        and serve.status().applications["app2"].status
        == ApplicationStatus.DEPLOY_FAILED,
        timeout=20,
    )

    # Ensure the request doesn't hang and actually returns a 503 error.
    # The timeout is there to prevent the test from hanging and blocking
    # the test suite if it does fail.
    r = httpx.post("http://localhost:8000/app2", timeout=10)
    assert r.status_code == 503 and "unavailable" in r.text.lower()


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
        lambda: httpx.get("http://localhost:8000/app1").text == "wonderful world",
        timeout=20,
    )
    wait_for_condition(
        lambda: httpx.post("http://localhost:8000/app2", json=["ADD", 2]).text
        == "4 pizzas please!",
        timeout=20,
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

    wait_for_condition(check, timeout=20)

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
    client.deploy_apps(
        ServeDeploySchema.model_validate({"applications": [config_template]})
    )
    wait_for_condition(check_running, timeout=15)
    handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)

    # Start off with signal ready, and send query
    handle.send.remote().result()
    pid1 = handle.remote().result()[0]
    print("PID of replica after first deployment:", pid1)

    # Redeploy with shutdown timeout set to 5 seconds
    config_template["deployments"][0]["graceful_shutdown_timeout_s"] = 5
    client.deploy_apps(
        ServeDeploySchema.model_validate({"applications": [config_template]})
    )
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


@pytest.mark.parametrize("rebuild", [True, False])
def test_failed_rolling_update_keeps_serving_from_old_replicas(serve_instance, rebuild):
    """A failed rolling update keeps serving through surviving old replicas."""
    client = serve_instance
    app_config = {
        "name": "default",
        "import_path": "ray.serve.tests.test_config_files.fail_on_flag.build",
        "deployments": [{"name": "FailOnFlag", "num_replicas": 2}],
    }
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    wait_for_condition(check_running, timeout=60)
    assert httpx.get("http://localhost:8000/").text == "ok"

    failing_config = app_config.copy()
    if rebuild:
        # A new app argument changes the code version: rebuild, then rolling
        # restart of every replica.
        failing_config["args"] = {"fail": True}
    else:
        # A new runtime_env changes the actor options: rolling restart without
        # a rebuild.
        failing_config["deployments"] = [
            {
                "name": "FailOnFlag",
                "num_replicas": 2,
                "ray_actor_options": {
                    "runtime_env": {"env_vars": {"FAIL_ON_INIT": "1"}}
                },
            }
        ]
    client.deploy_apps(ServeDeploySchema(**{"applications": [failing_config]}))

    def check_deploy_failed():
        status = serve.status().applications["default"]
        assert status.status == ApplicationStatus.DEPLOY_FAILED
        deployment = status.deployments["FailOnFlag"]
        assert deployment.status == "DEPLOY_FAILED"
        assert deployment.status_trigger == "REPLICA_STARTUP_FAILED"
        assert deployment.replica_states.get("RUNNING") == 1
        return True

    wait_for_condition(check_deploy_failed, timeout=120)

    # The surviving old replica keeps returning 200.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        r = httpx.get("http://localhost:8000/", timeout=10)
        assert r.status_code == 200 and r.text == "ok"
        time.sleep(0.1)


def _get_retrying_dropped_connection(http: httpx.Client, url: str) -> httpx.Response:
    """GET again if HAProxy dropped the connection before responding: it reloads
    on every replica change during a rolling update, and httpx does not retry."""
    try:
        return http.get(url)
    except httpx.RemoteProtocolError:
        return http.get(url)


@pytest.mark.parametrize("rebuild", [True, False])
def test_rolling_update_chain_with_rollback(serve_instance, rebuild):
    """Traffic survives a healthy update, a downstream failure, and rollback."""
    client = serve_instance

    def config(version, fail=False):
        app = {
            "name": "default",
            "import_path": "ray.serve.tests.test_config_files.rolling_update_chain.build",
            "deployments": [{"name": name, "num_replicas": 2} for name in ("D1", "D2")],
        }
        if rebuild:
            app["args"] = {"version": version, "fail": fail}
        else:
            for deployment in app["deployments"]:
                deployment["ray_actor_options"] = {
                    "runtime_env": {
                        "env_vars": {
                            "TEST_VERSION": version,
                            "FAIL_ON_INIT": "1"
                            if fail and deployment["name"] == "D2"
                            else "0",
                        }
                    }
                }
        return ServeDeploySchema(applications=[app])

    def check_healthy(version):
        app = serve.status().applications["default"]
        assert app.status == ApplicationStatus.RUNNING
        assert set(app.deployments) == {"D1", "D2"}
        for deployment in app.deployments.values():
            assert deployment.status == "HEALTHY"
            assert deployment.replica_states.get("RUNNING") == 2
        response = httpx.get("http://localhost:8000/", timeout=10)
        assert response.status_code == 200 and response.text == version
        return True

    client.deploy_apps(config("v1"))
    wait_for_condition(check_healthy, version="v1", timeout=120)

    stop = threading.Event()
    started = threading.Event()
    errors = []
    responses = []

    def send_requests():
        with httpx.Client(timeout=10) as http:
            while not stop.is_set():
                try:
                    response = _get_retrying_dropped_connection(
                        http, "http://localhost:8000/"
                    )
                    assert response.status_code == 200, response.text
                    assert response.text in {"v1", "v2"}, response.text
                    responses.append(response.text)
                except Exception as exc:
                    errors.append(repr(exc))
                finally:
                    started.set()
                stop.wait(0.005)

    traffic = threading.Thread(target=send_requests, daemon=True)
    traffic.start()
    try:
        assert started.wait(timeout=15)
        healthy_config = config("v2")
        client.deploy_apps(healthy_config)
        wait_for_condition(check_healthy, version="v2", timeout=120)

        client.deploy_apps(config("v3", fail=True))

        def check_failed():
            app = serve.status().applications["default"]
            assert app.status == ApplicationStatus.DEPLOY_FAILED
            downstream = app.deployments["D2"]
            assert downstream.status == "DEPLOY_FAILED"
            assert downstream.status_trigger == "REPLICA_STARTUP_FAILED"
            assert downstream.replica_states.get("RUNNING") == 1
            return True

        wait_for_condition(check_failed, timeout=120)
        # Keep traffic running after failure, before an external caller rolls back.
        before = len(responses)
        time.sleep(5)
        assert len(responses) > before
        assert not errors, errors

        client.deploy_apps(healthy_config)
        wait_for_condition(check_healthy, version="v2", timeout=120)
        before = len(responses)
        time.sleep(5)
        assert len(responses) > before
    finally:
        stop.set()
        traffic.join(timeout=15)

    assert not traffic.is_alive()
    assert not errors, errors
    assert {"v1", "v2"}.issubset(responses)
    # The faulty version's downstream never starts, so it must never answer.
    assert "v3" not in responses


def _deployment_details(client, deployment: str = "FailOnFlag"):
    details = ServeInstanceDetails(
        **ray.get(client._controller.get_serve_instance_details.remote())
    )
    return details.applications["default"].deployments[deployment]


def _running_replica_pids(client, deployment: str = "FailOnFlag") -> List[int]:
    replicas = _deployment_details(client, deployment).replicas
    return sorted(r.pid for r in replicas if r.state == "RUNNING")


def _assert_rollout_stays_stopped(client, old_pids: List[int], seconds: float = 10):
    """Check that only old_pids serve requests, with no replacements, for seconds."""
    deadline = time.monotonic() + seconds

    def check_stays_stopped():
        status = serve.status().applications["default"]
        assert status.status == ApplicationStatus.DEPLOY_FAILED
        replica_states = status.deployments["FailOnFlag"].replica_states
        assert "STARTING" not in replica_states, replica_states
        assert "STOPPING" not in replica_states, replica_states
        assert _running_replica_pids(client) == old_pids
        r = httpx.get("http://localhost:8000/", timeout=10)
        assert r.status_code == 200 and r.text == "ok"
        return time.monotonic() >= deadline

    # Observe the full interval and fail immediately if any invariant breaks.
    wait_for_condition(
        check_stays_stopped,
        timeout=seconds + 15,
        retry_interval_ms=500,
        raise_exceptions=True,
    )


def test_flapping_rolling_update_stops_consuming_old_replicas(serve_instance):
    """Health check failures stop a rolling update before it replaces all old replicas."""
    client = serve_instance
    deployment = {
        "name": "FailOnFlag",
        "num_replicas": 5,
        "health_check_period_s": 0.1,
    }
    app_config = {
        "name": "default",
        "import_path": "ray.serve.tests.test_config_files.fail_on_flag.build",
        "deployments": [deployment],
    }
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    wait_for_condition(check_running, timeout=60)
    initial_pids = _running_replica_pids(client)
    assert len(initial_pids) == 5

    failing_config = copy(app_config)
    failing_config["deployments"] = [
        {
            **deployment,
            "ray_actor_options": {
                "runtime_env": {"env_vars": {"FAIL_HEALTH_CHECK": "1"}}
            },
        }
    ]
    client.deploy_apps(ServeDeploySchema(**{"applications": [failing_config]}))

    def check_deploy_failed():
        status = serve.status().applications["default"]
        assert status.status == ApplicationStatus.DEPLOY_FAILED
        assert status.deployments["FailOnFlag"].status == "DEPLOY_FAILED"
        return True

    wait_for_condition(check_deploy_failed, timeout=60)

    # Let pending replacements settle. A replica of the flapping version passes
    # its first health check, so it is RUNNING until the next ones fail and it
    # is stopped; the rollout is terminal, so it is never replaced. Wait until
    # only old replicas remain rather than sampling mid-flap.
    def check_settled():
        replica_states = (
            serve.status()
            .applications["default"]
            .deployments["FailOnFlag"]
            .replica_states
        )
        assert set(replica_states) == {"RUNNING"}, replica_states
        running = _running_replica_pids(client)
        assert set(running) <= set(initial_pids), (running, initial_pids)
        return True

    wait_for_condition(check_settled, timeout=60)
    surviving_pids = _running_replica_pids(client)
    # The fixture's threshold of 3 failures bounds how many old replicas the
    # flapping version could replace before the update stopped.
    assert set(surviving_pids) <= set(initial_pids)
    assert len(surviving_pids) >= 2
    _assert_rollout_stays_stopped(client, surviving_pids)


def test_terminally_failed_rolling_update_survives_controller_restart(
    serve_instance,
):
    """A controller restart preserves a failed update and its surviving old replicas."""
    client = serve_instance
    app_config = {
        "name": "default",
        "import_path": "ray.serve.tests.test_config_files.fail_on_flag.build",
        "deployments": [{"name": "FailOnFlag", "num_replicas": 2}],
    }
    client.deploy_apps(ServeDeploySchema(**{"applications": [app_config]}))
    wait_for_condition(check_running, timeout=60)

    failing_config = copy(app_config)
    failing_config["deployments"] = [
        {
            "name": "FailOnFlag",
            "num_replicas": 2,
            "ray_actor_options": {"runtime_env": {"env_vars": {"FAIL_ON_INIT": "1"}}},
        }
    ]
    client.deploy_apps(ServeDeploySchema(**{"applications": [failing_config]}))

    def check_deploy_failed():
        status = serve.status().applications["default"]
        assert status.status == ApplicationStatus.DEPLOY_FAILED
        deployment = status.deployments["FailOnFlag"]
        assert deployment.status_trigger == "REPLICA_STARTUP_FAILED"
        assert set(deployment.replica_states) == {"RUNNING"}
        assert deployment.replica_states["RUNNING"] == 1
        return True

    wait_for_condition(check_deploy_failed, timeout=60)
    old_pids = _running_replica_pids(client)
    assert len(old_pids) == 1

    old_controller_pid = ray.get(client._controller.get_pid.remote())
    ray.kill(client._controller, no_restart=False)
    # A status request immediately after ray.kill can reach the old process.
    wait_for_condition(
        lambda: ray.get(client._controller.get_pid.remote()) != old_controller_pid
    )
    wait_for_condition(check_deploy_failed, timeout=60)
    assert _running_replica_pids(client) == old_pids
    _assert_rollout_stays_stopped(client, old_pids)
    # The restarted controller never retried the failed version: the dead
    # replica list (in memory, empty after the restart) stays empty.
    assert _deployment_details(client).recent_dead_replicas == []


def _rolling_update_config(
    *deployments: dict,
    import_path: str = "ray.serve.tests.test_config_files.fail_on_flag.build",
) -> ServeDeploySchema:
    """Config for the default app; no deployments means no config overrides."""
    app = {"name": SERVE_DEFAULT_APP_NAME, "import_path": import_path}
    if deployments:
        app["deployments"] = list(deployments)
    return ServeDeploySchema(applications=[app])


def _config_options(client, deployment: str = "FailOnFlag") -> Tuple[int, int]:
    config = _deployment_details(client, deployment).deployment_config
    return config.num_replicas, config.max_ongoing_requests


def _autoscaling_config(client, deployment: str = "FailOnFlag"):
    config = _deployment_details(client, deployment).deployment_config
    # Details report an unset option as the DEFAULT sentinel rather than None.
    return (
        None
        if config.autoscaling_config is DEFAULT.VALUE
        else config.autoscaling_config
    )


def _check_terminal_rolling_update(client, running=None):
    app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
    assert app.status == ApplicationStatus.DEPLOY_FAILED
    deployment = _deployment_details(client)
    assert deployment.status == "DEPLOY_FAILED"
    # Health failures can report DEPLOY_FAILED before the retry budget is spent.
    assert "The update is stopped" in deployment.message
    if running is not None:
        assert len(deployment.replicas) == running
        assert all(r.state == "RUNNING" for r in deployment.replicas)
    return True


def test_delete_app_with_terminally_failed_rolling_update(serve_instance):
    client = serve_instance
    healthy = {"name": "FailOnFlag", "num_replicas": 2}
    client.deploy_apps(_rolling_update_config(healthy))
    wait_for_condition(check_running, timeout=60)
    failing = {
        **healthy,
        "ray_actor_options": {"runtime_env": {"env_vars": {"FAIL_ON_INIT": "1"}}},
    }
    client.deploy_apps(_rolling_update_config(failing))
    wait_for_condition(
        _check_terminal_rolling_update, client=client, running=1, timeout=60
    )

    client.delete_apps([SERVE_DEFAULT_APP_NAME])
    assert SERVE_DEFAULT_APP_NAME not in serve.status().applications
    wait_for_condition(
        check_deployments_dead,
        deployment_ids=[DeploymentID("FailOnFlag", SERVE_DEFAULT_APP_NAME)],
    )


def test_num_replicas_change_does_not_restart_failed_rolling_update(serve_instance):
    client = serve_instance
    healthy = {
        "name": "FailOnFlag",
        "num_replicas": 3,
        "ray_actor_options": {"runtime_env": {"env_vars": {"FAIL_ON_INIT": "0"}}},
    }
    client.deploy_apps(_rolling_update_config(healthy))
    wait_for_condition(check_running, timeout=60)
    initial_pids = _running_replica_pids(client)
    failing = {
        **healthy,
        "ray_actor_options": {"runtime_env": {"env_vars": {"FAIL_ON_INIT": "1"}}},
    }
    client.deploy_apps(_rolling_update_config(failing))
    wait_for_condition(
        _check_terminal_rolling_update, client=client, running=2, timeout=60
    )
    survivors = _running_replica_pids(client)
    assert set(survivors) < set(initial_pids)
    dead_ids = {r.replica_id for r in _deployment_details(client).recent_dead_replicas}

    client.deploy_apps(_rolling_update_config({**failing, "num_replicas": 5}))
    wait_for_condition(
        lambda: _deployment_details(client).target_num_replicas == 5, timeout=60
    )
    _assert_rollout_stays_stopped(client, survivors)
    assert {
        r.replica_id for r in _deployment_details(client).recent_dead_replicas
    } == dead_ids

    # Changing the actual version still permits a complete rollout.
    client.deploy_apps(_rolling_update_config({**healthy, "num_replicas": 5}))
    wait_for_condition(check_running, timeout=60)
    assert len(_running_replica_pids(client)) == 5
    assert httpx.get("http://localhost:8000/").text == "ok"


@pytest.mark.parametrize("gang_size", [2, 3])
def test_gang_rolling_update_stops_after_failed_gangs(serve_instance, gang_size):
    client = serve_instance
    failures = Accumulator.options(name="failed-gangs").remote()
    try:
        healthy = {
            "name": "FailOnFlag",
            "num_replicas": 3 * gang_size,
            "gang_scheduling_config": {"gang_size": gang_size},
            "ray_actor_options": {"num_cpus": 0.1},
        }
        client.deploy_apps(_rolling_update_config(healthy))
        wait_for_condition(check_running, timeout=60)
        initial_pids = _running_replica_pids(client)
        failing = {
            **healthy,
            "ray_actor_options": {
                "num_cpus": 0.1,
                "runtime_env": {
                    "env_vars": {
                        "FAIL_ON_INIT": "1",
                        "RECORD_FAILED_GANGS": "failed-gangs",
                    }
                },
            },
        }
        client.deploy_apps(_rolling_update_config(failing))
        wait_for_condition(
            _check_terminal_rolling_update,
            client=client,
            running=2 * gang_size,
            timeout=90,
        )
        # All failing members record their gang, but the retry budget is per gang.
        assert len(set(ray.get(failures.get.remote()))) == 3
        assert "failed to start 3 times" in _deployment_details(client).message
        survivors = _running_replica_pids(client)
        assert set(survivors) < set(initial_pids)
        _assert_rollout_stays_stopped(client, survivors)
        assert len(set(ray.get(failures.get.remote()))) == 3
    finally:
        client.delete_apps([SERVE_DEFAULT_APP_NAME])
        ray.kill(failures)


def test_runtime_env_failure_survives_controller_restart(serve_instance):
    """Real allocation failure before the constructor survives controller recovery.

    The synchronous scheduler-failure checkpoint boundary is covered separately
    by test_scheduling_failure_is_checkpointed_before_next_update.
    """
    client = serve_instance
    healthy = {"name": "FailOnFlag", "num_replicas": 2}
    client.deploy_apps(_rolling_update_config(healthy))
    wait_for_condition(check_running, timeout=60)
    initial_pids = _running_replica_pids(client)
    failing = {
        **healthy,
        "ray_actor_options": {
            "runtime_env": {
                "pip": {
                    "packages": ["ray-serve-deliberately-nonexistent-package==0.0.0"],
                    "pip_install_options": [
                        "--no-index",
                        "--disable-pip-version-check",
                    ],
                }
            }
        },
    }
    client.deploy_apps(_rolling_update_config(failing))
    wait_for_condition(
        _check_terminal_rolling_update, client=client, running=1, timeout=90
    )
    assert (
        "ray-serve-deliberately-nonexistent-package"
        in _deployment_details(client).message
    )
    survivors = _running_replica_pids(client)
    assert set(survivors) < set(initial_pids)

    old_pid = ray.get(client._controller.get_pid.remote())
    ray.kill(client._controller, no_restart=False)
    wait_for_condition(lambda: ray.get(client._controller.get_pid.remote()) != old_pid)
    wait_for_condition(
        _check_terminal_rolling_update, client=client, running=1, timeout=60
    )
    _assert_rollout_stays_stopped(client, survivors)
    assert _deployment_details(client).recent_dead_replicas == []


@pytest.mark.parametrize("min_replicas", [0, 1])
def test_autoscaling_after_terminal_rolling_update(
    serve_instance_with_signal, min_replicas
):
    client, signal = serve_instance_with_signal
    healthy = {
        "name": "FailOnFlag",
        "autoscaling_config": {
            "min_replicas": min_replicas,
            "initial_replicas": 3,
            "max_replicas": 4,
            "target_ongoing_requests": 1,
            "upscale_delay_s": 0,
            "downscale_delay_s": 5,
            "downscale_to_zero_delay_s": 5,
            "metrics_interval_s": 0.1,
            "look_back_period_s": 1,
        },
        "graceful_shutdown_timeout_s": 60,
        "ray_actor_options": {"runtime_env": {"env_vars": {"BLOCK_ON_SIGNAL": "1"}}},
    }
    client.deploy_apps(_rolling_update_config(healthy))
    wait_for_condition(check_running, timeout=60)
    handle = serve.get_app_handle(SERVE_DEFAULT_APP_NAME)
    responses = [handle.remote() for _ in range(8)]
    try:
        wait_for_condition(lambda: len(_running_replica_pids(client)) == 4, timeout=60)
        initial_pids = _running_replica_pids(client)
        failing = {
            **healthy,
            "ray_actor_options": {
                "runtime_env": {
                    "env_vars": {"BLOCK_ON_SIGNAL": "1", "FAIL_ON_INIT": "1"}
                }
            },
        }
        client.deploy_apps(_rolling_update_config(failing))
        wait_for_condition(_check_terminal_rolling_update, client=client, timeout=60)
        survivors = _running_replica_pids(client)
        assert 0 < len(survivors) < len(initial_pids)
        assert set(survivors) < set(initial_pids)
        # Verify the autoscaler actually requests more than the survivors.
        wait_for_condition(lambda: _deployment_details(client).target_num_replicas == 4)
        deadline = time.monotonic() + 5

        def check_upscale_stays_blocked():
            _check_terminal_rolling_update(client)
            assert _running_replica_pids(client) == survivors
            assert all(
                r.state != "STARTING" for r in _deployment_details(client).replicas
            )
            return time.monotonic() >= deadline

        wait_for_condition(
            check_upscale_stays_blocked, raise_exceptions=True, timeout=15
        )
        ray.get(signal.send.remote())
        assert [r.result(timeout_s=30) for r in responses] == ["ok"] * 8
        wait_for_condition(
            _check_terminal_rolling_update,
            client=client,
            running=min_replicas,
            timeout=90,
        )
        assert _deployment_details(client).target_num_replicas == min_replicas
        assert set(_running_replica_pids(client)) <= set(survivors)

        if min_replicas == 0:
            # Requests cannot wake the failed version after scale-to-zero.
            with pytest.raises(DeploymentUnavailableError):
                handle.remote().result(timeout_s=10)
            _check_terminal_rolling_update(client, running=0)

        # A working version can recover even after the failed version scaled to zero.
        client.deploy_apps(_rolling_update_config(healthy))
        wait_for_condition(check_running, timeout=60)
        assert handle.remote().result(timeout_s=60) == "ok"
    finally:
        ray.get(signal.send.remote())
        for response in responses:
            response.cancel()


def test_instance_details_report_restoring_unset_options(serve_instance):
    """The controller reports that removing overrides restores the original values."""
    client = serve_instance
    details = ray.get(client._controller.get_serve_instance_details.remote())
    assert details["restores_unset_config_options"] is True


@pytest.mark.parametrize("restart_controller", [False, True])
def test_sparse_config_rollback_restores_code_defined_options(
    serve_instance, restart_controller
):
    """Rollback restores values from the decorator and reuses surviving replicas.

    Traffic runs from before the failing update until after the rollback and
    must never see a non-200 response.
    """
    client = serve_instance
    client.deploy_apps(_rolling_update_config())
    wait_for_condition(check_running, timeout=60)
    initial_pids = _running_replica_pids(client)
    assert len(initial_pids) == 2
    assert _config_options(client) == (2, 7)

    stop = threading.Event()
    started = threading.Event()
    status_codes = []
    errors = []

    def send_requests():
        with httpx.Client(timeout=10) as http:
            while not stop.is_set():
                try:
                    status_codes.append(
                        _get_retrying_dropped_connection(
                            http, "http://localhost:8000/"
                        ).status_code
                    )
                except Exception as exc:
                    errors.append(repr(exc))
                finally:
                    started.set()
                stop.wait(0.005)

    def check_deploy_failed():
        status = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        assert status.status == ApplicationStatus.DEPLOY_FAILED
        deployment = status.deployments["FailOnFlag"]
        assert deployment.status_trigger == "REPLICA_STARTUP_FAILED"
        assert set(deployment.replica_states) == {"RUNNING"}
        return True

    traffic = threading.Thread(target=send_requests, daemon=True)
    traffic.start()
    try:
        assert started.wait(timeout=15)
        # The runtime environment override makes new replicas fail to start.
        client.deploy_apps(
            _rolling_update_config(
                {
                    "name": "FailOnFlag",
                    "max_ongoing_requests": 3,
                    "ray_actor_options": {
                        "runtime_env": {"env_vars": {"FAIL_ON_INIT": "1"}}
                    },
                }
            )
        )
        wait_for_condition(check_deploy_failed, timeout=60)
        surviving_pids = _running_replica_pids(client)
        assert len(surviving_pids) == 1 and set(surviving_pids) <= set(initial_pids)
        assert _config_options(client) == (2, 3)

        if restart_controller:
            old_pid = ray.get(client._controller.get_pid.remote())
            ray.kill(client._controller, no_restart=False)
            wait_for_condition(
                lambda: ray.get(client._controller.get_pid.remote()) != old_pid
            )
            wait_for_condition(check_deploy_failed, timeout=60)

        # Removing both overrides reuses the survivor and replaces the missing
        # replica.
        client.deploy_apps(_rolling_update_config())
        wait_for_condition(check_running, timeout=60)
        wait_for_condition(lambda: len(_running_replica_pids(client)) == 2, timeout=60)
        assert set(surviving_pids) <= set(_running_replica_pids(client))
        assert _config_options(client) == (2, 7)
        served_before_rollback = len(status_codes)
        wait_for_condition(lambda: len(status_codes) > served_before_rollback + 10)
    finally:
        stop.set()
        traffic.join(timeout=15)

    assert not traffic.is_alive()
    assert not errors, errors
    assert set(status_codes) == {200}, {
        code: status_codes.count(code) for code in set(status_codes)
    }


def test_new_config_restores_only_the_overrides_it_drops(serve_instance):
    """Options the new config still sets update; the ones it drops return to code."""
    client = serve_instance
    client.deploy_apps(_rolling_update_config())
    wait_for_condition(check_running, timeout=60)
    assert _config_options(client) == (2, 7)

    client.deploy_apps(
        _rolling_update_config(
            {"name": "FailOnFlag", "num_replicas": 5, "max_ongoing_requests": 3}
        )
    )
    wait_for_condition(lambda: _config_options(client) == (5, 3), timeout=60)
    wait_for_condition(check_running, timeout=60)
    wait_for_condition(lambda: len(_running_replica_pids(client)) == 5, timeout=60)

    client.deploy_apps(
        _rolling_update_config({"name": "FailOnFlag", "max_ongoing_requests": 4})
    )
    wait_for_condition(lambda: _config_options(client) == (2, 4), timeout=60)
    wait_for_condition(check_running, timeout=60)
    wait_for_condition(lambda: len(_running_replica_pids(client)) == 2, timeout=60)
    assert httpx.get("http://localhost:8000/", timeout=10).text == "ok"


def test_removing_autoscaling_config_restores_static_num_replicas(serve_instance):
    """Dropping autoscaling_config stops autoscaling and restores num_replicas."""
    client = serve_instance
    deployment_id = DeploymentID("FailOnFlag", SERVE_DEFAULT_APP_NAME)

    def autoscaled() -> bool:
        return ray.get(
            client._controller._should_autoscale_deployment_for_testing.remote(
                deployment_id
            )
        )

    client.deploy_apps(_rolling_update_config())
    wait_for_condition(check_running, timeout=60)
    assert _autoscaling_config(client) is None
    assert not autoscaled()

    client.deploy_apps(
        _rolling_update_config(
            {
                "name": "FailOnFlag",
                "autoscaling_config": {"min_replicas": 1, "max_replicas": 5},
            }
        )
    )
    wait_for_condition(lambda: _autoscaling_config(client) is not None, timeout=60)
    wait_for_condition(check_running, timeout=60)
    assert autoscaled()

    client.deploy_apps(_rolling_update_config())
    wait_for_condition(lambda: _autoscaling_config(client) is None, timeout=60)
    wait_for_condition(check_running, timeout=60)
    wait_for_condition(lambda: len(_running_replica_pids(client)) == 2, timeout=60)
    assert _config_options(client) == (2, 7)
    assert not autoscaled()


def test_multi_deployment_overrides_revert_independently(serve_instance):
    """Each deployment reverts only the overrides its own config entry drops."""
    client = serve_instance
    chain = "ray.serve.tests.test_config_files.rolling_update_chain.build"
    code_defined = {"D1": (1, 11), "D2": (2, 13)}
    d1_override = {"name": "D1", "num_replicas": 2, "max_ongoing_requests": 3}
    d2_override = {"name": "D2", "num_replicas": 3, "max_ongoing_requests": 4}

    def check(expected):
        app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        assert app.status == ApplicationStatus.RUNNING
        for name, (num_replicas, max_ongoing_requests) in expected.items():
            assert _config_options(client, name) == (num_replicas, max_ongoing_requests)
            assert len(_running_replica_pids(client, name)) == num_replicas
        assert httpx.get("http://localhost:8000/", timeout=10).text == "v1"
        return True

    client.deploy_apps(_rolling_update_config(import_path=chain))
    wait_for_condition(check, expected=code_defined, timeout=60)

    client.deploy_apps(
        _rolling_update_config(d1_override, d2_override, import_path=chain)
    )
    wait_for_condition(check, expected={"D1": (2, 3), "D2": (3, 4)}, timeout=60)

    # D2 reverts whether its entry is left without options or dropped entirely.
    client.deploy_apps(
        _rolling_update_config(d1_override, {"name": "D2"}, import_path=chain)
    )
    wait_for_condition(
        check, expected={"D1": (2, 3), "D2": code_defined["D2"]}, timeout=60
    )
    client.deploy_apps(
        _rolling_update_config(d1_override, import_path=chain), _blocking=True
    )
    check({"D1": (2, 3), "D2": code_defined["D2"]})


if __name__ == "__main__":
    # Forward bazel `args` (the -k filter in BUILD.bazel) through to pytest.
    sys.exit(pytest.main(["-v", "-s", __file__] + sys.argv[1:]))
