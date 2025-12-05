import glob
import json
import os
import time

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.common import DeploymentID
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import (
    DEFAULT_AUTOSCALING_POLICY_NAME,
    SERVE_DEFAULT_APP_NAME,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve._private.logging_utils import get_serve_logs_dir
from ray.serve.autoscaling_policy import default_autoscaling_policy
from ray.serve.context import _get_global_client
from ray.serve.generated.serve_pb2 import DeploymentRoute
from ray.serve.schema import ApplicationStatus, ServeDeploySchema
from ray.serve.tests.conftest import TEST_GRPC_SERVICER_FUNCTIONS


def test_redeploy_start_time(serve_instance):
    """Check that redeploying a deployment doesn't reset its start time."""

    controller = _get_global_client()._controller

    @serve.deployment
    def test(_):
        return "1"

    serve.run(test.bind())
    deployment_route = DeploymentRoute.FromString(
        ray.get(controller.get_deployment_info.remote("test", SERVE_DEFAULT_APP_NAME))
    )
    deployment_info_1 = DeploymentInfo.from_proto(deployment_route.deployment_info)
    start_time_ms_1 = deployment_info_1.start_time_ms

    time.sleep(0.1)

    @serve.deployment
    def test(_):
        return "2"

    serve.run(test.bind())
    deployment_route = DeploymentRoute.FromString(
        ray.get(controller.get_deployment_info.remote("test", SERVE_DEFAULT_APP_NAME))
    )
    deployment_info_2 = DeploymentInfo.from_proto(deployment_route.deployment_info)
    start_time_ms_2 = deployment_info_2.start_time_ms

    assert start_time_ms_1 == start_time_ms_2


def test_deploy_app_custom_exception(serve_instance):
    """Check that controller doesn't deserialize an exception from deploy_app."""

    controller = _get_global_client()._controller

    config = {
        "applications": [
            {
                "name": "broken_app",
                "route_prefix": "/broken",
                "import_path": "ray.serve.tests.test_config_files.broken_app:app",
            }
        ]
    }

    ray.get(controller.apply_config.remote(config=ServeDeploySchema.parse_obj(config)))

    def check_custom_exception() -> bool:
        status = serve.status().applications["broken_app"]
        assert status.status == ApplicationStatus.DEPLOY_FAILED
        assert "custom exception info" in status.message
        return True

    wait_for_condition(check_custom_exception, timeout=10)


@pytest.mark.parametrize(
    "policy_name", [None, DEFAULT_AUTOSCALING_POLICY_NAME, default_autoscaling_policy]
)
def test_get_serve_instance_details_json_serializable(serve_instance, policy_name):
    """Test the result from get_serve_instance_details is json serializable."""

    controller = _get_global_client()._controller

    autoscaling_config = {
        "min_replicas": 1,
        "max_replicas": 10,
        "_policy": {"name": policy_name},
    }
    if policy_name is None:
        autoscaling_config.pop("_policy")

    @serve.deployment(autoscaling_config=autoscaling_config)
    def autoscaling_app():
        return "1"

    serve.run(autoscaling_app.bind())
    details = ray.get(controller.get_serve_instance_details.remote())
    details_json = json.dumps(details)
    controller_details = ray.get(controller.get_actor_details.remote())
    node_id = controller_details.node_id
    node_ip = controller_details.node_ip
    node_instance_id = controller_details.node_instance_id
    proxy_details = ray.get(controller.get_proxy_details.remote(node_id=node_id))
    deployment_timestamp = ray.get(
        controller.get_deployment_timestamps.remote(app_name="default")
    )
    deployment_details = ray.get(
        controller.get_deployment_details.remote("default", "autoscaling_app")
    )
    replica = deployment_details.replicas[0]
    expected_json = json.dumps(
        {
            "controller_info": {
                "node_id": node_id,
                "node_ip": node_ip,
                "node_instance_id": node_instance_id,
                "actor_id": controller_details.actor_id,
                "actor_name": controller_details.actor_name,
                "worker_id": controller_details.worker_id,
                "log_file_path": controller_details.log_file_path,
            },
            "proxy_location": "HeadOnly",
            "http_options": {"host": "0.0.0.0"},
            "grpc_options": {
                "port": 9000,
                "grpc_servicer_functions": TEST_GRPC_SERVICER_FUNCTIONS,
            },
            "proxies": {
                node_id: {
                    "node_id": node_id,
                    "node_ip": node_ip,
                    "node_instance_id": node_instance_id,
                    "actor_id": proxy_details.actor_id,
                    "actor_name": proxy_details.actor_name,
                    "worker_id": proxy_details.worker_id,
                    "log_file_path": proxy_details.log_file_path,
                    "status": proxy_details.status,
                }
            },
            "applications": {
                "default": {
                    "name": "default",
                    "route_prefix": "/",
                    "docs_path": None,
                    "status": "RUNNING",
                    "message": "",
                    "last_deployed_time_s": deployment_timestamp,
                    "deployed_app_config": None,
                    "source": "imperative",
                    "deployments": {
                        "autoscaling_app": {
                            "name": "autoscaling_app",
                            "status": "HEALTHY",
                            "status_trigger": "CONFIG_UPDATE_COMPLETED",
                            "message": "",
                            "deployment_config": {
                                "name": "autoscaling_app",
                                "max_ongoing_requests": 5,
                                "max_queued_requests": -1,
                                "user_config": None,
                                "autoscaling_config": {
                                    "min_replicas": 1,
                                    "initial_replicas": None,
                                    "max_replicas": 10,
                                    "target_ongoing_requests": 2.0,
                                    "metrics_interval_s": 10.0,
                                    "look_back_period_s": 30.0,
                                    "smoothing_factor": 1.0,
                                    "upscale_smoothing_factor": None,
                                    "downscale_smoothing_factor": None,
                                    "upscaling_factor": None,
                                    "downscaling_factor": None,
                                    "downscale_delay_s": 600.0,
                                    "downscale_to_zero_delay_s": None,
                                    "upscale_delay_s": 30.0,
                                    "aggregation_function": "mean",
                                    "policy": {
                                        "policy_function": "ray.serve.autoscaling_policy:default_autoscaling_policy"
                                    },
                                },
                                "graceful_shutdown_wait_loop_s": 2.0,
                                "graceful_shutdown_timeout_s": 20.0,
                                "health_check_period_s": 10.0,
                                "health_check_timeout_s": 30.0,
                                "ray_actor_options": {
                                    "num_cpus": 1.0,
                                },
                                "request_router_config": {
                                    "request_router_class": "ray.serve._private.request_router:PowerOfTwoChoicesRequestRouter",
                                    "request_router_kwargs": {},
                                    "request_routing_stats_period_s": 10.0,
                                    "request_routing_stats_timeout_s": 30.0,
                                },
                            },
                            "target_num_replicas": 1,
                            "required_resources": {"CPU": 1},
                            "replicas": [
                                {
                                    "node_id": node_id,
                                    "node_ip": node_ip,
                                    "node_instance_id": node_instance_id,
                                    "actor_id": replica.actor_id,
                                    "actor_name": replica.actor_name,
                                    "worker_id": replica.worker_id,
                                    "log_file_path": replica.log_file_path,
                                    "replica_id": replica.replica_id,
                                    "state": "RUNNING",
                                    "pid": replica.pid,
                                    "start_time_s": replica.start_time_s,
                                }
                            ],
                        }
                    },
                    "external_scaler_enabled": False,
                    "deployment_topology": {
                        "app_name": "default",
                        "nodes": {
                            "autoscaling_app": {
                                "name": "autoscaling_app",
                                "app_name": "default",
                                "outbound_deployments": [],
                                "is_ingress": True,
                            }
                        },
                        "ingress_deployment": "autoscaling_app",
                    },
                }
            },
            "target_capacity": None,
            "target_groups": [
                {
                    "targets": [
                        {
                            "ip": node_ip,
                            "port": 8000,
                            "instance_id": node_instance_id,
                            "name": proxy_details.actor_name,
                        },
                    ],
                    "route_prefix": "/",
                    "protocol": "HTTP",
                },
                {
                    "targets": [
                        {
                            "ip": node_ip,
                            "port": 9000,
                            "instance_id": node_instance_id,
                            "name": proxy_details.actor_name,
                        },
                    ],
                    "route_prefix": "/",
                    "protocol": "gRPC",
                },
            ],
        }
    )
    assert details_json == expected_json

    # ensure internal field, serialized_policy_def, is not exposed
    application = details["applications"]["default"]
    deployment = application["deployments"]["autoscaling_app"]
    autoscaling_config = deployment["deployment_config"]["autoscaling_config"]
    assert "_serialized_policy_def" not in autoscaling_config


def test_get_deployment_config(serve_instance):
    """Test getting deployment config."""

    controller = _get_global_client()._controller
    deployment_id = DeploymentID(name="App", app_name="default")
    deployment_config = ray.get(
        controller.get_deployment_config.remote(deployment_id=deployment_id)
    )
    # Before any deployment is created, the config should be None.
    assert deployment_config is None

    @serve.deployment
    class App:
        pass

    serve.run(App.bind())

    deployment_config = ray.get(
        controller.get_deployment_config.remote(deployment_id=deployment_id)
    )
    # After the deployment is created, the config should be DeploymentConfig.
    assert isinstance(deployment_config, DeploymentConfig)


def test_autoscaling_snapshot_log_emitted_and_well_formed(serve_instance):
    """Validate controller emits well-formed autoscaling snapshot structured logs.

    Tests deterministic autoscaling: 1 -> 2 replicas.
    """

    DEPLOY_NAME = f"snap_app_{int(time.time())}"

    @serve.deployment(
        name=DEPLOY_NAME,
        autoscaling_config={
            "min_replicas": 1,
            "max_replicas": 2,
            "initial_replicas": 1,
            "metrics_interval_s": 0.2,
            "look_back_period_s": 0.5,
            "upscale_delay_s": 0.0,
            "downscale_delay_s": 600.0,
            "target_ongoing_requests": 1.0,
        },
    )
    async def snap_app():
        return "ok"

    handle = serve.run(snap_app.bind())

    serve_logs_dir = get_serve_logs_dir()

    def get_snapshots():
        """Read all snapshots for deployment."""
        log_paths = glob.glob(
            os.path.join(serve_logs_dir, "autoscaling_snapshot_*.log")
        )
        snaps = []
        for path in log_paths:
            if not os.path.exists(path):
                continue
            with open(path, "r", errors="ignore") as f:
                for line in f:
                    rec = json.loads(line)
                    if (
                        rec.get("type") == "deployment"
                        and rec.get("snapshot", {}).get("deployment") == DEPLOY_NAME
                    ):
                        snaps.append(rec["snapshot"])
        return sorted(snaps, key=lambda s: s.get("timestamp_str", ""))

    def wait_for_replicas(current, timeout=10):
        """Wait for exact current replica count."""
        wait_for_condition(
            lambda: any(s.get("current_replicas") == current for s in get_snapshots()),
            timeout=timeout,
        )

    # Wait for initial replica to be ready
    wait_for_replicas(1, timeout=5)
    initial = [s for s in get_snapshots() if s["current_replicas"] == 1][0]
    assert initial["current_replicas"] == 1
    assert initial["min_replicas"] == 1
    assert initial["max_replicas"] == 2

    reqs = [handle.remote() for _ in range(6)]
    # Wait for scaling to 2 replicas.
    wait_for_replicas(2, timeout=5)

    all_snaps = get_snapshots()

    snap_1 = next((s for s in all_snaps if s["current_replicas"] == 1), None)
    snap_2 = next((s for s in all_snaps if s["current_replicas"] == 2), None)

    assert snap_1 is not None
    assert snap_2 is not None and snap_2["target_replicas"] == 2

    seen_states = []
    for snap in all_snaps:
        current = snap["current_replicas"]
        if current not in seen_states:
            seen_states.append(current)

    assert seen_states in [[0, 1, 2], [1, 2]]
    assert 1 in seen_states and 2 in seen_states

    for snap in [snap_1, snap_2]:
        for key in [
            "timestamp_str",
            "app",
            "deployment",
            "current_replicas",
            "target_replicas",
            "min_replicas",
            "max_replicas",
            "decisions",
            "policy_name",
            "metrics_health",
            "look_back_period_s",
        ]:
            assert key in snap, f"Missing {key}"

    for req in reqs:
        assert req.result() == "ok"


# Test that no autoscaling snapshot logs are emitted for deployments without autoscaling_config
def test_autoscaling_snapshot_not_emitted_without_config(serve_instance):
    """Ensure no deployment-type autoscaling snapshot logs are emitted without autoscaling_config."""

    DEPLOY_NAME = f"snap_no_auto_{int(time.time())}"

    @serve.deployment(name=DEPLOY_NAME)
    def app():
        return "no autoscale"

    serve.run(app.bind())

    serve_logs_dir = get_serve_logs_dir()
    candidate_paths = sorted(
        glob.glob(os.path.join(serve_logs_dir, "autoscaling_snapshot_*.log"))
    )
    assert (
        candidate_paths
    ), f"No autoscaling snapshot logs found; checked {serve_logs_dir}"

    found = []
    for path in candidate_paths:
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                rec = json.loads(line)
                if rec.get("type") != "deployment":
                    continue
                snap = rec.get("snapshot", {})
                if not isinstance(snap, dict):
                    continue
                if snap.get("deployment") == DEPLOY_NAME:
                    found.append(snap)

    assert not found, (
        f"Found deployment-type autoscaling snapshot logs for deployment {DEPLOY_NAME} "
        f"even though no autoscaling_config was set: {found}"
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
