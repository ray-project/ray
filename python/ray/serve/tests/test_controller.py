import ast
import glob
import json
import os
import time
from typing import Any, Dict, List, Optional, Sequence

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


def _parse_snapshot_payload(message: Any) -> Optional[Dict[str, Any]]:
    """Try to parse a snapshot payload from a log message."""
    if not message:
        return None

    # If message is already a dict (e.g., from JSON parsing)
    if isinstance(message, dict):
        return message

    # Try to parse as a string representation of a dict
    try:
        payload = ast.literal_eval(str(message))
        if isinstance(payload, dict):
            return payload
    except (ValueError, SyntaxError):
        pass

    # Try JSON parsing
    try:
        payload = json.loads(str(message))
        if isinstance(payload, dict):
            return payload
    except (json.JSONDecodeError, TypeError):
        pass

    return None


def _parse_batched_snapshots_from_log(
    log_paths: Sequence[str],
    target_deployment_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Parse autoscaling snapshots from batched log format.

    The new log format writes snapshots in batches:
    {"snapshots": [snap1, snap2, ...]}

    Args:
        log_paths: List of log file paths to parse.
        target_deployment_name: If provided, only return snapshots for this deployment.

    Returns:
        List of individual snapshot dicts, sorted by timestamp.
    """
    snaps = []
    for path in log_paths:
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                # Try to parse the line as JSON first
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Get the message field
                message = rec.get("message", "")
                if not message:
                    continue

                # Parse the message payload
                payload = _parse_snapshot_payload(message)
                if not payload:
                    continue

                # Handle batched format: {"snapshots": [...]}
                if "snapshots" in payload:
                    for snap in payload.get("snapshots", []):
                        if not isinstance(snap, dict):
                            continue
                        if snap.get("snapshot_type") != "deployment":
                            continue
                        if (
                            target_deployment_name is None
                            or snap.get("deployment") == target_deployment_name
                        ):
                            snaps.append(snap)
                # Handle legacy single snapshot format (for backward compatibility)
                elif payload.get("snapshot_type") == "deployment":
                    if (
                        target_deployment_name is None
                        or payload.get("deployment") == target_deployment_name
                    ):
                        snaps.append(payload)

    return sorted(snaps, key=lambda s: s.get("timestamp_str", ""))


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
        """Read all snapshots for deployment from batched log format."""
        log_paths = glob.glob(
            os.path.join(serve_logs_dir, "autoscaling_snapshot_*.log")
        )
        return _parse_batched_snapshots_from_log(log_paths, DEPLOY_NAME)

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
            "policy_name",
            "metrics_health",
            "look_back_period_s",
            "snapshot_type",
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

    found = _parse_batched_snapshots_from_log(candidate_paths, DEPLOY_NAME)

    assert not found, (
        f"Found deployment-type autoscaling snapshot logs for deployment {DEPLOY_NAME} "
        f"even though no autoscaling_config was set: {found}"
    )


def test_autoscaling_snapshot_not_emitted_every_iteration(serve_instance):
    """Ensure identical autoscaling snapshots are not written repeatedly."""

    DEPLOY_NAME = f"snap_dedupe_{int(time.time())}"

    @serve.deployment(
        name=DEPLOY_NAME,
        autoscaling_config={
            "min_replicas": 1,
            "max_replicas": 1,
            "initial_replicas": 1,
            "metrics_interval_s": 0.1,
            "look_back_period_s": 0.2,
            "upscale_delay_s": 0.0,
            "downscale_delay_s": 600.0,
            "target_ongoing_requests": 1.0,
        },
    )
    def app():
        return "ok"

    serve.run(app.bind())

    serve_logs_dir = get_serve_logs_dir()

    def get_snapshots():
        log_paths = glob.glob(
            os.path.join(serve_logs_dir, "autoscaling_snapshot_*.log")
        )
        return _parse_batched_snapshots_from_log(log_paths, DEPLOY_NAME)

    # Wait until the first stable snapshot shows up
    def has_initial_snapshot():
        snaps = get_snapshots()
        return bool(snaps) and snaps[-1]["current_replicas"] == 1

    wait_for_condition(has_initial_snapshot, timeout=10)

    controller = _get_global_client()._controller

    # ensure deployment is in autoscaling cache
    ray.get(controller._refresh_autoscaling_deployments_cache.remote())

    # Count current snapshots
    initial_count = len(get_snapshots())

    # Force multiple emits
    for _ in range(5):
        ray.get(controller._emit_deployment_autoscaling_snapshots.remote())

    final_count = len(get_snapshots())

    # No new snapshots should be added after the first stable write
    assert final_count == initial_count


def test_autoscaling_snapshot_batched_single_write_per_loop(serve_instance):
    """Ensure multiple deployments are batched in a single log write per loop."""

    ts = int(time.time())
    NUM_DEPLOYMENTS = 3
    APP_NAME = f"batch_app_{ts}"

    # Create multiple deployments in a SINGLE application
    deployments = []
    deployment_names = []

    for i in range(NUM_DEPLOYMENTS):
        dep_name = f"Dep{i}"
        deployment_names.append(dep_name)

        @serve.deployment(
            name=dep_name,
            autoscaling_config={
                "min_replicas": 1,
                "max_replicas": 1,
                "initial_replicas": 1,
                "metrics_interval_s": 0.1,
                "look_back_period_s": 0.2,
            },
        )
        class Deployment:
            def __call__(self):
                return "ok"

        deployments.append(Deployment.bind())

    # Create ingress that references all deployments
    @serve.deployment
    class Ingress:
        def __init__(self, *deps):
            self.deps = deps

        def __call__(self):
            return "ok"

    # Bind all deployments to ingress and run as single app
    app = Ingress.bind(*deployments)
    serve.run(app, name=APP_NAME, route_prefix=f"/{APP_NAME}")

    serve_logs_dir = get_serve_logs_dir()

    def get_batch_payloads():
        """Return list of (deployment_names_set, batch_dict) for target deployments."""
        log_paths = glob.glob(
            os.path.join(serve_logs_dir, "autoscaling_snapshot_*.log")
        )
        batches = []
        for path in log_paths:
            if not os.path.exists(path):
                continue
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    message = rec.get("message")
                    if message is None:
                        continue

                    payload = _parse_snapshot_payload(message)
                    if not payload or "snapshots" not in payload:
                        continue

                    names_in_batch = set()
                    target_snaps = []
                    for snap in payload.get("snapshots", []):
                        if not isinstance(snap, dict):
                            continue
                        if snap.get("snapshot_type") != "deployment":
                            continue
                        dep_name = snap.get("deployment")
                        if snap.get("app") == APP_NAME and dep_name in deployment_names:
                            names_in_batch.add(dep_name)
                            target_snaps.append(snap)

                    if names_in_batch:
                        batches.append((names_in_batch, {"snapshots": target_snaps}))
        return batches

    # Wait until a batch containing ALL deployments appears
    def has_full_batch():
        batches = get_batch_payloads()
        for names, _ in batches:
            if names == set(deployment_names):
                return True
        return False

    wait_for_condition(has_full_batch, timeout=15)
    batches = get_batch_payloads()

    # Only verify batches that contain all deployments (full batches)
    # Partial batches can occur due to individual deployment state changes
    full_batches = [
        (names, batch) for names, batch in batches if names == set(deployment_names)
    ]

    assert len(full_batches) >= 1, (
        f"Expected at least one batch containing all {NUM_DEPLOYMENTS} deployments "
        f"{sorted(deployment_names)}, but none found"
    )

    for names, batch in full_batches:
        assert len(batch["snapshots"]) == NUM_DEPLOYMENTS, (
            f"Expected {NUM_DEPLOYMENTS} snapshots in batch, "
            f"but found {len(batch['snapshots'])}"
        )


def test_get_health_metrics(serve_instance):
    """Test that get_health_metrics returns valid controller health metrics."""

    controller = _get_global_client()._controller

    # Deploy a simple application to ensure controller is active
    @serve.deployment
    def health_test_app():
        return "ok"

    serve.run(health_test_app.bind())

    # Get health metrics
    metrics = ray.get(controller.get_health_metrics.remote())

    # Verify it's a dictionary
    assert isinstance(metrics, dict)

    # Verify all expected fields are present
    expected_fields = [
        "timestamp",
        "controller_start_time",
        "uptime_s",
        "num_control_loops",
        "loop_duration_s",
        "loops_per_second",
        "last_sleep_duration_s",
        "expected_sleep_duration_s",
        "event_loop_delay_s",
        "num_asyncio_tasks",
        "deployment_state_update_duration_s",
        "application_state_update_duration_s",
        "proxy_state_update_duration_s",
        "node_update_duration_s",
        "handle_metrics_delay_ms",
        "replica_metrics_delay_ms",
        "process_memory_mb",
    ]

    for field in expected_fields:
        assert field in metrics, f"Missing field: {field}"

    # Verify types and basic sanity checks
    assert metrics["timestamp"] > 0
    assert metrics["controller_start_time"] > 0
    assert metrics["uptime_s"] >= 0
    assert metrics["expected_sleep_duration_s"] > 0  # Should be CONTROL_LOOP_INTERVAL_S

    # Wait for at least one control loop to complete
    def has_control_loops():
        m = ray.get(controller.get_health_metrics.remote())
        return m["num_control_loops"] > 0

    wait_for_condition(has_control_loops, timeout=10)

    # Get updated metrics after control loops have run
    metrics = ray.get(controller.get_health_metrics.remote())

    # Verify control loop metrics are populated
    assert metrics["num_control_loops"] > 0
    assert metrics["loops_per_second"] > 0

    # Verify DurationStats structure for loop_duration_s
    loop_stats = metrics["loop_duration_s"]
    assert loop_stats is not None
    assert "mean" in loop_stats
    assert "std" in loop_stats
    assert "min" in loop_stats
    assert "max" in loop_stats
    assert loop_stats["mean"] > 0

    # Verify DurationStats structure for component update durations
    for field in [
        "deployment_state_update_duration_s",
        "application_state_update_duration_s",
        "proxy_state_update_duration_s",
        "node_update_duration_s",
    ]:
        stats = metrics[field]
        assert stats is not None, f"{field} should not be None"
        assert "mean" in stats, f"{field} should have 'mean'"
        assert "std" in stats, f"{field} should have 'std'"
        assert "min" in stats, f"{field} should have 'min'"
        assert "max" in stats, f"{field} should have 'max'"

    # Verify the metrics are JSON serializable
    metrics_json = json.dumps(metrics)
    assert isinstance(metrics_json, str)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
