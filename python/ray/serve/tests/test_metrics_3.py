import asyncio
import concurrent.futures
import sys
import threading
import time
from typing import Dict, List

import httpx
import pytest
from starlette.requests import Request

import ray
from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.test_utils import PrometheusTimeseries
from ray.serve._private.constants import (
    RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP,
    RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD,
)
from ray.serve._private.long_poll import LongPollClient, LongPollHost, UpdatedObject
from ray.serve._private.test_utils import (
    check_metric_float_eq,
    get_application_url,
    get_metric_dictionaries,
    get_metric_float,
)
from ray.util.state import list_actors


def test_deployment_and_application_status_metrics(metrics_start_shutdown):
    """Test that deployment and application status metrics are exported correctly.

    These metrics track the numeric status of deployments and applications:
    - serve_deployment_status: 0=UNKNOWN, 1=DEPLOY_FAILED, 2=UNHEALTHY,
      3=UPDATING, 4=UPSCALING, 5=DOWNSCALING, 6=HEALTHY
    - serve_application_status: 0=UNKNOWN, 1=NOT_STARTED, 2=DEPLOYING,
      3=DEPLOY_FAILED, 4=RUNNING, 5=UNHEALTHY, 6=DELETING
    """

    signal = SignalActor.remote()

    @serve.deployment(name="deployment_a")
    class DeploymentA:
        async def __init__(self):
            await signal.wait.remote()

        async def __call__(self):
            return "hello"

    @serve.deployment
    def deployment_b():
        return "world"

    # Deploy two applications with different deployments
    serve._run(DeploymentA.bind(), name="app1", route_prefix="/app1", _blocking=False)
    serve._run(deployment_b.bind(), name="app2", route_prefix="/app2", _blocking=False)

    timeseries = PrometheusTimeseries()

    # Wait for deployments to become healthy
    def check_status_metrics():
        # Check deployment status metrics
        deployment_metrics = get_metric_dictionaries(
            "ray_serve_deployment_status", timeseries=timeseries
        )
        if len(deployment_metrics) < 2:
            return False

        # Check application status metrics
        app_metrics = get_metric_dictionaries(
            "ray_serve_application_status", timeseries=timeseries
        )
        if len(app_metrics) < 2:
            return False

        return True

    wait_for_condition(check_status_metrics, timeout=30)

    wait_for_condition(
        check_metric_float_eq,
        metric="ray_serve_deployment_status",
        expected=3,  # UPDATING
        expected_tags={"deployment": "deployment_a", "application": "app1"},
        timeseries=timeseries,
    )
    wait_for_condition(
        check_metric_float_eq,
        metric="ray_serve_application_status",
        expected=5,  # DEPLOYING
        expected_tags={"application": "app1"},
        timeseries=timeseries,
    )

    wait_for_condition(
        check_metric_float_eq,
        metric="ray_serve_deployment_status",
        expected=6,
        expected_tags={"deployment": "deployment_b", "application": "app2"},
        timeseries=timeseries,
    )
    wait_for_condition(
        check_metric_float_eq,
        metric="ray_serve_application_status",
        expected=6,
        expected_tags={"application": "app2"},
        timeseries=timeseries,
    )

    ray.get(signal.send.remote())

    wait_for_condition(
        check_metric_float_eq,
        metric="ray_serve_deployment_status",
        expected=6,
        expected_tags={"deployment": "deployment_a", "application": "app1"},
        timeseries=timeseries,
    )
    wait_for_condition(
        check_metric_float_eq,
        metric="ray_serve_application_status",
        expected=6,
        expected_tags={"application": "app1"},
        timeseries=timeseries,
    )


def test_replica_startup_and_initialization_latency_metrics(metrics_start_shutdown):
    """Test that replica startup and initialization latency metrics are recorded."""

    @serve.deployment(num_replicas=2)
    class MyDeployment:
        def __init__(self):
            time.sleep(1)

        def __call__(self):
            return "hello"

    serve.run(MyDeployment.bind(), name="app", route_prefix="/f")
    url = get_application_url("HTTP", "app")
    assert "hello" == httpx.get(url).text

    # Verify startup latency metric count is exactly 1 (one replica started)
    wait_for_condition(
        check_metric_float_eq,
        timeout=20,
        metric="ray_serve_replica_startup_latency_ms_count",
        expected=1,
        expected_tags={"deployment": "MyDeployment", "application": "app"},
    )

    # Verify initialization latency metric count is exactly 1
    wait_for_condition(
        check_metric_float_eq,
        timeout=20,
        metric="ray_serve_replica_initialization_latency_ms_count",
        expected=1,
        expected_tags={"deployment": "MyDeployment", "application": "app"},
    )

    # Verify initialization latency metric value is greater than 500ms
    def check_initialization_latency_value():
        value = get_metric_float(
            "ray_serve_replica_initialization_latency_ms_sum",
            expected_tags={"deployment": "MyDeployment", "application": "app"},
        )
        assert (
            value > 500
        ), f"Initialization latency value is {value}, expected to be greater than 500ms"
        return True

    wait_for_condition(check_initialization_latency_value, timeout=20)

    # Assert that 2 metrics are recorded (one per replica)
    def check_metrics_count():
        metrics = get_metric_dictionaries(
            "ray_serve_replica_initialization_latency_ms_count"
        )
        assert len(metrics) == 2, f"Expected 2 metrics, got {len(metrics)}"
        # All metrics should have same deployment and application
        for metric in metrics:
            assert metric["deployment"] == "MyDeployment"
            assert metric["application"] == "app"
        # Each replica should have a unique replica tag
        replica_ids = {metric["replica"] for metric in metrics}
        assert (
            len(replica_ids) == 2
        ), f"Expected 2 unique replica IDs, got {replica_ids}"
        return True

    wait_for_condition(check_metrics_count, timeout=20)


def test_replica_reconfigure_latency_metrics(metrics_start_shutdown):
    """Test that replica reconfigure latency metrics are recorded when user_config changes."""

    @serve.deployment(version="1")
    class Configurable:
        def __init__(self):
            self.config = None

        def reconfigure(self, config):
            time.sleep(1)
            self.config = config

        def __call__(self):
            return self.config

    # Initial deployment with version specified to enable in-place reconfigure
    serve.run(
        Configurable.options(user_config={"version": 1}).bind(),
        name="app",
        route_prefix="/config",
    )
    url = get_application_url("HTTP", "app")
    assert httpx.get(url).json() == {"version": 1}

    # Update user_config to trigger in-place reconfigure (same version, different config)
    serve.run(
        Configurable.options(user_config={"version": 2}).bind(),
        name="app",
        route_prefix="/config",
    )

    # Wait for the new config to take effect
    def config_updated():
        return httpx.get(url).json() == {"version": 2}

    wait_for_condition(config_updated, timeout=20)

    # Verify reconfigure latency metric count is exactly 1 (one reconfigure happened)
    wait_for_condition(
        check_metric_float_eq,
        timeout=20,
        metric="ray_serve_replica_reconfigure_latency_ms_count",
        expected=1,
        expected_tags={"deployment": "Configurable", "application": "app"},
    )

    # Verify reconfigure latency metric value is greater than 500ms (we slept for 1s)
    def check_reconfigure_latency_value():
        value = get_metric_float(
            "ray_serve_replica_reconfigure_latency_ms_sum",
            expected_tags={"deployment": "Configurable", "application": "app"},
        )
        assert value > 500, f"Reconfigure latency value is {value}, expected > 500ms"
        return True

    wait_for_condition(check_reconfigure_latency_value, timeout=20)


def test_health_check_latency_metrics(metrics_start_shutdown):
    """Test that health check latency metrics are recorded."""

    @serve.deployment(health_check_period_s=1)
    class MyDeployment:
        def __call__(self):
            return "hello"

        def check_health(self):
            time.sleep(1)

    serve.run(MyDeployment.bind(), name="app", route_prefix="/f")
    url = get_application_url("HTTP", "app")
    assert "hello" == httpx.get(url).text

    # Wait for at least one health check to complete and verify metric is recorded
    def check_health_check_latency_metrics():
        value = get_metric_float(
            "ray_serve_health_check_latency_ms_count",
            expected_tags={"deployment": "MyDeployment", "application": "app"},
        )
        # Health check count should be at least 1
        assert value >= 1, f"Health check count is {value}, expected to be 1"
        return True

    wait_for_condition(check_health_check_latency_metrics, timeout=30)

    # Verify health check latency metric value is greater than 500ms
    def check_health_check_latency_value():
        value = get_metric_float(
            "ray_serve_health_check_latency_ms_sum",
            expected_tags={"deployment": "MyDeployment", "application": "app"},
        )
        assert (
            value > 500
        ), f"Health check latency value is {value}, expected to be greater than 500ms"
        return True

    wait_for_condition(check_health_check_latency_value, timeout=30)


def test_health_check_failures_metrics(metrics_start_shutdown):
    """Test that health check failure metrics are recorded when health checks fail."""

    @serve.deployment(health_check_period_s=1, health_check_timeout_s=2)
    class FailingHealthCheck:
        def __init__(self):
            self.should_fail = False

        async def check_health(self):
            if self.should_fail:
                raise Exception("Health check failed!")

        async def __call__(self, request):
            action = (await request.body()).decode("utf-8")
            if action == "fail":
                self.should_fail = True
            return "ok"

    serve.run(FailingHealthCheck.bind(), name="app", route_prefix="/health")
    url = get_application_url("HTTP", "app")

    # Verify deployment is healthy initially
    assert httpx.get(url).text == "ok"

    # Trigger health check failure
    httpx.request("GET", url, content=b"fail")

    # Wait for at least one health check failure to be recorded
    def check_health_check_failure_metrics():
        value = get_metric_float(
            "ray_serve_health_check_failures_total",
            expected_tags={"deployment": "FailingHealthCheck", "application": "app"},
        )
        # Should have at least 1 failure
        return value >= 1

    wait_for_condition(check_health_check_failure_metrics, timeout=30)


def test_replica_shutdown_duration_metrics(metrics_start_shutdown):
    """Test that replica shutdown duration metrics are recorded."""

    @serve.deployment
    class MyDeployment:
        def __call__(self):
            return "hello"

        def __del__(self):
            time.sleep(1)

    # Deploy the application
    serve.run(MyDeployment.bind(), name="app", route_prefix="/f")
    url = get_application_url("HTTP", "app")
    assert "hello" == httpx.get(url).text

    # Delete the application to trigger shutdown
    serve.delete("app", _blocking=True)

    # Verify shutdown duration metric count is exactly 1 (one replica stopped)
    wait_for_condition(
        check_metric_float_eq,
        timeout=30,
        metric="ray_serve_replica_shutdown_duration_ms_count",
        expected=1,
        expected_tags={"deployment": "MyDeployment", "application": "app"},
    )
    print("serve_replica_shutdown_duration_ms working as expected.")

    # Verify shutdown duration metric value is greater than 500ms
    def check_shutdown_duration_value():
        value = get_metric_float(
            "ray_serve_replica_shutdown_duration_ms_sum",
            expected_tags={"deployment": "MyDeployment", "application": "app"},
        )
        assert (
            value > 500
        ), f"Shutdown duration value is {value}, expected to be greater than 500ms"
        return True

    wait_for_condition(check_shutdown_duration_value, timeout=30)


def test_batching_metrics(metrics_start_shutdown):
    @serve.deployment
    class BatchedDeployment:
        @serve.batch(max_batch_size=4, batch_wait_timeout_s=0.5)
        async def batch_handler(self, requests: List[str]) -> List[str]:
            # Simulate some processing time
            await asyncio.sleep(0.05)
            return [f"processed:{r}" for r in requests]

        async def __call__(self, request: Request):
            data = await request.body()
            return await self.batch_handler(data.decode())

    app_name = "batched_app"
    serve.run(BatchedDeployment.bind(), name=app_name, route_prefix="/batch")

    http_url = "http://localhost:8000/batch"

    # Send multiple concurrent requests to trigger batching
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(lambda i=i: httpx.post(http_url, content=f"req{i}"))
            for i in range(8)
        ]
        results = [f.result() for f in futures]

    # Verify all requests succeeded
    assert all(r.status_code == 200 for r in results)

    # Verify specific metric values and tags
    timeseries = PrometheusTimeseries()
    expected_tags = {
        "deployment": "BatchedDeployment",
        "application": app_name,
        "function_name": "batch_handler",
    }

    # Check batches_processed_total counter exists and has correct tags
    wait_for_condition(
        lambda: check_metric_float_eq(
            "ray_serve_batches_processed_total",
            expected=2,
            expected_tags=expected_tags,
            timeseries=timeseries,
        ),
        timeout=10,
    )

    # Check batch_wait_time_ms histogram was recorded for 2 batches
    wait_for_condition(
        lambda: check_metric_float_eq(
            "ray_serve_batch_wait_time_ms_count",
            expected=2,
            expected_tags=expected_tags,
            timeseries=timeseries,
        ),
        timeout=10,
    )

    # Check batch_execution_time_ms histogram was recorded for 2 batches
    wait_for_condition(
        lambda: check_metric_float_eq(
            "ray_serve_batch_execution_time_ms_count",
            expected=2,
            expected_tags=expected_tags,
            timeseries=timeseries,
        ),
        timeout=10,
    )

    # Check batch_utilization_percent histogram: 2 batches at 100% each = 200 sum
    wait_for_condition(
        lambda: check_metric_float_eq(
            "ray_serve_batch_utilization_percent_count",
            expected=2,
            expected_tags=expected_tags,
            timeseries=timeseries,
        ),
        timeout=10,
    )

    # Check actual_batch_size histogram: 2 batches of 4 requests each = 8 sum
    wait_for_condition(
        lambda: check_metric_float_eq(
            "ray_serve_actual_batch_size_count",
            expected=2,
            expected_tags=expected_tags,
            timeseries=timeseries,
        ),
        timeout=10,
    )

    # Check batch_queue_length gauge exists (should be 0 after processing)
    wait_for_condition(
        lambda: check_metric_float_eq(
            "ray_serve_batch_queue_length",
            expected=0,
            expected_tags=expected_tags,
            timeseries=timeseries,
        ),
        timeout=10,
    )


def test_autoscaling_metrics(metrics_start_shutdown):
    """Test that autoscaling metrics are emitted correctly.

    This tests the following metrics:
    - ray_serve_autoscaling_target_replicas: Target number of replicas
        Tags: deployment, application
    - ray_serve_autoscaling_desired_replicas: Raw decision before bounds
        Tags: deployment, application
    - ray_serve_autoscaling_total_requests: Total requests seen by autoscaler
        Tags: deployment, application
    - ray_serve_autoscaling_policy_execution_time_ms: Policy execution time
        Tags: deployment, application, policy_scope
    - ray_serve_autoscaling_replica_metrics_delay_ms: Replica metrics delay
        Tags: deployment, application, replica
    - ray_serve_autoscaling_handle_metrics_delay_ms: Handle metrics delay
        Tags: deployment, application, handle
    """
    signal = SignalActor.remote()

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 1,
            "max_replicas": 5,
            "target_ongoing_requests": 2,
            "upscale_delay_s": 0,
            "downscale_delay_s": 5,
            "look_back_period_s": 1,
        },
        max_ongoing_requests=10,
        graceful_shutdown_timeout_s=0.1,
    )
    class AutoscalingDeployment:
        async def __call__(self):
            await signal.wait.remote()

    serve.run(AutoscalingDeployment.bind(), name="autoscaling_app")

    # Send requests to trigger autoscaling
    handle = serve.get_deployment_handle("AutoscalingDeployment", "autoscaling_app")
    [handle.remote() for _ in range(10)]

    timeseries = PrometheusTimeseries()
    base_tags = {
        "deployment": "AutoscalingDeployment",
        "application": "autoscaling_app",
    }

    # Test 1: Check that target_replicas metric is 5 (10 requests / target_ongoing_requests=2)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_autoscaling_target_replicas",
        expected=5,
        expected_tags=base_tags,
        timeseries=timeseries,
    )
    print("Target replicas metric verified.")

    # Test 2: Check that autoscaling decision metric is 5 (10 requests / target_ongoing_requests=2)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_autoscaling_desired_replicas",
        expected=5,
        expected_tags=base_tags,
        timeseries=timeseries,
    )
    print("Autoscaling decision metric verified.")

    # Test 3: Check that total requests metric is 10
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_autoscaling_total_requests",
        expected=10,
        expected_tags=base_tags,
        timeseries=timeseries,
    )
    print("Total requests metric verified.")

    # Test 4: Check that policy execution time metric is emitted with policy_scope=deployment
    def check_policy_execution_time_metric():
        value = get_metric_float(
            "ray_serve_autoscaling_policy_execution_time_ms",
            expected_tags={**base_tags, "policy_scope": "deployment"},
            timeseries=timeseries,
        )
        assert value >= 0
        return True

    wait_for_condition(check_policy_execution_time_metric, timeout=15)
    print("Policy execution time metric verified.")

    # Test 5: Check that metrics delay gauges are emitted with proper tags
    def check_metrics_delay_metrics():
        # Check for handle metrics delay (depends on where metrics are collected)
        value = get_metric_float(
            "ray_serve_autoscaling_handle_metrics_delay_ms",
            expected_tags=base_tags,
            timeseries=timeseries,
        )
        if value >= 0:
            # Verify handle tag exists by checking metric dictionaries
            metrics_dicts = get_metric_dictionaries(
                "ray_serve_autoscaling_handle_metrics_delay_ms",
                timeout=5,
                timeseries=timeseries,
            )
            for m in metrics_dicts:
                if (
                    m.get("deployment") == "AutoscalingDeployment"
                    and m.get("application") == "autoscaling_app"
                ):
                    assert m.get("handle") is not None
                    print(
                        f"Handle delay metric verified with handle tag: {m.get('handle')}"
                    )
                    return True

        # Fallback: Check for replica metrics delay
        value = get_metric_float(
            "ray_serve_autoscaling_replica_metrics_delay_ms",
            expected_tags=base_tags,
            timeseries=timeseries,
        )
        if value >= 0:
            metrics_dicts = get_metric_dictionaries(
                "ray_serve_autoscaling_replica_metrics_delay_ms",
                timeout=5,
                timeseries=timeseries,
            )
            for m in metrics_dicts:
                if (
                    m.get("deployment") == "AutoscalingDeployment"
                    and m.get("application") == "autoscaling_app"
                ):
                    assert m.get("replica") is not None
                    print(
                        f"Replica delay metric verified with replica tag: {m.get('replica')}"
                    )
                    return True

        return False

    wait_for_condition(check_metrics_delay_metrics, timeout=15)
    print("Metrics delay metrics verified.")

    # Release signal to complete requests
    ray.get(signal.send.remote())


def test_user_autoscaling_stats_metrics(metrics_start_shutdown):
    """Test that user-defined autoscaling stats metrics are emitted correctly.

    This tests the following metrics:
    - ray_serve_user_autoscaling_stats_latency_ms: Time to execute user stats function
        Tags: application, deployment, replica
    - ray_serve_record_autoscaling_stats_failed_total: Failed stats collection
        Tags: application, deployment, replica, exception_name
    """

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 1,
            "max_replicas": 5,
            "target_ongoing_requests": 2,
        },
    )
    class DeploymentWithCustomStats:
        def __init__(self):
            self.call_count = 0

        async def record_autoscaling_stats(self):
            """Custom autoscaling stats function."""
            self.call_count += 1
            return {"custom_metric": self.call_count}

        def __call__(self):
            return "ok"

    serve.run(DeploymentWithCustomStats.bind(), name="custom_stats_app")

    # Make a request to ensure the deployment is running
    handle = serve.get_deployment_handle(
        "DeploymentWithCustomStats", "custom_stats_app"
    )
    handle.remote().result()

    timeseries = PrometheusTimeseries()
    base_tags = {
        "deployment": "DeploymentWithCustomStats",
        "application": "custom_stats_app",
    }

    # Test: Check that user autoscaling stats latency metric is emitted
    def check_user_stats_latency_metric():
        value = get_metric_float(
            "ray_serve_user_autoscaling_stats_latency_ms_sum",
            expected_tags=base_tags,
            timeseries=timeseries,
        )
        if value >= 0:
            # Verify replica tag exists
            metrics_dicts = get_metric_dictionaries(
                "ray_serve_user_autoscaling_stats_latency_ms_sum",
                timeout=5,
                timeseries=timeseries,
            )
            for m in metrics_dicts:
                if (
                    m.get("deployment") == "DeploymentWithCustomStats"
                    and m.get("application") == "custom_stats_app"
                ):
                    assert m.get("replica") is not None
                    print(
                        f"User stats latency metric verified with replica tag: {m.get('replica')}"
                    )
                    return True
        return False

    wait_for_condition(check_user_stats_latency_metric, timeout=15)
    print("User autoscaling stats latency metric verified.")


def test_user_autoscaling_stats_failure_metrics(metrics_start_shutdown):
    """Test that user autoscaling stats failure metrics are emitted on error."""

    @serve.deployment(
        autoscaling_config={
            "metrics_interval_s": 0.1,
            "min_replicas": 1,
            "max_replicas": 5,
            "target_ongoing_requests": 2,
        },
    )
    class DeploymentWithFailingStats:
        async def record_autoscaling_stats(self):
            """Custom autoscaling stats function that raises an error."""
            raise ValueError("Intentional error for testing")

        def __call__(self):
            return "ok"

    serve.run(DeploymentWithFailingStats.bind(), name="failing_stats_app")

    # Make a request to ensure the deployment is running
    handle = serve.get_deployment_handle(
        "DeploymentWithFailingStats", "failing_stats_app"
    )
    handle.remote().result()

    timeseries = PrometheusTimeseries()

    # Test: Check that failure counter is incremented
    def check_stats_failure_metric():
        metrics_dicts = get_metric_dictionaries(
            "ray_serve_record_autoscaling_stats_failed_total",
            timeout=5,
            timeseries=timeseries,
        )
        for m in metrics_dicts:
            if (
                m.get("deployment") == "DeploymentWithFailingStats"
                and m.get("application") == "failing_stats_app"
            ):
                assert m.get("replica") is not None
                assert m.get("exception_name") == "ValueError"
                print(
                    f"Stats failure metric verified with exception_name: {m.get('exception_name')}"
                )
                return True
        return False

    wait_for_condition(check_stats_failure_metric, timeout=15)
    print("User autoscaling stats failure metric verified.")


def test_long_poll_pending_clients_metric(metrics_start_shutdown):
    """Check that pending clients gauge is tracked correctly."""
    timeseries = PrometheusTimeseries()

    # Create a LongPollHost with a longer timeout so we can observe pending state
    host = ray.remote(LongPollHost).remote(
        listen_for_change_request_timeout_s=(5.0, 5.0)
    )

    # Write initial values
    ray.get(host.notify_changed.remote({"key_1": 100}))
    ray.get(host.notify_changed.remote({"key_2": 200}))

    # Get the current snapshot IDs
    result = ray.get(host.listen_for_change.remote({"key_1": -1, "key_2": -1}))
    key_1_snapshot_id = result["key_1"].snapshot_id
    key_2_snapshot_id = result["key_2"].snapshot_id

    # Start a listen call that will block waiting for updates
    # (since we're using up-to-date snapshot IDs)
    pending_ref = host.listen_for_change.remote(
        {"key_1": key_1_snapshot_id, "key_2": key_2_snapshot_id}
    )

    # Check that pending clients gauge shows 1 for each key
    # (wait_for_condition will retry until the metric is available)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_pending_clients",
        expected=1,
        expected_tags={"namespace": "key_1"},
        timeseries=timeseries,
    )
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_pending_clients",
        expected=1,
        expected_tags={"namespace": "key_2"},
        timeseries=timeseries,
    )

    # Trigger an update for key_1
    ray.get(host.notify_changed.remote({"key_1": 101}))

    # Wait for the pending call to complete
    ray.get(pending_ref)

    # After update, pending clients for key_1 should be 0
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_pending_clients",
        expected=0,
        expected_tags={"namespace": "key_1"},
        timeseries=timeseries,
    )


def test_long_poll_latency_metric(metrics_start_shutdown):
    """Check that long poll latency histogram is recorded on the client side."""
    timeseries = PrometheusTimeseries()

    # Create a LongPollHost
    host = ray.remote(LongPollHost).remote(
        listen_for_change_request_timeout_s=(0.5, 0.5)
    )

    # Write initial value so the key exists
    ray.get(host.notify_changed.remote({"test_key": "initial_value"}))

    # Track received updates
    received_updates = []
    update_event = threading.Event()

    def on_update(value):
        received_updates.append(value)
        update_event.set()

    # Create event loop for the client
    loop = asyncio.new_event_loop()

    def run_loop():
        asyncio.set_event_loop(loop)
        loop.run_forever()

    loop_thread = threading.Thread(target=run_loop, daemon=True)
    loop_thread.start()

    # Create the LongPollClient
    client = LongPollClient(
        host_actor=host,
        key_listeners={"test_key": on_update},
        call_in_event_loop=loop,
    )

    # Wait for initial update (client starts with snapshot_id -1)
    assert update_event.wait(timeout=10), "Timed out waiting for initial update"
    assert len(received_updates) == 1
    assert received_updates[0] == "initial_value"

    # Clear event and trigger another update
    update_event.clear()
    ray.get(host.notify_changed.remote({"test_key": "updated_value"}))

    # Wait for the update to be received
    assert update_event.wait(timeout=10), "Timed out waiting for update"
    assert len(received_updates) == 2
    assert received_updates[1] == "updated_value"

    # Stop the client
    client.stop()
    loop.call_soon_threadsafe(loop.stop)
    loop_thread.join(timeout=5)

    # Check that latency metric was recorded
    # The metric should have at least 2 observations (initial + update)
    def check_latency_metric_exists():
        metric_value = get_metric_float(
            "ray_serve_long_poll_latency_ms_count",
            expected_tags={"namespace": "test_key"},
            timeseries=timeseries,
        )
        # Should have at least 2 observations
        return metric_value == 2

    wait_for_condition(check_latency_metric_exists, timeout=15)

    # Verify the latency sum is positive (latency > 0)
    latency_sum = get_metric_float(
        "ray_serve_long_poll_latency_ms_sum",
        expected_tags={"namespace": "test_key"},
        timeseries=timeseries,
    )
    assert latency_sum > 0, "Latency sum should be positive"


def test_long_poll_host_sends_counted(metrics_start_shutdown):
    """Check that the transmissions by the long_poll are counted."""

    timeseries = PrometheusTimeseries()
    host = ray.remote(LongPollHost).remote(
        listen_for_change_request_timeout_s=(0.01, 0.01)
    )

    # Write a value.
    ray.get(host.notify_changed.remote({"key_1": 999}))
    object_ref = host.listen_for_change.remote({"key_1": -1})

    # Check that the result's size is reported.
    result_1: Dict[str, UpdatedObject] = ray.get(object_ref)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_host_transmission_counter_total",
        expected=1,
        expected_tags={"namespace_or_state": "key_1"},
        timeseries=timeseries,
    )

    # Write two new values.
    ray.get(host.notify_changed.remote({"key_1": 1000}))
    ray.get(host.notify_changed.remote({"key_2": 1000}))
    object_ref = host.listen_for_change.remote(
        {"key_1": result_1["key_1"].snapshot_id, "key_2": -1}
    )

    # Check that the new objects are transmitted.
    result_2: Dict[str, UpdatedObject] = ray.get(object_ref)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_host_transmission_counter_total",
        expected=1,
        expected_tags={"namespace_or_state": "key_2"},
        timeseries=timeseries,
    )
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_host_transmission_counter_total",
        expected=2,
        expected_tags={"namespace_or_state": "key_1"},
        timeseries=timeseries,
    )

    # Check that a timeout result is counted.
    object_ref = host.listen_for_change.remote({"key_2": result_2["key_2"].snapshot_id})
    _ = ray.get(object_ref)
    wait_for_condition(
        check_metric_float_eq,
        timeout=15,
        metric="ray_serve_long_poll_host_transmission_counter_total",
        expected=1,
        expected_tags={"namespace_or_state": "TIMEOUT"},
        timeseries=timeseries,
    )


def test_event_loop_monitoring_metrics(metrics_start_shutdown):
    """Test that event loop monitoring metrics are emitted correctly.

    This tests the following metrics:
    - serve_event_loop_scheduling_latency_ms: Event loop lag in milliseconds
        Tags: component, loop_type, actor_id
    - serve_event_loop_monitoring_iterations: Heartbeat counter
        Tags: component, loop_type, actor_id
    - serve_event_loop_tasks: Number of pending asyncio tasks
        Tags: component, loop_type, actor_id

    Components monitored:
    - Proxy: main loop only
    - Replica: main loop + user_code loop (when separate thread enabled)
    - Router: router loop (when separate loop enabled, runs on replica)
    """

    @serve.deployment(name="g")
    class ChildDeployment:
        def __call__(self):
            return "child"

    @serve.deployment(name="f")
    class SimpleDeployment:
        def __init__(self, child):
            self.child = child

        async def __call__(self):
            return await self.child.remote()

    serve.run(
        SimpleDeployment.bind(ChildDeployment.bind()), name="app", route_prefix="/test"
    )

    # Make a request to ensure everything is running
    url = get_application_url("HTTP", "app")
    assert httpx.get(url).text == "child"

    timeseries = PrometheusTimeseries()

    # Test 1: Check proxy main loop metrics
    def check_proxy_main_loop_metrics():
        metrics = get_metric_dictionaries(
            "ray_serve_event_loop_monitoring_iterations_total",
            timeout=10,
            timeseries=timeseries,
        )
        for m in metrics:
            if m.get("component") == "proxy" and m.get("loop_type") == "main":
                assert "actor_id" in m, "actor_id tag should be present"
                print(f"Proxy main loop metric found: {m}")
                return True
        return False

    wait_for_condition(check_proxy_main_loop_metrics, timeout=30)
    print("Proxy main loop monitoring metrics verified.")

    # Test 1a: Check proxy router loop metrics
    def check_proxy_router_loop_metrics():
        metrics = get_metric_dictionaries(
            "ray_serve_event_loop_monitoring_iterations_total",
            timeout=10,
            timeseries=timeseries,
        )
        for m in metrics:
            if m.get("component") == "proxy" and m.get("loop_type") == "router":
                assert "actor_id" in m, "actor_id tag should be present"
                print(f"Proxy router loop metric found: {m}")
                return True
        return False

    if RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP:
        wait_for_condition(check_proxy_router_loop_metrics, timeout=30)
        print("Proxy router loop monitoring metrics verified.")
    else:
        print("Proxy router loop monitoring metrics not verified.")

    # Test 2: Check replica main loop metrics
    def check_replica_main_loop_metrics():
        metrics = get_metric_dictionaries(
            "ray_serve_event_loop_monitoring_iterations_total",
            timeout=10,
            timeseries=timeseries,
        )
        for m in metrics:
            if m.get("component") == "replica" and m.get("loop_type") == "main":
                assert "actor_id" in m, "actor_id tag should be present"
                assert m.get("deployment") in [
                    "f",
                    "g",
                ], "deployment tag should be 'f' or 'g'"
                assert m.get("application") == "app", "application tag should be 'app'"
                print(f"Replica main loop metric found: {m}")
                return True
        return False

    wait_for_condition(check_replica_main_loop_metrics, timeout=30)
    print("Replica main loop monitoring metrics verified.")

    # Test 3: Check replica user_code loop metrics (enabled by default)
    def check_replica_user_code_loop_metrics():
        metrics = get_metric_dictionaries(
            "ray_serve_event_loop_monitoring_iterations_total",
            timeout=10,
            timeseries=timeseries,
        )
        for m in metrics:
            if m.get("component") == "replica" and m.get("loop_type") == "user_code":
                assert "actor_id" in m, "actor_id tag should be present"
                assert m.get("deployment") in [
                    "f",
                    "g",
                ], "deployment tag should be 'f' or 'g'"
                assert m.get("application") == "app", "application tag should be 'app'"
                print(f"Replica user_code loop metric found: {m}")
                return True
        return False

    if RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD:
        wait_for_condition(check_replica_user_code_loop_metrics, timeout=30)
        print("Replica user_code loop monitoring metrics verified.")
    else:
        print("Replica user_code loop monitoring metrics not verified.")

    # Test 4: Check router loop metrics (enabled by default)
    def check_router_loop_metrics():
        metrics = get_metric_dictionaries(
            "ray_serve_event_loop_monitoring_iterations_total",
            timeout=10,
            timeseries=timeseries,
        )
        for m in metrics:
            if m.get("component") == "replica" and m.get("loop_type") == "router":
                assert "actor_id" in m, "actor_id tag should be present"
                print(f"Router loop metric found: {m}")
                return True
        return False

    if RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP:
        wait_for_condition(check_router_loop_metrics, timeout=30)
        print("Router loop monitoring metrics verified.")
    else:
        print("Router loop monitoring metrics not verified.")

    # Test 5: Check that scheduling latency histogram exists and has reasonable values
    def check_scheduling_latency_metric():
        # Check for the histogram count metric
        metrics = get_metric_dictionaries(
            "ray_serve_event_loop_scheduling_latency_ms_count",
            timeout=10,
            timeseries=timeseries,
        )
        # Should have metrics for proxy main, replica main, replica user_code, router
        component_loop_pairs = set()
        for m in metrics:
            component = m.get("component")
            loop_type = m.get("loop_type")
            if component and loop_type:
                component_loop_pairs.add((component, loop_type))

        expected_pairs = {
            ("proxy", "main"),
            ("replica", "main"),
        }
        if RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD:
            expected_pairs.add(("replica", "user_code"))
        if RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP:
            expected_pairs.add(("replica", "router"))
            expected_pairs.add(("proxy", "router"))
        return expected_pairs.issubset(component_loop_pairs)

    wait_for_condition(check_scheduling_latency_metric, timeout=30)
    print("Scheduling latency histogram metrics verified.")

    # Test 6: Check that tasks gauge exists
    def check_tasks_gauge_metric():
        metrics = get_metric_dictionaries(
            "ray_serve_event_loop_tasks",
            timeout=10,
            timeseries=timeseries,
        )
        # Should have metrics for proxy main, replica main, replica user_code, router
        component_loop_pairs = set()
        for m in metrics:
            component = m.get("component")
            loop_type = m.get("loop_type")
            if component and loop_type:
                component_loop_pairs.add((component, loop_type))

        expected_pairs = {
            ("proxy", "main"),
            ("replica", "main"),
        }
        if RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD:
            expected_pairs.add(("replica", "user_code"))
        if RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP:
            expected_pairs.add(("replica", "router"))
            expected_pairs.add(("proxy", "router"))
        return expected_pairs.issubset(component_loop_pairs)

    wait_for_condition(check_tasks_gauge_metric, timeout=30)
    print("Event loop tasks gauge metrics verified.")


def test_actor_summary(serve_instance):
    @serve.deployment
    def f():
        pass

    serve.run(f.bind(), name="app")
    actors = list_actors(filters=[("state", "=", "ALIVE")])
    class_names = {actor.class_name for actor in actors}
    assert class_names.issuperset(
        {"ServeController", "ProxyActor", "ServeReplica:app:f"}
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
