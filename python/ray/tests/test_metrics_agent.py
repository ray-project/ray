import json
import os
import pathlib
import re
import signal
import sys
import time
import warnings
from collections import defaultdict
from pprint import pformat
from unittest.mock import MagicMock

import numpy as np
import pytest
import requests
from google.protobuf.timestamp_pb2 import Timestamp

import ray
from ray._common.network_utils import build_address, find_free_port
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.metrics_agent import (
    Gauge as MetricsAgentGauge,
    PrometheusServiceDiscoveryWriter,
)
from ray._private.ray_constants import (
    PROMETHEUS_SERVICE_DISCOVERY_FILE,
    RAY_ENABLE_OPEN_TELEMETRY,
)
from ray._private.test_utils import (
    PrometheusTimeseries,
    fetch_prometheus_metric_timeseries,
    fetch_prometheus_timeseries,
    get_log_batch,
    raw_metric_timeseries,
)
from ray.autoscaler._private.constants import AUTOSCALER_METRIC_PORT
from ray.core.generated.common_pb2 import TaskAttempt
from ray.core.generated.events_base_event_pb2 import RayEvent
from ray.core.generated.events_event_aggregator_service_pb2 import (
    AddEventsRequest,
    RayEventsData,
    TaskEventsMetadata,
)
from ray.dashboard.consts import DASHBOARD_METRIC_PORT
from ray.dashboard.modules.aggregator.constants import CONSUMER_TAG_KEY
from ray.dashboard.modules.aggregator.tests.test_aggregator_agent import (
    get_event_aggregator_grpc_stub,
)
from ray.util.metrics import Counter, Gauge, Histogram, Metric
from ray.util.state import list_nodes

os.environ["RAY_event_stats"] = "1"

try:
    import prometheus_client
except ImportError:
    prometheus_client = None

# This list of metrics should be kept in sync with src/ray/stats/metric_defs.h
# NOTE: Commented out metrics are not available in this test.
# TODO(Clark): Find ways to trigger commented out metrics in cluster setup.
_METRICS = [
    "ray_node_disk_usage",
    "ray_node_mem_used",
    "ray_node_mem_total",
    "ray_node_cpu_utilization",
    # TODO(rickyx): refactoring the below 3 metric seem to be a bit involved
    # , e.g. need to see how users currently depend on them.
    "ray_object_store_available_memory",
    "ray_object_store_used_memory",
    "ray_object_store_num_local_objects",
    "ray_object_store_memory",
    "ray_object_manager_num_pull_requests",
    "ray_object_directory_subscriptions",
    "ray_object_directory_updates",
    "ray_object_directory_lookups",
    "ray_object_directory_added_locations",
    "ray_object_directory_removed_locations",
    "ray_internal_num_processes_started",
    "ray_internal_num_spilled_tasks",
    # "ray_unintentional_worker_failures_total",
    # "ray_node_failure_total",
    "ray_grpc_server_req_process_time_ms_sum",
    "ray_grpc_server_req_process_time_ms_bucket",
    "ray_grpc_server_req_process_time_ms_count",
    "ray_grpc_server_req_new_total",
    "ray_grpc_server_req_handling_total",
    "ray_grpc_server_req_finished_total",
    "ray_object_manager_received_chunks",
    "ray_pull_manager_usage_bytes",
    "ray_pull_manager_requested_bundles",
    "ray_pull_manager_requests",
    "ray_pull_manager_active_bundles",
    "ray_pull_manager_retries_total",
    "ray_push_manager_num_pushes_remaining",
    "ray_push_manager_chunks",
    "ray_scheduler_failed_worker_startup_total",
    "ray_scheduler_tasks",
    "ray_scheduler_unscheduleable_tasks",
    "ray_spill_manager_objects",
    "ray_spill_manager_objects_bytes",
    "ray_spill_manager_request_total",
    # "ray_spill_manager_throughput_mb",
    "ray_gcs_placement_group_creation_latency_ms_sum",
    "ray_gcs_placement_group_scheduling_latency_ms_sum",
    "ray_gcs_placement_group_count",
    "ray_gcs_actors_count",
]

# This list of metrics should be kept in sync with
# ray/python/ray/autoscaler/_private/prom_metrics.py
_AUTOSCALER_METRICS = [
    "autoscaler_config_validation_exceptions",
    "autoscaler_node_launch_exceptions",
    "autoscaler_pending_nodes",
    "autoscaler_reset_exceptions",
    "autoscaler_running_workers",
    "autoscaler_started_nodes",
    "autoscaler_stopped_nodes",
    "autoscaler_update_loop_exceptions",
    "autoscaler_worker_create_node_time",
    "autoscaler_worker_update_time",
    "autoscaler_updating_nodes",
    "autoscaler_successful_updates",
    "autoscaler_failed_updates",
    "autoscaler_failed_create_nodes",
    "autoscaler_recovering_nodes",
    "autoscaler_successful_recoveries",
    "autoscaler_failed_recoveries",
    "autoscaler_drain_node_exceptions",
    "autoscaler_update_time",
    "autoscaler_cluster_resources",
    "autoscaler_pending_resources",
]


# This list of metrics should be kept in sync with
# dashboard/dashboard_metrics.py
_DASHBOARD_METRICS = [
    "ray_dashboard_api_requests_duration_seconds_bucket",
    "ray_dashboard_api_requests_duration_seconds_created",
    "ray_dashboard_api_requests_count_requests_total",
    "ray_dashboard_api_requests_count_requests_created",
    "ray_component_cpu_percentage",
    "ray_component_uss_mb",
]

_EVENT_AGGREGATOR_METRICS = [
    "ray_aggregator_agent_events_received_total",
    "ray_aggregator_agent_published_events_total",
    "ray_aggregator_agent_filtered_events_total",
    "ray_aggregator_agent_queue_dropped_events_total",
    "ray_aggregator_agent_consecutive_failures_since_last_success",
    "ray_aggregator_agent_time_since_last_success_seconds",
    "ray_aggregator_agent_publish_latency_seconds_bucket",
    "ray_aggregator_agent_publish_latency_seconds_count",
    "ray_aggregator_agent_publish_latency_seconds_sum",
]

_NODE_METRICS = [
    "ray_node_cpu_utilization",
    "ray_node_cpu_count",
    "ray_node_mem_used",
    "ray_node_mem_available",
    "ray_node_mem_total",
    "ray_node_disk_io_read",
    "ray_node_disk_io_write",
    "ray_node_disk_io_read_count",
    "ray_node_disk_io_write_count",
    "ray_node_disk_io_read_speed",
    "ray_node_disk_io_write_speed",
    "ray_node_disk_read_iops",
    "ray_node_disk_write_iops",
    "ray_node_disk_usage",
    "ray_node_disk_free",
    "ray_node_disk_utilization_percentage",
    "ray_node_network_sent",
    "ray_node_network_received",
    "ray_node_network_send_speed",
    "ray_node_network_receive_speed",
]


if sys.platform == "linux" or sys.platform == "linux2":
    _NODE_METRICS.append("ray_node_mem_shared_bytes")


_NODE_COMPONENT_METRICS = [
    "ray_component_cpu_percentage",
    "ray_component_rss_mb",
    "ray_component_uss_mb",
    "ray_component_num_fds",
]

_METRICS.append("ray_health_check_rpc_latency_ms_sum")


@pytest.fixture
def _setup_cluster_for_test(request, ray_start_cluster):
    enable_metrics_collection = request.param
    NUM_NODES = 2
    cluster = ray_start_cluster
    # Add a head node.
    cluster.add_node(
        _system_config={
            "metrics_report_interval_ms": 1000,
            "event_stats_print_interval_ms": 500,
            "event_stats": True,
            "enable_metrics_collection": enable_metrics_collection,
            "enable_open_telemetry": RAY_ENABLE_OPEN_TELEMETRY,
        }
    )
    # Add worker nodes.
    [cluster.add_node() for _ in range(NUM_NODES - 1)]
    cluster.wait_for_nodes()
    ray_context = ray.init(address=cluster.address)

    worker_should_exit = SignalActor.remote()

    extra_tags = {"ray_version": ray.__version__}

    # Generate metrics in the driver.
    counter = Counter("test_driver_counter", description="desc")
    counter.inc(tags=extra_tags)
    gauge = Gauge("test_gauge", description="gauge")
    gauge.set(1, tags=extra_tags)

    # Generate some metrics from actor & tasks.
    @ray.remote
    def f():
        counter = Counter("test_counter", description="desc")
        counter.inc()
        counter = ray.get(ray.put(counter))  # Test serialization.
        counter.inc(tags=extra_tags)
        counter.inc(2, tags=extra_tags)
        ray.get(worker_should_exit.wait.remote())

    # Generate some metrics for the placement group.
    pg = ray.util.placement_group(bundles=[{"CPU": 1}])
    ray.get(pg.ready())
    ray.util.remove_placement_group(pg)

    @ray.remote
    class A:
        async def ping(self):
            histogram = Histogram(
                "test_histogram", description="desc", boundaries=[0.1, 1.6]
            )
            histogram = ray.get(ray.put(histogram))  # Test serialization.
            histogram.observe(1.5, tags=extra_tags)
            histogram.observe(0.0, tags=extra_tags)
            ray.get(worker_should_exit.wait.remote())

    a = A.remote()
    obj_refs = [f.remote(), a.ping.remote()]
    # Infeasible task
    b = f.options(resources={"a": 1})  # noqa

    # Make a request to the dashboard to produce some dashboard metrics
    requests.get(f"http://{ray_context.dashboard_url}/nodes")

    node_info_list = ray.nodes()
    prom_addresses = []
    for node_info in node_info_list:
        metrics_export_port = node_info["MetricsExportPort"]
        addr = node_info["NodeManagerAddress"]
        prom_addresses.append(build_address(addr, metrics_export_port))
    autoscaler_export_addr = build_address(
        cluster.head_node.node_ip_address, AUTOSCALER_METRIC_PORT
    )
    dashboard_export_addr = build_address(
        cluster.head_node.node_ip_address, DASHBOARD_METRIC_PORT
    )
    yield prom_addresses, autoscaler_export_addr, dashboard_export_addr

    ray.get(worker_should_exit.send.remote())
    ray.get(obj_refs)
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.skipif(prometheus_client is None, reason="Prometheus not installed")
@pytest.mark.parametrize("_setup_cluster_for_test", [True], indirect=True)
def test_metrics_export_end_to_end(_setup_cluster_for_test):
    TEST_TIMEOUT_S = 30
    (
        prom_addresses,
        autoscaler_export_addr,
        dashboard_export_addr,
    ) = _setup_cluster_for_test
    ray_timeseries = PrometheusTimeseries()
    autoscaler_timeseries = PrometheusTimeseries()
    dashboard_timeseries = PrometheusTimeseries()

    def test_cases():
        fetch_prometheus_timeseries(prom_addresses, ray_timeseries)
        components_dict = ray_timeseries.components_dict
        metric_descriptors = ray_timeseries.metric_descriptors
        metric_samples = ray_timeseries.metric_samples.values()
        metric_names = metric_descriptors.keys()

        session_name = ray._private.worker.global_worker.node.session_name

        # Raylet should be on every node
        assert all("raylet" in components for components in components_dict.values())

        # GCS server should be on one node
        assert any(
            "gcs_server" in components for components in components_dict.values()
        )

        # Core worker should be on at least on node
        assert any(
            "core_worker" in components for components in components_dict.values()
        )
        # The list of custom or user defined metrics. Open Telemetry backend does not
        # support exporting Counter as Gauge, so we skip some metrics in that case.
        custom_metrics = (
            [
                "test_counter",
                "test_counter_total",
                "test_driver_counter",
                "test_driver_counter_total",
                "test_gauge",
            ]
            if not RAY_ENABLE_OPEN_TELEMETRY
            else [
                "test_counter_total",
                "test_driver_counter_total",
                "test_gauge",
            ]
        )

        # Make sure our user defined metrics exist and have the correct types
        for metric_name in custom_metrics:
            metric_name = f"ray_{metric_name}"
            assert metric_name in metric_names
            if metric_name.endswith("_total"):
                assert metric_descriptors[metric_name].type == "counter"
            elif metric_name.endswith("_counter"):
                # Make sure we emit counter as gauge for bug compatibility
                assert metric_descriptors[metric_name].type == "gauge"
            elif metric_name.endswith("_bucket"):
                assert metric_descriptors[metric_name].type == "histogram"
            elif metric_name.endswith("_gauge"):
                assert metric_descriptors[metric_name].type == "gauge"

        # Make sure metrics are recorded.
        for metric in _METRICS:
            assert metric in metric_names, f"metric {metric} not in {metric_names}"

        for sample in metric_samples:
            # All Ray metrics have label "Version" and "SessionName".
            if sample.name in _METRICS or sample.name in _DASHBOARD_METRICS:
                assert sample.labels.get("Version") == ray.__version__, sample
                assert sample.labels["SessionName"] == session_name, sample

        # Make sure the numeric values are correct
        test_counter_sample = [m for m in metric_samples if "test_counter" in m.name][0]
        assert test_counter_sample.value == 4.0

        test_driver_counter_sample = [
            m for m in metric_samples if "test_driver_counter" in m.name
        ][0]
        assert test_driver_counter_sample.value == 1.0

        # Make sure the gRPC stats are not reported from workers. We disabled
        # it there because it has too high cardinality.
        grpc_metrics = [
            "ray_grpc_server_req_process_time_ms_sum",
            "ray_grpc_server_req_process_time_ms_bucket",
            "ray_grpc_server_req_process_time_ms_count",
            "ray_grpc_server_req_new_total",
            "ray_grpc_server_req_handling_total",
            "ray_grpc_server_req_finished_total",
        ]
        for grpc_metric in grpc_metrics:
            grpc_samples = [m for m in metric_samples if grpc_metric in m.name]
            for grpc_sample in grpc_samples:
                assert grpc_sample.labels["Component"] != "core_worker"

        # Autoscaler metrics
        fetch_prometheus_timeseries([autoscaler_export_addr], autoscaler_timeseries)
        autoscaler_metric_descriptors = autoscaler_timeseries.metric_descriptors
        autoscaler_samples = autoscaler_timeseries.metric_samples.values()
        autoscaler_metric_names = autoscaler_metric_descriptors.keys()
        for metric in _AUTOSCALER_METRICS:
            # Metric name should appear with some suffix (_count, _total,
            # etc...) in the list of all names
            assert any(
                name.startswith(metric) for name in autoscaler_metric_names
            ), f"{metric} not in {autoscaler_metric_names}"
            for sample in autoscaler_samples:
                assert sample.labels["SessionName"] == session_name

        # Dashboard metrics
        fetch_prometheus_timeseries([dashboard_export_addr], dashboard_timeseries)
        dashboard_metric_descriptors = dashboard_timeseries.metric_descriptors
        dashboard_metric_names = dashboard_metric_descriptors.keys()
        for metric in _DASHBOARD_METRICS:
            # Metric name should appear with some suffix (_count, _total,
            # etc...) in the list of all names
            assert any(
                name.startswith(metric) for name in dashboard_metric_names
            ), f"{metric} not in {dashboard_metric_names}"

    def wrap_test_case_for_retry():
        try:
            test_cases()
            return True
        except AssertionError:
            return False

    try:
        wait_for_condition(
            wrap_test_case_for_retry,
            timeout=TEST_TIMEOUT_S,
            retry_interval_ms=1000,  # Yield resource for other processes
        )
    except RuntimeError:
        # print(f"The components are {pformat(ray_timeseries)}")
        test_cases()  # Should fail assert


@pytest.mark.skipif(sys.platform == "win32", reason="Not working in Windows.")
@pytest.mark.skipif(prometheus_client is None, reason="Prometheus not installed")
def test_metrics_export_node_metrics(shutdown_only):
    # Verify node metrics are available.
    addr = ray.init()
    dashboard_export_addr = build_address(
        addr["node_ip_address"], DASHBOARD_METRIC_PORT
    )
    node_timeseries = PrometheusTimeseries()
    dashboard_timeseries = PrometheusTimeseries()

    def verify_node_metrics():
        avail_metrics = raw_metric_timeseries(addr, node_timeseries)

        components = set()
        for metric in _NODE_COMPONENT_METRICS:
            samples = avail_metrics[metric]
            for sample in samples:
                components.add(sample.labels["Component"])
        assert components == {"gcs", "raylet", "agent", "ray::IDLE", sys.executable}

        avail_metrics = set(avail_metrics)

        for node_metric in _NODE_METRICS:
            assert node_metric in avail_metrics
        for node_metric in _NODE_COMPONENT_METRICS:
            assert node_metric in avail_metrics
        return True

    def verify_dashboard_metrics():
        avail_metrics = fetch_prometheus_metric_timeseries(
            [dashboard_export_addr], dashboard_timeseries
        )
        # Run list nodes to trigger dashboard API.
        list_nodes()

        # Verify metrics exist.
        for metric in _DASHBOARD_METRICS:
            # Metric name should appear with some suffix (_count, _total,
            # etc...) in the list of all names
            assert len(avail_metrics[metric]) > 0

            samples = avail_metrics[metric]
            for sample in samples:
                assert sample.labels["Component"].startswith("dashboard")

        return True

    wait_for_condition(verify_node_metrics)
    wait_for_condition(verify_dashboard_metrics)


_EVENT_AGGREGATOR_AGENT_TARGET_PORT = find_free_port()
_EVENT_AGGREGATOR_AGENT_TARGET_IP = "127.0.0.1"
_EVENT_AGGREGATOR_AGENT_TARGET_ADDR = (
    f"http://{_EVENT_AGGREGATOR_AGENT_TARGET_IP}:{_EVENT_AGGREGATOR_AGENT_TARGET_PORT}"
)


@pytest.fixture(scope="module")
def httpserver_listen_address():
    return ("127.0.0.1", _EVENT_AGGREGATOR_AGENT_TARGET_PORT)


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "env_vars": {
                "RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_BUFFER_SIZE": 2,
                "RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR": _EVENT_AGGREGATOR_AGENT_TARGET_ADDR,
                # Turn off task events generation to avoid the task events from the
                # cluster impacting the test result
                "RAY_task_events_report_interval_ms": 0,
                "RAY_enable_open_telemetry": "true",
            },
        },
    ],
    indirect=True,
)
def test_metrics_export_event_aggregator_agent(
    ray_start_cluster_head_with_env_vars, httpserver
):
    cluster = ray_start_cluster_head_with_env_vars
    stub = get_event_aggregator_grpc_stub(
        cluster.gcs_address, cluster.head_node.node_id
    )
    httpserver.expect_request("/", method="POST").respond_with_data("", status=200)

    metrics_export_port = cluster.head_node.metrics_export_port
    addr = cluster.head_node.node_ip_address
    prom_addresses = [build_address(addr, metrics_export_port)]
    timeseries = PrometheusTimeseries()

    def test_case_stats_exist():
        fetch_prometheus_timeseries(prom_addresses, timeseries)
        metric_descriptors = timeseries.metric_descriptors
        metrics_names = metric_descriptors.keys()
        event_aggregator_metrics = [
            "ray_aggregator_agent_events_received_total",
            "ray_aggregator_agent_published_events_total",
            "ray_aggregator_agent_filtered_events_total",
            "ray_aggregator_agent_queue_dropped_events_total",
            "ray_aggregator_agent_consecutive_failures_since_last_success",
            "ray_aggregator_agent_time_since_last_success_seconds",
            "ray_aggregator_agent_publish_latency_seconds_bucket",
            "ray_aggregator_agent_publish_latency_seconds_count",
            "ray_aggregator_agent_publish_latency_seconds_sum",
        ]
        return all(metric in metrics_names for metric in event_aggregator_metrics)

    def test_case_value_correct():
        fetch_prometheus_timeseries(prom_addresses, timeseries)
        metric_samples = timeseries.metric_samples.values()
        expected_metrics_values = {
            "ray_aggregator_agent_events_received_total": 3.0,
        }
        for descriptor, expected_value in expected_metrics_values.items():
            samples = [m for m in metric_samples if m.name == descriptor]
            if not samples:
                return False
            if samples[0].value != expected_value:
                return False
        return True

    def test_case_publisher_specific_metrics_correct(publisher_name: str):
        fetch_prometheus_timeseries(prom_addresses, timeseries)
        metric_samples = timeseries.metric_samples.values()
        expected_metrics_values = {
            "ray_aggregator_agent_published_events_total": 1.0,
            "ray_aggregator_agent_filtered_events_total": 1.0,
            "ray_aggregator_agent_queue_dropped_events_total": 1.0,
        }
        for descriptor, expected_value in expected_metrics_values.items():
            samples = [m for m in metric_samples if m.name == descriptor]
            if not samples:
                return False
            if (
                samples[0].value != expected_value
                or samples[0].labels[CONSUMER_TAG_KEY] != publisher_name
            ):
                return False
        return True

    now = time.time_ns()
    seconds, nanos = divmod(now, 10**9)
    timestamp = Timestamp(seconds=seconds, nanos=nanos)
    request = AddEventsRequest(
        events_data=RayEventsData(
            events=[
                RayEvent(
                    event_id=b"1",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="hello",
                ),
                RayEvent(
                    event_id=b"2",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_PROFILE_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="hello 2",
                ),
                RayEvent(
                    event_id=b"3",
                    source_type=RayEvent.SourceType.CORE_WORKER,
                    event_type=RayEvent.EventType.TASK_DEFINITION_EVENT,
                    timestamp=timestamp,
                    severity=RayEvent.Severity.INFO,
                    message="hello 3",
                ),
            ],
            task_events_metadata=TaskEventsMetadata(
                dropped_task_attempts=[
                    TaskAttempt(
                        task_id=b"1",
                        attempt_number=1,
                    ),
                ],
            ),
        )
    )

    stub.AddEvents(request)
    wait_for_condition(lambda: len(httpserver.log) == 1)

    wait_for_condition(test_case_stats_exist, timeout=30, retry_interval_ms=1000)

    wait_for_condition(test_case_value_correct, timeout=30, retry_interval_ms=1000)

    wait_for_condition(
        lambda: test_case_publisher_specific_metrics_correct("http_publisher"),
        timeout=30,
        retry_interval_ms=1000,
    )


def test_operation_stats(monkeypatch, shutdown_only):
    # Test operation stats are available when flag is on.
    operation_metrics = [
        "ray_operation_count_total",
        "ray_operation_run_time_ms_bucket",
        "ray_operation_queue_time_ms_bucket",
        "ray_operation_active_count",
    ]

    monkeypatch.setenv("RAY_emit_main_service_metrics", "1")
    timeseries = PrometheusTimeseries()
    addr = ray.init()
    remote_signal = SignalActor.remote()

    @ray.remote
    class Actor:
        def __init__(self, signal):
            self.signal = signal

        def get_worker_id(self):
            return ray.get_runtime_context().get_worker_id()

        def wait(self):
            ray.get(self.signal.wait.remote())

    actor = Actor.remote(remote_signal)
    ray.get(actor.get_worker_id.remote())
    obj_ref = actor.wait.remote()

    ray.get(remote_signal.send.remote())
    ray.get(obj_ref)

    def verify():
        metrics = raw_metric_timeseries(addr, timeseries)

        samples = metrics["ray_operation_active_count"]
        found = False
        for sample in samples:
            if (
                sample.labels["Name"] == "gcs_server_main_io_context"
                and sample.labels["Component"] == "gcs_server"
            ):
                found = True
        if not found:
            return False

        found = False
        for sample in samples:
            if (
                sample.labels["Name"] == "raylet_main_io_context"
                and sample.labels["Component"] == "raylet"
            ):
                found = True
        if not found:
            return False

        metric_names = set(metrics.keys())
        for op_metric in operation_metrics:
            assert op_metric in metric_names
            samples = metrics[op_metric]
            components = set()
            print(components)
            for sample in samples:
                components.add(sample.labels["Component"])
            assert {"raylet", "gcs_server"} == components
        return True

    wait_for_condition(verify, timeout=30)


@pytest.mark.skipif(prometheus_client is None, reason="Prometheus not installed")
@pytest.mark.parametrize("_setup_cluster_for_test", [True], indirect=True)
def test_histogram(_setup_cluster_for_test):
    TEST_TIMEOUT_S = 30
    (
        prom_addresses,
        autoscaler_export_addr,
        dashboard_export_addr,
    ) = _setup_cluster_for_test
    timeseries = PrometheusTimeseries()

    def test_cases():
        fetch_prometheus_timeseries(prom_addresses, timeseries)
        metric_descriptors = timeseries.metric_descriptors
        metric_samples = timeseries.metric_samples.values()
        metric_names = metric_descriptors.keys()
        custom_histogram_metric_name = "ray_test_histogram_bucket"
        assert custom_histogram_metric_name in metric_names
        assert metric_descriptors[custom_histogram_metric_name].type == "histogram"

        test_histogram_samples = [
            m for m in metric_samples if "test_histogram" in m.name
        ]
        buckets = {
            m.labels["le"]: m.value
            for m in test_histogram_samples
            if "_bucket" in m.name
        }
        # In Prometheus data model
        # the histogram is cumulative. So we expect the count to appear in
        # <1.1 and <+Inf buckets.
        assert buckets == {"0.1": 1.0, "1.6": 2.0, "+Inf": 2.0}
        hist_count = [m for m in test_histogram_samples if "_count" in m.name][0].value
        assert hist_count == 2

    def wrap_test_case_for_retry():
        try:
            test_cases()
            return True
        except AssertionError:
            return False

    try:
        wait_for_condition(
            wrap_test_case_for_retry,
            timeout=TEST_TIMEOUT_S,
            retry_interval_ms=1000,  # Yield resource for other processes
        )
    except RuntimeError:
        print(f"The components are {pformat(timeseries)}")
        test_cases()  # Should fail assert


@pytest.mark.skipif(sys.platform == "win32", reason="Not working in Windows.")
@pytest.mark.skipif(
    RAY_ENABLE_OPEN_TELEMETRY,
    reason="OpenTelemetry backend does not support Counter exported as gauge.",
)
def test_counter_exported_as_gauge(shutdown_only):
    # Test to make sure Counter emits the right Prometheus metrics
    context = ray.init()
    timeseries = PrometheusTimeseries()

    @ray.remote
    class Actor:
        def __init__(self):
            self.counter = Counter("test_counter", description="desc")
            self.counter.inc(2.0)
            self.counter.inc(3.0)

            self.counter_with_total_suffix = Counter(
                "test_counter2_total", description="desc2"
            )
            self.counter_with_total_suffix.inc(1.5)

    _ = Actor.remote()

    def check_metrics():
        metrics_page = "localhost:{}".format(
            context.address_info["metrics_export_port"]
        )
        fetch_prometheus_timeseries([metrics_page], timeseries)
        metric_descriptors = timeseries.metric_descriptors
        metric_samples = timeseries.metric_samples.values()
        metric_samples_by_name = defaultdict(list)
        for metric_sample in metric_samples:
            metric_samples_by_name[metric_sample.name].append(metric_sample)

        assert "ray_test_counter" in metric_descriptors
        assert metric_descriptors["ray_test_counter"].type == "gauge"
        assert (
            metric_descriptors["ray_test_counter"].documentation
            == "(DEPRECATED, use ray_test_counter_total metric instead) desc"
        )
        assert metric_samples_by_name["ray_test_counter"][-1].value == 5.0

        assert "ray_test_counter_total" in metric_descriptors
        assert metric_descriptors["ray_test_counter_total"].type == "counter"
        assert metric_descriptors["ray_test_counter_total"].documentation == "desc"
        assert metric_samples_by_name["ray_test_counter_total"][-1].value == 5.0

        assert "ray_test_counter2_total" in metric_descriptors
        assert metric_descriptors["ray_test_counter2_total"].type == "counter"
        assert metric_descriptors["ray_test_counter2_total"].documentation == "desc2"
        assert metric_samples_by_name["ray_test_counter2_total"][-1].value == 1.5

        return True

    wait_for_condition(check_metrics, timeout=60)


@pytest.mark.skipif(sys.platform == "win32", reason="Not working in Windows.")
def test_counter(monkeypatch, shutdown_only):
    # Test to make sure we don't export counter as gauge
    # if RAY_EXPORT_COUNTER_AS_GAUGE is 0
    monkeypatch.setenv("RAY_EXPORT_COUNTER_AS_GAUGE", "0")
    context = ray.init()
    timeseries = PrometheusTimeseries()

    @ray.remote
    class Actor:
        def __init__(self):
            self.counter = Counter("test_counter", description="desc")
            self.counter.inc(2.0)

    _ = Actor.remote()

    def check_metrics():
        metrics_page = "localhost:{}".format(
            context.address_info["metrics_export_port"]
        )
        fetch_prometheus_timeseries([metrics_page], timeseries)
        metric_descriptors = timeseries.metric_descriptors

        assert "ray_test_counter" not in metric_descriptors
        assert "ray_test_counter_total" in metric_descriptors

        return True

    wait_for_condition(check_metrics, timeout=60)


@pytest.mark.skipif(sys.platform == "win32", reason="Not working in Windows.")
def test_per_func_name_stats(shutdown_only):
    # Test operation stats are available when flag is on.
    comp_metrics = [
        "ray_component_cpu_percentage",
        "ray_component_rss_mb",
        "ray_component_num_fds",
    ]
    timeseries = PrometheusTimeseries()
    if sys.platform == "linux" or sys.platform == "linux2":
        # Uss only available from Linux
        comp_metrics.append("ray_component_uss_mb")
        comp_metrics.append("ray_component_mem_shared_bytes")
    addr = ray.init(num_cpus=2)

    @ray.remote
    class Actor:
        def __init__(self):
            self.arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
            self.shared_arr = ray.put(np.random.rand(5 * 1024 * 1024))

        def pid(self):
            return os.getpid()

    @ray.remote
    class ActorB:
        def __init__(self):
            self.arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
            self.shared_arr = ray.put(np.random.rand(5 * 1024 * 1024))

    a = Actor.remote()  # noqa
    b = ActorB.remote()

    ray.get(a.__ray_ready__.remote())
    ray.get(b.__ray_ready__.remote())

    # Run a short lived task to make sure there's a ray::IDLE component.
    @ray.remote
    def do_nothing():
        pass

    ray.get(do_nothing.remote())

    def verify_components():
        metrics = raw_metric_timeseries(addr, timeseries)
        metric_names = set(metrics.keys())
        components = set()
        for metric in comp_metrics:
            assert metric in metric_names
            samples = metrics[metric]
            for sample in samples:
                components.add(sample.labels["Component"])
        print(components)
        assert {
            sys.executable,  # driver process
            "raylet",
            "agent",
            "ray::Actor",
            "ray::ActorB",
            "ray::IDLE",
        } <= components
        return True

    wait_for_condition(verify_components, timeout=30)

    def verify_mem_usage():
        metrics = raw_metric_timeseries(addr, timeseries)
        for metric in comp_metrics:
            samples = metrics[metric]
            for sample in samples:
                if sample.labels["Component"] == "ray::ActorB":
                    assert sample.value > 0.0
                    print(sample)
                    print(sample.value)
                if sample.labels["Component"] == "ray::Actor":
                    assert sample.value > 0.0
                    print(sample)
                    print(sample.value)
        return True

    wait_for_condition(verify_mem_usage, timeout=30)

    # Verify ActorB is reported as value 0 because it is killed.
    ray.kill(b)
    # Kill Actor by sigkill, which happens upon OOM.
    pid = ray.get(a.pid.remote())
    os.kill(pid, signal.SIGKILL)

    def verify_mem_cleaned():
        metrics = raw_metric_timeseries(addr, timeseries)
        for metric in comp_metrics:
            samples = metrics[metric]
            for sample in samples:
                if sample.labels["Component"] == "ray::ActorB":
                    assert sample.value == 0.0
                if sample.labels["Component"] == "ray::Actor":
                    assert sample.value == 0.0
        return True

    wait_for_condition(verify_mem_cleaned, timeout=30)


def test_prometheus_file_based_service_discovery(ray_start_cluster):
    # Make sure Prometheus service discovery file is correctly written
    # when number of nodes are dynamically changed.
    NUM_NODES = 5
    cluster = ray_start_cluster
    nodes = [cluster.add_node() for _ in range(NUM_NODES)]
    cluster.wait_for_nodes()
    addr = ray.init(address=cluster.address)
    writer = PrometheusServiceDiscoveryWriter(
        addr["gcs_address"],
        "/tmp/ray",
    )

    def get_metrics_export_address_from_node(nodes):
        node_export_addrs = [
            build_address(node.node_ip_address, node.metrics_export_port)
            for node in nodes
        ]
        # monitor should be run on head node for `ray_start_cluster` fixture
        autoscaler_export_addr = build_address(
            cluster.head_node.node_ip_address, AUTOSCALER_METRIC_PORT
        )
        dashboard_export_addr = build_address(
            cluster.head_node.node_ip_address, DASHBOARD_METRIC_PORT
        )
        return node_export_addrs + [autoscaler_export_addr, dashboard_export_addr]

    loaded_json_data = json.loads(writer.get_file_discovery_content())
    assert loaded_json_data == writer.get_latest_service_discovery_content()
    assert set(get_metrics_export_address_from_node(nodes)) == set(
        loaded_json_data[0]["targets"]
    )

    # Let's update nodes.
    for _ in range(3):
        nodes.append(cluster.add_node())

    # Make sure service discovery file content is correctly updated.
    loaded_json_data = json.loads(writer.get_file_discovery_content())
    assert loaded_json_data == writer.get_latest_service_discovery_content()
    assert set(get_metrics_export_address_from_node(nodes)) == set(
        loaded_json_data[0]["targets"]
    )


def test_prome_file_discovery_run_by_dashboard(shutdown_only):
    ray.init(num_cpus=0)
    global_node = ray._private.worker._global_node
    temp_dir = global_node.get_temp_dir_path()

    def is_service_discovery_exist():
        for path in pathlib.Path(temp_dir).iterdir():
            if PROMETHEUS_SERVICE_DISCOVERY_FILE in str(path):
                return True
        return False

    wait_for_condition(is_service_discovery_exist)


@pytest.fixture
def metric_mock():
    mock = MagicMock()
    mock.record.return_value = "haha"
    yield mock


"""
Unit test custom metrics.
"""


def test_basic_custom_metrics(metric_mock):
    # Make sure each of metric works as expected.
    # -- Counter --
    count = Counter("count", tag_keys=("a",))
    with pytest.raises(TypeError):
        count.inc("hi")
    with pytest.raises(ValueError):
        count.inc(0)
    with pytest.raises(ValueError):
        count.inc(-1)
    count._metric = metric_mock
    count.inc(1, {"a": "1"})
    metric_mock.record.assert_called_with(1, tags={"a": "1"})

    # -- Gauge --
    gauge = Gauge("gauge", description="gauge")
    gauge._metric = metric_mock
    gauge.set(4)
    metric_mock.record.assert_called_with(4, tags={})

    # -- Histogram
    histogram = Histogram(
        "hist", description="hist", boundaries=[1.0, 3.0], tag_keys=("a", "b")
    )
    histogram._metric = metric_mock
    tags = {"a": "10", "b": "b"}
    histogram.observe(8, tags=tags)
    metric_mock.record.assert_called_with(8, tags=tags)


def test_custom_metrics_with_extra_tags(metric_mock):
    base_tags = {"a": "1"}
    extra_tags = {"a": "1", "b": "2"}

    # -- Counter --
    count = Counter("count", tag_keys=("a",))
    with pytest.raises(ValueError):
        count.inc(1)

    count._metric = metric_mock

    # Increment with base tags
    count.inc(1, tags=base_tags)
    metric_mock.record.assert_called_with(1, tags=base_tags)
    metric_mock.reset_mock()

    # Increment with extra tags
    count.inc(1, tags=extra_tags)
    metric_mock.record.assert_called_with(1, tags=extra_tags)
    metric_mock.reset_mock()

    # -- Gauge --
    gauge = Gauge("gauge", description="gauge", tag_keys=("a",))
    gauge._metric = metric_mock

    # Record with base tags
    gauge.set(4, tags=base_tags)
    metric_mock.record.assert_called_with(4, tags=base_tags)
    metric_mock.reset_mock()

    # Record with extra tags
    gauge.set(4, tags=extra_tags)
    metric_mock.record.assert_called_with(4, tags=extra_tags)
    metric_mock.reset_mock()

    # -- Histogram
    histogram = Histogram(
        "hist", description="hist", boundaries=[1.0, 3.0], tag_keys=("a",)
    )
    histogram._metric = metric_mock

    # Record with base tags
    histogram.observe(8, tags=base_tags)
    metric_mock.record.assert_called_with(8, tags=base_tags)
    metric_mock.reset_mock()

    # Record with extra tags
    histogram.observe(8, tags=extra_tags)
    metric_mock.record.assert_called_with(8, tags=extra_tags)
    metric_mock.reset_mock()


def test_custom_metrics_info(metric_mock):
    # Make sure .info public method works.
    histogram = Histogram(
        "hist", description="hist", boundaries=[1.0, 2.0], tag_keys=("a", "b")
    )
    assert histogram.info["name"] == "hist"
    assert histogram.info["description"] == "hist"
    assert histogram.info["boundaries"] == [1.0, 2.0]
    assert histogram.info["tag_keys"] == ("a", "b")
    assert histogram.info["default_tags"] == {}
    histogram.set_default_tags({"a": "a"})
    assert histogram.info["default_tags"] == {"a": "a"}


def test_custom_metrics_default_tags(metric_mock):
    histogram = Histogram(
        "hist", description="hist", boundaries=[1.0, 2.0], tag_keys=("a", "b")
    ).set_default_tags({"b": "b"})
    histogram._metric = metric_mock

    # Check specifying non-default tags.
    histogram.observe(10, tags={"a": "a"})
    metric_mock.record.assert_called_with(10, tags={"a": "a", "b": "b"})

    # Check overriding default tags.
    tags = {"a": "10", "b": "c"}
    histogram.observe(8, tags=tags)
    metric_mock.record.assert_called_with(8, tags=tags)


def test_custom_metrics_edge_cases(metric_mock):
    # None or empty boundaries are not allowed.
    with pytest.raises(ValueError):
        Histogram("hist")

    with pytest.raises(ValueError):
        Histogram("hist", boundaries=[])

    # Empty name is not allowed.
    with pytest.raises(ValueError):
        Counter("")

    # The tag keys must be a tuple type.
    with pytest.raises(TypeError):
        Counter("name", tag_keys=("a"))

    with pytest.raises(ValueError):
        Histogram("hist", boundaries=[-1, 1, 2])

    with pytest.raises(ValueError):
        Histogram("hist", boundaries=[0, 1, 2])

    with pytest.raises(ValueError):
        Histogram("hist", boundaries=[-1, -0.5, -0.1])


def test_metrics_override_shouldnt_warn(ray_start_regular, log_pubsub):
    # https://github.com/ray-project/ray/issues/12859

    @ray.remote
    def override():
        a = Counter("num_count", description="")
        b = Counter("num_count", description="")
        a.inc(1)
        b.inc(1)

    ray.get(override.remote())

    # Check the stderr from the worker.
    def matcher(log_batch):
        return any("Attempt to register measure" in line for line in log_batch["lines"])

    match = get_log_batch(log_pubsub, 1, timeout=5, matcher=matcher)
    assert len(match) == 0, match


def test_custom_metrics_validation(shutdown_only):
    ray.init()
    # Missing tag(s) from tag_keys.
    metric = Counter("name", tag_keys=("a", "b"))
    metric.set_default_tags({"a": "1"})

    metric.inc(1.0, {"b": "2"})
    metric.inc(1.0, {"a": "1", "b": "2"})

    with pytest.raises(ValueError):
        metric.inc(1.0)

    with pytest.raises(ValueError):
        metric.inc(1.0, {"a": "2"})

    # tag_keys must be tuple.
    with pytest.raises(TypeError):
        Counter("name", tag_keys="a")
    # tag_keys must be strs.
    with pytest.raises(TypeError):
        Counter("name", tag_keys=(1,))

    metric = Counter("name", tag_keys=("a",))
    # Set default tag that isn't in tag_keys.
    with pytest.raises(ValueError):
        metric.set_default_tags({"a": "1", "c": "2"})
    # Default tag value must be str.
    with pytest.raises(TypeError):
        metric.set_default_tags({"a": 1})
    # Tag value must be str.
    with pytest.raises(TypeError):
        metric.inc(1.0, {"a": 1})


@pytest.mark.parametrize("_setup_cluster_for_test", [False], indirect=True)
def test_metrics_disablement(_setup_cluster_for_test):
    """Make sure the metrics are not exported when it is disabled."""
    prom_addresses, autoscaler_export_addr, _ = _setup_cluster_for_test
    timeseries = PrometheusTimeseries()

    def verify_metrics_not_collected():
        fetch_prometheus_timeseries(prom_addresses, timeseries)
        components_dict = timeseries.components_dict
        metric_descriptors = timeseries.metric_descriptors
        metric_names = metric_descriptors.keys()
        # Make sure no component is reported.
        for _, comp in components_dict.items():
            if len(comp) > 0:
                print(f"metrics from a component {comp} exists although it should not.")
                return False

        # Make sure metrics are not there.
        for metric in (
            _METRICS
            + _AUTOSCALER_METRICS
            + _DASHBOARD_METRICS
            + _EVENT_AGGREGATOR_METRICS
        ):
            if metric in metric_names:
                print("f{metric} exists although it should not.")
                return False
        return True

    # Make sure metrics are not collected for more than 10 seconds.
    for _ in range(10):
        assert verify_metrics_not_collected()
        import time

        time.sleep(1)


_FAULTY_METRIC_REGEX = re.compile(".*Invalid metric name.*")


def test_invalid_application_metric_names():
    warnings.simplefilter("always")
    with pytest.raises(
        ValueError, match="Empty name is not allowed. Please provide a metric name."
    ):
        Metric("")
    with pytest.warns(UserWarning, match=_FAULTY_METRIC_REGEX):
        Metric("name-cannot-have-dashes")
    with pytest.warns(UserWarning, match=_FAULTY_METRIC_REGEX):
        Metric("1namecannotstartwithnumber")
    with pytest.warns(UserWarning, match=_FAULTY_METRIC_REGEX):
        Metric("name.cannot.have.dots")


def test_invalid_system_metric_names(caplog):
    with pytest.raises(
        ValueError, match="Empty name is not allowed. Please provide a metric name."
    ):
        MetricsAgentGauge("", "", "", [])
    with pytest.raises(ValueError, match=_FAULTY_METRIC_REGEX):
        MetricsAgentGauge("name-cannot-have-dashes", "", "", [])
    with pytest.raises(ValueError, match=_FAULTY_METRIC_REGEX):
        MetricsAgentGauge("1namecannotstartwithnumber", "", "", [])
    with pytest.raises(ValueError, match=_FAULTY_METRIC_REGEX):
        MetricsAgentGauge("name.cannot.have.dots", "", "", [])


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
