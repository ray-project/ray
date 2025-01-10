import pytest

import ray
from ray import serve
from ray._private.test_utils import wait_for_condition
from ray._private.usage import usage_lib
from ray.cluster_utils import AutoscalingCluster
from ray.serve._private.test_utils import TELEMETRY_ROUTE_PREFIX, start_telemetry_app


@pytest.fixture
def autoscaling_cluster(request, monkeypatch):
    with monkeypatch.context() as m:
        m.setenv("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "0.01")

        params = getattr(request, "param") if hasattr(request, "param") else None
        cluster = AutoscalingCluster(
            **(
                params
                if params
                else {
                    "head_resources": {"CPU": 0},
                    "worker_node_types": {
                        "cpu_node": {
                            "resources": {"CPU": 3},
                            "node_config": {},
                            "min_workers": 0,
                            "max_workers": 10,
                        },
                    },
                    "idle_timeout_minutes": 0.01,
                    "autoscaler_v2": True,
                }
            )
        )
        cluster.start()
        ray.init()
        serve.start()
        yield

        # Call Python API shutdown() methods to clear global variable state
        serve.shutdown()
        ray.shutdown()
        cluster.shutdown()

        # Reset global state (any keys that may have been set and cached while the
        # workload was running).
        usage_lib.reset_global_state()


@pytest.fixture
def autoscaling_cluster_with_metrics(monkeypatch):
    """Fixture provides a fresh Ray cluster to prevent metrics state sharing."""

    with monkeypatch.context() as m:
        m.setenv("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "0.01")
        cluster = AutoscalingCluster(
            **{
                "head_resources": {"CPU": 0},
                "worker_node_types": {
                    "cpu_node": {
                        "resources": {"CPU": 3},
                        "node_config": {},
                        "min_workers": 0,
                        "max_workers": 10,
                    },
                },
                "idle_timeout_minutes": 0.01,
                "autoscaler_v2": True,
            }
        )
        cluster.start(
            _system_config={
                "metrics_report_interval_ms": 100,
                "task_retry_delay_ms": 50,
            },
        )
        ray.init(
            _metrics_export_port=9999,
        )
        serve.start()
        yield

        serve.shutdown()
        ray.shutdown()
        ray._private.utils.reset_ray_address()


@pytest.fixture
def autoscaling_cluster_with_telemetry(monkeypatch):
    with monkeypatch.context() as m:
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv(
            "RAY_USAGE_STATS_REPORT_URL",
            f"http://127.0.0.1:8000{TELEMETRY_ROUTE_PREFIX}",
        )
        m.setenv("RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S", "0.01")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")

        cluster = AutoscalingCluster(
            **{
                "head_resources": {"CPU": 0},
                "worker_node_types": {
                    "cpu_node": {
                        "resources": {"CPU": 3},
                        "node_config": {},
                        "min_workers": 0,
                        "max_workers": 10,
                    },
                },
                "idle_timeout_minutes": 0.01,
                "autoscaler_v2": True,
            }
        )
        cluster.start()
        ray.init()
        serve.start()
        storage_handle = start_telemetry_app()
        wait_for_condition(
            lambda: ray.get(storage_handle.get_reports_received.remote()) > 0, timeout=5
        )

        yield storage_handle

        # Call Python API shutdown() methods to clear global variable state
        serve.shutdown()
        ray.shutdown()
        cluster.shutdown()

        # Reset global state (any keys that may have been set and cached while the
        # workload was running).
        usage_lib.reset_global_state()
