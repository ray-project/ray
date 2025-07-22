from typing import Optional

from ray.dashboard.consts import COMPONENT_METRICS_TAG_KEYS


class NullMetric:
    """Mock metric class to be used in case of prometheus_client import error."""

    def set(self, *args, **kwargs):
        pass

    def observe(self, *args, **kwargs):
        pass

    def inc(self, *args, **kwargs):
        pass


try:

    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

    # The metrics in this class should be kept in sync with
    # python/ray/tests/test_metrics_agent.py
    class DashboardPrometheusMetrics:
        def __init__(self, registry: Optional[CollectorRegistry] = None):
            self.registry: CollectorRegistry = registry or CollectorRegistry(
                auto_describe=True
            )
            # Buckets: 5ms, 10ms, 25ms, 50ms, 75ms
            #          100ms, 250ms, 500ms, 750ms
            #          1s, 2.5s, 5s, 7.5s, 10s
            #          20s, 40s, 60s
            # used for API duration
            histogram_buckets_s = [
                0.005,
                0.01,
                0.025,
                0.05,
                0.075,
                0.1,
                0.25,
                0.5,
                0.75,
                1,
                2.5,
                5,
                7.5,
                10,
                20,
                40,
                60,
            ]
            self.metrics_request_duration = Histogram(
                "dashboard_api_requests_duration_seconds",
                "Total duration in seconds per endpoint",
                ("endpoint", "http_status", "Version", "SessionName", "Component"),
                unit="seconds",
                namespace="ray",
                registry=self.registry,
                buckets=histogram_buckets_s,
            )
            self.metrics_request_count = Counter(
                "dashboard_api_requests_count",
                "Total requests count per endpoint",
                (
                    "method",
                    "endpoint",
                    "http_status",
                    "Version",
                    "SessionName",
                    "Component",
                ),
                unit="requests",
                namespace="ray",
                registry=self.registry,
            )
            self.metrics_event_loop_tasks = Gauge(
                "dashboard_event_loop_tasks",
                "Number of tasks currently pending in the event loop's queue.",
                tuple(COMPONENT_METRICS_TAG_KEYS),
                unit="tasks",
                namespace="ray",
                registry=self.registry,
            )
            self.metrics_event_loop_lag = Gauge(
                "dashboard_event_loop_lag",
                "Event loop lag in seconds.",
                tuple(COMPONENT_METRICS_TAG_KEYS),
                unit="seconds",
                namespace="ray",
                registry=self.registry,
            )
            self.metrics_dashboard_cpu = Gauge(
                "component_cpu",
                "Dashboard CPU percentage usage.",
                tuple(COMPONENT_METRICS_TAG_KEYS),
                unit="percentage",
                namespace="ray",
                registry=self.registry,
            )
            self.metrics_dashboard_mem_uss = Gauge(
                "component_uss",
                "USS usage of all components on the node.",
                tuple(COMPONENT_METRICS_TAG_KEYS),
                unit="mb",
                namespace="ray",
                registry=self.registry,
            )
            self.metrics_dashboard_mem_rss = Gauge(
                "component_rss",
                "RSS usage of all components on the node.",
                tuple(COMPONENT_METRICS_TAG_KEYS),
                unit="mb",
                namespace="ray",
                registry=self.registry,
            )

except ImportError:

    class DashboardPrometheusMetrics(object):
        def __getattr__(self, attr):
            return NullMetric()
