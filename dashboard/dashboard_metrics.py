from typing import Optional


class NullMetric:
    """Mock metric class to be used in case of prometheus_client import error."""

    def set(self, *args, **kwargs):
        pass

    def observe(self, *args, **kwargs):
        pass

    def inc(self, *args, **kwargs):
        pass


try:

    from prometheus_client import CollectorRegistry, Counter, Histogram

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
                "api_requests_duration_seconds",
                "Total duration in seconds per endpoint",
                ("endpoint", "http_status"),
                unit="seconds",
                namespace="dashboard",
                registry=self.registry,
                buckets=histogram_buckets_s,
            )
            self.metrics_request_count = Counter(
                "api_requests_count",
                "Total requests count per endpoint",
                ("method", "endpoint", "http_status"),
                unit="requests",
                namespace="dashboard",
                registry=self.registry,
            )

except ImportError:

    class DashboardPrometheusMetrics(object):
        def __getattr__(self, attr):
            return NullMetric()
