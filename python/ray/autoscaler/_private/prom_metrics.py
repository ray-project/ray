from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
)


class AutoscalerPrometheusMetrics:
    def __init__(self, registry: CollectorRegistry):
        self.registry: CollectorRegistry = registry
        self.worker_startup_time: Histogram = Histogram(
            "worker_startup_time_seconds",
            "Worker startup time",
            unit="seconds",
            namespace="autoscaler",
            registry=self.registry)
        self.started_nodes: Counter = Counter(
            "started_nodes",
            "Number of nodes started",
            unit="nodes",
            namespace="autoscaler",
            registry=self.registry)
        self.stopped_nodes: Counter = Counter(
            "stopped_nodes",
            "Number of nodes stopped",
            unit="nodes",
            namespace="autoscaler",
            registry=self.registry)
        self.running_nodes: Gauge = Gauge(
            "running_nodes",
            "Number of nodes running",
            unit="nodes",
            namespace="autoscaler",
            registry=self.registry)
        self.exceptions: Counter = Counter(
            "exceptions",
            "Number of exceptions",
            unit="exceptions",
            namespace="autoscaler",
            registry=self.registry)


DEFAULT_AUTOSCALER_METRICS = AutoscalerPrometheusMetrics(
    CollectorRegistry(auto_describe=True))
