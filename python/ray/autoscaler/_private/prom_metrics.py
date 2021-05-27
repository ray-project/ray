from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
)


class AutoscalerPrometheusMetrics:
    def __init__(self, registry: CollectorRegistry = None):
        self.registry: CollectorRegistry = registry or \
            CollectorRegistry(auto_describe=True)
        # Buckets: 30 seconds, 1 minute, 2 minutes, 4 minutes,
        #          6 minutes, 8 minutes, 10 minutes, 12 minutes,
        #          15 minutes, 20 minutes, 30 minutes, 1 hour
        self.worker_startup_time: Histogram = Histogram(
            "worker_startup_time_seconds",
            "Worker startup time.",
            unit="seconds",
            namespace="autoscaler",
            registry=self.registry,
            buckets=[30, 60, 120, 240, 360, 480, 600, 720, 900, 1800, 3600])
        self.pending_nodes: Gauge = Gauge(
            "pending_nodes",
            "Number of nodes pending to be started.",
            unit="nodes",
            namespace="autoscaler",
            registry=self.registry)
        self.started_nodes: Counter = Counter(
            "started_nodes",
            "Number of nodes started.",
            unit="nodes",
            namespace="autoscaler",
            registry=self.registry)
        self.stopped_nodes: Counter = Counter(
            "stopped_nodes",
            "Number of nodes stopped.",
            unit="nodes",
            namespace="autoscaler",
            registry=self.registry)
        self.running_nodes: Gauge = Gauge(
            "running_nodes",
            "Number of nodes running.",
            unit="nodes",
            namespace="autoscaler",
            registry=self.registry)
        self.update_loop_exceptions: Counter = Counter(
            "update_loop_exceptions",
            "Number of exceptions raised in the update loop of the "
            "autoscaler.",
            unit="exceptions",
            namespace="autoscaler",
            registry=self.registry)
        self.node_launch_exceptions: Counter = Counter(
            "node_launch_exceptions",
            "Number of exceptions raised while launching nodes.",
            unit="exceptions",
            namespace="autoscaler",
            registry=self.registry)
        self.reset_exceptions: Counter = Counter(
            "reset_exceptions",
            "Number of exceptions raised while resetting the autoscaler.",
            unit="exceptions",
            namespace="autoscaler",
            registry=self.registry)
        self.config_validation_exceptions: Counter = Counter(
            "config_validation_exceptions",
            "Number of exceptions raised while validating the config "
            "during a reset",
            unit="exceptions",
            namespace="autoscaler",
            registry=self.registry)
