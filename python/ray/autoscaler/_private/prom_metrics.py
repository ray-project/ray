from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
)


# The metrics in this class should be kept in sync with
# python/ray/tests/test_metrics_agent.py
class AutoscalerPrometheusMetrics:
    def __init__(self, registry: CollectorRegistry = None):
        self.registry: CollectorRegistry = registry or \
            CollectorRegistry(auto_describe=True)
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
        self.running_workers: Gauge = Gauge(
            "running_workers",
            "Number of worker nodes running.",
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
            "during a reset.",
            unit="exceptions",
            namespace="autoscaler",
            registry=self.registry)
