try:

    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

    # The metrics in this class should be kept in sync with
    # python/ray/tests/test_metrics_agent.py
    class AutoscalerPrometheusMetrics:
        def __init__(self, registry: CollectorRegistry = None):
            self.registry: CollectorRegistry = registry or CollectorRegistry(
                auto_describe=True
            )
            # Buckets: 5 seconds, 10 seconds, 20 seconds, 30 seconds,
            #          45 seconds, 1 minute, 1.5 minutes, 2 minutes,
            #          3 minutes, 4 minutes, 5 minutes, 6 minutes,
            #          8 minutes, 10 minutes, 12 minutes, 15 minutes
            #          20 minutes, 25 minutes, 30 minutes
            # used for both worker launch time and worker update time
            histogram_buckets = [
                5,
                10,
                20,
                30,
                45,
                60,
                90,
                120,
                180,
                240,
                300,
                360,
                480,
                600,
                720,
                900,
                1200,
                1500,
                1800,
            ]
            # Buckets: .01 seconds to 1000 seconds.
            # Used for autoscaler update time.
            update_time_buckets = [0.01, 0.1, 1, 10, 100, 1000]
            self.worker_create_node_time: Histogram = Histogram(
                "worker_create_node_time_seconds",
                "Worker launch time. This is the time it takes for a call to "
                "a node provider's create_node method to return. Note that "
                "when nodes are launched in batches, the launch time for that "
                "batch will be observed once for *each* node in that batch. "
                "For example, if 8 nodes are launched in 3 minutes, a launch "
                "time of 3 minutes will be observed 8 times.",
                unit="seconds",
                namespace="autoscaler",
                registry=self.registry,
                buckets=histogram_buckets,
            )
            self.worker_update_time: Histogram = Histogram(
                "worker_update_time_seconds",
                "Worker update time. This is the time between when an updater "
                "thread begins executing and when it exits successfully. This "
                "metric only observes times for successful updates.",
                unit="seconds",
                namespace="autoscaler",
                registry=self.registry,
                buckets=histogram_buckets,
            )
            self.update_time: Histogram = Histogram(
                "update_time",
                "Autoscaler update time. This is the time for an autoscaler "
                "update iteration to complete.",
                unit="seconds",
                namespace="autoscaler",
                registry=self.registry,
                buckets=update_time_buckets,
            )
            self.pending_nodes: Gauge = Gauge(
                "pending_nodes",
                "Number of nodes pending to be started.",
                unit="nodes",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.started_nodes: Counter = Counter(
                "started_nodes",
                "Number of nodes started.",
                unit="nodes",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.stopped_nodes: Counter = Counter(
                "stopped_nodes",
                "Number of nodes stopped.",
                unit="nodes",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.updating_nodes: Gauge = Gauge(
                "updating_nodes",
                "Number of nodes in the process of updating.",
                unit="nodes",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.recovering_nodes: Gauge = Gauge(
                "recovering_nodes",
                "Number of nodes in the process of recovering.",
                unit="nodes",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.running_workers: Gauge = Gauge(
                "running_workers",
                "Number of worker nodes running.",
                unit="nodes",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.failed_create_nodes: Counter = Counter(
                "failed_create_nodes",
                "Number of nodes that failed to be created due to an "
                "exception in the node provider's create_node method.",
                unit="nodes",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.failed_updates: Counter = Counter(
                "failed_updates",
                "Number of failed worker node updates.",
                unit="updates",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.successful_updates: Counter = Counter(
                "successful_updates",
                "Number of succesfful worker node updates.",
                unit="updates",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.failed_recoveries: Counter = Counter(
                "failed_recoveries",
                "Number of failed node recoveries.",
                unit="recoveries",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.successful_recoveries: Counter = Counter(
                "successful_recoveries",
                "Number of successful node recoveries.",
                unit="recoveries",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.update_loop_exceptions: Counter = Counter(
                "update_loop_exceptions",
                "Number of exceptions raised in the update loop of the autoscaler.",
                unit="exceptions",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.node_launch_exceptions: Counter = Counter(
                "node_launch_exceptions",
                "Number of exceptions raised while launching nodes.",
                unit="exceptions",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.reset_exceptions: Counter = Counter(
                "reset_exceptions",
                "Number of exceptions raised while resetting the autoscaler.",
                unit="exceptions",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.config_validation_exceptions: Counter = Counter(
                "config_validation_exceptions",
                "Number of exceptions raised while validating the config "
                "during a reset.",
                unit="exceptions",
                namespace="autoscaler",
                registry=self.registry,
            )
            self.drain_node_exceptions: Counter = Counter(
                "drain_node_exceptions",
                "Number of exceptions raised when making a DrainNode rpc"
                "prior to node termination.",
                unit="exceptions",
                namespace="autoscaler",
                registry=self.registry,
            )

except ImportError:

    class NullMetric:
        def set(self, value):
            pass

        def observe(self, value):
            pass

        def inc(self):
            pass

    class AutoscalerPrometheusMetrics(object):
        def __getattr__(self, attr):
            return NullMetric()
