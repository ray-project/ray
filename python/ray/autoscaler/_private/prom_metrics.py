import prometheus_client

AUTOSCALER_METRIC_REGISTRY = prometheus_client.CollectorRegistry()
AUTOSCALER_WORKER_STARTUP_TIME_HISTOGRAM = prometheus_client.Histogram(
    "worker_startup_time_seconds",
    "Worker startup time",
    unit="seconds",
    namespace="autoscaler",
    registry=AUTOSCALER_METRIC_REGISTRY)
AUTOSCALER_STARTED_NODES_COUNTER = prometheus_client.Counter(
    "started_nodes",
    "Number of nodes started",
    unit="nodes",
    namespace="autoscaler",
    registry=AUTOSCALER_METRIC_REGISTRY)
AUTOSCALER_STOPPED_NODES_COUNTER = prometheus_client.Counter(
    "stopped_nodes",
    "Number of nodes stopped",
    unit="nodes",
    namespace="autoscaler",
    registry=AUTOSCALER_METRIC_REGISTRY)
AUTOSCALER_RUNNING_NODES_GAUGE = prometheus_client.Gauge(
    "running_nodes",
    "Number of nodes running",
    unit="nodes",
    namespace="autoscaler",
    registry=AUTOSCALER_METRIC_REGISTRY)
AUTOSCALER_EXCEPTIONS_COUNTER = prometheus_client.Counter(
    "exceptions",
    "Number of exceptions",
    unit="exceptions",
    namespace="autoscaler",
    registry=AUTOSCALER_METRIC_REGISTRY)
