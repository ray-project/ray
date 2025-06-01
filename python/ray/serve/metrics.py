from typing import Dict, List, Optional, Tuple, Union

import ray
from ray.serve import context
from ray.util import metrics
from ray.util.annotations import PublicAPI

DEPLOYMENT_TAG = "deployment"
REPLICA_TAG = "replica"
APPLICATION_TAG = "application"
ROUTE_TAG = "route"


def _add_serve_metric_tags(tag_keys: Optional[Tuple[str]] = None) -> Tuple[str]:
    """Add serve context tags to the tag_keys"""
    if tag_keys is None:
        tag_keys = tuple()

    # If the context doesn't exist, no serve tag is added.
    if context._get_internal_replica_context() is None:
        return tag_keys
    # Check no collision with customer tag
    if DEPLOYMENT_TAG in tag_keys:
        raise ValueError(f"'{DEPLOYMENT_TAG}' tag is reserved for Ray Serve metrics")
    if REPLICA_TAG in tag_keys:
        raise ValueError(f"'{REPLICA_TAG}' tag is reserved for Ray Serve metrics")
    if APPLICATION_TAG in tag_keys:
        raise ValueError(f"'{APPLICATION_TAG}' tag is reserved for Ray Serve metrics")

    # Get serve tag inserted:
    ray_serve_tags = (DEPLOYMENT_TAG, REPLICA_TAG)
    if context._get_internal_replica_context().app_name:
        ray_serve_tags += (APPLICATION_TAG,)
    if tag_keys:
        tag_keys = ray_serve_tags + tag_keys
    else:
        tag_keys = ray_serve_tags
    return tag_keys


def _add_serve_metric_default_tags(default_tags: Dict[str, str]):
    """Add serve context tags and values to the default_tags"""
    if context._get_internal_replica_context() is None:
        return default_tags
    if DEPLOYMENT_TAG in default_tags:
        raise ValueError(f"'{DEPLOYMENT_TAG}' tag is reserved for Ray Serve metrics")
    if REPLICA_TAG in default_tags:
        raise ValueError(f"'{REPLICA_TAG}' tag is reserved for Ray Serve metrics")
    if APPLICATION_TAG in default_tags:
        raise ValueError(f"'{APPLICATION_TAG}' tag is reserved for Ray Serve metrics")
    replica_context = context._get_internal_replica_context()
    # TODO(zcin): use replica_context.deployment for deployment tag
    default_tags[DEPLOYMENT_TAG] = replica_context.deployment
    default_tags[REPLICA_TAG] = replica_context.replica_tag
    if replica_context.app_name:
        default_tags[APPLICATION_TAG] = replica_context.app_name
    return default_tags


def _add_serve_context_tag_values(tag_keys: Tuple, tags: Dict[str, str]):
    """Add serve context tag values to the metric tags"""

    _request_context = ray.serve.context._get_serve_request_context()
    if ROUTE_TAG in tag_keys and ROUTE_TAG not in tags:
        tags[ROUTE_TAG] = _request_context.route


@PublicAPI(stability="beta")
class Counter(metrics.Counter):
    """A serve cumulative metric that is monotonically increasing.

    This corresponds to Prometheus' counter metric:
    https://prometheus.io/docs/concepts/metric_types/#counter

    Serve-related tags ("deployment", "replica", "application", "route")
    are added automatically if not provided.

    .. code-block:: python

            @serve.deployment
            class MyDeployment:
                def __init__(self):
                    self.num_requests = 0
                    self.my_counter = metrics.Counter(
                        "my_counter",
                        description=("The number of odd-numbered requests "
                            "to this deployment."),
                        tag_keys=("model",),
                    )
                    self.my_counter.set_default_tags({"model": "123"})

                def __call__(self):
                    self.num_requests += 1
                    if self.num_requests % 2 == 1:
                        self.my_counter.inc()

    .. note::

        Before Ray 2.10, this exports a Prometheus gauge metric instead of
        a counter metric.
        Starting in Ray 2.10, this exports both the proper counter metric
        (with a suffix "_total") and gauge metric (for compatibility).
        The gauge metric will be removed in a future Ray release and you can set
        `RAY_EXPORT_COUNTER_AS_GAUGE=0` to disable exporting it in the meantime.

    Args:
        name: Name of the metric.
        description: Description of the metric.
        tag_keys: Tag keys of the metric.
    """

    def __init__(
        self, name: str, description: str = "", tag_keys: Optional[Tuple[str]] = None
    ):
        if tag_keys and not isinstance(tag_keys, tuple):
            raise TypeError(
                "tag_keys should be a tuple type, got: " f"{type(tag_keys)}"
            )
        tag_keys = _add_serve_metric_tags(tag_keys)
        super().__init__(name, description, tag_keys)
        self.set_default_tags({})

    def set_default_tags(self, default_tags: Dict[str, str]):
        super().set_default_tags(_add_serve_metric_default_tags(default_tags))

    def inc(self, value: Union[int, float] = 1.0, tags: Dict[str, str] = None):
        """Increment the counter by the given value, add serve context
        tag values to the tags
        """
        _add_serve_context_tag_values(self._tag_keys, tags)
        super().inc(value, tags)


@PublicAPI(stability="beta")
class Gauge(metrics.Gauge):
    """Gauges keep the last recorded value and drop everything before.

    This corresponds to Prometheus' gauge metric:
    https://prometheus.io/docs/concepts/metric_types/#gauge

    Serve-related tags ("deployment", "replica", "application", "route")
    are added automatically if not provided.

    .. code-block:: python

            @serve.deployment
            class MyDeployment:
                def __init__(self):
                    self.num_requests = 0
                    self.my_gauge = metrics.Gauge(
                        "my_gauge",
                        description=("The current memory usage."),
                        tag_keys=("model",),
                    )
                    self.my_counter.set_default_tags({"model": "123"})

                def __call__(self):
                    process = psutil.Process()
                    self.gauge.set(process.memory_info().rss)

    Args:
        name: Name of the metric.
        description: Description of the metric.
        tag_keys: Tag keys of the metric.
    """

    def __init__(
        self, name: str, description: str = "", tag_keys: Optional[Tuple[str]] = None
    ):
        if tag_keys and not isinstance(tag_keys, tuple):
            raise TypeError(
                "tag_keys should be a tuple type, got: " f"{type(tag_keys)}"
            )
        tag_keys = _add_serve_metric_tags(tag_keys)
        super().__init__(name, description, tag_keys)
        self.set_default_tags({})

    def set_default_tags(self, default_tags: Dict[str, str]):
        super().set_default_tags(_add_serve_metric_default_tags(default_tags))

    def set(self, value: Union[int, float], tags: Dict[str, str] = None):
        """Set the gauge to the given value, add serve context
        tag values to the tags
        """
        _add_serve_context_tag_values(self._tag_keys, tags)
        super().set(value, tags)


@PublicAPI(stability="beta")
class Histogram(metrics.Histogram):
    """Tracks the size and number of events in buckets.

    Histograms allow you to calculate aggregate quantiles
    such as 25, 50, 95, 99 percentile latency for an RPC.

    This corresponds to Prometheus' histogram metric:
    https://prometheus.io/docs/concepts/metric_types/#histogram

    Serve-related tags ("deployment", "replica", "application", "route")
    are added automatically if not provided.

    .. code-block:: python

            @serve.deployment
            class MyDeployment:
                def __init__(self):
                    self.my_histogram = Histogram(
                        "my_histogram",
                        description=("Histogram of the __call__ method running time."),
                        boundaries=[1,2,4,8,16,32,64],
                        tag_keys=("model",),
                    )
                    self.my_histogram.set_default_tags({"model": "123"})

                def __call__(self):
                    start = time.time()
                    self.my_histogram.observe(time.time() - start)

    Args:
        name: Name of the metric.
        description: Description of the metric.
        boundaries: Boundaries of histogram buckets.
        tag_keys: Tag keys of the metric.
    """

    def __init__(
        self,
        name: str,
        description: str = "",
        boundaries: List[float] = None,
        tag_keys: Optional[Tuple[str]] = None,
    ):
        if tag_keys and not isinstance(tag_keys, tuple):
            raise TypeError(
                "tag_keys should be a tuple type, got: " f"{type(tag_keys)}"
            )
        tag_keys = _add_serve_metric_tags(tag_keys)
        super().__init__(name, description, boundaries, tag_keys)
        self.set_default_tags({})

    def set_default_tags(self, default_tags: Dict[str, str]):
        super().set_default_tags(_add_serve_metric_default_tags(default_tags))

    def observe(self, value: Union[int, float], tags: Dict[str, str] = None):
        """Observe the given value, add serve context
        tag values to the tags
        """
        _add_serve_context_tag_values(self._tag_keys, tags)
        super().observe(value, tags)
