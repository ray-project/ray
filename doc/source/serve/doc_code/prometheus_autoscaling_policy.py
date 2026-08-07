# __begin_prometheus_autoscaling_policy__
from typing import Any, Dict, Optional, Tuple, Union

from ray.serve.autoscaling_policy import PrometheusQueryMixin
from ray.serve.config import AutoscalingContext


class QueueDepthAutoscalingPolicy(PrometheusQueryMixin):
    """Scale on a Prometheus gauge that reports pending work.

    ``PrometheusQueryMixin`` starts a background thread on first use that
    evaluates the queries every ``fetch_interval_s`` and caches the scalar
    results. ``self.prometheus_metrics`` returns those values keyed by query
    string, or ``None`` when Prometheus is unset, unreachable, or the cache is
    older than ``cache_ttl_s``. Reads never block on the network, so the
    autoscaling tick stays fast even when Prometheus is slow.
    """

    def __init__(
        self,
        query: str,
        scale_up_threshold: float = 10.0,
        prometheus_address: Optional[str] = None,
        **kwargs,
    ):
        # prometheus_address defaults to the RAY_PROMETHEUS_HOST environment
        # variable, which Ray's dashboard and managed clusters already set.
        super().__init__(
            prometheus_address=prometheus_address,
            prometheus_queries=[query],
            **kwargs,
        )
        self.query = query
        self.scale_up_threshold = scale_up_threshold

    def __call__(
        self, ctx: AutoscalingContext
    ) -> Tuple[Union[int, float], Dict[str, Any]]:
        # Step relative to the current target, not the running count. Serve
        # compares the returned value against target_num_replicas to choose a
        # direction, so stepping off the running count reads as a downscale
        # while replicas are still starting up.
        target = ctx.target_num_replicas
        depth = (self.prometheus_metrics or {}).get(self.query)
        if depth is None:
            # Hold the current target when no fresh metric is available.
            return float(target), {"signal": "no_data"}
        if depth > self.scale_up_threshold:
            return float(target + 1), {"signal": "scale_up", "queue_depth": depth}
        return float(target), {"signal": "steady", "queue_depth": depth}


# __end_prometheus_autoscaling_policy__
