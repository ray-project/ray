"""Autoscaling policies for Ray Serve LLM deployments.

``TTFTAutoscalingPolicy`` scales on vLLM's p99 time-to-first-token Prometheus
metric. The reusable Prometheus-fetch machinery lives in
``ray.serve.autoscaling_policy.PrometheusQueryMixin``.
"""
from typing import Any, Dict, Optional, Tuple, Union

from ray.serve.autoscaling_policy import PrometheusQueryMixin
from ray.serve.config import AutoscalingContext
from ray.util.annotations import PublicAPI

# Default PromQL for p99 TTFT across all vLLM replicas.
P99_TTFT_QUERY = (
    "histogram_quantile(0.99, "
    "sum(rate(ray_vllm_time_to_first_token_seconds_bucket[1m])) by (le))"
)


@PublicAPI(stability="alpha")
class TTFTAutoscalingPolicy(PrometheusQueryMixin):
    """Scale replicas on p99 time-to-first-token from vLLM Prometheus metrics.

    Scale-up is reactive: when p99 TTFT exceeds ``ttft_target_s`` the policy
    requests one more replica. Scale-down is conservative: low TTFT does not
    prove excess capacity, so the policy only scales down when TTFT is below
    target AND ongoing requests per replica is below ``idle_threshold``. When
    Prometheus data is unavailable the policy holds the current count.

    Configure it through the deployment's AutoscalingConfig::

        from ray.serve.config import AutoscalingConfig, AutoscalingPolicy
        from ray.serve.llm.autoscaling import TTFTAutoscalingPolicy

        AutoscalingConfig(
            min_replicas=1,
            max_replicas=8,
            policy=AutoscalingPolicy(
                policy_function=TTFTAutoscalingPolicy,
                policy_kwargs=dict(
                    ttft_target_s=2.0,
                    prometheus_address="localhost:9090",
                ),
            ),
        )

    Args:
        ttft_target_s: p99 TTFT threshold in seconds; above this it scales up.
        idle_threshold: Max ongoing requests per replica to be considered idle.
        query: PromQL expression to read; must match the ``P99_TTFT_QUERY`` shape.
        prometheus_address: Prometheus server, ``host:port`` or a full URL.
        **kwargs: Forwarded to ``PrometheusQueryMixin`` (``fetch_interval_s``,
            ``cache_ttl_s``).
    """

    def __init__(
        self,
        ttft_target_s: float = 2.0,
        idle_threshold: float = 1.0,
        query: str = P99_TTFT_QUERY,
        prometheus_address: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(
            prometheus_address=prometheus_address,
            prometheus_queries=[query],
            **kwargs,
        )
        self.ttft_target_s = ttft_target_s
        self.idle_threshold = idle_threshold
        self.query = query

    def __call__(
        self, ctx: AutoscalingContext
    ) -> Tuple[Union[int, float], Dict[str, Any]]:
        current = ctx.current_num_replicas
        metrics = self.prometheus_metrics
        if metrics is None:
            return float(current), {"signal": "no_metrics"}
        p99_ttft = metrics.get(self.query)
        if p99_ttft is None:
            return float(current), {"signal": "no_data", "p99_ttft_s": None}
        state = {"p99_ttft_s": p99_ttft}
        if p99_ttft > self.ttft_target_s:
            return float(current + 1), {**state, "signal": "scale_up"}
        requests_per_replica = ctx.total_num_requests / current if current > 0 else 0.0
        state["requests_per_replica"] = requests_per_replica
        if requests_per_replica < self.idle_threshold:
            # AutoscalingConfig.min_replicas owns the floor, so this may reach 0
            # when scale-to-zero is configured.
            return float(max(0, current - 1)), {**state, "signal": "scale_down"}
        return float(current), {**state, "signal": "steady"}
