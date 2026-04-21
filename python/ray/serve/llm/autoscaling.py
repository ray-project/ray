"""Autoscaling policies for Ray Serve LLM deployments.

These policies use Prometheus metrics exposed by vLLM (via Ray metrics
wrappers) to make scaling decisions.  They require ``prometheus_address``
and ``prometheus_queries`` to be set in the deployment's
``AutoscalingConfig``.
"""

from typing import Any, Dict, Tuple, Union

from ray.serve.config import AutoscalingContext

# Default PromQL for p99 TTFT across all vLLM replicas.
P99_TTFT_QUERY = (
    "histogram_quantile(0.99, "
    "sum(rate(ray_vllm_time_to_first_token_seconds_bucket[1m])) by (le))"
)


class TTFTAutoscalingPolicy:
    """Scale replicas based on p99 time-to-first-token latency.

    Scale-up is reactive: when p99 TTFT exceeds ``ttft_target_s``, the
    policy requests one additional replica.

    Scale-down is conservative: TTFT being low doesn't prove excess
    capacity — it may be low *because* the current replica count is
    right.  The policy only scales down when TTFT is below target AND
    ongoing requests per replica is below ``idle_threshold``, indicating
    the replicas genuinely have spare capacity.  The autoscaler's
    ``downscale_delay_s`` (default 600s) provides additional protection
    against premature removal.

    If Prometheus data is unavailable the policy holds the current
    replica count and lets the built-in request-based autoscaler take
    over.

    Example::

        from ray.serve.config import AutoscalingConfig, AutoscalingPolicy
        from ray.serve.llm.autoscaling import TTFTAutoscalingPolicy, P99_TTFT_QUERY

        autoscaling_config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=8,
            prometheus_address="localhost:9090",
            prometheus_queries=[P99_TTFT_QUERY],
            policy=AutoscalingPolicy(
                policy_function=TTFTAutoscalingPolicy,
                policy_kwargs=dict(ttft_target_s=2.0),
            ),
        )

    Args:
        ttft_target_s: p99 TTFT threshold in seconds.  Above this the
            policy scales up.
        idle_threshold: Maximum ongoing requests per replica to be
            considered idle.  Scale-down only happens when TTFT is
            below target AND requests per replica is below this value.
        query: PromQL expression whose result to read from
            ``ctx.prometheus_metrics``.  Must match an entry in
            ``AutoscalingConfig.prometheus_queries``.  Defaults to
            ``P99_TTFT_QUERY``.
    """

    def __init__(
        self,
        ttft_target_s: float = 2.0,
        idle_threshold: float = 1.0,
        query: str = P99_TTFT_QUERY,
    ):
        self.ttft_target_s = ttft_target_s
        self.idle_threshold = idle_threshold
        self.query = query

    def __call__(
        self, ctx: AutoscalingContext
    ) -> Tuple[Union[int, float], Dict[str, Any]]:
        current = ctx.current_num_replicas
        metrics = ctx.prometheus_metrics

        # Prometheus unavailable — hold steady.
        if metrics is None:
            return float(current), {"signal": "no_metrics"}

        p99_ttft = metrics.get(self.query)

        # No data point yet (e.g. no traffic) — hold steady.
        if p99_ttft is None:
            return float(current), {"signal": "no_data", "p99_ttft_s": None}

        state = {"p99_ttft_s": p99_ttft}

        # Scale up: latency is above target.
        if p99_ttft > self.ttft_target_s:
            return float(current + 1), {**state, "signal": "scale_up"}

        # Scale down: latency is below target AND replicas are idle.
        # TTFT alone doesn't prove excess capacity — low latency may be
        # the result of having the right replica count under load.
        requests_per_replica = ctx.total_num_requests / current if current > 0 else 0.0
        state["requests_per_replica"] = requests_per_replica

        if requests_per_replica < self.idle_threshold:
            return float(max(1, current - 1)), {**state, "signal": "scale_down"}

        return float(current), {**state, "signal": "steady"}
