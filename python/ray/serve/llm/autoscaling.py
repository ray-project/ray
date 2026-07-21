"""Autoscaling policies for Ray Serve LLM deployments.

``TTFTAutoscalingPolicy`` scales on vLLM's p99 time-to-first-token Prometheus
metric. The reusable Prometheus-fetch machinery lives in
``ray.serve.autoscaling_policy.PrometheusQueryMixin``.
"""
from typing import Any, Dict, Optional, Tuple, Union

from ray.serve.autoscaling_policy import PrometheusQueryMixin
from ray.serve.config import AutoscalingContext
from ray.util.annotations import PublicAPI

DEFAULT_RATE_WINDOW = "1m"


def _ttft_query(model_id: str, rate_window: str = DEFAULT_RATE_WINDOW) -> str:
    """Build the p99 TTFT PromQL scoped to ``model_id``.

    ``rate_window`` is the PromQL range for the inner ``rate()``. It must span
    at least two metric samples or ``rate()`` returns empty and the policy reads
    no data. The default suits the common scrape cadence; widen it if the metrics
    pipeline delivers samples further apart than roughly half the window.
    """
    selector = f'{{model_name="{model_id}"}}'
    return (
        "histogram_quantile(0.99, "
        f"sum(rate(ray_vllm_time_to_first_token_seconds_bucket{selector}[{rate_window}])) "
        "by (le))"
    )


@PublicAPI(stability="alpha")
class TTFTAutoscalingPolicy(PrometheusQueryMixin):
    """Scale replicas on p99 time-to-first-token from vLLM Prometheus metrics.

    Scale-up is reactive: when p99 TTFT exceeds ``ttft_target_s`` the policy
    requests one more replica. Scale-down is conservative: low TTFT does not
    prove excess capacity, so the policy only scales down when TTFT is below
    target AND ongoing requests per replica is below ``idle_threshold``. When
    Prometheus data is unavailable the policy holds the current count.

    The query is scoped to a single model, so pass ``model_id`` (or a full
    ``query``). ``prometheus_address`` defaults to the ``RAY_PROMETHEUS_HOST``
    environment variable::

        from ray.serve.config import AutoscalingConfig, AutoscalingPolicy
        from ray.serve.llm.autoscaling import TTFTAutoscalingPolicy

        AutoscalingConfig(
            min_replicas=1,
            max_replicas=8,
            policy=AutoscalingPolicy(
                policy_function=TTFTAutoscalingPolicy,
                policy_kwargs=dict(ttft_target_s=2.0, model_id="my-org/my-model"),
            ),
        )

    Args:
        ttft_target_s: p99 TTFT threshold in seconds; above this it scales up.
        idle_threshold: Max ongoing requests per replica to be considered idle.
        model_id: Scope the p99 TTFT query to this vLLM model. Required unless
            ``query`` is given.
        rate_window: PromQL range for the inner ``rate()`` of the default TTFT
            query. Must span at least two metric samples or the query returns
            empty and the policy holds. Widen it if the metrics pipeline
            delivers samples slowly. Ignored when ``query`` is given.
        query: PromQL to read. Defaults to a p99 TTFT query scoped by
            ``model_id``. Must resolve to a single-sample instant vector.
        prometheus_address: Prometheus server, ``host:port`` or a full URL.
            Defaults to the ``RAY_PROMETHEUS_HOST`` environment variable.
        **kwargs: Forwarded to ``PrometheusQueryMixin`` (``fetch_interval_s``,
            ``cache_ttl_s``).
    """

    def __init__(
        self,
        ttft_target_s: float = 2.0,
        idle_threshold: float = 1.0,
        model_id: Optional[str] = None,
        rate_window: str = DEFAULT_RATE_WINDOW,
        query: Optional[str] = None,
        prometheus_address: Optional[str] = None,
        **kwargs,
    ):
        if query is None:
            if model_id is None:
                raise ValueError(
                    "TTFTAutoscalingPolicy needs model_id to scope the p99 TTFT "
                    "query, or an explicit query."
                )
            query = _ttft_query(model_id, rate_window)
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
        # Step relative to the current target, not the running count: Serve
        # compares the returned value against target_num_replicas to pick a
        # direction, so a step off the running count reads as a downscale while
        # replicas are still starting.
        target = ctx.target_num_replicas
        running = ctx.current_num_replicas
        metrics = self.prometheus_metrics
        if metrics is None:
            return float(target), {"signal": "no_metrics"}
        p99_ttft = metrics.get(self.query)
        if p99_ttft is None:
            return float(target), {"signal": "no_data", "p99_ttft_s": None}
        state = {"p99_ttft_s": p99_ttft}
        if p99_ttft > self.ttft_target_s:
            return float(target + 1), {**state, "signal": "scale_up"}
        requests_per_replica = ctx.total_num_requests / running if running > 0 else 0.0
        state["requests_per_replica"] = requests_per_replica
        if requests_per_replica < self.idle_threshold:
            # AutoscalingConfig.min_replicas owns the floor, so this may reach 0
            # when scale-to-zero is configured.
            return float(max(0, target - 1)), {**state, "signal": "scale_down"}
        return float(target), {**state, "signal": "steady"}
