"""Autoscaling policies for Ray Serve LLM deployments.

``SLOAutoscalingPolicy`` holds a latency SLO by learning per-replica capacity
from live vLLM metrics, with no offline profiling. The reusable Prometheus-fetch
machinery lives in ``ray.serve.autoscaling_policy.PrometheusQueryMixin``.
"""
import math
from typing import Any, Dict, Optional, Tuple, Union

from ray.serve.autoscaling_policy import PrometheusQueryMixin
from ray.serve.config import AutoscalingContext
from ray.util.annotations import PublicAPI

DEFAULT_RATE_WINDOW = "30s"


def _ttft_query(model_id: str) -> str:
    """p99 time-to-first-token for one model, over DEFAULT_RATE_WINDOW."""
    selector = f'{{model_name="{model_id}"}}'
    return (
        "histogram_quantile(0.99, sum(rate("
        f"ray_vllm_time_to_first_token_seconds_bucket{selector}[{DEFAULT_RATE_WINDOW}]"
        ")) by (le))"
    )


def _inflight_query(model_id: str) -> str:
    """Engine in-flight requests: running plus the vLLM queue, summed fleet-wide.

    Captures queue depth that builds inside the engine, which Serve's own
    request count can mask once requests are assigned to a replica.
    """
    selector = f'{{model_name="{model_id}"}}'
    return (
        f"sum(ray_vllm_num_requests_running{selector}) + "
        f"sum(ray_vllm_num_requests_waiting{selector})"
    )


def _hit_rate_query(model_id: str) -> str:
    """Prefix-cache hit rate over DEFAULT_RATE_WINDOW, in [0, 1]."""
    selector = f'{{model_name="{model_id}"}}'
    w = DEFAULT_RATE_WINDOW
    return (
        f"sum(rate(ray_vllm_prefix_cache_hits_total{selector}[{w}])) / "
        f"sum(rate(ray_vllm_prefix_cache_queries_total{selector}[{w}]))"
    )


# Control-loop constants. Not user knobs: production-sane defaults that pace the
# outer tuning loop and bound the learned capacity.
_TUNE_INTERVAL_S = 30.0
_TUNE_STEP_MAX = 1.5
_TUNE_DEADBAND = 0.1
_HIT_RATE_DAMP = 0.15
_C_CONCURRENCY_MIN = 1.0
_C_CONCURRENCY_MAX = 256.0
_CONCURRENCY_FALLBACK = 8.0


@PublicAPI(stability="alpha")
class SLOAutoscalingPolicy(PrometheusQueryMixin):
    """Scale a vLLM deployment to hold a latency SLO, with no offline profiling.

    The operator declares a latency goal and a model. The policy discovers the
    per-replica capacity on its own from live metrics. It runs two loops.

    Inner loop (every tick): scale on load. ``desired = load / c_concurrency``,
    where ``load`` is the larger of Serve's own request count and the vLLM engine
    in-flight count (running plus queued). The result is an absolute count, so it
    composes with the scaling factor and does not oscillate on consolidation. A
    full KV cache blocks admission, which grows the queue and thus ``load``, so a
    separate KV signal is not needed.

    Outer loop (every ``tune_interval_s``): learn the concurrency capacity from
    latency. When p99 TTFT is above ``ttft_target_s`` it lowers ``c_concurrency``
    so the inner loop provisions more; when TTFT is well below target it raises
    it. Tuning is frozen while replicas are still starting, while traffic is near
    zero, and when the prefix-cache hit rate swings, since those move latency for
    reasons that more replicas do not fix.

    Zero traffic drives the load signals to zero, so the fleet falls to
    ``min_replicas`` with no special case. When Prometheus is unreachable the
    inner concurrency loop still runs off Serve's own request count, seeded from
    ``target_ongoing_requests``, so scaling degrades to request-based rather than
    stopping.

    The policy scopes its queries to one model, so pass ``model_id``.
    ``prometheus_address`` defaults to the ``RAY_PROMETHEUS_HOST`` variable::

        from ray.serve.config import AutoscalingConfig, AutoscalingPolicy
        from ray.serve.llm.autoscaling import SLOAutoscalingPolicy

        AutoscalingConfig(
            min_replicas=1,
            max_replicas=8,
            policy=AutoscalingPolicy(
                policy_function=SLOAutoscalingPolicy,
                policy_kwargs=dict(ttft_target_s=2.0, model_id="my-org/my-model"),
            ),
        )

    Args:
        ttft_target_s: p99 TTFT goal in seconds. The one latency SLO.
        model_id: Scopes every query to this vLLM model. Required.
        prometheus_address: Prometheus server, ``host:port`` or a full URL.
            Defaults to the ``RAY_PROMETHEUS_HOST`` environment variable.
        **kwargs: Forwarded to ``PrometheusQueryMixin`` (``fetch_interval_s``,
            ``cache_ttl_s``).
    """

    def __init__(
        self,
        ttft_target_s: float,
        model_id: Optional[str] = None,
        prometheus_address: Optional[str] = None,
        **kwargs,
    ):
        if model_id is None:
            raise ValueError(
                "SLOAutoscalingPolicy needs model_id to scope its queries."
            )
        self.ttft_query = _ttft_query(model_id)
        self.hit_rate_query = _hit_rate_query(model_id)
        self.inflight_query = _inflight_query(model_id)
        super().__init__(
            prometheus_address=prometheus_address,
            prometheus_queries=[
                self.ttft_query,
                self.hit_rate_query,
                self.inflight_query,
            ],
            **kwargs,
        )
        self.ttft_target_s = ttft_target_s

    def _tune(self, capacity: float, p99_ttft: float) -> float:
        """Nudge the concurrency capacity toward the TTFT goal.

        TTFT above the goal shrinks the capacity so the inner loop adds replicas;
        TTFT below the goal grows it. Bounded per step and to the capacity range.
        """
        if p99_ttft <= 0:
            return capacity
        ratio = self.ttft_target_s / p99_ttft
        if abs(ratio - 1.0) <= _TUNE_DEADBAND:
            return capacity
        ratio = min(max(ratio, 1.0 / _TUNE_STEP_MAX), _TUNE_STEP_MAX)
        return min(max(capacity * ratio, _C_CONCURRENCY_MIN), _C_CONCURRENCY_MAX)

    def __call__(
        self, ctx: AutoscalingContext
    ) -> Tuple[Union[int, float], Dict[str, Any]]:
        state = dict(ctx.policy_state or {})
        seed = _CONCURRENCY_FALLBACK
        if ctx.config is not None:
            seed = float(ctx.config.get_target_ongoing_requests() or seed)
        c_concurrency = state.get("c_concurrency", seed)

        metrics = self.prometheus_metrics or {}
        p99_ttft = metrics.get(self.ttft_query)
        hit_rate = metrics.get(self.hit_rate_query)
        inflight = metrics.get(self.inflight_query)

        # Outer loop: learn capacity from latency, but only when the latency
        # reading reflects steady capacity. Ramp-up, idle, and hit-rate swings
        # move latency for reasons more replicas cannot fix.
        now = ctx.current_time or 0.0
        ramping = ctx.current_num_replicas < ctx.target_num_replicas
        has_traffic = ctx.total_num_requests > 0
        hit_rate_stable = (
            hit_rate is None
            or "last_hit_rate" not in state
            or abs(hit_rate - state["last_hit_rate"]) <= _HIT_RATE_DAMP
        )
        if (
            now - state.get("last_tune_s", 0.0) >= _TUNE_INTERVAL_S
            and not ramping
            and has_traffic
            and hit_rate_stable
        ):
            if p99_ttft is not None:
                c_concurrency = self._tune(c_concurrency, p99_ttft)
            state["last_tune_s"] = now
            if hit_rate is not None:
                state["last_hit_rate"] = hit_rate

        # Inner loop: size from the load. Load is the larger of Serve's own
        # ongoing count and the engine's in-flight count, so an engine-side queue
        # that Serve cannot see still drives scale-up. It falls back to Serve's
        # count when Prometheus is unavailable.
        load = ctx.total_num_requests
        if inflight is not None:
            load = max(load, inflight)
        desired = load / c_concurrency if c_concurrency > 0 else 0.0

        state["c_concurrency"] = c_concurrency
        return float(math.ceil(desired)), state
