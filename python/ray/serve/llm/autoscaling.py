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

DEFAULT_RATE_WINDOW = "1m"


def _p99_query(bucket: str, model_id: str, rate_window: str) -> str:
    """p99 of a vLLM latency histogram, scoped to one model."""
    selector = f'{{model_name="{model_id}"}}'
    return (
        "histogram_quantile(0.99, "
        f"sum(rate({bucket}{selector}[{rate_window}])) by (le))"
    )


def _kv_usage_query(model_id: str) -> str:
    """Fleet-average KV-cache utilization for one model, in [0, 1]."""
    return f'avg(ray_vllm_kv_cache_usage_perc{{model_name="{model_id}"}})'


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


def _hit_rate_query(model_id: str, rate_window: str) -> str:
    """Prefix-cache hit rate over ``rate_window``, in [0, 1]."""
    selector = f'{{model_name="{model_id}"}}'
    return (
        f"sum(rate(ray_vllm_prefix_cache_hits_total{selector}[{rate_window}])) / "
        f"sum(rate(ray_vllm_prefix_cache_queries_total{selector}[{rate_window}]))"
    )


DEFAULT_KV_TARGET = 0.9
DEFAULT_TUNE_INTERVAL_S = 60.0
DEFAULT_TUNE_STEP_MAX = 1.25
DEFAULT_TUNE_DEADBAND = 0.1
DEFAULT_HIT_RATE_DAMP = 0.15
DEFAULT_CONCURRENCY_FALLBACK = 8.0


@PublicAPI(stability="alpha")
class SLOAutoscalingPolicy(PrometheusQueryMixin):
    """Scale a vLLM deployment to hold a latency SLO, with no offline profiling.

    The operator declares a latency goal and a model. The policy discovers the
    per-replica capacity on its own from live metrics. It runs two loops.

    Inner loop (every tick): scale on load. It sizes the fleet to the tightest
    of two constraints and returns the larger replica count:

    - Concurrency: ``ongoing_requests / c_concurrency``. Absolute, so it composes
      with the scaling factor and does not oscillate on consolidation.
    - KV cache: ``current_replicas * kv_utilization / kv_target``. Caps the
      memory-bound decode phase.

    Outer loop (every ``tune_interval_s``): learn the capacity from latency.
    When p99 TTFT is above ``ttft_target_s`` it lowers ``c_concurrency`` so the
    inner loop provisions more; when TTFT is well below target it raises it. If
    ``itl_target_s`` is set, p99 ITL tunes ``kv_target`` the same way. Tuning is
    frozen while replicas are still starting, while traffic is near zero, and
    when the prefix-cache hit rate swings, since those move latency for reasons
    that more replicas do not fix.

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
        ttft_target_s: p99 TTFT goal in seconds. The only required latency knob.
        itl_target_s: p99 inter-token-latency goal in seconds. When set, ITL
            tunes ``kv_target``; otherwise ``kv_target`` stays fixed as a cap.
        model_id: Scopes every query to this vLLM model. Required.
        kv_target: Starting KV-cache utilization ceiling, in [0, 1].
        rate_window: PromQL range for the inner ``rate()`` of the latency and
            hit-rate queries. Must span at least two samples.
        tune_interval_s: Minimum seconds between capacity updates.
        tune_step_max: Largest multiplicative change to a capacity per update.
        tune_deadband: Skip tuning while latency is within this fraction of goal.
        hit_rate_damp: Skip one tuning step when the prefix-cache hit rate moves
            more than this since the last step.
        c_concurrency_min: Lower bound for the learned concurrency capacity.
        c_concurrency_max: Upper bound for the learned concurrency capacity.
        kv_target_min: Lower bound for the learned KV-cache utilization target.
        kv_target_max: Upper bound for the learned KV-cache utilization target.
        prometheus_address: Prometheus server, ``host:port`` or a full URL.
        **kwargs: Forwarded to ``PrometheusQueryMixin`` (``fetch_interval_s``,
            ``cache_ttl_s``).
    """

    def __init__(
        self,
        ttft_target_s: float,
        itl_target_s: Optional[float] = None,
        model_id: Optional[str] = None,
        kv_target: float = DEFAULT_KV_TARGET,
        rate_window: str = DEFAULT_RATE_WINDOW,
        tune_interval_s: float = DEFAULT_TUNE_INTERVAL_S,
        tune_step_max: float = DEFAULT_TUNE_STEP_MAX,
        tune_deadband: float = DEFAULT_TUNE_DEADBAND,
        hit_rate_damp: float = DEFAULT_HIT_RATE_DAMP,
        c_concurrency_min: float = 1.0,
        c_concurrency_max: float = 256.0,
        kv_target_min: float = 0.5,
        kv_target_max: float = 0.98,
        prometheus_address: Optional[str] = None,
        **kwargs,
    ):
        if model_id is None:
            raise ValueError(
                "SLOAutoscalingPolicy needs model_id to scope its queries."
            )
        self.ttft_query = _p99_query(
            "ray_vllm_time_to_first_token_seconds_bucket", model_id, rate_window
        )
        self.itl_query = (
            _p99_query(
                "ray_vllm_request_time_per_output_token_seconds_bucket",
                model_id,
                rate_window,
            )
            if itl_target_s is not None
            else None
        )
        self.kv_query = _kv_usage_query(model_id)
        self.hit_rate_query = _hit_rate_query(model_id, rate_window)
        self.inflight_query = _inflight_query(model_id)
        queries = [
            self.ttft_query,
            self.kv_query,
            self.hit_rate_query,
            self.inflight_query,
        ]
        if self.itl_query is not None:
            queries.append(self.itl_query)
        super().__init__(
            prometheus_address=prometheus_address,
            prometheus_queries=queries,
            **kwargs,
        )
        self.ttft_target_s = ttft_target_s
        self.itl_target_s = itl_target_s
        self.kv_target = kv_target
        self.tune_interval_s = tune_interval_s
        self.tune_step_max = tune_step_max
        self.tune_deadband = tune_deadband
        self.hit_rate_damp = hit_rate_damp
        self.c_concurrency_min = c_concurrency_min
        self.c_concurrency_max = c_concurrency_max
        self.kv_target_min = kv_target_min
        self.kv_target_max = kv_target_max

    def _tune(
        self, capacity: float, goal: float, observed: float, lo: float, hi: float
    ) -> float:
        """Nudge a capacity toward the goal, bounded per step and to [lo, hi].

        A latency above goal shrinks the capacity so the inner loop adds
        replicas; a latency below goal grows it so the loop removes them.
        """
        if observed <= 0:
            return capacity
        ratio = goal / observed
        if abs(ratio - 1.0) <= self.tune_deadband:
            return capacity
        ratio = min(max(ratio, 1.0 / self.tune_step_max), self.tune_step_max)
        return min(max(capacity * ratio, lo), hi)

    def __call__(
        self, ctx: AutoscalingContext
    ) -> Tuple[Union[int, float], Dict[str, Any]]:
        state = dict(ctx.policy_state or {})
        seed = DEFAULT_CONCURRENCY_FALLBACK
        if ctx.config is not None:
            seed = float(ctx.config.get_target_ongoing_requests() or seed)
        c_concurrency = state.get("c_concurrency", seed)
        kv_target = state.get("kv_target", self.kv_target)

        metrics = self.prometheus_metrics or {}
        p99_ttft = metrics.get(self.ttft_query)
        p99_itl = metrics.get(self.itl_query) if self.itl_query else None
        kv_util = metrics.get(self.kv_query)
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
            or abs(hit_rate - state["last_hit_rate"]) <= self.hit_rate_damp
        )
        if (
            now - state.get("last_tune_s", 0.0) >= self.tune_interval_s
            and not ramping
            and has_traffic
            and hit_rate_stable
        ):
            if p99_ttft is not None:
                c_concurrency = self._tune(
                    c_concurrency,
                    self.ttft_target_s,
                    p99_ttft,
                    self.c_concurrency_min,
                    self.c_concurrency_max,
                )
            if p99_itl is not None and self.itl_target_s is not None:
                kv_target = self._tune(
                    kv_target,
                    self.itl_target_s,
                    p99_itl,
                    self.kv_target_min,
                    self.kv_target_max,
                )
            state["last_tune_s"] = now
            if hit_rate is not None:
                state["last_hit_rate"] = hit_rate

        # Inner loop: size to the tightest constraint. Both terms are replica
        # counts; the larger one wins. Load is the larger of Serve's own ongoing
        # count and the engine's in-flight count, so an engine-side queue that
        # Serve cannot see still drives scale-up. It falls back to Serve's count
        # when Prometheus is unavailable.
        load = ctx.total_num_requests
        if inflight is not None:
            load = max(load, inflight)
        desired = load / c_concurrency if c_concurrency > 0 else 0.0
        if kv_util is not None and ctx.current_num_replicas > 0 and kv_target > 0:
            desired = max(desired, ctx.current_num_replicas * kv_util / kv_target)

        state.update(
            {
                "c_concurrency": c_concurrency,
                "kv_target": kv_target,
                "p99_ttft_s": p99_ttft,
                "p99_itl_s": p99_itl,
                "kv_util": kv_util,
                "hit_rate": hit_rate,
                "inflight": inflight,
            }
        )
        return float(math.ceil(desired)), state
