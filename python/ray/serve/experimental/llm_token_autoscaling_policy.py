import math
import time
from typing import Any, Dict, List, Literal, Tuple

from ray.serve.config import AutoscalingContext

OUTSTANDING_EFFECTIVE_PREFILL_TOKENS = "outstanding_effective_prefill_tokens"
ACTIVE_DECODE_REQUESTS = "active_decode_requests"

_UpscaleAggregation = Literal["max", "mean"]
_Snapshot = Tuple[float, float, float]


def _now(ctx: AutoscalingContext) -> float:
    return ctx.current_time if ctx.current_time is not None else time.time()


def _complete_snapshots_by_source(
    ctx: AutoscalingContext,
) -> Dict[str, List[_Snapshot]]:
    """Return fresh, atomically sampled ``(timestamp, prefill, decode)`` series."""
    raw_metrics = ctx.raw_router_metrics or {}
    prefill_by_source = raw_metrics.get(OUTSTANDING_EFFECTIVE_PREFILL_TOKENS, {})
    decode_by_source = raw_metrics.get(ACTIVE_DECODE_REQUESTS, {})
    cutoff = _now(ctx) - ctx.config.look_back_period_s
    snapshots: Dict[str, List[_Snapshot]] = {}

    for source_id in prefill_by_source.keys() & decode_by_source.keys():
        prefill_series = prefill_by_source[source_id]
        decode_series = decode_by_source[source_id]
        if not prefill_series or not decode_series:
            continue

        # Router metrics in one report are sampled atomically. If the latest
        # timestamps differ, the report is partial and must not be used.
        if prefill_series[-1].timestamp != decode_series[-1].timestamp:
            continue
        if prefill_series[-1].timestamp < cutoff:
            continue

        decode_at = {sample.timestamp: sample.value for sample in decode_series}
        source_snapshots = [
            (sample.timestamp, sample.value, decode_at[sample.timestamp])
            for sample in prefill_series
            if sample.timestamp >= cutoff and sample.timestamp in decode_at
        ]
        if source_snapshots:
            snapshots[source_id] = source_snapshots

    return snapshots


def _time_weighted_mean(
    samples: List[Tuple[float, float]], window_start: float, window_end: float
) -> float:
    """Average a right-continuous gauge over its observed part of a window."""
    observed_start = max(window_start, samples[0][0])
    if window_end <= observed_start:
        return samples[-1][1]

    weighted_sum = 0.0
    for index, (timestamp, value) in enumerate(samples):
        interval_start = max(timestamp, observed_start)
        interval_end = samples[index + 1][0] if index + 1 < len(samples) else window_end
        interval_end = min(interval_end, window_end)
        if interval_end > interval_start:
            weighted_sum += value * (interval_end - interval_start)

    return weighted_sum / (window_end - observed_start)


class LLMTokenAutoscalingPolicy:
    """Scale an aggregated LLM deployment from router-owned work snapshots.

    Scale-up recommendations are aggregated over a short lookback so bursty
    token load remains visible long enough for ``upscale_delay_s`` to elapse.
    Router replicas report the same global snapshot, so recommendations are
    maximized across reporters rather than summed.

    Scale-down is deliberately asymmetric. At most one replica is proposed for
    removal, and only when the latest snapshot would leave both prefill and
    decode utilization below ``scale_down_sensitivity`` on the survivors.
    Serve's existing ``downscale_delay_s`` is then applied to that proposal.
    """

    def __init__(
        self,
        target_prefill_tokens_per_replica: int,
        target_decode_requests_per_replica: int,
        upscale_lookback_s: float = 30,
        upscale_aggregation: _UpscaleAggregation = "max",
        scale_down_sensitivity: float = 0.8,
    ):
        if target_prefill_tokens_per_replica <= 0:
            raise ValueError("target_prefill_tokens_per_replica must be positive")
        if target_decode_requests_per_replica <= 0:
            raise ValueError("target_decode_requests_per_replica must be positive")
        if not math.isfinite(upscale_lookback_s) or upscale_lookback_s <= 0:
            raise ValueError("upscale_lookback_s must be positive and finite")
        if upscale_aggregation not in ("max", "mean"):
            raise ValueError("upscale_aggregation must be 'max' or 'mean'")
        if (
            not math.isfinite(scale_down_sensitivity)
            or scale_down_sensitivity <= 0
            or scale_down_sensitivity > 1
        ):
            raise ValueError("scale_down_sensitivity must be in the interval (0, 1]")

        self._target_prefill_tokens_per_replica = target_prefill_tokens_per_replica
        self._target_decode_requests_per_replica = target_decode_requests_per_replica
        self._upscale_lookback_s = upscale_lookback_s
        self._upscale_aggregation = upscale_aggregation
        self._scale_down_sensitivity = scale_down_sensitivity

    def _required_capacity(self, prefill: float, decode: float) -> float:
        return max(
            max(0, prefill) / self._target_prefill_tokens_per_replica,
            max(0, decode) / self._target_decode_requests_per_replica,
        )

    def _aggregate_upscale_capacity(
        self,
        snapshots_by_source: Dict[str, List[_Snapshot]],
        now: float,
    ) -> float:
        window_start = now - self._upscale_lookback_s
        capacity_by_source = []
        for snapshots in snapshots_by_source.values():
            capacity_samples = [
                (timestamp, self._required_capacity(prefill, decode))
                for timestamp, prefill, decode in snapshots
            ]
            samples = [
                sample for sample in capacity_samples if sample[0] >= window_start
            ]
            if not samples:
                continue
            if self._upscale_aggregation == "max":
                capacity_by_source.append(max(value for _, value in samples))
            else:
                # Metrics are gauges. Carry the last observation before the
                # window boundary forward so the mean covers the full window,
                # provided a new observation confirms that the source is live.
                samples_before_window = [
                    sample for sample in capacity_samples if sample[0] < window_start
                ]
                if samples_before_window:
                    samples.insert(0, samples_before_window[-1])
                capacity_by_source.append(
                    _time_weighted_mean(samples, window_start, now)
                )

        return max(capacity_by_source, default=0.0)

    def __call__(self, ctx: AutoscalingContext) -> Tuple[int, Dict[str, Any]]:
        if self._upscale_lookback_s > ctx.config.look_back_period_s:
            raise ValueError(
                "upscale_lookback_s must not exceed the autoscaling config's "
                "look_back_period_s"
            )

        snapshots_by_source = _complete_snapshots_by_source(ctx)
        # A missing or stale observation is not evidence of zero work.
        if not snapshots_by_source:
            return ctx.target_num_replicas, {}

        now = _now(ctx)
        window_start = now - self._upscale_lookback_s
        if not any(
            timestamp >= window_start
            for snapshots in snapshots_by_source.values()
            for timestamp, _, _ in snapshots
        ):
            # No complete observation in the scale-up window is also fail-closed.
            return ctx.target_num_replicas, {}

        aggregated_capacity = self._aggregate_upscale_capacity(snapshots_by_source, now)
        upscale_required_replicas = math.ceil(aggregated_capacity)
        if ctx.total_queued_requests > 0:
            upscale_required_replicas = max(1, upscale_required_replicas)

        latest_snapshots = [snapshots[-1] for snapshots in snapshots_by_source.values()]
        outstanding_prefill_tokens = max(
            max(0, prefill) for _, prefill, _ in latest_snapshots
        )
        active_decode_requests = max(
            max(0, decode) for _, _, decode in latest_snapshots
        )
        prefill_required_replicas = math.ceil(
            outstanding_prefill_tokens / self._target_prefill_tokens_per_replica
        )
        decode_required_replicas = math.ceil(
            active_decode_requests / self._target_decode_requests_per_replica
        )
        state: Dict[str, Any] = {
            OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: outstanding_prefill_tokens,
            ACTIVE_DECODE_REQUESTS: active_decode_requests,
            "prefill_required_replicas": prefill_required_replicas,
            "decode_required_replicas": decode_required_replicas,
            "upscale_required_replicas": upscale_required_replicas,
            "upscale_aggregation": self._upscale_aggregation,
            "upscale_lookback_s": self._upscale_lookback_s,
            "scale_down_sensitivity": self._scale_down_sensitivity,
        }

        target = ctx.target_num_replicas
        if upscale_required_replicas > target:
            return upscale_required_replicas, state
        if upscale_required_replicas == target:
            return target, state

        # Scaling from one to zero is safe only with an entirely idle latest
        # snapshot. Serve applies downscale_to_zero_delay_s after this proposal.
        if target == 1:
            idle = ctx.total_queued_requests <= 0 and all(
                prefill <= 0 and decode <= 0 for _, prefill, decode in latest_snapshots
            )
            state["scale_down_blocked"] = not idle
            return (0 if idle else 1), state
        if target <= 0:
            return target, state

        candidate = target - 1
        projected_prefill_utilization = max(
            max(0, prefill) / (candidate * self._target_prefill_tokens_per_replica)
            for _, prefill, _ in latest_snapshots
        )
        projected_decode_utilization = max(
            max(0, decode) / (candidate * self._target_decode_requests_per_replica)
            for _, _, decode in latest_snapshots
        )
        prefill_below_sensitivity = (
            projected_prefill_utilization < self._scale_down_sensitivity
            and not math.isclose(
                projected_prefill_utilization,
                self._scale_down_sensitivity,
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
        )
        decode_below_sensitivity = (
            projected_decode_utilization < self._scale_down_sensitivity
            and not math.isclose(
                projected_decode_utilization,
                self._scale_down_sensitivity,
                rel_tol=1e-12,
                abs_tol=1e-12,
            )
        )
        scale_down_safe = prefill_below_sensitivity and decode_below_sensitivity
        state.update(
            {
                "scale_down_candidate_replicas": candidate,
                "projected_prefill_utilization": projected_prefill_utilization,
                "projected_decode_utilization": projected_decode_utilization,
                "scale_down_blocked": not scale_down_safe,
            }
        )
        return (candidate if scale_down_safe else target), state
