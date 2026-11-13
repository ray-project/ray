import math
from typing import Any, Dict, List, Optional, Tuple

from ray.serve.config import AutoscalingContext

OUTSTANDING_EFFECTIVE_PREFILL_TOKENS = "outstanding_effective_prefill_tokens"
REQUIRED_REPLICAS = "required_replicas"

_Snapshot = Tuple[float, float]


def _prefill_snapshots_by_source(
    ctx: AutoscalingContext,
) -> Dict[str, List[_Snapshot]]:
    """Return nonempty prefill histories for each live reporting handle."""
    raw_metrics = ctx.raw_router_metrics or {}
    return {
        handle_id: [(sample.timestamp, max(0, sample.value)) for sample in series]
        for handle_id, series in raw_metrics.get(
            OUTSTANDING_EFFECTIVE_PREFILL_TOKENS, {}
        ).items()
        if series
    }


def _time_weighted_mean(
    samples: List[_Snapshot], window_start: float, window_end: float
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
    """Scale an LLM deployment based on outstanding uncached prefill tokens.

    Each KV router reports the same global workload history. The policy computes a
    time-weighted mean over ``upscale_lookback_s`` for each router, then takes the
    maximum to conservatively reconcile their replicated global views.

    Args:
        target_prefill_tokens_per_replica: Mean outstanding uncached prefill tokens
            that one replica should handle.
        upscale_lookback_s: Window used for the time-weighted mean.
    """

    def __init__(
        self,
        target_prefill_tokens_per_replica: int,
        upscale_lookback_s: float = 30,
    ):
        if target_prefill_tokens_per_replica <= 0:
            raise ValueError("target_prefill_tokens_per_replica must be positive")
        if not math.isfinite(upscale_lookback_s) or upscale_lookback_s <= 0:
            raise ValueError("upscale_lookback_s must be positive and finite")

        self._target_prefill_tokens_per_replica = target_prefill_tokens_per_replica
        self._upscale_lookback_s = upscale_lookback_s

    def _required_capacity(self, prefill_tokens: float) -> float:
        return prefill_tokens / self._target_prefill_tokens_per_replica

    def _mean_required_capacity(
        self, snapshots_by_source: Dict[str, List[_Snapshot]]
    ) -> Optional[float]:
        """Return the maximum per-source time-weighted mean capacity."""
        capacity_by_source = []
        for snapshots in snapshots_by_source.values():
            window_end = snapshots[-1][0]
            window_start = window_end - self._upscale_lookback_s
            capacity_samples = [
                (timestamp, self._required_capacity(prefill_tokens))
                for timestamp, prefill_tokens in snapshots
            ]
            samples = [
                sample for sample in capacity_samples if sample[0] >= window_start
            ]
            samples_before_window = [
                sample for sample in capacity_samples if sample[0] < window_start
            ]
            if samples_before_window:
                samples.insert(0, samples_before_window[-1])
            if samples:
                capacity_by_source.append(
                    _time_weighted_mean(samples, window_start, window_end)
                )

        return max(capacity_by_source) if capacity_by_source else None

    def __call__(self, ctx: AutoscalingContext) -> Tuple[int, Dict[str, Any]]:
        if ctx.config is None:
            raise ValueError("Autoscaling context must include an autoscaling config")
        if self._upscale_lookback_s > ctx.config.look_back_period_s:
            raise ValueError(
                "upscale_lookback_s must not exceed the autoscaling config's "
                "look_back_period_s"
            )

        snapshots_by_source = _prefill_snapshots_by_source(ctx)
        mean_required_capacity = self._mean_required_capacity(snapshots_by_source)
        if mean_required_capacity is None:
            return ctx.target_num_replicas, {}

        required_replicas = math.ceil(mean_required_capacity)
        if ctx.total_num_requests > 0:
            required_replicas = max(1, required_replicas)

        outstanding_prefill_tokens = max(
            snapshots[-1][1] for snapshots in snapshots_by_source.values()
        )
        return required_replicas, {
            OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: outstanding_prefill_tokens,
            REQUIRED_REPLICAS: required_replicas,
        }
