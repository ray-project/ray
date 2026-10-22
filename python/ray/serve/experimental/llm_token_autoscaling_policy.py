import math
import time
from typing import Any, Dict, Optional, Tuple

from ray.serve.config import AutoscalingContext

OUTSTANDING_EFFECTIVE_PREFILL_TOKENS = "outstanding_effective_prefill_tokens"
ACTIVE_DECODE_REQUESTS = "active_decode_requests"


def _latest_fresh_max(ctx: AutoscalingContext, metric_name: str) -> Optional[float]:
    """Return the maximum latest value reported by a live engine replica."""
    raw_metrics = ctx.raw_metrics or {}
    replica_metrics = raw_metrics.get(metric_name, {})
    if not replica_metrics:
        return None

    now = ctx.current_time if ctx.current_time is not None else time.time()
    cutoff = now - ctx.config.look_back_period_s
    running_replicas = set(ctx.running_replicas)
    values = []
    for replica_id, series in replica_metrics.items():
        if replica_id not in running_replicas or not series:
            continue
        latest = series[-1]
        if latest.timestamp >= cutoff:
            values.append(latest.value)

    return max(values) if values else None


class LLMTokenAutoscalingPolicy:
    """Scale an aggregated LLM deployment from router-owned work snapshots.

    Configure this class through ``AutoscalingPolicy.policy_function`` and pass
    the two per-replica targets through ``policy_kwargs``. Each engine replica
    reports the same global snapshot, so this policy takes a maximum across
    reporters instead of summing them.

    This prototype requires ``min_replicas >= 1``. Engine replicas relay the
    router snapshot, so after scaling to zero there would be no reporter to
    trigger a scale-up.
    """

    def __init__(
        self,
        target_prefill_tokens_per_replica: int,
        target_decode_requests_per_replica: int,
    ):
        if target_prefill_tokens_per_replica <= 0:
            raise ValueError("target_prefill_tokens_per_replica must be positive")
        if target_decode_requests_per_replica <= 0:
            raise ValueError("target_decode_requests_per_replica must be positive")
        self._target_prefill_tokens_per_replica = target_prefill_tokens_per_replica
        self._target_decode_requests_per_replica = target_decode_requests_per_replica

    def __call__(self, ctx: AutoscalingContext) -> Tuple[int, Dict[str, Any]]:
        outstanding_prefill_tokens = _latest_fresh_max(
            ctx, OUTSTANDING_EFFECTIVE_PREFILL_TOKENS
        )
        active_decode_requests = _latest_fresh_max(ctx, ACTIVE_DECODE_REQUESTS)

        # Both values come from one atomic router snapshot. A partial or stale
        # observation is not evidence of zero work, so keep the existing target.
        if outstanding_prefill_tokens is None or active_decode_requests is None:
            return ctx.target_num_replicas, {}

        prefill_replicas = math.ceil(
            max(0, outstanding_prefill_tokens) / self._target_prefill_tokens_per_replica
        )
        decode_replicas = math.ceil(
            max(0, active_decode_requests) / self._target_decode_requests_per_replica
        )
        # Keep one relay alive even when no work is outstanding. Without an
        # engine replica, router snapshots cannot reach this deployment's
        # custom autoscaling metrics channel.
        desired_replicas = max(1, prefill_replicas, decode_replicas)
        return desired_replicas, {
            OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: outstanding_prefill_tokens,
            ACTIVE_DECODE_REQUESTS: active_decode_requests,
            "prefill_required_replicas": prefill_replicas,
            "decode_required_replicas": decode_replicas,
        }
