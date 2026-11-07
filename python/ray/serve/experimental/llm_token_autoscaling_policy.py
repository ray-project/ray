import math
import time
from typing import Any, Dict, Optional, Tuple

from ray.serve.config import AutoscalingContext

OUTSTANDING_EFFECTIVE_PREFILL_TOKENS = "outstanding_effective_prefill_tokens"
REQUIRED_REPLICAS = "required_replicas"


def _latest_fresh_prefill_tokens(ctx: AutoscalingContext) -> Optional[float]:
    """Return the largest latest snapshot across live reporting handles."""
    raw_metrics = ctx.raw_router_metrics or {}
    prefill_by_handle = raw_metrics.get(OUTSTANDING_EFFECTIVE_PREFILL_TOKENS, {})
    if ctx.config is None:
        raise ValueError("Autoscaling context must include an autoscaling config")

    now = ctx.current_time if ctx.current_time is not None else time.time()
    cutoff = now - ctx.config.look_back_period_s
    fresh_values = [
        max(0, series[-1].value)
        for series in prefill_by_handle.values()
        if series and series[-1].timestamp >= cutoff
    ]
    return max(fresh_values) if fresh_values else None


class LLMTokenAutoscalingPolicy:
    """Scale up an LLM deployment based on outstanding uncached prefill tokens.

    KV-aware routers report the same global workload snapshot, so this policy
    takes the maximum fresh value across reporting handles instead of summing
    them. Missing or stale observations hold the existing target. This base
    policy only scales up; it never proposes fewer replicas than Serve's current
    target.

    Args:
        target_prefill_tokens_per_replica: Outstanding uncached prefill tokens
            that one replica should handle.
    """

    def __init__(self, target_prefill_tokens_per_replica: int):
        if target_prefill_tokens_per_replica <= 0:
            raise ValueError("target_prefill_tokens_per_replica must be positive")

        self._target_prefill_tokens_per_replica = target_prefill_tokens_per_replica

    def __call__(self, ctx: AutoscalingContext) -> Tuple[int, Dict[str, Any]]:
        outstanding_prefill_tokens = _latest_fresh_prefill_tokens(ctx)
        if outstanding_prefill_tokens is None:
            return ctx.target_num_replicas, {}

        required_replicas = math.ceil(
            outstanding_prefill_tokens / self._target_prefill_tokens_per_replica
        )
        if ctx.total_queued_requests > 0:
            required_replicas = max(1, required_replicas)

        return max(ctx.target_num_replicas, required_replicas), {
            OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: outstanding_prefill_tokens,
            REQUIRED_REPLICAS: required_replicas,
        }
