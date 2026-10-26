import math
import time
from typing import Any, Dict, Tuple

from ray.serve.config import AutoscalingContext

OUTSTANDING_EFFECTIVE_PREFILL_TOKENS = "outstanding_effective_prefill_tokens"
ACTIVE_DECODE_REQUESTS = "active_decode_requests"


def _latest_fresh_by_source(
    ctx: AutoscalingContext, metric_name: str
) -> Dict[str, Any]:
    """Return the latest fresh router-owned sample from each reporting handle."""
    raw_metrics = ctx.raw_router_metrics or {}
    source_metrics = raw_metrics.get(metric_name, {})
    if not source_metrics:
        return {}

    now = ctx.current_time if ctx.current_time is not None else time.time()
    cutoff = now - ctx.config.look_back_period_s
    values = {}
    for source_id, series in source_metrics.items():
        if not series:
            continue
        latest = series[-1]
        if latest.timestamp >= cutoff:
            values[source_id] = latest

    return values


class LLMTokenAutoscalingPolicy:
    """Scale an aggregated LLM deployment from router-owned work snapshots.

    Configure this class through ``AutoscalingPolicy.policy_function`` and pass
    the two per-replica targets through ``policy_kwargs``. Each router replica
    reports the same global snapshot, so this policy takes a maximum across
    reporters instead of summing them.

    Router replicas report through their handles, so metric collection does not
    depend on an engine replica remaining alive.
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
        prefill_by_source = _latest_fresh_by_source(
            ctx, OUTSTANDING_EFFECTIVE_PREFILL_TOKENS
        )
        decode_by_source = _latest_fresh_by_source(ctx, ACTIVE_DECODE_REQUESTS)

        desired_by_source = []
        complete_snapshots = []
        for source_id in prefill_by_source.keys() & decode_by_source.keys():
            prefill = prefill_by_source[source_id]
            decode = decode_by_source[source_id]
            # Router metrics are sampled atomically. Never combine a partial
            # snapshot or values sampled at different times.
            if prefill.timestamp != decode.timestamp:
                continue
            prefill_replicas = math.ceil(
                max(0, prefill.value) / self._target_prefill_tokens_per_replica
            )
            decode_replicas = math.ceil(
                max(0, decode.value) / self._target_decode_requests_per_replica
            )
            desired_by_source.append(max(prefill_replicas, decode_replicas))
            complete_snapshots.append(
                (prefill.value, decode.value, prefill_replicas, decode_replicas)
            )

        # A missing or stale observation is not evidence of zero work.
        if not desired_by_source:
            return ctx.target_num_replicas, {}

        desired_replicas = max(desired_by_source)
        if ctx.total_queued_requests > 0:
            desired_replicas = max(1, desired_replicas)

        outstanding_prefill_tokens = max(s[0] for s in complete_snapshots)
        active_decode_requests = max(s[1] for s in complete_snapshots)
        prefill_replicas = max(s[2] for s in complete_snapshots)
        decode_replicas = max(s[3] for s in complete_snapshots)
        return desired_replicas, {
            OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: outstanding_prefill_tokens,
            ACTIVE_DECODE_REQUESTS: active_decode_requests,
            "prefill_required_replicas": prefill_replicas,
            "decode_required_replicas": decode_replicas,
        }
