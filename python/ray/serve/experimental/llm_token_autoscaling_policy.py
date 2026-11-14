import math
from typing import Any, Dict, Tuple

from ray.serve.config import AutoscalingContext

OUTSTANDING_EFFECTIVE_PREFILL_TOKENS = "outstanding_effective_prefill_tokens"
REQUIRED_REPLICAS = "required_replicas"


class LLMTokenAutoscalingPolicy:
    """Scale an LLM deployment based on outstanding uncached prefill tokens.

    Serve aggregates each KV router's reported tokens over ``look_back_period_s``.
    Each router reports the same global workload, so the policy takes the maximum
    across routers to conservatively reconcile their replicated global views.

    Args:
        target_prefill_tokens_per_replica: Outstanding uncached prefill tokens
            that one replica should handle.
    """

    def __init__(self, target_prefill_tokens_per_replica: int):
        if (
            not math.isfinite(target_prefill_tokens_per_replica)
            or target_prefill_tokens_per_replica <= 0
        ):
            raise ValueError(
                "target_prefill_tokens_per_replica must be positive and finite"
            )

        self._target_prefill_tokens_per_replica = target_prefill_tokens_per_replica

    def __call__(self, ctx: AutoscalingContext) -> Tuple[int, Dict[str, Any]]:
        prefill_tokens_by_handle = (ctx.aggregated_router_metrics or {}).get(
            OUTSTANDING_EFFECTIVE_PREFILL_TOKENS, {}
        )
        if not prefill_tokens_by_handle:
            if ctx.total_num_requests > 0:
                return max(1, ctx.target_num_replicas), {}
            return ctx.target_num_replicas, {}

        prefill_tokens = max(0, max(prefill_tokens_by_handle.values()))
        required_replicas = math.ceil(
            prefill_tokens / self._target_prefill_tokens_per_replica
        )
        if ctx.total_num_requests > 0:
            required_replicas = max(1, required_replicas)

        return required_replicas, {
            OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: prefill_tokens,
            REQUIRED_REPLICAS: required_replicas,
        }
