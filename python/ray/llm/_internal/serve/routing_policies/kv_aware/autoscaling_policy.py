import math
from typing import Optional

from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    OUTSTANDING_EFFECTIVE_PREFILL_TOKENS,
)
from ray.serve._private.common import DeploymentID
from ray.serve.config import AutoscalingContext


def _get_target_and_reports(contexts):
    """Resolve the bound server and its per-router mean token loads."""
    source = next((dep for dep in contexts if dep.name == "LLMRouter"), None)
    if source is None:
        return None
    metrics = contexts[source].aggregated_metrics or {}
    prefix = OUTSTANDING_EFFECTIVE_PREFILL_TOKENS + ":"
    keys = [key for key in metrics if key.startswith(prefix)]
    if len(keys) != 1:
        return None
    key = keys[0]
    target = DeploymentID(name=key[len(prefix) :], app_name=source.app_name)
    if target == source or not isinstance(contexts.get(target), AutoscalingContext):
        return None
    return target, metrics[key]


def _token_target_from_queue_fit(
    queue_time_slo_ms: Optional[float],
    slope_ms_per_token: Optional[float],
    intercept_ms: Optional[float],
) -> int:
    """Invert the single-replica queue-time fit and round down to whole tokens."""
    if queue_time_slo_ms is None or slope_ms_per_token is None or intercept_ms is None:
        raise ValueError(
            "Provide target_prefill_tokens_per_replica or all three of "
            "queue_time_slo_ms, slope_ms_per_token, and intercept_ms"
        )
    if not math.isfinite(queue_time_slo_ms) or queue_time_slo_ms <= 0:
        raise ValueError("queue_time_slo_ms must be positive and finite")
    if not math.isfinite(slope_ms_per_token) or slope_ms_per_token <= 0:
        raise ValueError("slope_ms_per_token must be positive and finite")
    if not math.isfinite(intercept_ms):
        raise ValueError("intercept_ms must be finite")

    tokens = (queue_time_slo_ms - intercept_ms) / slope_ms_per_token
    if not math.isfinite(tokens) or tokens < 1:
        raise ValueError(
            "The queue-time fit and SLO must allow a finite target of at least one token"
        )
    return math.floor(tokens)


class LLMTokenAutoscalingPolicy:
    """Application policy scaling the bound server from replicated KV workload."""

    def __init__(
        self,
        target_prefill_tokens_per_replica: Optional[int] = None,
        post_downscale_cooldown_s: float = 0,
        *,
        queue_time_slo_ms: Optional[float] = None,
        slope_ms_per_token: Optional[float] = None,
        intercept_ms: Optional[float] = None,
    ):
        if target_prefill_tokens_per_replica is None:
            target_prefill_tokens_per_replica = _token_target_from_queue_fit(
                queue_time_slo_ms, slope_ms_per_token, intercept_ms
            )
        elif any(
            value is not None
            for value in (queue_time_slo_ms, slope_ms_per_token, intercept_ms)
        ):
            raise ValueError(
                "Choose either target_prefill_tokens_per_replica or the queue-time fit"
            )

        if (
            not math.isfinite(target_prefill_tokens_per_replica)
            or target_prefill_tokens_per_replica <= 0
        ):
            raise ValueError(
                "target_prefill_tokens_per_replica must be positive and finite"
            )
        if (
            not math.isfinite(post_downscale_cooldown_s)
            or post_downscale_cooldown_s < 0
        ):
            raise ValueError("post_downscale_cooldown_s must be nonnegative and finite")
        self.post_downscale_cooldown_s = post_downscale_cooldown_s
        self.target_prefill_tokens_per_replica = target_prefill_tokens_per_replica

    def __call__(self, contexts):
        resolved = _get_target_and_reports(contexts)
        if resolved is None:
            return {}, {}
        target, reports = resolved
        server = contexts[target]
        if not reports or not all(math.isfinite(value) for value in reports.values()):
            return {}, {}
        load = max(0, max(reports.values()))
        desired = math.ceil(load / self.target_prefill_tokens_per_replica)
        if server.total_num_requests > 0:
            desired = max(1, desired)
        if self.post_downscale_cooldown_s == 0:
            return {target: desired}, {}

        if (
            server.config.get_upscaling_factor() != 1
            or server.config.get_downscaling_factor() != 1
        ):
            raise ValueError(
                "post_downscale_cooldown_s requires upscaling and downscaling factors of 1"
            )
        state = {
            target: {
                OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: load,
                "required_replicas": desired,
            }
        }
        if (
            server.last_scale_down_time is not None
            and server.current_time - server.last_scale_down_time
            < self.post_downscale_cooldown_s
        ):
            # Draining replicas can still be running after the target was lowered.
            desired = max(
                server.target_num_replicas, int(server.total_num_requests > 0)
            )
        return {target: desired}, state
