import math
from itertools import groupby

from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    OUTSTANDING_EFFECTIVE_PREFILL_TOKENS,
)
from ray.serve._private.common import DeploymentID, TimeStampedValue
from ray.serve._private.metrics_utils import time_weighted_average


def _get_target_and_reports(contexts):
    """Resolve the bound server and its router-reported token series."""
    source = next((dep for dep in contexts if dep.name == "LLMRouter"), None)
    if source is None:
        return None
    metrics = contexts[source].raw_metrics or {}
    prefix = OUTSTANDING_EFFECTIVE_PREFILL_TOKENS + ":"
    keys = [key for key in metrics if key.startswith(prefix)]
    if len(keys) != 1:
        return None
    key = keys[0]
    target = DeploymentID(name=key[len(prefix) :], app_name=source.app_name)
    if target not in contexts or contexts[target].config is None or target == source:
        return None
    return target, metrics[key]


def _mean_prefill_tokens(reports, now, look_back_period_s):
    """Take the maximum across replicated router views before averaging over time."""
    events = sorted(
        (
            (p.timestamp, replica, p.value)
            for replica, series in reports.items()
            for p in series
        ),
        key=lambda event: event[0],
    )
    if not events:
        return None
    latest = {}
    merged = []
    for timestamp, updates in groupby(events, key=lambda event: event[0]):
        for _, replica, value in updates:
            if not math.isfinite(value):
                return None
            latest[replica] = max(0, value)
        merged.append(TimeStampedValue(timestamp, max(latest.values())))
    start = max(merged[0].timestamp, now - look_back_period_s)
    if start > now:
        return None
    return (
        merged[-1].value
        if start == now
        else time_weighted_average(merged, window_start=start, window_end=now)
    )


class LLMTokenAutoscalingPolicy:
    """Application policy scaling the bound server from replicated KV workload."""

    def __init__(self, target_prefill_tokens_per_replica: int):
        if (
            not math.isfinite(target_prefill_tokens_per_replica)
            or target_prefill_tokens_per_replica <= 0
        ):
            raise ValueError(
                "target_prefill_tokens_per_replica must be positive and finite"
            )
        self.target_prefill_tokens_per_replica = target_prefill_tokens_per_replica

    def __call__(self, contexts):
        resolved = _get_target_and_reports(contexts)
        if resolved is None:
            return {}, {}
        target, reports = resolved
        server = contexts[target]
        load = _mean_prefill_tokens(
            reports, server.current_time, server.config.look_back_period_s
        )
        if load is None:
            return {}, {}
        desired = math.ceil(load / self.target_prefill_tokens_per_replica)
        if server.total_num_requests > 0:
            desired = max(1, desired)
        return {target: desired}, {}
