import math
from itertools import groupby

from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    OUTSTANDING_EFFECTIVE_PREFILL_TOKENS,
)
from ray.serve._private.common import DeploymentID, TimeStampedValue
from ray.serve._private.metrics_utils import time_weighted_average


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
        if not contexts:
            return {}, {}
        ctx = next(iter(contexts.values()))
        source = DeploymentID(name="LLMRouter", app_name=ctx.app_name)
        metrics = ctx.application_raw_metrics.get(source, {})
        prefix = OUTSTANDING_EFFECTIVE_PREFILL_TOKENS + ":"
        keys = [key for key in metrics if key.startswith(prefix)]
        if len(keys) != 1:
            return {}, {}
        key = keys[0]
        target = DeploymentID(name=key[len(prefix) :], app_name=ctx.app_name)
        if target not in contexts or target == source:
            return {}, {}
        server = contexts[target]
        events = sorted(
            (
                (p.timestamp, replica, p.value)
                for replica, series in metrics[key].items()
                for p in series
            ),
            key=lambda event: event[0],
        )
        if not events:
            return {}, {}
        latest = {}
        merged = []
        for timestamp, updates in groupby(events, key=lambda event: event[0]):
            for _, replica, value in updates:
                if not math.isfinite(value):
                    return {}, {}
                latest[replica] = max(0, value)
            merged.append(TimeStampedValue(timestamp, max(latest.values())))
        start = max(
            merged[0].timestamp, server.current_time - server.config.look_back_period_s
        )
        if start > server.current_time:
            return {}, {}
        load = (
            merged[-1].value
            if start == server.current_time
            else time_weighted_average(
                merged, window_start=start, window_end=server.current_time
            )
        )
        if load is None:
            return {}, {}
        desired = math.ceil(load / self.target_prefill_tokens_per_replica)
        if server.total_num_requests > 0:
            desired = max(1, desired)
        return {target: desired}, {}
