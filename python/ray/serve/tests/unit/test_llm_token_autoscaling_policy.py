import math
import sys
from types import SimpleNamespace

import pytest

from ray.serve.experimental.llm_token_autoscaling_policy import (
    OUTSTANDING_EFFECTIVE_PREFILL_TOKENS,
    REQUIRED_REPLICAS,
    LLMTokenAutoscalingPolicy,
)


def _context(
    *,
    prefill_by_handle=None,
    target_num_replicas=2,
    total_num_requests=0,
):
    return SimpleNamespace(
        aggregated_router_metrics={
            OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: prefill_by_handle or {},
        },
        target_num_replicas=target_num_replicas,
        total_num_requests=total_num_requests,
    )


def test_scales_from_aggregated_tokens():
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, state = policy(
        _context(prefill_by_handle={"a": 350.0}, target_num_replicas=1)
    )

    assert replicas == 4
    assert state == {
        OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: 350.0,
        REQUIRED_REPLICAS: 4,
    }


def test_takes_max_across_handles():
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, state = policy(
        _context(prefill_by_handle={"a": 200.0, "b": 300.0}, target_num_replicas=1)
    )

    assert replicas == 3
    assert state[OUTSTANDING_EFFECTIVE_PREFILL_TOKENS] == 300.0


@pytest.mark.parametrize("outstanding_tokens", [0, 100])
def test_scales_down(outstanding_tokens):
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, state = policy(
        _context(
            prefill_by_handle={"a": float(outstanding_tokens)},
            target_num_replicas=7,
        )
    )

    assert replicas == math.ceil(outstanding_tokens / 100)
    assert state[REQUIRED_REPLICAS] == math.ceil(outstanding_tokens / 100)


@pytest.mark.parametrize("aggregated_router_metrics", [None, {}, {"other": {}}])
def test_holds_target_for_missing_metric(aggregated_router_metrics):
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)
    ctx = _context(target_num_replicas=7)
    ctx.aggregated_router_metrics = aggregated_router_metrics

    replicas, state = policy(ctx)

    assert replicas == 7
    assert state == {}


def test_keeps_one_replica_when_requests_are_in_flight():
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, state = policy(
        _context(
            prefill_by_handle={"a": 0.0},
            target_num_replicas=0,
            total_num_requests=1,
        )
    )

    assert replicas == 1
    assert state[REQUIRED_REPLICAS] == 1


def test_clamps_negative_tokens_to_zero():
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, state = policy(
        _context(prefill_by_handle={"a": -50.0}, target_num_replicas=0)
    )

    assert replicas == 0
    assert state[OUTSTANDING_EFFECTIVE_PREFILL_TOKENS] == 0


@pytest.mark.parametrize("target", [0, -1, float("nan"), float("inf")])
def test_rejects_invalid_target(target):
    with pytest.raises(ValueError, match="positive and finite"):
        LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=target)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
