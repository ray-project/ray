import math
import sys
from types import SimpleNamespace

import pytest

from ray.serve._private.common import TimeStampedValue
from ray.serve.experimental.llm_token_autoscaling_policy import (
    OUTSTANDING_EFFECTIVE_PREFILL_TOKENS,
    REQUIRED_REPLICAS,
    LLMTokenAutoscalingPolicy,
)


def _context(
    *,
    prefill_by_handle=None,
    now=100.0,
    target_num_replicas=2,
    total_queued_requests=0,
    look_back_period_s=30,
):
    prefill_by_handle = prefill_by_handle or {}
    series_by_handle = {}
    for handle_id, samples in prefill_by_handle.items():
        if isinstance(samples, tuple):
            samples = [samples]
        series_by_handle[handle_id] = [
            TimeStampedValue(timestamp=timestamp, value=value)
            for timestamp, value in samples
        ]

    return SimpleNamespace(
        raw_router_metrics={
            OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: series_by_handle,
        },
        current_time=now,
        config=SimpleNamespace(look_back_period_s=look_back_period_s),
        target_num_replicas=target_num_replicas,
        total_queued_requests=total_queued_requests,
    )


def test_scales_up_from_max_global_snapshot_across_handles():
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, state = policy(
        _context(prefill_by_handle={"a": (99, 250), "b": (99, 200)})
    )

    assert replicas == 3
    assert state == {
        OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: 250,
        REQUIRED_REPLICAS: 3,
    }


def test_does_not_sum_snapshots_from_multiple_handles():
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, _ = policy(
        _context(
            prefill_by_handle={"a": (99, 200), "b": (99, 300)},
            target_num_replicas=1,
        )
    )

    assert replicas == 3


@pytest.mark.parametrize("outstanding_tokens", [0, 100])
def test_never_scales_down(outstanding_tokens):
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, state = policy(
        _context(
            prefill_by_handle={"a": (99, outstanding_tokens)},
            target_num_replicas=7,
        )
    )

    assert replicas == 7
    assert state[REQUIRED_REPLICAS] == math.ceil(outstanding_tokens / 100)


@pytest.mark.parametrize(
    "prefill_by_handle",
    [
        {},
        {"a": (60, 100)},
        {"a": []},
    ],
)
def test_holds_target_for_missing_or_stale_snapshot(prefill_by_handle):
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, state = policy(
        _context(prefill_by_handle=prefill_by_handle, target_num_replicas=7)
    )

    assert replicas == 7
    assert state == {}


def test_keeps_one_replica_when_requests_are_queued():
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, state = policy(
        _context(
            prefill_by_handle={"a": (99, 0)},
            target_num_replicas=0,
            total_queued_requests=1,
        )
    )

    assert replicas == 1
    assert state[REQUIRED_REPLICAS] == 1


def test_clamps_negative_snapshot_to_zero():
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, state = policy(
        _context(
            prefill_by_handle={"a": (99, -1)},
            target_num_replicas=0,
        )
    )

    assert replicas == 0
    assert state[OUTSTANDING_EFFECTIVE_PREFILL_TOKENS] == 0


def test_rejects_nonpositive_target():
    with pytest.raises(
        ValueError, match="target_prefill_tokens_per_replica must be positive"
    ):
        LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=0)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
