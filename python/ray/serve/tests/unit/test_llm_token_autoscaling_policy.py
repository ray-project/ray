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
    target_num_replicas=2,
    total_num_requests=0,
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
        target_num_replicas=target_num_replicas,
        total_num_requests=total_num_requests,
    )


def test_scales_from_time_weighted_mean():
    policy = LLMTokenAutoscalingPolicy(
        target_prefill_tokens_per_replica=100,
        lookback_s=30,
    )

    replicas, state = policy(
        _context(
            prefill_by_handle={"a": [(0, 300), (20, 700), (25, 300), (30, 300)]},
            target_num_replicas=1,
        )
    )

    assert replicas == 4
    assert state == {
        OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: 300,
        REQUIRED_REPLICAS: 4,
    }


def test_takes_max_mean_across_replicated_histories():
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, _ = policy(
        _context(
            prefill_by_handle={
                "a": [(0, 200), (30, 200)],
                "b": [(0, 300), (30, 300)],
            },
            target_num_replicas=1,
        )
    )

    assert replicas == 3


def test_router_clock_offsets_do_not_change_mean():
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, _ = policy(
        _context(
            prefill_by_handle={
                "behind": [(-10_030, 100), (-10_015, 300), (-10_000, 300)],
                "ahead": [(9_970, 100), (9_985, 300), (10_000, 300)],
            },
            target_num_replicas=1,
        )
    )

    assert replicas == 2


def test_carries_gauge_across_window_boundary():
    policy = LLMTokenAutoscalingPolicy(
        target_prefill_tokens_per_replica=100,
        lookback_s=20,
    )

    replicas, _ = policy(
        _context(
            prefill_by_handle={"a": [(0, 300), (20, 0), (30, 0)]},
            target_num_replicas=1,
        )
    )

    assert replicas == 2


@pytest.mark.parametrize("outstanding_tokens", [0, 100])
def test_scales_down(outstanding_tokens):
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, state = policy(
        _context(
            prefill_by_handle={
                "a": [(0, outstanding_tokens), (30, outstanding_tokens)]
            },
            target_num_replicas=7,
        )
    )

    assert replicas == math.ceil(outstanding_tokens / 100)
    assert state[REQUIRED_REPLICAS] == math.ceil(outstanding_tokens / 100)


@pytest.mark.parametrize("prefill_by_handle", [{}, {"a": []}])
def test_holds_target_for_missing_snapshot(prefill_by_handle):
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, state = policy(
        _context(prefill_by_handle=prefill_by_handle, target_num_replicas=7)
    )

    assert replicas == 7
    assert state == {}


def test_keeps_one_replica_when_requests_are_in_flight():
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, state = policy(
        _context(
            prefill_by_handle={"a": [(0, 0), (30, 0)]},
            target_num_replicas=0,
            total_num_requests=1,
        )
    )

    assert replicas == 1
    assert state[REQUIRED_REPLICAS] == 1


def test_clamps_negative_snapshots_to_zero():
    policy = LLMTokenAutoscalingPolicy(target_prefill_tokens_per_replica=100)

    replicas, state = policy(
        _context(
            prefill_by_handle={"a": [(0, -100), (30, -1)]},
            target_num_replicas=0,
        )
    )

    assert replicas == 0
    assert state[OUTSTANDING_EFFECTIVE_PREFILL_TOKENS] == 0


def test_lookback_longer_than_history_averages_available_samples():
    policy = LLMTokenAutoscalingPolicy(
        target_prefill_tokens_per_replica=100,
        lookback_s=60,
    )

    replicas, _ = policy(
        _context(prefill_by_handle={"a": [(0, 100), (15, 300), (30, 300)]})
    )

    # (100 * 15s + 300 * 15s) / 30s observed = 200 tokens -> 2 replicas.
    assert replicas == 2


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"target_prefill_tokens_per_replica": 0}, "positive and finite"),
        ({"target_prefill_tokens_per_replica": float("nan")}, "positive and finite"),
        ({"target_prefill_tokens_per_replica": float("inf")}, "positive and finite"),
        (
            {
                "target_prefill_tokens_per_replica": 100,
                "lookback_s": 0,
            },
            "positive and finite",
        ),
    ],
)
def test_rejects_invalid_config(kwargs, match):
    with pytest.raises(ValueError, match=match):
        LLMTokenAutoscalingPolicy(**kwargs)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
