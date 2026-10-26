from types import SimpleNamespace

import pytest

from ray.serve._private.common import TimeStampedValue
from ray.serve.experimental.llm_token_autoscaling_policy import (
    ACTIVE_DECODE_REQUESTS,
    OUTSTANDING_EFFECTIVE_PREFILL_TOKENS,
    LLMTokenAutoscalingPolicy,
)


def _context(
    *,
    prefill_by_replica=None,
    decode_by_replica=None,
    now=100.0,
    target_num_replicas=2,
):
    replica_ids = {*(prefill_by_replica or {}), *(decode_by_replica or {})}

    def to_series(values):
        return {
            replica_id: [TimeStampedValue(timestamp=timestamp, value=value)]
            for replica_id, (timestamp, value) in values.items()
        }

    return SimpleNamespace(
        raw_router_metrics={
            OUTSTANDING_EFFECTIVE_PREFILL_TOKENS: to_series(prefill_by_replica or {}),
            ACTIVE_DECODE_REQUESTS: to_series(decode_by_replica or {}),
        },
        current_time=now,
        config=SimpleNamespace(look_back_period_s=30),
        running_replicas=list(replica_ids),
        target_num_replicas=target_num_replicas,
        total_queued_requests=0,
    )


def test_scales_from_max_global_snapshot_across_reporters():
    policy = LLMTokenAutoscalingPolicy(
        target_prefill_tokens_per_replica=100,
        target_decode_requests_per_replica=4,
    )
    ctx = _context(
        prefill_by_replica={"a": (99, 250), "b": (99, 200)},
        decode_by_replica={"a": (99, 3), "b": (99, 9)},
    )

    replicas, state = policy(ctx)

    assert replicas == 3
    assert state[OUTSTANDING_EFFECTIVE_PREFILL_TOKENS] == 250
    assert state[ACTIVE_DECODE_REQUESTS] == 9
    assert state["prefill_required_replicas"] == 3
    assert state["decode_required_replicas"] == 3


def test_scales_to_zero_when_idle():
    policy = LLMTokenAutoscalingPolicy(100, 4)

    replicas, _ = policy(
        _context(
            prefill_by_replica={"a": (99, 0)},
            decode_by_replica={"a": (99, 0)},
        )
    )

    assert replicas == 0


def test_keeps_one_replica_when_requests_are_queued():
    policy = LLMTokenAutoscalingPolicy(100, 4)
    ctx = _context(
        prefill_by_replica={"a": (99, 0)},
        decode_by_replica={"a": (99, 0)},
    )
    ctx.total_queued_requests = 1

    replicas, _ = policy(ctx)

    assert replicas == 1


@pytest.mark.parametrize(
    "prefill_by_replica, decode_by_replica",
    [
        ({}, {"a": (99, 2)}),
        ({"a": (99, 100)}, {}),
        ({"a": (60, 100)}, {"a": (60, 2)}),
    ],
)
def test_holds_target_for_partial_or_stale_snapshot(
    prefill_by_replica, decode_by_replica
):
    policy = LLMTokenAutoscalingPolicy(100, 4)

    replicas, state = policy(
        _context(
            prefill_by_replica=prefill_by_replica,
            decode_by_replica=decode_by_replica,
            target_num_replicas=7,
        )
    )

    assert replicas == 7
    assert state == {}


def test_holds_target_for_mismatched_snapshot_timestamps():
    policy = LLMTokenAutoscalingPolicy(100, 4)

    replicas, state = policy(
        _context(
            prefill_by_replica={"a": (98, 100)},
            decode_by_replica={"a": (99, 2)},
            target_num_replicas=7,
        )
    )

    assert replicas == 7
    assert state == {}


@pytest.mark.parametrize("prefill_target, decode_target", [(0, 1), (1, 0)])
def test_rejects_nonpositive_targets(prefill_target, decode_target):
    with pytest.raises(ValueError, match="must be positive"):
        LLMTokenAutoscalingPolicy(prefill_target, decode_target)
