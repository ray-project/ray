"""Unit tests for the broadcast-join decision object (no Ray cluster)."""

import pytest

from ray.data._internal.execution.operators.join import (
    BuildSide,
    _BroadcastDecision,
    valid_build_sides,
)
from ray.data._internal.logical.operators import JoinType

LEFT = BuildSide.BUILD_LEFT
RIGHT = BuildSide.BUILD_RIGHT
NONE = BuildSide.NONE
UNDECIDED = BuildSide.UNDECIDED


def test_valid_build_sides_covers_every_join_type():
    for join_type in JoinType:
        valid_build_sides(join_type)


def test_no_candidate_side_is_shuffle_from_construction():
    # full_outer, or the feature flag off: nothing to wait for.
    assert _BroadcastDecision(set()).build_side is NONE


class TestExceedThreshold:
    def test_falls_back_to_shuffle_when_no_side_is_left(self):
        decision = _BroadcastDecision({LEFT, RIGHT})
        decision.exceed_threshold(LEFT)
        decision.exceed_threshold(RIGHT)
        assert decision.build_side is NONE


class TestChooseBuild:
    def test_side_finishing_under_threshold_is_broadcast(self):
        decision = _BroadcastDecision({LEFT, RIGHT})
        decision.choose_build(LEFT)
        assert decision.build_side is LEFT

    def test_first_caller_wins(self):
        decision = _BroadcastDecision({LEFT, RIGHT})
        decision.choose_build(RIGHT)
        decision.choose_build(LEFT)
        assert decision.build_side is RIGHT

    def test_ignores_side_disallowed_by_join_type(self):
        decision = _BroadcastDecision({RIGHT})  # e.g. left_outer
        decision.choose_build(LEFT)
        assert decision.build_side is UNDECIDED
        decision.choose_build(RIGHT)
        assert decision.build_side is RIGHT

    def test_ignores_side_that_already_exceeded_threshold(self):
        decision = _BroadcastDecision({LEFT, RIGHT})
        decision.exceed_threshold(LEFT)
        decision.choose_build(LEFT)
        assert decision.build_side is UNDECIDED
        decision.choose_build(RIGHT)
        assert decision.build_side is RIGHT

    def test_ignored_once_fallen_back_to_shuffle(self):
        # Timeout demoted the join, a side finishing late must not revive it.
        decision = _BroadcastDecision({LEFT, RIGHT})
        decision.demote()
        decision.choose_build(RIGHT)
        assert decision.build_side is NONE


class TestDemote:
    def test_falls_back_to_shuffle_while_waiting(self):
        decision = _BroadcastDecision({LEFT, RIGHT})
        decision.exceed_threshold(LEFT)
        decision.demote()
        assert decision.build_side is NONE


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
