"""Unit tests for ``TopicSpec`` and its step and match built-ins (``mcap_topic_spec``)."""

from fractions import Fraction
from typing import Any

import pytest

from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import (
    MessageSteps,
    TimeSteps,
    TopicSpec,
)
from ray.data.datasource.mcap import match, steps

SECOND = 1_000_000_000
# Values of the wrong type, which the built-ins and TopicSpec must refuse.
NOT_A_COUNT: Any = 2.0
NOT_STEPS: Any = "before"
NOT_A_MATCH: Any = "nearest"
UNKNOWN_PAD: Any = "repeat"


@pytest.mark.parametrize(
    "build, expected",
    [
        (lambda: steps.before(1, fps=10), TimeSteps("before", 10, Fraction(10))),
        (lambda: steps.after(2, fps=2), TimeSteps("after", 4, Fraction(2))),
        (
            lambda: steps.before(frames=3, fps=Fraction(30000, 1001)),
            TimeSteps("before", 3, Fraction(30000, 1001)),
        ),
        # A float fps is read as the decimal it prints as.
        (
            lambda: steps.after(frames=2, fps=29.97),
            TimeSteps("after", 2, Fraction(2997, 100)),
        ),
        (lambda: steps.last(4), MessageSteps("before", count=4)),
        (lambda: steps.next(2), MessageSteps("after", count=2)),
        (
            lambda: steps.every(before_s=0.25),
            MessageSteps("before", span_ns=SECOND // 4),
        ),
        (lambda: steps.every(after_s=1), MessageSteps("after", span_ns=SECOND)),
    ],
)
def test_steps_built_ins(build, expected):
    """Each built-in gives the side, count or span and rate it names."""
    assert build() == expected


@pytest.mark.parametrize(
    "build, error",
    [
        (lambda: steps.before(fps=10), "exactly one of them"),
        (lambda: steps.before(1, frames=2, fps=10), "exactly one of them"),
        (lambda: steps.before(frames=0, fps=10), "positive integer"),
        (
            lambda: steps.after(frames=NOT_A_COUNT, fps=10),
            "positive integer",
        ),
        (lambda: steps.before(frames=True, fps=10), "positive integer"),
        (lambda: steps.before(-0.5, fps=10), "positive"),
        (lambda: steps.after(float("nan"), fps=10), "finite"),
        (lambda: steps.before(0.5, fps=0), "positive"),
        (lambda: steps.before(0.5, fps=15), "7.5 steps"),
        (lambda: steps.last(0), "positive integer"),
        (lambda: steps.every(), "exactly one of them"),
        (lambda: steps.every(before_s=1, after_s=1), "exactly one of them"),
        (lambda: steps.every(before_s=1e-12), "at least one nanosecond"),
    ],
)
def test_steps_built_ins_reject(build, error):
    """A built-in needs one positive horizon, and a whole number of steps when
    the horizon is in seconds."""
    with pytest.raises(ValueError, match=error):
        build()


def test_topic_spec_defaults():
    """A spec matches time steps to the nearest message and pads at the edge
    unless told otherwise."""
    spec = TopicSpec(steps.before(1, fps=5))

    assert (spec.match, spec.pad) == (None, "edge")
    assert TopicSpec(steps.before(1, fps=5), match.nearest()).match == match.nearest()


@pytest.mark.parametrize(
    "build, error",
    [
        (
            lambda: TopicSpec(NOT_STEPS),
            "needs steps",
        ),
        (
            lambda: TopicSpec(steps.before(1, fps=5), NOT_A_MATCH),
            "needs match",
        ),
        (lambda: TopicSpec(steps.last(2), match.nearest()), "take no match"),
        (lambda: TopicSpec(steps.every(before_s=1), match.nearest()), "take no match"),
        (
            lambda: TopicSpec(steps.last(2), pad=UNKNOWN_PAD),
            "pad must be one of",
        ),
    ],
)
def test_topic_spec_rejects(build, error):
    """A spec takes built-in steps and match only, no match for message steps,
    and a known pad mode."""
    with pytest.raises(ValueError, match=error):
        build()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
