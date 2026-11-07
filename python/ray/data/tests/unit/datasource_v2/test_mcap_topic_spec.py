"""Unit tests for ``TopicSpec`` and its step and match built-ins (``mcap_topic_spec``)."""

from fractions import Fraction
from typing import Any

import numpy as np
import pytest

from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import (
    AllInStep,
    Backward,
    ClipSteps,
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
UNKNOWN_POINT: Any = "middle"
NOT_A_SIDE: Any = "sideways"
NOT_A_WITHIN: Any = "file"
# A rate given as a float, as a caller building a step class directly might.
FLOAT_FPS: Any = 10.0
# Counts read from numpy arrays, which the built-ins take as plain integers.
NUMPY_THREE: Any = np.int64(3)
NUMPY_FOUR: Any = np.int64(4)


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
        # Every message of the clip measures from no point.
        (lambda: steps.every(), MessageSteps("before")),
        (
            lambda: steps.before(1, fps=10, at="clip_end"),
            TimeSteps("before", 10, Fraction(10), "clip_end"),
        ),
        (
            lambda: steps.next(3, at="clip_start"),
            MessageSteps("after", count=3, at="clip_start"),
        ),
        (
            lambda: steps.every(before_s=1, at="clip_end"),
            MessageSteps("before", span_ns=SECOND, at="clip_end"),
        ),
        (lambda: steps.across(8), ClipSteps(count=8)),
        (lambda: steps.across(fps=2), ClipSteps(fps=Fraction(2))),
        (lambda: steps.at("clip_end"), ClipSteps(point="clip_end")),
        # ``within`` reaches past the clip, on every built-in.
        (
            lambda: steps.after(1, fps=2, within="recording"),
            TimeSteps("after", 2, Fraction(2), within="recording"),
        ),
        (
            lambda: steps.last(3, within="recording"),
            MessageSteps("before", count=3, within="recording"),
        ),
        (
            lambda: steps.every(before_s=1, within="recording"),
            MessageSteps("before", span_ns=SECOND, within="recording"),
        ),
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
        # 2/3 as a float reads as 0.6666666666666666, which is just short of
        # 20 steps at 30 fps; the message gives the exact count.
        (lambda: steps.before(2 / 3, fps=30), r"is 19\.999999999999998 steps"),
        (lambda: steps.before(2 / 3, fps=30), "frames="),
        (lambda: steps.after(1, fps=Fraction(30000, 1001)), "is 30000/1001 steps"),
        (lambda: steps.last(0), "positive integer"),
        (lambda: steps.every(before_s=1, after_s=1), "not both"),
        (lambda: steps.every(at="clip_end"), "takes no at= or within="),
        (lambda: steps.every(within="recording"), "takes no at= or within="),
        (lambda: steps.before(1, fps=10, at=UNKNOWN_POINT), "at must be one of"),
        (lambda: steps.across(), "exactly one of them"),
        (lambda: steps.across(4, fps=2), "exactly one of them"),
        (lambda: steps.across(0), "positive integer"),
        (lambda: steps.across(fps=0), "positive"),
        (lambda: steps.at(UNKNOWN_POINT), "point must be one of"),
        (lambda: steps.every(before_s=1e-12), "at least one nanosecond"),
        (lambda: steps.before(1, fps=5, within="file"), "within must be one of"),
        (lambda: steps.next(2, within="episode"), "within must be one of"),
    ],
)
def test_steps_built_ins_reject(build, error):
    """A built-in needs one positive horizon, and a whole number of steps when
    the horizon is in seconds."""
    with pytest.raises(ValueError, match=error):
        build()


@pytest.mark.parametrize(
    "build",
    [
        lambda: steps.after(1, fps=10, at="clip_end"),
        lambda: steps.next(2, at="clip_end"),
        lambda: steps.every(after_s=1, at="clip_end"),
        lambda: steps.before(frames=3, fps=10, at="clip_start"),
        lambda: steps.last(2, at="clip_start"),
    ],
)
def test_steps_no_clip_can_fill_are_refused(build):
    """An after-window from the clip's end, or a before-window of several steps
    from its start, lies outside every clip, so it is refused unless it may
    read past the clip."""
    with pytest.raises(ValueError, match="within='recording'"):
        build()


@pytest.mark.parametrize(
    "build",
    [
        lambda: steps.after(1, fps=10, at="clip_end", within="recording"),
        lambda: steps.last(2, at="clip_start", within="recording"),
        # One step on the clip's first nanosecond lies inside the clip.
        lambda: steps.before(frames=1, fps=10, at="clip_start"),
        lambda: steps.last(1, at="clip_start"),
    ],
)
def test_steps_at_the_clips_edges_that_a_read_can_fill(build):
    """The same windows build when they may read past the clip, and a single
    step on the clip's first nanosecond builds as it is."""
    assert TopicSpec(build()).pad == "edge"


@pytest.mark.parametrize(
    "build, expected",
    [
        (lambda: steps.last(NUMPY_FOUR), MessageSteps("before", count=4)),
        (
            lambda: steps.before(frames=NUMPY_THREE, fps=10),
            TimeSteps("before", 3, Fraction(10)),
        ),
        (lambda: steps.across(NUMPY_FOUR), ClipSteps(count=4)),
    ],
)
def test_counts_accept_numpy_integers(build, expected):
    """A count may be any integer type, such as a numpy integer read from an
    array."""
    assert build() == expected


@pytest.mark.parametrize(
    "build, error",
    [
        (lambda: TimeSteps(NOT_A_SIDE, 3, Fraction(10)), "side must be one of"),
        (lambda: TimeSteps("before", -3, Fraction(10)), "positive integer"),
        (lambda: TimeSteps("before", 3, Fraction(0)), "positive"),
        (
            lambda: TimeSteps("before", 3, Fraction(10), at=UNKNOWN_POINT),
            "at must be one of",
        ),
        (lambda: TimeSteps("after", 3, Fraction(10), at="clip_end"), "within="),
        (
            lambda: MessageSteps("before", count=3, span_ns=5),
            "count or span_ns, not both",
        ),
        (lambda: MessageSteps("before", span_ns=0), "positive integer"),
        (lambda: MessageSteps("before", count=2, within=NOT_A_WITHIN), "within"),
        (lambda: ClipSteps(), "exactly one of"),
        (lambda: ClipSteps(count=4, fps=Fraction(10)), "exactly one of"),
        (lambda: ClipSteps(point=UNKNOWN_POINT), "point must be one of"),
    ],
)
def test_steps_built_directly_are_checked(build, error):
    """The step classes check their fields as the built-ins do, so steps built
    without a built-in are refused for the same mistakes."""
    with pytest.raises(ValueError, match=error):
        build()


def test_steps_built_directly_equal_the_built_ins():
    """A step class built with a float rate holds it as the exact fraction a
    built-in gives."""
    direct = TimeSteps("before", 3, FLOAT_FPS)

    assert direct == steps.before(frames=3, fps=10)
    assert isinstance(direct.fps, Fraction)
    assert isinstance(ClipSteps(fps=FLOAT_FPS).fps, Fraction)


@pytest.mark.parametrize(
    "spec, needs_anchor",
    [
        (TopicSpec(steps.before(1, fps=5)), True),
        (TopicSpec(steps.before(1, fps=5, at="clip_end")), False),
        (TopicSpec(steps.last(1, at="clip_start")), False),
        (TopicSpec(steps.every()), False),
        (TopicSpec(steps.across(8)), False),
        (TopicSpec(steps.at("anchor")), True),
        (TopicSpec(steps.at("clip_end")), False),
    ],
)
def test_topic_spec_needs_anchor(spec, needs_anchor):
    """A spec needs the sample's anchor only when it measures from it."""
    assert spec.needs_anchor == needs_anchor


def test_topic_spec_defaults():
    """A spec matches time steps to the nearest message and pads at the edge
    unless told otherwise."""
    spec = TopicSpec(steps.before(1, fps=5))

    assert (spec.match, spec.pad) == (None, "edge")
    assert TopicSpec(steps.before(1, fps=5), match.nearest()).match == match.nearest()
    assert TopicSpec(steps.before(1, fps=5), match.backward()).match == Backward()

    assert TopicSpec(steps.before(1, fps=5), match.all_in_step()).match == AllInStep()


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
        (lambda: TopicSpec(steps.next(2), match.backward()), "take no match"),
        (lambda: TopicSpec(steps.every(before_s=1), match.nearest()), "take no match"),
        (lambda: TopicSpec(steps.every(), match.nearest()), "take no match"),
        (lambda: TopicSpec(steps.last(2), match.all_in_step()), "take no match"),
        (
            lambda: TopicSpec(steps.last(2), pad=UNKNOWN_PAD),
            "pad must be one of",
        ),
        (
            lambda: TopicSpec(steps.across(4), match.all_in_step()),
            "needs steps.before or steps.after",
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
