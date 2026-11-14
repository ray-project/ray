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
    Nearest,
    TimeSteps,
    TopicSpec,
)
from ray.data.datasource.mcap import match, steps

SECOND = 1_000_000_000
# Values of the wrong type, which the built-ins and TopicSpec must refuse.
NOT_STEPS: Any = "before"
NOT_A_MATCH: Any = "nearest"
UNKNOWN_PAD: Any = "repeat"
UNKNOWN_POINT: Any = "middle"
NOT_A_SIDE: Any = "sideways"
# A rate given as a float, as a caller building a step class directly might.
FLOAT_FPS: Any = 10.0
# A count read from a numpy array, which the built-ins take as a plain integer.
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
        (lambda: steps.across(fps=29.97), ClipSteps(fps=Fraction(2997, 100))),
        (lambda: steps.at("clip_end"), ClipSteps(point="clip_end")),
        # ``within`` reaches past the clip, on every built-in, so a window that
        # no clip can fill builds with it.
        (
            lambda: steps.after(1, fps=10, at="clip_end", within="recording"),
            TimeSteps("after", 10, Fraction(10), "clip_end", "recording"),
        ),
        (
            lambda: steps.last(2, at="clip_start", within="recording"),
            MessageSteps("before", count=2, at="clip_start", within="recording"),
        ),
        (
            lambda: steps.every(before_s=1, within="recording"),
            MessageSteps("before", span_ns=SECOND, within="recording"),
        ),
        # One step on the clip's first nanosecond lies inside the clip.
        (
            lambda: steps.before(frames=1, fps=10, at="clip_start"),
            TimeSteps("before", 1, Fraction(10), "clip_start"),
        ),
        (
            lambda: steps.last(1, at="clip_start"),
            MessageSteps("before", count=1, at="clip_start"),
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
        (lambda: steps.before(-0.5, fps=10), "positive"),
        (lambda: steps.after(float("nan"), fps=10), "finite"),
        # 2/3 as a float reads as 0.6666666666666666, which is just short of
        # 20 steps at 30 fps; the message gives the exact count.
        (
            lambda: steps.before(2 / 3, fps=30),
            r"is 19\.999999999999998 steps.*frames=",
        ),
        (lambda: steps.after(1, fps=Fraction(30000, 1001)), "is 30000/1001 steps"),
        (lambda: steps.last(0), "positive integer"),
        # ``True`` is an ``int`` in Python, but not a count.
        (lambda: steps.before(frames=True, fps=10), "positive integer"),
        (lambda: steps.every(before_s=1, after_s=1), "not both"),
        (lambda: steps.every(at="clip_end"), "takes no at= or within="),
        (lambda: steps.before(1, fps=10, at=UNKNOWN_POINT), "at must be one of"),
        (lambda: steps.across(4, fps=2), "exactly one of them"),
        (lambda: steps.across(fps=0), "positive"),
        (lambda: steps.at(UNKNOWN_POINT), "point must be one of"),
        (lambda: steps.every(before_s=1e-12), "at least one nanosecond"),
        (lambda: steps.before(1, fps=5, within="file"), "within must be one of"),
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
        lambda: steps.before(frames=3, fps=10, at="clip_start"),
        lambda: steps.last(2, at="clip_start"),
        lambda: steps.every(before_s=1, at="clip_start"),
        lambda: steps.every(after_s=1, at="clip_end"),
    ],
)
def test_steps_no_clip_can_fill_are_refused(build):
    """An after-window from the clip's end, or a before-window of several steps
    or of a span up to its start, lies outside every clip but its first
    nanosecond, so it is refused unless it may read past the clip."""
    with pytest.raises(ValueError, match="within='recording'"):
        build()


def test_counts_accept_numpy_integers():
    """A count may be any integer type, such as a numpy integer read from an
    array, and is held as a plain ``int``."""
    built = steps.last(NUMPY_FOUR)

    assert built == MessageSteps("before", count=4)
    assert type(built.count) is int


@pytest.mark.parametrize(
    "build, error",
    [
        (lambda: TimeSteps(NOT_A_SIDE, 3, Fraction(10)), "side must be one of"),
        (
            lambda: MessageSteps("before", count=3, span_ns=5),
            "count or span_ns, not both",
        ),
        (lambda: ClipSteps(count=4, fps=Fraction(10)), "exactly one of"),
    ],
)
def test_steps_built_directly_are_checked(build, error):
    """The step classes refuse fields that no built-in passes: an unknown side,
    or two fields of which a class takes one."""
    with pytest.raises(ValueError, match=error):
        build()


def test_steps_built_directly_equal_the_built_ins():
    """A step class built with a float rate holds it as the exact fraction a
    built-in gives."""
    direct = TimeSteps("before", 3, FLOAT_FPS)

    assert direct == steps.before(frames=3, fps=10)
    assert isinstance(direct.fps, Fraction)


@pytest.mark.parametrize(
    "spec, needs_anchor",
    [
        (TopicSpec(steps.before(1, fps=5)), True),
        (TopicSpec(steps.before(1, fps=5, at="clip_end")), False),
        (TopicSpec(steps.last(1)), True),
        (TopicSpec(steps.every()), False),
        (TopicSpec(steps.at("anchor")), True),
        (TopicSpec(steps.at("clip_end")), False),
    ],
)
def test_topic_spec_needs_anchor(spec, needs_anchor):
    """A spec needs the sample's anchor only when it measures from it."""
    assert spec.needs_anchor == needs_anchor


def test_topic_spec_defaults():
    """A spec matches time steps to the nearest message and pads at the edge
    unless told otherwise, and each match built-in gives its class."""
    spec = TopicSpec(steps.before(1, fps=5))

    assert (spec.match, spec.pad) == (None, "edge")
    assert (match.nearest(), match.backward(), match.all_in_step()) == (
        Nearest(),
        Backward(),
        AllInStep(),
    )


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
