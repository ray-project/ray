"""``TopicSpec``: how ``read_mcap(samples=...)`` reads one topic of a sample.

A spec has two slots. ``steps`` says where the topic's steps fall, from the
sample row alone. ``match`` says which message fills each step. ``pad`` says
what a step that no message fills holds. ``ray.data.datasource.mcap.steps`` and
``ray.data.datasource.mcap.match`` export the built-ins below::

    TopicSpec(steps.before(1, fps=10))                time steps, nearest message
    TopicSpec(steps.before(1, fps=5), match.backward())
                                                      the last value at each step
    TopicSpec(steps.after(2, fps=2), pad="zeros")     time steps after the anchor
    TopicSpec(steps.last(4))                          the last 4 messages
    TopicSpec(steps.every(before_s=1))                every message of the last 1 s
    TopicSpec(steps.across(8))                        8 steps across the whole clip
    TopicSpec(steps.at("clip_end"))                   one step at the clip's end
    TopicSpec(steps.before(0.5, fps=10), match.all_in_step())
                                                      every message of each step

Time steps (``before``, ``after``) lie ``1/fps`` seconds apart and are filled by
``match``. Message steps (``last``, ``next``, ``every``) are recorded messages
themselves, so they take no ``match``. Each measures from a reference point
(``at=``): the sample's anchor, or the clip's first or last nanosecond. Clip
steps (``across``, ``at``) lie inside the clip and are filled by ``match``.
Every other built-in reads inside the sample's clip, or, with
``within="recording"``, up to the recording's edges.
``mcap_samples`` resolves the steps.
"""

import math
import numbers
from dataclasses import dataclass
from fractions import Fraction
from typing import Literal, Optional, Tuple, Union, cast

from ray.util.annotations import PublicAPI

_NS_PER_S = 1_000_000_000

Side = Literal["before", "after"]
# Where a window measures from: the sample's anchor, or the clip's first or
# last nanosecond (``clip_end`` is exclusive, so ``"clip_end"`` is its last).
Point = Literal["anchor", "clip_start", "clip_end"]
POINTS: Tuple[str, ...] = ("anchor", "clip_start", "clip_end")
Within = Literal["clip", "recording"]
# How ``read_mcap(samples=...)`` fills a step that no message fills.
PAD_MODES: Tuple[str, ...] = ("edge", "zeros", "raise")
# How far a topic's steps may reach: inside the sample's clip, or up to the
# recording's edges.
WITHIN: Tuple[str, ...] = ("clip", "recording")


def exact_number(value: Union[float, Fraction], name: str) -> Fraction:
    """``value`` as a fraction. A float is read as the decimal it prints as."""
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise ValueError(
            f"{name} must be a positive number or a Fraction, got {value!r}"
        )
    if isinstance(value, numbers.Rational):
        return Fraction(value.numerator, value.denominator)
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite, got {value!r}")
    return Fraction(str(value))


def _positive_count(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer, got {value!r}")
    return value


def _point(value: str, name: str) -> Point:
    if value not in POINTS:
        raise ValueError(f"{name} must be one of {list(POINTS)}, got {value!r}")
    return value  # type: ignore[return-value]


def _rate(fps: Union[float, Fraction]) -> Fraction:
    rate = exact_number(fps, "fps")
    if rate <= 0:
        raise ValueError(f"fps must be a positive number, got {fps!r}")
    if rate > _NS_PER_S:
        raise ValueError(
            f"fps must leave at least one nanosecond per step, got {fps!r}"
        )
    return rate


def _within(value: str) -> Within:
    if value not in WITHIN:
        raise ValueError(f"within must be one of {list(WITHIN)}, got {value!r}")
    return cast(Within, value)


def _positive_seconds(value: Union[float, Fraction], name: str) -> Fraction:
    seconds = exact_number(value, name)
    if seconds <= 0:
        raise ValueError(f"{name} must be positive, got {value!r}")
    if round(seconds * _NS_PER_S) < 1:
        raise ValueError(f"{name} must be at least one nanosecond, got {value!r}")
    return seconds


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class TimeSteps:
    """Steps ``1/fps`` seconds apart on one side of a reference point.

    Build it with ``steps.before`` or ``steps.after``. A before-window's last
    step is the reference point; an after-window's first step is one step after
    it.

    Attributes:
        side: ``"before"`` or ``"after"`` the reference point.
        count: The number of steps.
        fps: Steps per second, as an exact fraction.
        at: The reference point: ``"anchor"``, ``"clip_start"`` or
            ``"clip_end"``.
        within: ``"clip"`` keeps the steps and their messages inside the
            sample's clip; ``"recording"`` lets them reach past it.
    """

    side: Side
    count: int
    fps: Fraction
    at: Point = "anchor"
    within: Within = "clip"


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class MessageSteps:
    """Steps that are the topic's recorded messages.

    Build it with ``steps.last``, ``steps.next`` or ``steps.every``. With
    ``count`` or ``span_ns``, the messages lie on one side of a reference
    point. With neither, they are every message of the clip.

    Attributes:
        side: ``"before"`` (up to and including the reference point) or
            ``"after"`` it. Ignored for every message of the clip.
        count: That many messages, or ``None``.
        span_ns: Every message within this many nanoseconds, or ``None``. The
            number of steps then varies from row to row.
        at: The reference point: ``"anchor"``, ``"clip_start"`` or
            ``"clip_end"``.
        within: ``"clip"`` takes messages inside the sample's clip only;
            ``"recording"`` takes them up to the recording's edges.
    """

    side: Side
    count: Optional[int] = None
    span_ns: Optional[int] = None
    at: Point = "anchor"
    within: Within = "clip"

    @property
    def whole_clip(self) -> bool:
        """Whether the steps are every message of the clip."""
        return self.count is None and self.span_ns is None


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class ClipSteps:
    """Steps placed inside the clip rather than around the anchor.

    Build it with ``steps.across`` or ``steps.at``. Exactly one of ``count``,
    ``fps`` and ``point`` is set. Each step is filled by ``match`` from the
    clip's messages, on either side of it.

    Attributes:
        count: That many steps, evenly spaced from the clip's first nanosecond
            to its last, or ``None``.
        fps: One step every ``1/fps`` seconds from the clip's first nanosecond,
            or ``None``. The number of steps then varies with the clip.
        point: One step at this point, or ``None``.
    """

    count: Optional[int] = None
    fps: Optional[Fraction] = None
    point: Optional[Point] = None


Steps = Union[TimeSteps, MessageSteps, ClipSteps]


def measures_from_anchor(steps: Steps) -> bool:
    """Whether ``steps`` needs the sample's anchor."""
    if isinstance(steps, ClipSteps):
        return steps.point == "anchor"
    if isinstance(steps, MessageSteps) and steps.whole_clip:
        return False
    return steps.at == "anchor"


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class Nearest:
    """Each step takes the message logged nearest to it, the earlier on a tie.

    A step before the reference point takes only messages logged up to it, and
    a step after it only messages logged after it, so no step looks past the
    reference point from its side. A clip step takes the nearest message
    inside the clip. Build it with ``match.nearest``.
    """


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class Backward:
    """Each step takes the last message logged at or before it.

    As pandas' ``merge_asof(direction="backward")``, no step sees a message
    logged after its own time. A step after the anchor takes only messages
    logged after the anchor, so it never reaches back across it. Build it with
    ``match.backward``.
    """


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class AllInStep:
    """Each time step takes every message logged in its slot.

    A step's slot runs from just after the previous step up to and including
    the step; the first step's slot reaches one step period back. A row then
    holds a list of messages per step. Build it with ``match.all_in_step``.
    """


Match = Union[Nearest, Backward, AllInStep]


def time_steps(
    side: Side,
    seconds: Optional[Union[float, Fraction]],
    frames: Optional[int],
    fps: Union[float, Fraction],
    at: str = "anchor",
    within: str = "clip",
) -> TimeSteps:
    """The ``TimeSteps`` that ``steps.before`` and ``steps.after`` build."""
    name = "steps.before" if side == "before" else "steps.after"
    if (seconds is None) == (frames is None):
        raise ValueError(f"{name} takes seconds or frames=, exactly one of them")
    rate = _rate(fps)
    point = _point(at, "at")
    bound = _within(within)
    if frames is not None:
        return TimeSteps(side, _positive_count(frames, "frames"), rate, point, bound)
    assert seconds is not None
    count = _positive_seconds(seconds, "seconds") * rate
    if count.denominator != 1:
        raise ValueError(
            f"{name}({seconds!r}, fps={fps!r}) is {float(count):g} steps. Give a "
            "whole number of steps, or a step count with frames=."
        )
    return TimeSteps(side, int(count), rate, point, bound)


def message_count_steps(
    side: Side, count: int, at: str = "anchor", within: str = "clip"
) -> MessageSteps:
    """The ``MessageSteps`` that ``steps.last`` and ``steps.next`` build."""
    return MessageSteps(
        side,
        count=_positive_count(count, "n"),
        at=_point(at, "at"),
        within=_within(within),
    )


def message_span_steps(
    before_s: Optional[Union[float, Fraction]],
    after_s: Optional[Union[float, Fraction]],
    at: Optional[str] = None,
    within: str = "clip",
) -> MessageSteps:
    """The ``MessageSteps`` that ``steps.every`` builds.

    With neither ``before_s`` nor ``after_s``, every message of the clip, which
    measures from no point and so takes no ``at``.
    """
    if before_s is not None and after_s is not None:
        raise ValueError("steps.every takes before_s= or after_s=, not both")
    if before_s is None and after_s is None:
        if at is not None or within != "clip":
            raise ValueError(
                "steps.every() takes every message of the clip, so it takes no "
                "at= or within=; give before_s= or after_s= to measure from a "
                "point"
            )
        return MessageSteps("before")
    side: Side = "before" if before_s is not None else "after"
    seconds = _positive_seconds(
        before_s if before_s is not None else after_s,  # type: ignore[arg-type]
        "before_s" if before_s is not None else "after_s",
    )
    return MessageSteps(
        side,
        span_ns=round(seconds * _NS_PER_S),
        at=_point(at or "anchor", "at"),
        within=_within(within),
    )


def clip_steps_across(
    n: Optional[int], fps: Optional[Union[float, Fraction]]
) -> ClipSteps:
    """The ``ClipSteps`` that ``steps.across`` builds."""
    if (n is None) == (fps is None):
        raise ValueError("steps.across takes n or fps=, exactly one of them")
    if n is not None:
        return ClipSteps(count=_positive_count(n, "n"))
    assert fps is not None
    return ClipSteps(fps=_rate(fps))


def clip_step_at(point: str) -> ClipSteps:
    """The ``ClipSteps`` that ``steps.at`` builds."""
    return ClipSteps(point=_point(point, "point"))


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class TopicSpec:
    """How ``read_mcap(samples=...)`` reads one topic of each sample.

    Args:
        steps: Where the topic's steps fall: one of the built-ins of
            ``ray.data.datasource.mcap.steps``.
        match: Which messages fill each time step: a built-in of
            ``ray.data.datasource.mcap.match``. Defaults to ``match.nearest()``;
            ``match.backward()`` takes the last message at or before each
            step, and ``match.all_in_step()`` every message of each step's
            slot, for time steps (``steps.before``, ``steps.after``) only.
            Message steps (``steps.last``, ``steps.next``, ``steps.every``) are
            messages already and refuse a ``match``.
        pad: What a step that no message fills holds, such as one past the
            clip's edge. ``"edge"`` repeats the nearest real step, or the clip's
            first or last message on the topic when the window has none.
            ``"zeros"`` gives an empty payload. ``"raise"`` fails the read,
            naming the sample. ``valid:<topic>`` marks a padded step ``False``.
            A window of every message has no fixed steps to pad: ``"raise"``
            fails the read when it reaches past the clip, and the other two cut
            it there. With ``within="recording"``, the recording's edges take
            the clip's place throughout.
    """

    steps: Steps
    match: Optional[Match] = None
    pad: Literal["edge", "zeros", "raise"] = "edge"

    @property
    def needs_anchor(self) -> bool:
        """Whether the spec measures from the sample's anchor."""
        return measures_from_anchor(self.steps)

    def __post_init__(self):
        if not isinstance(self.steps, (TimeSteps, MessageSteps, ClipSteps)):
            raise ValueError(
                "TopicSpec needs steps from ray.data.datasource.mcap.steps, got "
                f"{self.steps!r}"
            )
        if self.match is not None and not isinstance(
            self.match, (Nearest, Backward, AllInStep)
        ):
            raise ValueError(
                "TopicSpec needs match from ray.data.datasource.mcap.match, got "
                f"{self.match!r}"
            )
        if self.match is not None and isinstance(self.steps, MessageSteps):
            raise ValueError(
                "steps.last, steps.next and steps.every take recorded messages "
                "as their steps, so they take no match"
            )
        if isinstance(self.match, AllInStep) and isinstance(self.steps, ClipSteps):
            raise ValueError(
                "match.all_in_step() takes the messages between consecutive time "
                "steps, so it needs steps.before or steps.after, not steps.across "
                "or steps.at"
            )
        if self.pad not in PAD_MODES:
            raise ValueError(f"pad must be one of {list(PAD_MODES)}, got {self.pad!r}")
