"""``TopicSpec``: how ``read_mcap(samples=...)`` reads one topic of a sample.

A spec has two slots. ``steps`` says where the topic's steps fall, from the
sample row alone. ``match`` says which message fills each step. ``pad`` says
what a step that no message fills holds. ``ray.data.datasource.mcap.steps`` and
``ray.data.datasource.mcap.match`` export the built-ins below::

    TopicSpec(steps.before(1, fps=10))                time steps, nearest message
    TopicSpec(steps.after(2, fps=2), pad="zeros")     time steps after the anchor
    TopicSpec(steps.last(4))                          the last 4 messages
    TopicSpec(steps.every(before_s=1))                every message of the last 1 s

Time steps (``before``, ``after``) lie ``1/fps`` seconds apart and are filled by
``match``. Message steps (``last``, ``next``, ``every``) are recorded messages
themselves, so they take no ``match``. ``mcap_samples`` resolves the steps.
"""

import math
import numbers
from dataclasses import dataclass
from fractions import Fraction
from typing import Literal, Optional, Tuple, Union

from ray.util.annotations import PublicAPI

_NS_PER_S = 1_000_000_000

Side = Literal["before", "after"]
# How ``read_mcap(samples=...)`` fills a step that no message fills.
PAD_MODES: Tuple[str, ...] = ("edge", "zeros", "raise")


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
    """Steps ``1/fps`` seconds apart on one side of the anchor.

    Build it with ``steps.before`` or ``steps.after``. A before-window's last
    step is the anchor; an after-window's first step is one step after it.

    Attributes:
        side: ``"before"`` or ``"after"`` the anchor.
        count: The number of steps.
        fps: Steps per second, as an exact fraction.
    """

    side: Side
    count: int
    fps: Fraction


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class MessageSteps:
    """Steps that are the topic's recorded messages next to the anchor.

    Build it with ``steps.last``, ``steps.next`` or ``steps.every``. Exactly one
    of ``count`` and ``span_ns`` is set.

    Attributes:
        side: ``"before"`` (up to and including the anchor) or ``"after"`` it.
        count: That many messages, or ``None``.
        span_ns: Every message within this many nanoseconds, or ``None``. The
            number of steps then varies from row to row.
    """

    side: Side
    count: Optional[int] = None
    span_ns: Optional[int] = None


Steps = Union[TimeSteps, MessageSteps]


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class Nearest:
    """Each step takes the message logged nearest to it, the earlier on a tie.

    A step before the anchor takes only messages logged up to the anchor, and a
    step after it only messages logged after it, so no step looks past the
    anchor from its side. Build it with ``match.nearest``.
    """


Match = Nearest


def time_steps(
    side: Side,
    seconds: Optional[Union[float, Fraction]],
    frames: Optional[int],
    fps: Union[float, Fraction],
) -> TimeSteps:
    """The ``TimeSteps`` that ``steps.before`` and ``steps.after`` build."""
    name = "steps.before" if side == "before" else "steps.after"
    if (seconds is None) == (frames is None):
        raise ValueError(f"{name} takes seconds or frames=, exactly one of them")
    rate = exact_number(fps, "fps")
    if rate <= 0:
        raise ValueError(f"fps must be a positive number, got {fps!r}")
    if rate > _NS_PER_S:
        raise ValueError(
            f"fps must leave at least one nanosecond per step, got {fps!r}"
        )
    if frames is not None:
        return TimeSteps(side, _positive_count(frames, "frames"), rate)
    assert seconds is not None
    count = _positive_seconds(seconds, "seconds") * rate
    if count.denominator != 1:
        raise ValueError(
            f"{name}({seconds!r}, fps={fps!r}) is {float(count):g} steps. Give a "
            "whole number of steps, or a step count with frames=."
        )
    return TimeSteps(side, int(count), rate)


def message_count_steps(side: Side, count: int) -> MessageSteps:
    """The ``MessageSteps`` that ``steps.last`` and ``steps.next`` build."""
    return MessageSteps(side, count=_positive_count(count, "n"))


def message_span_steps(
    before_s: Optional[Union[float, Fraction]],
    after_s: Optional[Union[float, Fraction]],
) -> MessageSteps:
    """The ``MessageSteps`` that ``steps.every`` builds."""
    if (before_s is None) == (after_s is None):
        raise ValueError("steps.every takes before_s= or after_s=, exactly one of them")
    side: Side = "before" if before_s is not None else "after"
    seconds = _positive_seconds(
        before_s if before_s is not None else after_s,  # type: ignore[arg-type]
        "before_s" if before_s is not None else "after_s",
    )
    return MessageSteps(side, span_ns=round(seconds * _NS_PER_S))


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class TopicSpec:
    """How ``read_mcap(samples=...)`` reads one topic of each sample.

    Args:
        steps: Where the topic's steps fall: one of the built-ins of
            ``ray.data.datasource.mcap.steps``.
        match: Which message fills each time step: a built-in of
            ``ray.data.datasource.mcap.match``. Defaults to ``match.nearest()``.
            Message steps (``steps.last``, ``steps.next``, ``steps.every``) are
            messages already and refuse a ``match``.
        pad: What a step that no message fills holds, such as one past the
            clip's edge. ``"edge"`` repeats the nearest real step, or the clip's
            first or last message on the topic when the window has none.
            ``"zeros"`` gives an empty payload. ``"raise"`` fails the read,
            naming the sample. ``valid:<topic>`` marks a padded step ``False``.
            A window of every message has no fixed steps to pad: ``"raise"``
            fails the read when it reaches past the clip, and the other two cut
            it there.
    """

    steps: Steps
    match: Optional[Match] = None
    pad: Literal["edge", "zeros", "raise"] = "edge"

    def __post_init__(self):
        if not isinstance(self.steps, (TimeSteps, MessageSteps)):
            raise ValueError(
                "TopicSpec needs steps from ray.data.datasource.mcap.steps, got "
                f"{self.steps!r}"
            )
        if self.match is not None and not isinstance(self.match, Nearest):
            raise ValueError(
                "TopicSpec needs match from ray.data.datasource.mcap.match, got "
                f"{self.match!r}"
            )
        if self.match is not None and isinstance(self.steps, MessageSteps):
            raise ValueError(
                "steps.last, steps.next and steps.every take recorded messages "
                "as their steps, so they take no match"
            )
        if self.pad not in PAD_MODES:
            raise ValueError(f"pad must be one of {list(PAD_MODES)}, got {self.pad!r}")
