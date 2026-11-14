"""Where a topic's steps fall in a sample, for ``TopicSpec``.

``before`` and ``after`` place steps at fixed times, ``1/fps`` seconds apart,
which ``match`` fills. ``last``, ``next`` and ``every`` take the topic's
recorded messages themselves as the steps. Each measures from a reference point,
``at=``: the sample's anchor (the default), ``"clip_start"`` or ``"clip_end"``.
``across`` and ``at`` place steps inside the clip itself, so of them only
``at("anchor")`` needs the sample's anchor.
"""

from fractions import Fraction
from typing import Optional, Union

from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import (
    ClipSteps,
    MessageSteps,
    TimeSteps,
    clip_step_at,
    clip_steps_across,
    message_count_steps,
    message_span_steps,
    time_steps,
)
from ray.util.annotations import PublicAPI

Number = Union[float, Fraction]


@PublicAPI(stability="alpha")
def before(
    seconds: Optional[Number] = None,
    *,
    frames: Optional[int] = None,
    fps: Number,
    at: str = "anchor",
    within: str = "clip",
) -> TimeSteps:
    """Steps ``1/fps`` seconds apart up to a reference point, the last one on it.

    Args:
        seconds: The window's length, a whole number of steps at ``fps``.
        frames: The window's length as a number of steps, in place of
            ``seconds``.
        fps: Steps per second. Accepts a ``fractions.Fraction``, such as
            ``Fraction(30000, 1001)``, and reads a ``float`` as the decimal it
            prints as.
        at: The reference point: ``"anchor"``, ``"clip_start"`` or
            ``"clip_end"`` (the clip's last nanosecond). Steps that fall outside
            the clip are padded as ``TopicSpec.pad`` says. No clip holds a
            step after ``"clip_end"``, or more than one step up to
            ``"clip_start"``, so such windows need ``within="recording"``.
        within: ``"clip"`` keeps the steps and their messages inside the
            sample's clip. ``"recording"`` lets them reach past it, up to the
            recording's edges.

    Returns:
        The steps, for ``TopicSpec(steps=...)``.

    Examples:
        >>> from ray.data.datasource.mcap import steps
        >>> steps.before(1, fps=10).count
        10
    """
    return time_steps("before", seconds, frames, fps, at, within)


@PublicAPI(stability="alpha")
def after(
    seconds: Optional[Number] = None,
    *,
    frames: Optional[int] = None,
    fps: Number,
    at: str = "anchor",
    within: str = "clip",
) -> TimeSteps:
    """Steps ``1/fps`` seconds apart after a reference point, the first one step
    after it.

    Args:
        seconds: The window's length, a whole number of steps at ``fps``.
        frames: The window's length as a number of steps, in place of
            ``seconds``.
        fps: Steps per second, as for :func:`before`.
        at: The reference point, as for :func:`before`.
        within: How far the steps may reach, as for :func:`before`.

    Returns:
        The steps, for ``TopicSpec(steps=...)``.

    Examples:
        >>> from ray.data.datasource.mcap import steps
        >>> steps.after(2, fps=2).count
        4
    """
    return time_steps("after", seconds, frames, fps, at, within)


@PublicAPI(stability="alpha")
def last(n: int, *, at: str = "anchor", within: str = "clip") -> MessageSteps:
    """The last ``n`` messages logged up to and including a reference point, one
    step each, at whatever rate they were logged.

    Args:
        n: The number of messages, and so of steps.
        at: The reference point, as for :func:`before`.
        within: ``"clip"`` takes the messages from inside the sample's clip.
            ``"recording"`` lets them reach past it, up to the recording's
            edges.

    Returns:
        The steps, for ``TopicSpec(steps=...)``.
    """
    return message_count_steps("before", n, at, within)


@PublicAPI(stability="alpha")
def next(  # noqa: A001 - mirrors last(n)
    n: int, *, at: str = "anchor", within: str = "clip"
) -> MessageSteps:
    """The first ``n`` messages logged after a reference point, one step each.

    Args:
        n: The number of messages, and so of steps.
        at: The reference point, as for :func:`before`.
        within: Where the messages may come from, as for :func:`last`.

    Returns:
        The steps, for ``TopicSpec(steps=...)``.
    """
    return message_count_steps("after", n, at, within)


@PublicAPI(stability="alpha")
def every(
    *,
    before_s: Optional[Number] = None,
    after_s: Optional[Number] = None,
    at: Optional[str] = None,
    within: str = "clip",
) -> MessageSteps:
    """Every message logged within a span around a reference point, one step
    each. The number of steps varies from row to row.

    ``steps.every()``, with neither ``before_s`` nor ``after_s``, takes every
    message of the clip and accepts no ``at`` or ``within``.

    Args:
        before_s: Seconds up to and including the reference point.
        after_s: Seconds after the reference point, in place of ``before_s``.
        at: The reference point, as for :func:`before`. Defaults to the
            anchor when ``before_s`` or ``after_s`` is given. No clip holds a
            message after ``"clip_end"``, and a span up to ``"clip_start"``
            holds that nanosecond only, so such spans need
            ``within="recording"``.
        within: Where the span is cut, as for :func:`last`.

    Returns:
        The steps, for ``TopicSpec(steps=...)``.
    """
    return message_span_steps(before_s, after_s, at, within)


@PublicAPI(stability="alpha")
def across(n: Optional[int] = None, *, fps: Optional[Number] = None) -> ClipSteps:
    """Steps over the whole clip, filled by ``match`` from the clip's messages.

    Clip steps always read inside the clip, so they take no ``within``.

    Args:
        n: That many steps, evenly spaced from the clip's first nanosecond to
            its last. Every row then has ``n`` steps, whatever its clip's
            length. One step sits on the clip's first nanosecond.
        fps: In place of ``n``, one step every ``1/fps`` seconds from the
            clip's first nanosecond, so the number of steps grows with the
            clip.

    Returns:
        The steps, for ``TopicSpec(steps=...)``.

    Examples:
        >>> from ray.data.datasource.mcap import steps
        >>> steps.across(8).count
        8
    """
    return clip_steps_across(n, fps)


@PublicAPI(stability="alpha")
def at(point: str) -> ClipSteps:
    """One step at a point of the clip, filled by ``match`` from the clip's
    messages.

    Args:
        point: ``"anchor"``, ``"clip_start"`` or ``"clip_end"`` (the clip's
            last nanosecond).

    Returns:
        The steps, for ``TopicSpec(steps=...)``.
    """
    return clip_step_at(point)
