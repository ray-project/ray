"""Where a topic's steps fall around a sample's anchor, for ``TopicSpec``.

``before`` and ``after`` place steps at fixed times, ``1/fps`` seconds apart,
which ``match`` fills. ``last``, ``next`` and ``every`` take the topic's
recorded messages themselves as the steps.
"""

from fractions import Fraction
from typing import Optional, Union

from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import (
    MessageSteps,
    TimeSteps,
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
) -> TimeSteps:
    """Steps ``1/fps`` seconds apart up to the anchor, the last one on it.

    Args:
        seconds: The window's length, a whole number of steps at ``fps``.
        frames: The window's length as a number of steps, in place of
            ``seconds``.
        fps: Steps per second. Accepts a ``fractions.Fraction``, such as
            ``Fraction(30000, 1001)``, and reads a ``float`` as the decimal it
            prints as.

    Returns:
        The steps, for ``TopicSpec(steps=...)``.

    Examples:
        >>> from ray.data.datasource.mcap import steps
        >>> steps.before(1, fps=10).count
        10
    """
    return time_steps("before", seconds, frames, fps)


@PublicAPI(stability="alpha")
def after(
    seconds: Optional[Number] = None,
    *,
    frames: Optional[int] = None,
    fps: Number,
) -> TimeSteps:
    """Steps ``1/fps`` seconds apart after the anchor, the first one step after it.

    Args:
        seconds: The window's length, a whole number of steps at ``fps``.
        frames: The window's length as a number of steps, in place of
            ``seconds``.
        fps: Steps per second, as for :func:`before`.

    Returns:
        The steps, for ``TopicSpec(steps=...)``.

    Examples:
        >>> from ray.data.datasource.mcap import steps
        >>> steps.after(2, fps=2).count
        4
    """
    return time_steps("after", seconds, frames, fps)


@PublicAPI(stability="alpha")
def last(n: int) -> MessageSteps:
    """The last ``n`` messages logged up to and including the anchor, one step
    each, at whatever rate they were logged."""
    return message_count_steps("before", n)


@PublicAPI(stability="alpha")
def next(n: int) -> MessageSteps:  # noqa: A001 - mirrors last(n)
    """The first ``n`` messages logged after the anchor, one step each."""
    return message_count_steps("after", n)


@PublicAPI(stability="alpha")
def every(
    *, before_s: Optional[Number] = None, after_s: Optional[Number] = None
) -> MessageSteps:
    """Every message logged within ``before_s`` seconds up to the anchor, or
    within ``after_s`` seconds after it, one step each.

    The number of steps varies from row to row.
    """
    return message_span_steps(before_s, after_s)
