"""Training samples: which message fills each step of a sample row.

``read_mcap(samples=...)`` reads one row per sample. A sample names its file,
a clip ``[clip_start, clip_end)`` and, when a topic measures from it, an
``anchor`` inside the clip, in nanoseconds of log time. Each topic's
``TopicSpec`` places its steps before or after a reference point, or inside the
clip (``mcap_topic_spec``). This module turns the two into the message that
fills each step, and lays out a sample row's columns. It does no I/O:
``resolve_steps`` asks a ``TopicTimes`` for the topic's messages, and the
reader answers from the chunks it reads (``mcap_sample_rows``).

Time steps (``steps.before``, ``steps.after``) are fixed times counted from the
anchor. Each step takes the message logged nearest to it inside the clip, the
earlier one on a tie (``match.nearest``). A before-window takes only messages
logged up to the anchor, and an after-window only messages logged after it, so
no step looks past the anchor from its side (m is a message)::

    clip                        [---------------------------------------------)
    messages                     m   m   m   m   m   m   m   m   m   m   m   m
    anchor                                                 A
    steps.before(0.3, fps=10)                      s0  s1  s2
    steps.after(0.2, fps=10)                                   s0  s1

A window measured from the clip's first or last nanosecond (``at=``) is
resolved the same way, with that point in the anchor's place. Clip steps
(``steps.across``, ``steps.at``) take the nearest message anywhere in the clip.

With ``match.all_in_step``, each step takes every message of its slot instead,
from just after the previous step up to and including it::

    messages                     m m m m   m   m m m m m   m m m m m
    steps.before(0.3, fps=10)                      s0      s1      s2 = A
    slots                                   (------](------](------]

A step with no such message, such as one past the clip's edge, is padded:
``"edge"`` repeats the nearest real step, ``"zeros"`` leaves it empty, and
``"raise"`` fails the read.
"""

import bisect
from dataclasses import dataclass
from fractions import Fraction
from typing import (
    TYPE_CHECKING,
    Callable,
    Generic,
    List,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    TypeVar,
)

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import (
    AllInStep,
    Backward,
    ClipSteps,
    TimeSteps,
    TopicSpec,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
)
from ray.data._internal.tensor_extensions.arrow import (
    ArrowTensorTypeV2,
    ArrowVariableShapedTensorType,
)

if TYPE_CHECKING:
    import numpy as np

# The columns every samples dataset has. ``files`` lists a sample's recordings,
# one for now. The times are log times in nanoseconds; ``clip_end`` is
# exclusive. ``anchor`` lies inside the clip, and is needed only when a topic
# measures from it.
FILES_COLUMN = "files"
CLIP_START_COLUMN = "clip_start"
CLIP_END_COLUMN = "clip_end"
ANCHOR_COLUMN = "anchor"
SAMPLE_COLUMNS: Tuple[str, ...] = (FILES_COLUMN, CLIP_START_COLUMN, CLIP_END_COLUMN)
# An optional column whose value names a sample in error messages.
SAMPLE_ID_COLUMN = "sample_id"

# A sample row's columns per topic. A topic decoded with ``video`` has
# ``frames:`` in place of ``data:``.
DATA_PREFIX = "data:"
FRAMES_PREFIX = "frames:"
TIMES_PREFIX = "times:"
VALID_PREFIX = "valid:"
TOPIC_PREFIXES: Tuple[str, ...] = (
    DATA_PREFIX,
    FRAMES_PREFIX,
    TIMES_PREFIX,
    VALID_PREFIX,
)

_NS_PER_S = 1_000_000_000
# The latest log time an MCAP message can have: the edge of a recording that a
# topic read ``within="recording"`` may reach.
_MAX_LOG_TIME = 2**64 - 1

M = TypeVar("M")
M_co = TypeVar("M_co", covariant=True)


class TopicTimes(Protocol[M_co]):
    """One topic's messages in one recording, by log time.

    ``resolve_steps`` asks for the messages it needs through these methods.
    Each returns ``(log_time, message)`` pairs in log-time order, with messages
    of equal log time in the order they were recorded. Bounds are inclusive.
    """

    def last_in(self, low: int, high: int, count: int) -> List[Tuple[int, M_co]]:
        """The last ``count`` messages logged in ``[low, high]``, or all of them
        if there are fewer."""
        ...

    def first_in(self, low: int, high: int, count: int) -> List[Tuple[int, M_co]]:
        """The first ``count`` messages logged in ``[low, high]``, or all of
        them if there are fewer."""
        ...

    def all_in(self, low: int, high: int) -> List[Tuple[int, M_co]]:
        """Every message logged in ``[low, high]``."""
        ...


class SortedTimes(Generic[M]):
    """A ``TopicTimes`` over messages held in a list, sorted by log time."""

    def __init__(self, timed: Sequence[Tuple[int, M]]):
        self._timed = list(timed)
        self._times = [time for time, _ in self._timed]

    def _span(self, low: int, high: int) -> Tuple[int, int]:
        return (
            bisect.bisect_left(self._times, low),
            bisect.bisect_right(self._times, high),
        )

    def last_in(self, low: int, high: int, count: int) -> List[Tuple[int, M]]:
        start, end = self._span(low, high)
        return self._timed[max(start, end - count) : end]

    def first_in(self, low: int, high: int, count: int) -> List[Tuple[int, M]]:
        start, end = self._span(low, high)
        return self._timed[start : min(end, start + count)]

    def all_in(self, low: int, high: int) -> List[Tuple[int, M]]:
        start, end = self._span(low, high)
        return self._timed[start:end]


@dataclass(frozen=True)
class SampleTimes:
    """A sample's clip ``[clip_start, clip_end)`` and its anchor, in log-time ns.

    A sample without an anchor holds ``clip_start`` in its place. Only topics
    that do not measure from the anchor read it.
    """

    clip_start: int
    clip_end: int
    anchor: int

    @property
    def clip_last(self) -> int:
        """The clip's last nanosecond: ``clip_end`` is exclusive."""
        return self.clip_end - 1

    def point(self, name: str) -> int:
        """The log time of ``"anchor"``, ``"clip_start"`` or ``"clip_end"``,
        the last of which is the clip's last nanosecond."""
        if name == "clip_start":
            return self.clip_start
        if name == "clip_end":
            return self.clip_last
        return self.anchor


def sample_anchors(table: pa.Table) -> List[int]:
    """Each sample's anchor, or its ``clip_start`` when it has none."""
    starts = table.column(CLIP_START_COLUMN).to_pylist()
    if ANCHOR_COLUMN not in table.column_names:
        return list(starts)
    anchors = table.column(ANCHOR_COLUMN).to_pylist()
    return [start if a is None else a for start, a in zip(starts, anchors)]


@dataclass(frozen=True)
class TopicPlan:
    """One topic's ``TopicSpec`` in the terms its steps are resolved in.

    A window's steps lie on one side of a reference point, ``at``. Clip steps
    (``clip=True``) lie inside the clip instead: ``num_steps`` spread across
    it, one every ``1/fps``, one at ``point``, or, with none of these, every
    message of the clip.

    Attributes:
        topic: The topic name.
        before: Whether a window ends at its reference point rather than
            starting after it.
        pad: How a step with no message is filled (``TopicSpec.pad``).
        num_steps: The fixed number of steps, or ``None`` when it varies.
        fps: Steps per second of time steps, or ``None`` for message steps.
        span_ns: The horizon of ``steps.every``, in nanoseconds.
        at: A window's reference point: ``"anchor"``, ``"clip_start"`` or
            ``"clip_end"``.
        clip: Whether the steps lie inside the clip rather than around a point.
        point: The point of ``steps.at``, or ``None``.
        backward: Whether a time step takes the last message at or before it
            (``match.backward``) rather than the nearest.
        within_recording: Whether the steps may reach past the clip, up to the
            recording's edges (``within="recording"``).
        all_in_step: Whether each time step takes every message of its slot
            (``match.all_in_step``) rather than the nearest one.
    """

    topic: str
    before: bool
    pad: str
    num_steps: Optional[int]
    fps: Optional[Fraction] = None
    span_ns: Optional[int] = None
    at: str = "anchor"
    clip: bool = False
    point: Optional[str] = None
    backward: bool = False
    within_recording: bool = False
    all_in_step: bool = False

    @classmethod
    def from_spec(cls, topic: str, spec: TopicSpec) -> "TopicPlan":
        steps = spec.steps
        backward = isinstance(spec.match, Backward)
        if isinstance(steps, ClipSteps):
            count = 1 if steps.point is not None else steps.count
            return cls(
                topic,
                True,
                spec.pad,
                num_steps=count,
                fps=steps.fps,
                clip=True,
                point=steps.point,
                backward=backward,
            )
        if not isinstance(steps, TimeSteps) and steps.whole_clip:
            return cls(topic, True, spec.pad, num_steps=None, clip=True)
        common = dict(
            topic=topic,
            before=steps.side == "before",
            pad=spec.pad,
            at=steps.at,
            within_recording=steps.within == "recording",
        )
        if isinstance(steps, TimeSteps):
            return cls(
                num_steps=steps.count,
                fps=steps.fps,
                backward=backward,
                all_in_step=isinstance(spec.match, AllInStep),
                **common,
            )
        if steps.count is not None:
            return cls(num_steps=steps.count, **common)
        return cls(num_steps=None, span_ns=steps.span_ns, **common)

    @property
    def needs_anchor(self) -> bool:
        """Whether the steps measure from the sample's anchor."""
        if self.clip:
            return self.point == "anchor"
        return self.at == "anchor"

    def measured(self, sample: SampleTimes) -> SampleTimes:
        """``sample`` with this window's reference point in the anchor's place."""
        if self.clip or self.at == "anchor":
            return sample
        return SampleTimes(sample.clip_start, sample.clip_end, sample.point(self.at))

    def clip_step_times(self, sample: SampleTimes) -> List[int]:
        """The log time of each clip step, in order: ``num_steps`` evenly spaced
        from the clip's first nanosecond to its last, one every ``1/fps`` from
        its first, or the one ``point``."""
        if self.point is not None:
            return [sample.point(self.point)]
        start, last = sample.clip_start, sample.clip_last
        if self.fps is not None:
            steps = []
            k = 0
            while start + self.step_offset_ns(k) <= last:
                steps.append(start + self.step_offset_ns(k))
                k += 1
            return steps
        assert self.num_steps is not None
        if self.num_steps == 1:
            return [start]
        span = last - start
        return [
            start + round(Fraction(i * span, self.num_steps - 1))
            for i in range(self.num_steps)
        ]

    def bounds(self, sample: SampleTimes) -> Tuple[int, int]:
        """The first and last log time the topic may take a message from: the
        clip's, or the recording's with ``within="recording"``. Clip steps
        always stay in the clip."""
        if self.within_recording and not self.clip:
            return 0, _MAX_LOG_TIME
        return sample.clip_start, sample.clip_last

    def step_offset_ns(self, k: int) -> int:
        """How far step ``k`` lies from the anchor, rounded to the nanosecond."""
        assert self.fps is not None
        return round(Fraction(k * _NS_PER_S) / self.fps)

    def step_times(self, anchor: int) -> List[int]:
        """The log time of each step of a window with ``fps``, in order.

        A before-window's last step is the anchor. An after-window's first step
        is one step after it.
        """
        assert self.num_steps is not None and self.fps is not None
        n = self.num_steps
        if self.before:
            return [anchor - self.step_offset_ns(n - 1 - i) for i in range(n)]
        return [anchor + self.step_offset_ns(i + 1) for i in range(n)]

    def slot_bounds(self, anchor: int) -> List[int]:
        """The bounds of the slots of ``match.all_in_step``: step ``k``'s slot
        is ``(bounds[k], bounds[k + 1]]``.

        The first slot reaches one step period before the first step: for a
        before-window that is ``n`` steps before the anchor, and for an
        after-window the anchor itself, so no slot crosses the anchor.
        """
        assert self.num_steps is not None and self.fps is not None
        n = self.num_steps
        if self.before:
            return [anchor - self.step_offset_ns(n - i) for i in range(n + 1)]
        return [anchor + self.step_offset_ns(i) for i in range(n + 1)]

    def reach(self, sample: SampleTimes) -> Tuple[int, int]:
        """The span of log time within ``bounds`` that the window's steps fall in.

        For time steps it runs from the first step to the last, or from the
        first slot's start with ``match.all_in_step``, and for ``steps.every``
        over its horizon. ``steps.last`` and ``steps.next`` reach as far back
        or on as the recording needs, so only the reference point's side of
        them is known here. Clip steps reach the whole clip, and ``steps.at``
        its one point. The reader reads the chunks over this span first, and
        reads on as the steps need.
        """
        if self.clip:
            if self.point is not None:
                t = sample.point(self.point)
                return t, t
            return sample.clip_start, sample.clip_last
        sample = self.measured(sample)
        anchor = sample.anchor
        first, last = self.bounds(sample)
        if self.before:
            if self.fps is not None and self.all_in_step:
                low = self.slot_bounds(anchor)[0] + 1
            elif self.fps is not None:
                assert self.num_steps is not None
                low = anchor - self.step_offset_ns(self.num_steps - 1)
            elif self.span_ns is not None:
                low = anchor - self.span_ns + 1
            else:
                low = anchor
            return max(low, first), anchor
        if self.fps is not None:
            assert self.num_steps is not None
            high = anchor + self.step_offset_ns(self.num_steps)
        elif self.span_ns is not None:
            high = anchor + self.span_ns
        else:
            high = anchor + 1
        return min(anchor + 1, last), min(high, last)


@dataclass(frozen=True)
class Step(Generic[M]):
    """One step of a topic's window in a sample row.

    Attributes:
        message: What fills the step: the message the step takes, the message
            ``"edge"`` padding repeats, or ``None`` for an empty step.
        time: The log time of ``message``. An empty step has its own time with
            ``fps``, and otherwise the clip's edge on the window's side.
        valid: Whether the step took a message of its own.
    """

    message: Optional[M]
    time: int
    valid: bool


@dataclass(frozen=True)
class SlotStep(Generic[M]):
    """One step of ``match.all_in_step``: every message logged in its slot.

    Attributes:
        messages: The messages, in log-time order; for an empty slot, the ones
            ``"edge"`` padding repeats, or none.
        times: Their log times.
        valid: Whether the slot held a message of its own.
    """

    messages: List[M]
    times: List[int]
    valid: bool


@dataclass(frozen=True)
class _Slot(Generic[M]):
    """A step before padding: its time, and the message it took, if any.

    ``outside`` marks a step that lies outside the clip.
    """

    time: int
    taken: Optional[Tuple[int, M]]
    outside: bool = False


def resolve_steps(
    plan: TopicPlan, times: TopicTimes[M], sample: SampleTimes, where: str
) -> List[Step[M]]:
    """The steps of one topic's window for one sample, padded.

    ``where`` names the sample in the error that ``pad="raise"`` raises when a
    step has no message. ``steps.every`` has no fixed steps: it holds every
    message within its horizon and inside the clip.
    """
    if plan.clip:
        if plan.num_steps is None and plan.fps is None:
            all_in = times.all_in(sample.clip_start, sample.clip_last)
            return [Step(m, time, True) for time, m in all_in]
        slots = _clip_slots(plan, times, sample)
        return _padded(plan, slots, times, sample, where)
    sample = plan.measured(sample)
    if plan.num_steps is None:
        return _variable_steps(plan, times, sample, where)
    if plan.fps is not None:
        slots = _timed_slots(plan, times, sample)
    else:
        slots = _counted_slots(plan, times, sample)
    return _padded(plan, slots, times, sample, where)


def resolve_slots(
    plan: TopicPlan, times: TopicTimes[M], sample: SampleTimes, where: str
) -> List[SlotStep[M]]:
    """The steps of ``match.all_in_step`` for one sample, padded.

    Each step takes every message logged in its slot, within the topic's
    ``bounds``. An empty slot is padded as ``plan.pad`` says: ``"edge"``
    repeats the nearest slot that held messages, the earlier one on a tie, or
    the clip's edge message (``_clip_edge``) when none did. ``"zeros"`` leaves
    it empty, and ``"raise"`` fails the read naming the sample.
    """
    assert plan.all_in_step
    sample = plan.measured(sample)
    first_time, last_time = plan.bounds(sample)
    bounds = plan.slot_bounds(sample.anchor)
    filled: List[List[Tuple[int, M]]] = []
    for low, high in zip(bounds, bounds[1:]):
        low, high = max(low + 1, first_time), min(high, last_time)
        filled.append(times.all_in(low, high) if low <= high else [])
    real = [i for i, slot in enumerate(filled) if slot]
    if len(real) < len(filled) and plan.pad == "raise":
        raise ValueError(
            f"{where}: {len(filled) - len(real)} of {len(filled)} steps of topic "
            f"{plan.topic!r} hold no message in {_region(plan, sample)}, and its "
            "pad is 'raise'. Use pad='edge' or pad='zeros' to pad such steps."
        )
    edge: List[Tuple[int, M]] = []
    if not real and plan.pad == "edge":
        found = _clip_edge(plan, times, sample)
        edge = [found] if found is not None else []
    steps: List[SlotStep[M]] = []
    for i, slot in enumerate(filled):
        valid = bool(slot)
        if not valid and plan.pad == "edge":
            slot = filled[min(real, key=lambda j: (abs(j - i), j))] if real else edge
        steps.append(SlotStep([m for _, m in slot], [t for t, _ in slot], valid))
    return steps


def _timed_slots(
    plan: TopicPlan, times: TopicTimes[M], sample: SampleTimes
) -> List[_Slot[M]]:
    """The steps of a window with ``fps``, each with the message it takes.

    Each step within the topic's ``bounds`` takes the message logged nearest
    to it, the earlier one on a tie, from the window's side of the anchor. Log
    times jitter, so a step meant to fall on a frame lands just before or
    after it; the nearest message is still that frame. With ``backward``, a
    step takes the last message logged at or before it instead. The messages
    are asked for once: the last one at or before the first step, every one up
    to the last step, and, for nearest, the first one after it.
    """
    step_times = plan.step_times(sample.anchor)
    first_time, last_time = plan.bounds(sample)
    if plan.before:
        low, high = first_time, sample.anchor
    else:
        low, high = sample.anchor + 1, last_time
    return _matched_slots(
        step_times, low, high, (first_time, last_time), times, plan.backward
    )


def _clip_slots(
    plan: TopicPlan, times: TopicTimes[M], sample: SampleTimes
) -> List[_Slot[M]]:
    """The clip steps of ``steps.across`` or ``steps.at``, each filled as
    ``match`` says from anywhere in the clip."""
    step_times = plan.clip_step_times(sample)
    clip = (sample.clip_start, sample.clip_last)
    return _matched_slots(step_times, *clip, clip, times, plan.backward)


def _matched_slots(
    step_times: List[int],
    low: int,
    high: int,
    bounds: Tuple[int, int],
    times: TopicTimes[M],
    backward: bool,
) -> List[_Slot[M]]:
    """Each step with the message it takes from ``[low, high]``: the nearest,
    or with ``backward`` the last at or before it. A step outside ``bounds``
    takes none.
    """
    first_time, last_time = bounds
    inside = [t for t in step_times if first_time <= t <= last_time]
    timed: List[Tuple[int, M]] = []
    if inside:
        first, last = inside[0], inside[-1]
        timed = times.last_in(low, first, 1)
        if first < last:
            timed += times.all_in(first + 1, last)
        if last < high and not backward:
            timed += times.first_in(last + 1, high, 1)
    known = [time for time, _ in timed]
    pick = _backward if backward else _nearest
    slots: List[_Slot[M]] = []
    for t in step_times:
        if not first_time <= t <= last_time:
            slots.append(_Slot(t, None, outside=True))
        else:
            slots.append(_Slot(t, pick(timed, known, t)))
    return slots


def _nearest(
    timed: List[Tuple[int, M]], known: List[int], t: int
) -> Optional[Tuple[int, M]]:
    """The message of ``timed`` logged nearest to ``t``, the earlier on a tie.

    ``known`` holds the log times of ``timed``, in order.
    """
    after = bisect.bisect_right(known, t)
    candidates = [i for i in (after - 1, after) if 0 <= i < len(known)]
    if not candidates:
        return None
    return timed[min(candidates, key=lambda i: (abs(known[i] - t), known[i]))]


def _backward(
    timed: List[Tuple[int, M]], known: List[int], t: int
) -> Optional[Tuple[int, M]]:
    """The last message of ``timed`` logged at or before ``t``.

    ``known`` holds the log times of ``timed``, in order. Of messages with equal
    log times, the last recorded is taken.
    """
    at_or_before = bisect.bisect_right(known, t) - 1
    return timed[at_or_before] if at_or_before >= 0 else None


def _counted_slots(
    plan: TopicPlan, times: TopicTimes[M], sample: SampleTimes
) -> List[_Slot[M]]:
    """The steps of ``steps.last`` or ``steps.next``: that many recorded
    messages next to the anchor, within the topic's ``bounds``.

    A before-window takes the last messages logged at or before the anchor,
    an after-window the first logged after it. The steps the bounds cannot
    fill lie past their last message on that side, and take the clip's edge
    as their time.
    """
    assert plan.num_steps is not None
    count = plan.num_steps
    first_time, last_time = plan.bounds(sample)
    if plan.before:
        taken = times.last_in(first_time, sample.anchor, count)
        missing = [_Slot(sample.clip_start, None) for _ in range(count - len(taken))]
        return missing + [_Slot(time, (time, m)) for time, m in taken]
    taken = times.first_in(sample.anchor + 1, last_time, count)
    missing = [_Slot(sample.clip_last, None) for _ in range(count - len(taken))]
    return [_Slot(time, (time, m)) for time, m in taken] + missing


def _padded(
    plan: TopicPlan,
    slots: List[_Slot[M]],
    times: TopicTimes[M],
    sample: SampleTimes,
    where: str,
) -> List[Step[M]]:
    """The steps, with those that took no message filled as ``plan.pad`` says.

    ``"edge"`` repeats the nearest step that took a message, the earlier one
    on a tie. A window where no step did repeats the clip's first message on
    the topic for a before-window, and its last for an after-window. A clip
    with no message on the topic leaves the steps empty.
    """
    real = [i for i, slot in enumerate(slots) if slot.taken is not None]
    if len(real) < len(slots) and plan.pad == "raise":
        raise ValueError(_unfilled_message(plan, slots, sample, where))
    edge: Optional[Tuple[int, M]] = None
    if len(real) < len(slots) and not real and plan.pad == "edge":
        edge = _clip_edge(plan, times, sample)
    steps: List[Step[M]] = []
    for i, slot in enumerate(slots):
        if slot.taken is not None:
            steps.append(Step(slot.taken[1], slot.taken[0], True))
            continue
        repeated = edge
        if plan.pad == "edge" and real:
            nearest = min(real, key=lambda j: (abs(j - i), j))
            repeated = slots[nearest].taken
        if repeated is None:
            steps.append(Step(None, slot.time, False))
        else:
            steps.append(Step(repeated[1], repeated[0], False))
    return steps


def _clip_edge(
    plan: TopicPlan, times: TopicTimes[M], sample: SampleTimes
) -> Optional[Tuple[int, M]]:
    """The message ``"edge"`` repeats in a window where no step took one: the
    first on the topic within its ``bounds`` for a before-window, the last for
    an after-window."""
    first_time, last_time = plan.bounds(sample)
    if plan.before:
        found = times.first_in(first_time, last_time, 1)
    else:
        found = times.last_in(first_time, last_time, 1)
    return found[0] if found else None


def _variable_steps(
    plan: TopicPlan, times: TopicTimes[M], sample: SampleTimes, where: str
) -> List[Step[M]]:
    """Every message of ``steps.every``'s window, within the topic's ``bounds``.

    A before-window holds ``(anchor - span, anchor]``, an after-window
    ``(anchor, anchor + span]``. One that reaches past the clip fails the read
    with ``pad="raise"``, and is cut at the clip's edge otherwise. With
    ``within="recording"`` it is cut at the recording's edges only.
    """
    assert plan.span_ns is not None
    anchor = sample.anchor
    first_time, last_time = plan.bounds(sample)
    if plan.before:
        low, high = anchor - plan.span_ns + 1, anchor
        past = first_time - low
    else:
        low, high = anchor + 1, anchor + plan.span_ns
        past = high - last_time
    if past > 0 and plan.pad == "raise":
        region = _region(plan, sample)
        reach = (
            f"before {region} starts" if plan.before else f"past the end of {region}"
        )
        raise ValueError(
            f"{where}: the window of topic {plan.topic!r} reaches "
            f"{past / _NS_PER_S:g} s {reach}, and its pad is 'raise'. Use "
            "pad='edge' or pad='zeros' to cut it at the edge."
        )
    low, high = max(low, first_time), min(high, last_time)
    if low > high:
        return []
    return [Step(m, time, True) for time, m in times.all_in(low, high)]


def _unfilled_message(
    plan: TopicPlan, slots: Sequence[_Slot], sample: SampleTimes, where: str
) -> str:
    """The error for a window with steps that took no message, under ``"raise"``."""
    empty = [slot for slot in slots if slot.taken is None]
    outside = [slot.time for slot in empty if slot.outside]
    first_time, last_time = plan.bounds(sample)
    name = "recording" if plan.within_recording else "clip"
    if plan.clip:
        reason = "No message on the topic is logged in the clip."
    elif outside:
        if plan.before:
            past = first_time - min(outside)
            reason = (
                f"The window reaches {past / _NS_PER_S:g} s before the {name} starts."
            )
        else:
            past = max(outside) - last_time
            reason = f"The window reaches {past / _NS_PER_S:g} s past the {name}'s end."
    elif plan.fps is None:
        side = "up to" if plan.before else "after"
        found = len(slots) - len(empty)
        reason = (
            f"The {name} holds only {found} messages on the topic {side} the anchor."
        )
    else:
        side = "up to" if plan.before else "after"
        reason = f"No message on the topic is logged in the {name} {side} the anchor."
    return (
        f"{where}: {len(empty)} of {len(slots)} steps of topic {plan.topic!r} have "
        f"no message in {_region(plan, sample)}, and its pad is 'raise'. {reason} "
        "Use pad='edge' or pad='zeros' to pad such steps."
    )


def _region(plan: TopicPlan, sample: SampleTimes) -> str:
    """How an error names where the topic may take messages."""
    if plan.within_recording:
        return "the recording"
    return f"the clip [{sample.clip_start}, {sample.clip_end})"


# -- the samples dataset and the row layout ---------------------------------


def sample_manifest(batch: pa.Table, *, needs_anchor: bool = True) -> pa.Table:
    """A batch of samples as manifest rows for the read tasks.

    Each sample keeps its columns and gains the manifest's: its file as the
    path, with no size and no chunks. Fails on a sample that lists no file or
    several, whose clip is empty, or whose anchor lies outside its clip. With
    ``needs_anchor``, because a topic measures from it, a sample must have an
    anchor; otherwise one may have none.
    """
    import numpy as np

    files = batch.column(FILES_COLUMN).combine_chunks()
    counts = files.value_lengths().fill_null(0).to_numpy(zero_copy_only=False)
    _check(batch, counts == 1, _files_problem)
    paths = files.flatten()
    _check(batch, ~paths.is_null().to_numpy(zero_copy_only=False), _files_problem)
    start, end = (batch.column(name) for name in (CLIP_START_COLUMN, CLIP_END_COLUMN))
    known = ~np.logical_or(
        start.is_null().to_numpy(zero_copy_only=False),
        end.is_null().to_numpy(zero_copy_only=False),
    )
    _check(batch, known, _clip_problem)
    start, end = start.to_numpy(), end.to_numpy()
    _check(batch, start < end, _clip_problem)
    if ANCHOR_COLUMN in batch.column_names:
        anchor = batch.column(ANCHOR_COLUMN)
        if pa.types.is_null(anchor.type):
            anchor = anchor.cast(pa.int64())
        missing = anchor.is_null().to_numpy(zero_copy_only=False)
        if needs_anchor:
            _check(batch, ~missing, _anchor_problem)
        values = anchor.fill_null(0).to_numpy()
        inside = np.logical_and(start <= values, values < end)
        _check(batch, np.logical_or(missing, inside), _clip_problem)
    num_rows = batch.num_rows
    return (
        batch.append_column(PATH_COLUMN_NAME, paths)
        .append_column(FILE_SIZE_COLUMN_NAME, pa.nulls(num_rows, pa.int64()))
        .append_column(FILE_CHUNK_METADATA_COLUMN_NAME, pa.nulls(num_rows))
    )


def _check(
    batch: pa.Table, ok: "np.ndarray", problem: Callable[[dict], Exception]
) -> None:
    """Raise ``problem(row)`` for the first sample of ``batch`` that is not ``ok``."""
    if ok.all():
        return
    first = int(ok.argmin())
    raise problem(batch.slice(first, 1).to_pylist()[0])


def _describe(row: dict) -> str:
    """How an error names a sample: by its id, else by its files and clip."""
    if row.get(SAMPLE_ID_COLUMN) is not None:
        return f"Sample {row[SAMPLE_ID_COLUMN]!r}"
    return (
        f"The sample with files {row[FILES_COLUMN]} and clip "
        f"[{row[CLIP_START_COLUMN]}, {row[CLIP_END_COLUMN]})"
    )


def _files_problem(row: dict) -> Exception:
    files = row[FILES_COLUMN] or []
    if len(files) > 1:
        return NotImplementedError(
            f"{_describe(row)} lists {len(files)} files. A sample that spans "
            "several files is not supported yet: give each sample one file."
        )
    return ValueError(f"{_describe(row)} lists no file: give each sample one.")


def _clip_problem(row: dict) -> Exception:
    return ValueError(
        f"{_describe(row)} has clip [{row[CLIP_START_COLUMN]}, "
        f"{row[CLIP_END_COLUMN]}) and anchor {row.get(ANCHOR_COLUMN)}. A clip must "
        "not be empty, and the anchor must lie inside it: clip_start <= anchor < "
        "clip_end, in nanoseconds of log time."
    )


def _anchor_problem(row: dict) -> Exception:
    return ValueError(
        f"{_describe(row)} has no anchor, and a topic measures from it. Give "
        "each sample an anchor, or measure from the clip (at='clip_start' or "
        "at='clip_end', steps.across, steps.at)."
    )


def check_samples_schema(
    schema: pa.Schema, topics: Sequence[str], anchor_topics: Sequence[str] = ()
) -> None:
    """Check that a samples dataset has the columns a sample read needs.

    ``files`` must be a list of strings, and the clip columns integers.
    ``anchor_topics`` are the topics that measure from the anchor: with any,
    the dataset needs an integer ``anchor`` column. No column may take the
    name of a column the read adds.
    """
    names = set(schema.names)
    missing = [name for name in SAMPLE_COLUMNS if name not in names]
    if missing:
        raise ValueError(
            f"The samples dataset lacks the columns {missing}. It needs "
            f"{list(SAMPLE_COLUMNS)}: the sample's files and its clip "
            "[clip_start, clip_end), in nanoseconds of log time."
        )
    if anchor_topics and ANCHOR_COLUMN not in names:
        raise ValueError(
            f"The samples dataset lacks the column {ANCHOR_COLUMN!r}, and topics "
            f"{sorted(anchor_topics)} measure from it. Add each sample's anchor, "
            "or measure those topics from the clip (at='clip_start' or "
            "at='clip_end', steps.across, steps.at)."
        )
    files_type = schema.field(FILES_COLUMN).type
    if not (
        (pa.types.is_list(files_type) or pa.types.is_large_list(files_type))
        and (
            pa.types.is_string(files_type.value_type)
            or pa.types.is_large_string(files_type.value_type)
        )
    ):
        raise ValueError(
            f"The samples column {FILES_COLUMN!r} must be a list of paths, got "
            f"{files_type}"
        )
    for name in (CLIP_START_COLUMN, CLIP_END_COLUMN, ANCHOR_COLUMN):
        type_ = schema.field(name).type if name in names else None
        if type_ is not None and not (
            pa.types.is_integer(type_)
            or (name == ANCHOR_COLUMN and pa.types.is_null(type_))
        ):
            raise ValueError(
                f"The samples column {name!r} must hold integer nanoseconds of log "
                f"time, got {type_}"
            )
    added = {name for topic in topics for name in topic_column_names(topic)}
    added |= {PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME, FILE_CHUNK_METADATA_COLUMN_NAME}
    taken = sorted(names & added)
    if taken:
        raise ValueError(
            f"The samples dataset has columns {taken} that the read adds. Rename "
            "or drop them."
        )


def topic_column_names(topic: str) -> List[str]:
    """Every name a sample row may give one topic's columns."""
    return [f"{prefix}{topic}" for prefix in TOPIC_PREFIXES]


def frames_type(plan: TopicPlan, resize: Optional[Tuple[int, int]]) -> pa.DataType:
    """The type of a decoded topic's ``frames:`` column.

    Fixed steps decoded to a fixed ``resize`` have one shape in every row.
    Otherwise frames keep their coded size, which can differ between
    recordings, and ``steps.every`` varies in length.
    """
    if plan.num_steps is not None and resize is not None:
        return ArrowTensorTypeV2((plan.num_steps, *resize, 3), pa.uint8())
    return ArrowVariableShapedTensorType(pa.uint8(), 4)


def topic_fields(
    plan: TopicPlan, decoded: bool, resize: Optional[Tuple[int, int]] = None
) -> List[pa.Field]:
    """A topic's columns in a sample row, one list entry per step.

    ``data:<topic>`` holds each step's payload, empty for a step that
    ``"zeros"`` pads. A decoded topic has ``frames:<topic>`` instead, a
    ``uint8`` tensor of shape ``(steps, height, width, 3)``. ``times:<topic>``
    holds each step's log time and ``valid:<topic>`` whether the step took a
    message of its own. With ``match.all_in_step``, a step's ``data:`` and
    ``times:`` entries are lists of every message of its slot.
    """
    topic = plan.topic
    if plan.all_in_step:
        return [
            pa.field(f"{DATA_PREFIX}{topic}", pa.list_(pa.list_(pa.large_binary()))),
            pa.field(f"{TIMES_PREFIX}{topic}", pa.list_(pa.list_(pa.int64()))),
            pa.field(f"{VALID_PREFIX}{topic}", pa.list_(pa.bool_())),
        ]
    if decoded:
        payload = pa.field(f"{FRAMES_PREFIX}{topic}", frames_type(plan, resize))
    else:
        payload = pa.field(f"{DATA_PREFIX}{topic}", pa.list_(pa.large_binary()))
    return [
        payload,
        pa.field(f"{TIMES_PREFIX}{topic}", pa.list_(pa.int64())),
        pa.field(f"{VALID_PREFIX}{topic}", pa.list_(pa.bool_())),
    ]


def sample_row_schema(
    samples_schema: pa.Schema,
    plans: Sequence[TopicPlan],
    decoded_topics: Sequence[str] = (),
    resize: Optional[Tuple[int, int]] = None,
) -> pa.Schema:
    """The schema of sample rows: the samples' own columns, then each topic's.

    Topics come in the order ``read_mcap(topics=...)`` gives them.
    """
    decoded = set(decoded_topics)
    fields = list(samples_schema)
    for plan in plans:
        fields += topic_fields(plan, plan.topic in decoded, resize)
    return pa.schema(fields)
