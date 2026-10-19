"""Training samples: which message fills each step of a sample row.

``read_mcap(samples=...)`` reads one row per sample. A sample names its file,
a clip ``[clip_start, clip_end)`` and an ``anchor`` inside the clip, in
nanoseconds of log time. Each topic's ``TopicSpec`` places its steps before or
after the anchor (``mcap_topic_spec``). This module turns the two into the
message that fills each step, and lays out a sample row's columns. It does no I/O:
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
    TimeSteps,
    TopicSpec,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
)

if TYPE_CHECKING:
    import numpy as np

# The columns every samples dataset has. ``files`` lists a sample's recordings,
# one for now. The three times are log times in nanoseconds; ``clip_end`` is
# exclusive and the anchor lies inside the clip.
FILES_COLUMN = "files"
CLIP_START_COLUMN = "clip_start"
CLIP_END_COLUMN = "clip_end"
ANCHOR_COLUMN = "anchor"
SAMPLE_COLUMNS: Tuple[str, ...] = (
    FILES_COLUMN,
    CLIP_START_COLUMN,
    CLIP_END_COLUMN,
    ANCHOR_COLUMN,
)
# An optional column whose value names a sample in error messages.
SAMPLE_ID_COLUMN = "sample_id"

# A sample row's columns per topic.
DATA_PREFIX = "data:"
TIMES_PREFIX = "times:"
VALID_PREFIX = "valid:"
TOPIC_PREFIXES: Tuple[str, ...] = (DATA_PREFIX, TIMES_PREFIX, VALID_PREFIX)

_NS_PER_S = 1_000_000_000

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
    """A sample's clip ``[clip_start, clip_end)`` and its anchor, in log-time ns."""

    clip_start: int
    clip_end: int
    anchor: int

    @property
    def clip_last(self) -> int:
        """The clip's last nanosecond: ``clip_end`` is exclusive."""
        return self.clip_end - 1


@dataclass(frozen=True)
class TopicPlan:
    """One topic's ``TopicSpec`` in the terms its steps are resolved in.

    Attributes:
        topic: The topic name.
        before: Whether the window ends at the anchor rather than starting
            after it.
        pad: How a step with no message is filled (``TopicSpec.pad``).
        num_steps: The window's fixed number of steps, or ``None`` for
            ``steps.every``, whose length varies.
        fps: Steps per second of time steps, or ``None`` for message steps.
        span_ns: The horizon of ``steps.every``, in nanoseconds.
    """

    topic: str
    before: bool
    pad: str
    num_steps: Optional[int]
    fps: Optional[Fraction] = None
    span_ns: Optional[int] = None

    @classmethod
    def from_spec(cls, topic: str, spec: TopicSpec) -> "TopicPlan":
        steps = spec.steps
        before = steps.side == "before"
        if isinstance(steps, TimeSteps):
            return cls(topic, before, spec.pad, num_steps=steps.count, fps=steps.fps)
        if steps.count is not None:
            return cls(topic, before, spec.pad, num_steps=steps.count)
        return cls(topic, before, spec.pad, num_steps=None, span_ns=steps.span_ns)

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

    def reach(self, sample: SampleTimes) -> Tuple[int, int]:
        """The span of log time inside the clip that the window's steps fall in.

        For time steps it runs from the first step to the last, and for
        ``steps.every`` over its horizon. ``steps.last`` and ``steps.next``
        reach as far back or on as the recording needs, so only the anchor's
        side of them is known here. The reader reads the chunks over
        this span first, and reads on as the steps need.
        """
        anchor = sample.anchor
        if self.before:
            if self.fps is not None:
                assert self.num_steps is not None
                low = anchor - self.step_offset_ns(self.num_steps - 1)
            elif self.span_ns is not None:
                low = anchor - self.span_ns + 1
            else:
                low = anchor
            return max(low, sample.clip_start), anchor
        if self.fps is not None:
            assert self.num_steps is not None
            high = anchor + self.step_offset_ns(self.num_steps)
        elif self.span_ns is not None:
            high = anchor + self.span_ns
        else:
            high = anchor + 1
        return min(anchor + 1, sample.clip_last), min(high, sample.clip_last)


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
    if plan.num_steps is None:
        return _variable_steps(plan, times, sample, where)
    if plan.fps is not None:
        slots = _timed_slots(plan, times, sample)
    else:
        slots = _counted_slots(plan, times, sample)
    return _padded(plan, slots, times, sample, where)


def _timed_slots(
    plan: TopicPlan, times: TopicTimes[M], sample: SampleTimes
) -> List[_Slot[M]]:
    """The steps of a window with ``fps``, each with the message it takes.

    Each step inside the clip takes the message logged nearest to it, the
    earlier one on a tie, from the window's side of the anchor. Log times
    jitter, so a step meant to fall on a frame lands just before or after it;
    the nearest message is still that frame. The messages are asked for once:
    the nearest before the first step, every one up to the last step, and
    the nearest after it.
    """
    step_times = plan.step_times(sample.anchor)
    if plan.before:
        low, high = sample.clip_start, sample.anchor
    else:
        low, high = sample.anchor + 1, sample.clip_last
    inside = [t for t in step_times if sample.clip_start <= t <= sample.clip_last]
    timed: List[Tuple[int, M]] = []
    if inside:
        first, last = inside[0], inside[-1]
        timed = times.last_in(low, first, 1)
        if first < last:
            timed += times.all_in(first + 1, last)
        if last < high:
            timed += times.first_in(last + 1, high, 1)
    known = [time for time, _ in timed]
    slots: List[_Slot[M]] = []
    for t in step_times:
        if not sample.clip_start <= t <= sample.clip_last:
            slots.append(_Slot(t, None, outside=True))
        else:
            slots.append(_Slot(t, _nearest(timed, known, t)))
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


def _counted_slots(
    plan: TopicPlan, times: TopicTimes[M], sample: SampleTimes
) -> List[_Slot[M]]:
    """The steps of ``steps.last`` or ``steps.next``: that many recorded
    messages next to the anchor, inside the clip.

    A before-window takes the last messages logged at or before the anchor,
    an after-window the first logged after it. The steps the clip cannot fill
    lie past its last message on that side, and take the clip's edge as their
    time.
    """
    assert plan.num_steps is not None
    count = plan.num_steps
    if plan.before:
        taken = times.last_in(sample.clip_start, sample.anchor, count)
        missing = [_Slot(sample.clip_start, None) for _ in range(count - len(taken))]
        return missing + [_Slot(time, (time, m)) for time, m in taken]
    taken = times.first_in(sample.anchor + 1, sample.clip_last, count)
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
    clip's first on the topic for a before-window, its last for an after-window."""
    if plan.before:
        found = times.first_in(sample.clip_start, sample.clip_last, 1)
    else:
        found = times.last_in(sample.clip_start, sample.clip_last, 1)
    return found[0] if found else None


def _variable_steps(
    plan: TopicPlan, times: TopicTimes[M], sample: SampleTimes, where: str
) -> List[Step[M]]:
    """Every message of ``steps.every``'s window, inside the clip.

    A before-window holds ``(anchor - span, anchor]``, an after-window
    ``(anchor, anchor + span]``. One that reaches past the clip fails the read
    with ``pad="raise"``, and is cut at the clip's edge otherwise.
    """
    assert plan.span_ns is not None
    anchor = sample.anchor
    if plan.before:
        low, high = anchor - plan.span_ns + 1, anchor
        past = sample.clip_start - low
    else:
        low, high = anchor + 1, anchor + plan.span_ns
        past = high - sample.clip_last
    if past > 0 and plan.pad == "raise":
        clip = f"the clip [{sample.clip_start}, {sample.clip_end})"
        reach = f"before {clip} starts" if plan.before else f"past the end of {clip}"
        raise ValueError(
            f"{where}: the window of topic {plan.topic!r} reaches "
            f"{past / _NS_PER_S:g} s {reach}, and its pad is 'raise'. Use "
            "pad='edge' or pad='zeros' to cut it at the clip's edge."
        )
    low, high = max(low, sample.clip_start), min(high, sample.clip_last)
    if low > high:
        return []
    return [Step(m, time, True) for time, m in times.all_in(low, high)]


def _unfilled_message(
    plan: TopicPlan, slots: Sequence[_Slot], sample: SampleTimes, where: str
) -> str:
    """The error for a window with steps that took no message, under ``"raise"``."""
    empty = [slot for slot in slots if slot.taken is None]
    outside = [slot.time for slot in empty if slot.outside]
    if outside:
        if plan.before:
            past = sample.clip_start - min(outside)
            reason = (
                f"The window reaches {past / _NS_PER_S:g} s before the clip starts."
            )
        else:
            past = max(outside) - sample.clip_last
            reason = f"The window reaches {past / _NS_PER_S:g} s past the clip's end."
    elif plan.fps is None:
        side = "up to" if plan.before else "after"
        found = len(slots) - len(empty)
        reason = f"The clip holds only {found} messages on the topic {side} the anchor."
    else:
        side = "up to" if plan.before else "after"
        reason = f"No message on the topic is logged in the clip {side} the anchor."
    return (
        f"{where}: {len(empty)} of {len(slots)} steps of topic {plan.topic!r} have "
        f"no message in the clip [{sample.clip_start}, {sample.clip_end}), and its "
        f"pad is 'raise'. {reason} Use pad='edge' or pad='zeros' to pad such steps."
    )


# -- the samples dataset and the row layout ---------------------------------


def sample_manifest(batch: pa.Table) -> pa.Table:
    """A batch of samples as manifest rows for the read tasks.

    Each sample keeps its columns and gains the manifest's: its file as the
    path, with no size and no chunks. Fails on a sample that lists no file or
    several, or whose anchor lies outside its clip.
    """
    import numpy as np

    files = batch.column(FILES_COLUMN).combine_chunks()
    counts = files.value_lengths().fill_null(0).to_numpy(zero_copy_only=False)
    _check(batch, counts == 1, _files_problem)
    paths = files.flatten()
    _check(batch, ~paths.is_null().to_numpy(zero_copy_only=False), _files_problem)
    start, end, anchor = (
        batch.column(name)
        for name in (CLIP_START_COLUMN, CLIP_END_COLUMN, ANCHOR_COLUMN)
    )
    known = ~np.logical_or.reduce(
        [c.is_null().to_numpy(zero_copy_only=False) for c in (start, end, anchor)]
    )
    _check(batch, known, _clip_problem)
    start, end, anchor = (c.to_numpy() for c in (start, end, anchor))
    _check(batch, (start < end) & (start <= anchor) & (anchor < end), _clip_problem)
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
    """How an error names a sample: by its id, else by its files and anchor."""
    if row.get(SAMPLE_ID_COLUMN) is not None:
        return f"Sample {row[SAMPLE_ID_COLUMN]!r}"
    return f"The sample with files {row[FILES_COLUMN]} and anchor {row[ANCHOR_COLUMN]}"


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
        f"{row[CLIP_END_COLUMN]}) and anchor {row[ANCHOR_COLUMN]}. A clip must "
        "not be empty, and the anchor must lie inside it: clip_start <= anchor < "
        "clip_end, in nanoseconds of log time."
    )


def check_samples_schema(schema: pa.Schema, topics: Sequence[str]) -> None:
    """Check that a samples dataset has the columns a sample read needs.

    ``files`` must be a list of strings, and the clip and anchor columns
    integers. No column may take the name of a column the read adds.
    """
    names = set(schema.names)
    missing = [name for name in SAMPLE_COLUMNS if name not in names]
    if missing:
        raise ValueError(
            f"The samples dataset lacks the columns {missing}. It needs "
            f"{list(SAMPLE_COLUMNS)}: the sample's files, its clip [clip_start, "
            "clip_end) and its anchor, in nanoseconds of log time."
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
        if not pa.types.is_integer(schema.field(name).type):
            raise ValueError(
                f"The samples column {name!r} must hold integer nanoseconds of log "
                f"time, got {schema.field(name).type}"
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


def topic_fields(plan: TopicPlan) -> List[pa.Field]:
    """A topic's columns in a sample row, one list entry per step.

    ``data:<topic>`` holds each step's payload, empty for a step that
    ``"zeros"`` pads. ``times:<topic>`` holds each step's log time and
    ``valid:<topic>`` whether the step took a message of its own.
    """
    topic = plan.topic
    return [
        pa.field(f"{DATA_PREFIX}{topic}", pa.list_(pa.large_binary())),
        pa.field(f"{TIMES_PREFIX}{topic}", pa.list_(pa.int64())),
        pa.field(f"{VALID_PREFIX}{topic}", pa.list_(pa.bool_())),
    ]


def sample_row_schema(
    samples_schema: pa.Schema, plans: Sequence[TopicPlan]
) -> pa.Schema:
    """The schema of sample rows: the samples' own columns, then each topic's.

    Topics come in the order ``read_mcap(topics=...)`` gives them.
    """
    fields = list(samples_schema)
    for plan in plans:
        fields += topic_fields(plan)
    return pa.schema(fields)
