"""Sample rows: one row per sample, with each topic's steps around its anchor.

``MCAPReader`` hands this module a task's samples one file at a time, in anchor
order (``read_mcap(samples=...)``). A ``ChunkTimeline`` reads the chunks the
steps need. Samples whose windows meet form a run, and a run's chunks are read
in one coalesced pass. A step that reaches further, such as one in a gap of the
recording, reads on chunk by chunk, nearest first. ``mcap_samples`` decides
which message fills each step, and ``SampleTableBuilder`` lays the rows out::

    one file's samples, by anchor      a1   a2   a3              a4
    their windows, all topics       [-----]
                                         [-----]
                                              [-----]          [-----]
    runs                            [   run 1          ]       [run 2]
    chunks read                     c3   c4   c5   c6          c9  c10

A file without a chunk index is read whole, once per task.
"""

import bisect
import itertools
import urllib.parse
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

import pyarrow as pa

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
    _Selected,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_decoded_samples import (
    FrameColumns,
    FrameKey,
    RunDecoder,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_io import buffered
from ray.data._internal.datasource_v2.formats.mcap.mcap_samples import (
    CLIP_END_COLUMN,
    CLIP_START_COLUMN,
    DATA_PREFIX,
    FRAMES_PREFIX,
    SAMPLE_ID_COLUMN,
    TIMES_PREFIX,
    VALID_PREFIX,
    SampleTimes,
    SlotStep,
    SortedTimes,
    Step,
    TopicPlan,
    resolve_slots,
    resolve_steps,
    sample_anchors,
    topic_column_names,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.util import GiB
from ray.data.block import BlockAccessor

if TYPE_CHECKING:
    from mcap.records import ChunkIndex
    from mcap.summary import Summary
    from pyarrow.fs import FileSystem

# A message as a row holds it: schema, channel, record.
_Entry = Tuple[Any, Any, Any]
# One topic's steps in a row: one message per step, or with
# ``match.all_in_step`` every message of each step's slot.
_TopicSteps = Union[List[Step[_Entry]], List[SlotStep[_Entry]]]
# A timeline's sort key: log time, then where the message lies in the file.
_Key = Tuple[int, int, int]

# The columns ``read_mcap`` adds to each sample for the read, which rows drop.
MANIFEST_COLUMNS = (
    PATH_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    FILE_CHUNK_METADATA_COLUMN_NAME,
)

# Largest payload a sample row may carry: a memory guardrail, not a format
# limit. A row is built in the worker and copied into the object store as one
# object, so a read needs about twice its size. ``RAY_DATA_MCAP_MAX_ROW_BYTES``
# overrides it.
DEFAULT_MAX_ROW_BYTES = GiB

# How many chunks a step that reaches past the chunks read so far reads at a
# time, nearest first.
_CHUNKS_PER_REACH = 4
# The most samples one run holds, so a dense stretch of anchors still lets
# its chunks go now and then.
_MAX_RUN_SAMPLES = 256


class _TopicView:
    """One topic of a ``ChunkTimeline``, as ``resolve_steps`` asks for it."""

    def __init__(self, timeline: "ChunkTimeline", topic: str):
        self._timeline = timeline
        self._topic = topic

    def all_in(self, low: int, high: int) -> List[Tuple[int, _Entry]]:
        self._timeline.read_span(self._topic, low, high)
        return self._timeline.held(self._topic).all_in(low, high)

    def last_in(self, low: int, high: int, count: int) -> List[Tuple[int, _Entry]]:
        while True:
            found = self._timeline.held(self._topic).last_in(low, high, count)
            # Only a chunk reaching the earliest message kept could add one.
            reach = found[0][0] if len(found) == count else low
            if not self._timeline.read_nearest(self._topic, reach, high, last=True):
                return found

    def first_in(self, low: int, high: int, count: int) -> List[Tuple[int, _Entry]]:
        while True:
            found = self._timeline.held(self._topic).first_in(low, high, count)
            reach = found[-1][0] if len(found) == count else high
            if not self._timeline.read_nearest(self._topic, low, reach, last=False):
                return found


class ChunkTimeline:
    """The sample topics' messages of one open file, read chunk by chunk.

    The summary's chunk indexes tell which chunks may hold a topic: those whose
    message indexes name one of its channels, or every chunk when they name
    none. A chunk is read once and kept until ``forget_before`` lets it go.
    Each topic's messages are held sorted by log time, then by their place in
    the file, so equal log times keep the order they were recorded in.
    """

    def __init__(
        self,
        f: Any,
        path: str,
        summary: "Summary",
        file_summary: Callable[[], "Summary"],
        reader: SelectedMessageReader,
        topics: Sequence[str],
    ):
        self._f = f
        self._path = path
        self._summary = summary
        self._file_summary = file_summary
        self._reader = reader
        self._topics = set(topics)
        topic_ids: Dict[str, Set[int]] = {topic: set() for topic in topics}
        for channel_id, channel in summary.channels.items():
            if channel.topic in topic_ids:
                topic_ids[channel.topic].add(channel_id)
        self._selected = set().union(*topic_ids.values())
        # Each topic's candidate chunks, by first log time. A summary that
        # lists no channel cannot tell, so every chunk is a candidate. One that
        # lists others but none of the topic's says the file lacks the topic.
        self._candidates = {
            topic: sorted(
                (
                    c
                    for c in summary.chunk_indexes
                    if not summary.channels
                    or (
                        ids
                        and (
                            not c.message_index_offsets
                            or not ids.isdisjoint(c.message_index_offsets)
                        )
                    )
                ),
                key=lambda c: (c.message_start_time, c.chunk_start_offset),
            )
            for topic, ids in topic_ids.items()
        }
        # Chunk offset -> its index record and, per topic, its messages.
        self._chunks: Dict[int, "ChunkIndex"] = {}
        self._loaded: Dict[int, Dict[str, List[Tuple[_Key, _Entry]]]] = {}
        self._held: Dict[str, SortedTimes] = {}

    def view(self, topic: str) -> _TopicView:
        return _TopicView(self, topic)

    def held(self, topic: str) -> SortedTimes:
        """The topic's messages in the chunks read so far."""
        held = self._held.get(topic)
        if held is None:
            keyed = sorted(
                itertools.chain.from_iterable(
                    by_topic.get(topic, ()) for by_topic in self._loaded.values()
                ),
                key=lambda pair: pair[0],
            )
            held = self._held[topic] = SortedTimes(
                [(key[0], entry) for key, entry in keyed]
            )
        return held

    def read_spans(self, spans: Dict[str, Tuple[int, int]]) -> None:
        """Read, in one pass, every chunk not read yet that may hold one of the
        topics in its span ``[low, high]``."""
        wanted: Dict[int, "ChunkIndex"] = {}
        for topic, (low, high) in spans.items():
            for chunk in self._unread(topic, low, high):
                wanted[chunk.chunk_start_offset] = chunk
        self._read(list(wanted.values()))

    def read_span(self, topic: str, low: int, high: int) -> None:
        """Read every chunk not read yet that may hold the topic in ``[low, high]``."""
        self._read(self._unread(topic, low, high))

    def read_nearest(self, topic: str, low: int, high: int, *, last: bool) -> bool:
        """Read a few chunks not read yet that may hold the topic in
        ``[low, high]``: the latest ending with ``last``, else the earliest
        starting. Returns whether any chunk was left to read."""
        unread = self._unread(topic, low, high)
        if last:
            unread.sort(key=lambda c: c.message_end_time, reverse=True)
        self._read(unread[:_CHUNKS_PER_REACH])
        return bool(unread)

    def forget_before(self, time: int) -> None:
        """Let go of the chunks whose messages all lie before ``time``."""
        gone = [
            offset
            for offset, chunk in self._chunks.items()
            if chunk.message_end_time < time
        ]
        for offset in gone:
            del self._chunks[offset]
            del self._loaded[offset]
        if gone:
            self._held.clear()

    def _unread(self, topic: str, low: int, high: int) -> List["ChunkIndex"]:
        """The topic's candidate chunks not read yet that overlap ``[low, high]``."""
        candidates = self._candidates.get(topic, [])
        # Candidates are sorted by first log time, so those starting after
        # ``high`` are cut off at once.
        end = bisect.bisect_right(candidates, high, key=lambda c: c.message_start_time)
        return [
            c
            for c in candidates[:end]
            if c.message_end_time >= low and c.chunk_start_offset not in self._loaded
        ]

    def _read(self, chunks: List["ChunkIndex"]) -> None:
        if not chunks:
            return
        for chunk, messages in self._reader.iter_chunk_messages(
            self._f,
            self._path,
            self._summary,
            chunks,
            self._file_summary,
            selected=self._selected,
        ):
            offset = chunk.chunk_start_offset
            by_topic: Dict[str, List[Tuple[_Key, _Entry]]] = {}
            for position, (schema, channel, message, _) in enumerate(messages):
                if channel.topic in self._topics:
                    key = (message.log_time, offset, position)
                    by_topic.setdefault(channel.topic, []).append(
                        (key, (schema, channel, message))
                    )
            self._chunks[offset] = chunk
            self._loaded[offset] = by_topic
        self._held.clear()


class WholeFileTimeline:
    """The sample topics' messages of a file without a chunk index, read whole."""

    def __init__(self, messages: Iterator[_Selected], topics: Sequence[str]):
        by_topic: Dict[str, List[Tuple[int, _Entry]]] = {topic: [] for topic in topics}
        for schema, channel, message, _ in messages:
            if channel.topic in by_topic:
                by_topic[channel.topic].append(
                    (message.log_time, (schema, channel, message))
                )
        # The scan is in file order, and the sort is stable.
        self._held = {
            topic: SortedTimes(sorted(timed, key=lambda pair: pair[0]))
            for topic, timed in by_topic.items()
        }

    def view(self, topic: str) -> SortedTimes:
        return self._held[topic]

    def read_spans(self, spans: Dict[str, Tuple[int, int]]) -> None:
        pass

    def forget_before(self, time: int) -> None:
        pass


@dataclass(frozen=True)
class _Sample:
    """One sample of a task: its row in the file's manifest, and its times."""

    index: int
    times: SampleTimes
    where: str


@dataclass(frozen=True)
class SampleFile:
    """One file of a read task's samples.

    ``samples`` holds the file's manifest rows in anchor order, and
    ``filesystem`` is the one its path resolved against.
    """

    path: str
    samples: pa.Table
    filesystem: Optional["FileSystem"]


def group_samples_by_file(
    manifest: FileManifest, paths: "SamplePaths"
) -> List[SampleFile]:
    """Group a samples manifest by file, each file's samples in anchor order.

    Files come in the order the manifest first names them, and samples with
    equal anchors keep their manifest order. A sample without an anchor sorts
    by its clip's start.
    """
    table = BlockAccessor.for_block(manifest.as_block()).to_arrow()
    anchors = sample_anchors(table)
    rows_by_path: Dict[str, List[int]] = {}
    for index, path in enumerate(table.column(PATH_COLUMN_NAME).to_pylist()):
        rows_by_path.setdefault(path, []).append(index)
    files = []
    for path, rows in rows_by_path.items():
        rows.sort(key=lambda index: anchors[index])
        resolved, filesystem = paths.resolve(path)
        files.append(SampleFile(resolved, table.take(rows), filesystem))
    return files


@dataclass(frozen=True)
class _Run:
    """Samples of one file whose windows meet, and each topic's span over them."""

    samples: List[_Sample]
    spans: Dict[str, Tuple[int, int]]

    @property
    def low(self) -> int:
        return min(low for low, _ in self.spans.values())


@dataclass(frozen=True)
class _Resolved:
    """A sample with its topics' steps, and the log times their messages span.

    A sample whose steps hold no message spans its anchor.
    """

    sample: _Sample
    steps: Dict[str, _TopicSteps]
    low: int
    high: int

    @classmethod
    def of(cls, sample: _Sample, steps: Dict[str, _TopicSteps]) -> "_Resolved":
        times: List[int] = []
        for topic_steps in steps.values():
            for step in topic_steps:
                if isinstance(step, SlotStep):
                    times.extend(m[2].log_time for m in step.messages)
                elif step.message is not None:
                    times.append(step.message[2].log_time)
        anchor = sample.times.anchor
        return cls(
            sample, steps, min(times, default=anchor), max(times, default=anchor)
        )


class _Group:
    """Resolved samples whose steps' messages meet, whose rows are built together.

    A group ends at ``_MAX_RUN_SAMPLES`` samples, so the frames it decodes stay
    few.
    """

    def __init__(self):
        self.members: List[_Resolved] = []
        self.low: Optional[int] = None
        self._high: Optional[int] = None

    def joins(self, resolved: _Resolved) -> bool:
        """Whether ``resolved`` can join the group: it is empty, or the sample's
        messages start before the group's end."""
        if self._high is None:
            return True
        return len(self.members) < _MAX_RUN_SAMPLES and resolved.low <= self._high

    def add(self, resolved: _Resolved) -> None:
        self.members.append(resolved)
        self.low = resolved.low if self.low is None else min(self.low, resolved.low)
        self._high = (
            resolved.high if self._high is None else max(self._high, resolved.high)
        )


def _samples_of(file: SampleFile) -> List[_Sample]:
    """The samples of one file, in anchor order as the manifest holds them."""
    table = file.samples
    starts = table.column(CLIP_START_COLUMN).to_pylist()
    ends = table.column(CLIP_END_COLUMN).to_pylist()
    anchors = sample_anchors(table)
    ids = (
        table.column(SAMPLE_ID_COLUMN).to_pylist()
        if SAMPLE_ID_COLUMN in table.column_names
        else None
    )
    samples = []
    for i, (start, end, anchor) in enumerate(zip(starts, ends, anchors)):
        if ids is not None:
            where = f"Sample {ids[i]!r} of {file.path!r}"
        else:
            where = f"The sample with clip [{start}, {end}) in {file.path!r}"
        samples.append(_Sample(i, SampleTimes(start, end, anchor), where))
    return samples


def _runs(samples: List[_Sample], plans: Sequence[TopicPlan]) -> Iterator[_Run]:
    """Group consecutive samples whose windows meet or touch, over every topic.

    Back-to-back clips touch: one ends the nanosecond before the next starts.
    A run is read in one pass, so such clips read like one long window. A run
    ends at ``_MAX_RUN_SAMPLES`` samples, so its chunks can be let go.
    """
    run: List[_Sample] = []
    spans: Dict[str, Tuple[int, int]] = {}
    high = 0
    for sample in samples:
        reach = {plan.topic: plan.reach(sample.times) for plan in plans}
        low = min(topic_low for topic_low, _ in reach.values())
        if run and (len(run) >= _MAX_RUN_SAMPLES or low > high + 1):
            yield _Run(run, spans)
            run, spans = [], {}
        if not run:
            high = low
        run.append(sample)
        for topic, (topic_low, topic_high) in reach.items():
            old_low, old_high = spans.get(topic, (topic_low, topic_high))
            spans[topic] = (min(old_low, topic_low), max(old_high, topic_high))
            high = max(high, topic_high)
    if run:
        yield _Run(run, spans)


class SampleTableBuilder:
    """Accumulates the sample rows of one file and builds Arrow tables of them.

    A row is the sample's own columns, taken from its manifest row, then the
    wanted columns of each topic. A decoded topic has ``frames:`` in place of
    ``data:`` (``FrameColumns``). Only the wanted columns are built.
    """

    def __init__(
        self,
        samples: pa.Table,
        plans: Sequence[TopicPlan],
        wanted: Optional[Set[str]],
        *,
        decoded: Sequence[str] = (),
        resize: Optional[Tuple[int, int]] = None,
    ):
        self._samples = samples.select(
            [
                name
                for name in samples.column_names
                if name not in MANIFEST_COLUMNS and (wanted is None or name in wanted)
            ]
        )
        self._sample_row_bytes = self._samples.nbytes // max(self._samples.num_rows, 1)
        self._plans = list(plans)
        self._wanted = wanted
        self._rows: List[int] = []
        self._columns: Dict[str, Any] = {
            plan.topic: FrameColumns(plan, resize)
            if plan.topic in decoded
            else _TopicColumns(
                with_data=self._wants(DATA_PREFIX, plan.topic),
                nested=plan.all_in_step,
            )
            for plan in plans
        }
        self.estimated_bytes = 0

    @property
    def num_rows(self) -> int:
        return len(self._rows)

    def add_row(
        self,
        index: int,
        steps: Dict[str, _TopicSteps],
        frames: Optional[Dict[FrameKey, Any]] = None,
        where: str = "",
    ) -> int:
        """Add the sample at ``index`` of the manifest with its topics' steps.

        ``frames`` holds the decoded frames the steps of decoded topics take, or
        is ``None`` when no frame is built. Returns the bytes of the payloads
        and frames the row carries.
        """
        self._rows.append(index)
        added = 0
        for plan in self._plans:
            columns = self._columns[plan.topic]
            topic_steps = steps.get(plan.topic, [])
            if isinstance(columns, FrameColumns):
                # Planning refuses all_in_step on a decoded topic, so its steps
                # are plain steps.
                plain = cast(List[Step[_Entry]], topic_steps)
                # A camera whose frames are projected away is not decoded, so
                # its steps keep their validity.
                built = frames if self._wants(FRAMES_PREFIX, plan.topic) else None
                added += columns.add(plain, built, where)
            else:
                added += columns.add(topic_steps)
        self.estimated_bytes += (
            self._sample_row_bytes
            + added
            + sum(9 * len(topic_steps) + 16 for topic_steps in steps.values())
        )
        return added

    def build(self) -> pa.Table:
        """The rows added so far, as one table; the builder starts over.

        A table without columns keeps its row count in a stub column, as
        ``MCAPReader`` projections do.
        """
        if self._samples.num_columns:
            table = self._samples.take(self._rows)
        else:
            table = pa.table(
                {_BATCH_SIZE_PRESERVING_STUB_COL_NAME: pa.nulls(len(self._rows))}
            )
        for plan in self._plans:
            columns = self._columns[plan.topic]
            for name, array in columns.finish(lambda p: self._wants(p, plan.topic)):
                table = table.append_column(f"{name}{plan.topic}", array)
        if table.num_columns > 1 and not self._samples.num_columns:
            table = table.drop([_BATCH_SIZE_PRESERVING_STUB_COL_NAME])
        self._rows = []
        self.estimated_bytes = 0
        return table

    def _wants(self, prefix: str, topic: str) -> bool:
        return self._wanted is None or f"{prefix}{topic}" in self._wanted


class _TopicColumns:
    """One topic's list columns while rows are added: an entry per step.

    ``with_data`` keeps the steps' payloads, for the ``data:`` column. With
    ``nested``, each step of ``data:`` and ``times:`` is itself a list, of every
    message of its slot.
    """

    def __init__(self, *, with_data: bool, nested: bool = False):
        self._with_data = with_data
        self._nested = nested
        self._start()

    def _start(self) -> None:
        self.offsets: List[int] = [0]
        # With ``nested``, where each step's messages start in ``payloads`` and
        # ``step_times``.
        self.step_offsets: List[int] = [0]
        self.payloads: List[bytes] = []
        self.step_times: List[int] = []
        self.step_valid: List[bool] = []

    def add(self, steps: _TopicSteps) -> int:
        """Add one row's steps; return the bytes of their payloads."""
        added = 0
        for step in steps:
            if isinstance(step, SlotStep):
                messages, times = step.messages, step.times
            else:
                messages, times = [step.message], [step.time]
            if self._with_data:
                for message in messages:
                    payload = b"" if message is None else message[2].data
                    self.payloads.append(payload)
                    added += len(payload)
            self.step_times.extend(times)
            self.step_offsets.append(self.step_offsets[-1] + len(times))
            self.step_valid.append(step.valid)
        self.offsets.append(self.offsets[-1] + len(steps))
        return added

    def finish(self, wants: Callable[[str], bool]) -> List[Tuple[str, pa.Array]]:
        """The wanted columns of the rows added so far; the columns start over."""
        offsets = pa.array(self.offsets, pa.int32())
        step_offsets = pa.array(self.step_offsets, pa.int32())
        columns = []
        for prefix, values, per_message in (
            (DATA_PREFIX, lambda: pa.array(self.payloads, pa.large_binary()), True),
            (TIMES_PREFIX, lambda: pa.array(self.step_times, pa.int64()), True),
            (VALID_PREFIX, lambda: pa.array(self.step_valid, pa.bool_()), False),
        ):
            if wants(prefix):
                column = values()
                if self._nested and per_message:
                    column = pa.ListArray.from_arrays(
                        step_offsets, column, type=pa.list_(column.type)
                    )
                columns.append(
                    (
                        prefix,
                        pa.ListArray.from_arrays(
                            offsets, column, type=pa.list_(column.type)
                        ),
                    )
                )
        self._start()
        return columns


class SampleRows:
    """Builds the sample rows of one read task, one file at a time.

    ``plans`` are the topics of ``read_mcap(topics=...)``, in order. Only the
    topics with a column left after projection are read, and a decoded topic
    is decoded only when its ``frames:`` column is left. A row whose payloads
    or frames pass ``max_row_bytes`` fails the read.
    """

    def __init__(
        self,
        plans: Sequence[TopicPlan],
        message_reader: SelectedMessageReader,
        finish: Callable[[pa.Table], pa.Table],
        *,
        columns: Optional[Sequence[str]],
        target_block_size: Optional[int],
        max_row_bytes: int,
        decoded_topics: Sequence[str] = (),
        resize: Optional[Tuple[int, int]] = None,
        max_lead_in_ns: int = 0,
    ):
        self._wanted = set(columns) if columns is not None else None
        self._plans = [
            plan
            for plan in plans
            if self._wanted is None
            or not self._wanted.isdisjoint(topic_column_names(plan.topic))
        ]
        self._decoded = [p.topic for p in self._plans if p.topic in decoded_topics]
        self._decodes = [
            topic
            for topic in self._decoded
            if self._wanted is None or f"{FRAMES_PREFIX}{topic}" in self._wanted
        ]
        self._message_reader = message_reader
        self._finish = finish
        self._target_block_size = target_block_size
        self._max_row_bytes = max_row_bytes
        self._resize = resize
        self._max_lead_in_ns = max_lead_in_ns

    @property
    def reads_messages(self) -> bool:
        """Whether a row needs any message: not when the projection keeps only
        the samples' own columns, as ``count()`` does."""
        return bool(self._plans)

    def tables_without_messages(self, file: SampleFile) -> Iterator[pa.Table]:
        """The rows of a read that keeps only the samples' own columns."""
        builder = SampleTableBuilder(file.samples, [], self._wanted)
        for index in range(file.samples.num_rows):
            builder.add_row(index, {})
        yield self._finish(builder.build())

    def tables(
        self,
        f: Any,
        file: SampleFile,
        summary: Optional["Summary"],
    ) -> Iterator[pa.Table]:
        """Yield the rows of the samples of one open file, in anchor order.

        ``summary`` is the file's whole summary, or ``None`` when the file has
        no chunk index. The chunks of a run of samples are read together.
        Samples whose steps' messages meet form a group, whose rows are built
        once its last sample is resolved, so each camera is decoded once per
        group. A camera's decoder may run on into the next group
        (``RunDecoder``).
        """
        timeline = self._timeline(f, file.path, summary)
        decoder = self._decoder(f, file.path, summary)
        builder = SampleTableBuilder(
            file.samples,
            self._plans,
            self._wanted,
            decoded=self._decoded,
            resize=self._resize,
        )
        group = _Group()
        runs = list(_runs(_samples_of(file), self._plans))
        try:
            for i, run in enumerate(runs):
                timeline.read_spans(run.spans)
                for sample in run.samples:
                    resolved = _Resolved.of(sample, self._steps(timeline, sample))
                    if not group.joins(resolved):
                        yield from self._add_rows(builder, decoder, timeline, group)
                        group = _Group()
                    group.add(resolved)
                if i + 1 < len(runs):
                    kept_from = self._kept_from(runs[i + 1], group, decoder)
                    timeline.forget_before(kept_from)
            yield from self._add_rows(builder, decoder, timeline, group)
        finally:
            if decoder is not None:
                decoder.close()
        if builder.num_rows > 0:
            yield self._finish(builder.build())

    def _decoder(
        self, f: Any, path: str, summary: Optional["Summary"]
    ) -> Optional[RunDecoder]:
        """The file's decoder of camera frames, when a decoded topic is built."""
        if not self._decodes:
            return None
        return RunDecoder(
            f,
            path,
            summary,
            resize=self._resize,
            max_lead_in_ns=self._max_lead_in_ns,
        )

    def _add_rows(
        self,
        builder: SampleTableBuilder,
        decoder: Optional[RunDecoder],
        timeline: Any,
        group: "_Group",
    ) -> Iterator[pa.Table]:
        """Decode a group's frames and add its rows, yielding each full table."""
        frames = None if decoder is None else self._frames(decoder, timeline, group)
        for resolved in group.members:
            sample = resolved.sample
            added = builder.add_row(sample.index, resolved.steps, frames, sample.where)
            if added > self._max_row_bytes:
                raise ValueError(
                    f"{sample.where}: its payloads and frames would exceed "
                    f"{self._max_row_bytes} bytes (RAY_DATA_MCAP_MAX_ROW_BYTES). "
                    "Shorten its windows, or shrink frames with "
                    "VideoOptions(resize=(height, width))."
                )
            if (
                self._target_block_size is not None
                and builder.estimated_bytes >= self._target_block_size
            ):
                yield self._finish(builder.build())

    def _kept_from(
        self, next_run: _Run, group: "_Group", decoder: Optional[RunDecoder]
    ) -> int:
        """The earliest log time whose chunks the reads to come may need.

        The next run starts there, unless the group still to build starts
        earlier. A decoder looks back up to the look-back cap before it.
        """
        low = next_run.low if group.low is None else min(next_run.low, group.low)
        return low - (self._max_lead_in_ns if decoder is not None else 0)

    def _timeline(
        self,
        f: Any,
        path: str,
        summary: Optional["Summary"],
    ) -> Any:
        """The file's topics, read chunk by chunk, or whole without a chunk index."""
        topics = [plan.topic for plan in self._plans]
        if summary is None:
            from ray.data.context import DataContext

            buffer_size = DataContext.get_current().streaming_read_buffer_size
            messages = self._message_reader.iter_unindexed(
                buffered(f, buffer_size),
                path,
                time_bounds=(None, None),
                log_time_order=False,
            )
            return WholeFileTimeline(messages, topics)
        whole = summary
        return ChunkTimeline(
            f, path, summary, lambda: whole, self._message_reader, topics
        )

    def _steps(self, timeline: Any, sample: _Sample) -> Dict[str, _TopicSteps]:
        """Each topic's steps for one sample."""
        steps: Dict[str, _TopicSteps] = {}
        for plan in self._plans:
            resolve = resolve_slots if plan.all_in_step else resolve_steps
            steps[plan.topic] = resolve(
                plan, timeline.view(plan.topic), sample.times, sample.where
            )
        return steps

    def _frames(
        self, decoder: RunDecoder, timeline: Any, group: "_Group"
    ) -> Dict[FrameKey, Any]:
        """The frames the group's steps take, each camera decoded once."""
        frames: Dict[FrameKey, Any] = {}
        for topic in self._decodes:
            taken = [
                step.message
                for resolved in group.members
                for step in cast(List[Step[_Entry]], resolved.steps[topic])
                if step.message is not None
            ]
            if taken:
                frames.update(decoder.frames(timeline.view(topic), taken))
        return frames


class SamplePaths:
    """Resolves the paths that samples name, as the read tasks open them.

    With ``filesystem`` every path resolves against it. Without, each path's
    filesystem comes from its URI, created once per scheme and authority, such
    as one per S3 bucket.
    """

    def __init__(self, filesystem: Optional["FileSystem"]):
        self._filesystem = filesystem
        self._by_root: Dict[Tuple[str, str], "FileSystem"] = {}

    def resolve(self, path: str) -> Tuple[str, "FileSystem"]:
        from ray.data.datasource.path_util import _resolve_paths_and_filesystem

        if self._filesystem is not None:
            (resolved,), filesystem = _resolve_paths_and_filesystem(
                [path], self._filesystem
            )
            return resolved, filesystem
        parsed = urllib.parse.urlparse(path)
        root = (parsed.scheme, parsed.netloc)
        (resolved,), filesystem = _resolve_paths_and_filesystem(
            [path], self._by_root.get(root)
        )
        self._by_root.setdefault(root, filesystem)
        return resolved, filesystem
