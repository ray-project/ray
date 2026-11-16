"""Sample rows: one row per sample, with each topic's steps around its anchor.

``MCAPReader`` hands this module a task's samples one file at a time, in anchor
order (``read_mcap(samples=...)``). A ``ChunkTimeline`` reads the chunks the
steps need. Samples whose windows meet form a run, and a run's chunks are read
in one coalesced pass. A run ends before its chunks would hold more than a
read task's budget, so they can be let go. A window's extent is its
``TopicReach``. A step that reaches further, such as one in a gap of the
recording, reads on chunk by chunk, nearest first. ``mcap_samples`` decides
which message fills each step, and ``SampleTableBuilder`` lays the rows out::

    one file's samples, by anchor      a1   a2   a3              a4
    their windows, all topics       [-----]
                                         [-----]
                                              [-----]          [-----]
    runs                            [   run 1          ]       [run 2]
    chunks read                     c3   c4   c5   c6          c9  c10

A file without a chunk index is scanned once per task, keeping only the
messages the samples' steps can take (``_ScanKeep``).
"""

import bisect
import heapq
import itertools
import urllib.parse
from dataclasses import dataclass, field
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Protocol,
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
    PictureFilter,
    RunDecoder,
    frame_key,
    warn_undecoded_video,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_io import buffered
from ray.data._internal.datasource_v2.formats.mcap.mcap_samples import (
    CLIP_END_COLUMN,
    CLIP_START_COLUMN,
    DATA_PREFIX,
    FRAMES_PREFIX,
    IS_PAD_PREFIX,
    SAMPLE_ID_COLUMN,
    TIMES_PREFIX,
    SampleTimes,
    SlotStep,
    SortedTimes,
    Step,
    TopicPlan,
    TopicTimes,
    resolve_slots,
    resolve_steps,
    sample_anchors,
    topic_column_names,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    channel_ids_by_topic,
    estimate_chunk_rows,
    topic_chunks,
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
    from mcap.records import Channel, ChunkIndex, Message, Schema
    from mcap.summary import Summary
    from pyarrow.fs import FileSystem


class _Entry(NamedTuple):
    """A message as a sample row holds it."""

    schema: Optional["Schema"]
    channel: "Channel"
    message: "Message"


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

# A group of decoded samples ends before its frames pass this many blocks
# (``target_max_block_size``). Overlapping 2 s clips of 720p video without
# resize: 1x 0.93 GiB RSS in 5.1 s, 2x 1.05 GiB in 4.2 s, 4x 1.27 GiB in 3.85 s.
FRAME_BUDGET_BLOCKS = 2

# How many chunks a step that reaches past the chunks read so far reads at a
# time, nearest first.
_CHUNKS_PER_REACH = 4
# Bytes a ``ChunkTimeline`` holds per message beyond the message's record: the
# parsed ``Message``, its ``_Entry`` and sort key, and its pair in
# ``SortedTimes``. Measured at 470 with CPython 3.12 and mcap 1.x.
_HELD_MESSAGE_BYTES = 480
# A row's estimated Arrow bytes besides its payloads: per step its int64 log
# time and its padding flag, and per topic the offsets of its list columns.
_STEP_BYTES = 9
_TOPIC_LIST_BYTES = 16


class _Timeline(Protocol):
    """The sample topics' messages of one open file, as ``SampleRows`` reads
    them: a run's spans first, then each topic's steps."""

    def view(self, topic: str) -> TopicTimes[_Entry]:
        ...

    def read_spans(self, spans: Dict[str, Tuple[int, int]]) -> None:
        ...

    def forget_before(self, time: int) -> None:
        ...


# The bytes each chunk that may hold a topic in ``[low, high]`` holds once
# read, by offset (``ChunkTimeline.chunk_bytes``).
_ChunkBytes = Callable[[str, int, int], Dict[int, int]]


class _TopicView:
    """One topic of a ``ChunkTimeline``, as ``resolve_steps`` asks for it."""

    def __init__(self, timeline: "ChunkTimeline", topic: str):
        self._timeline = timeline
        self._topic = topic

    def all_in(self, low: int, high: int) -> List[Tuple[int, _Entry]]:
        # Steps ask only inside the reach, which ``read_spans`` has read, but a
        # decoder also asks from a keyframe before it (``RunDecoder``).
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

    The summary's chunk indexes tell which chunks may hold a topic
    (``topic_chunks``). A chunk is read once and kept until ``forget_before`` lets it go.
    Each topic's messages are held sorted by log time, then by their place in
    the file, so equal log times keep the order they were recorded in. A
    chunk that cannot be decoded raises.
    """

    def __init__(
        self,
        f: Any,
        path: str,
        summary: "Summary",
        reader: SelectedMessageReader,
        topics: Sequence[str],
    ):
        self._f = f
        self._path = path
        self._summary = summary
        self._reader = reader
        self._topics = set(topics)
        by_topic = channel_ids_by_topic(summary)
        topic_ids = {topic: by_topic.get(topic, set()) for topic in topics}
        self._selected = set().union(*topic_ids.values())
        # Each topic's candidate chunks (``topic_chunks``), by first log time.
        self._candidates = {
            topic: sorted(
                topic_chunks(summary, ids),
                key=lambda c: (c.message_start_time, c.chunk_start_offset),
            )
            for topic, ids in topic_ids.items()
        }
        # Per topic, the latest last log time of its candidates up to each one.
        self._max_ends = {
            topic: list(itertools.accumulate((c.message_end_time for c in chunks), max))
            for topic, chunks in self._candidates.items()
        }
        self._total_uncompressed = sum(
            c.uncompressed_size for c in summary.chunk_indexes
        )
        # Chunk offset -> its index record and, per topic, its messages.
        self._chunks: Dict[int, "ChunkIndex"] = {}
        self._loaded: Dict[int, Dict[str, List[Tuple[_Key, _Entry]]]] = {}
        self._held: Dict[str, SortedTimes[_Entry]] = {}

    def view(self, topic: str) -> _TopicView:
        return _TopicView(self, topic)

    def chunk_bytes(self, topic: str, low: int, high: int) -> Dict[int, int]:
        """The bytes each chunk that may hold the topic in ``[low, high]`` holds
        once read, by offset: its records, plus ``_HELD_MESSAGE_BYTES`` for
        each message it is estimated to hold (``estimate_chunk_rows``)."""
        statistics = self._summary.statistics
        return {
            c.chunk_start_offset: c.uncompressed_size
            + _HELD_MESSAGE_BYTES
            * estimate_chunk_rows(c, statistics, self._total_uncompressed)
            for c in self._overlapping(topic, low, high)
        }

    def held(self, topic: str) -> SortedTimes[_Entry]:
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

    def _overlapping(self, topic: str, low: int, high: int) -> List["ChunkIndex"]:
        """The topic's candidate chunks that overlap ``[low, high]``."""
        candidates = self._candidates.get(topic, [])
        # Candidates are sorted by first log time, so those starting after
        # ``high`` are cut off at once. Those before the first whose running
        # max of last log times reaches ``low`` all end before ``low``.
        begin = bisect.bisect_left(self._max_ends.get(topic, []), low)
        end = bisect.bisect_right(candidates, high, key=lambda c: c.message_start_time)
        return [c for c in candidates[begin:end] if c.message_end_time >= low]

    def _unread(self, topic: str, low: int, high: int) -> List["ChunkIndex"]:
        """The topic's candidate chunks not read yet that overlap ``[low, high]``."""
        return [
            c
            for c in self._overlapping(topic, low, high)
            if c.chunk_start_offset not in self._loaded
        ]

    def _read(self, chunks: List["ChunkIndex"]) -> None:
        if not chunks:
            return
        for chunk, messages in self._reader.iter_chunk_messages(
            self._f,
            self._path,
            self._summary,
            chunks,
            selected=self._selected,
        ):
            offset = chunk.chunk_start_offset
            by_topic: Dict[str, List[Tuple[_Key, _Entry]]] = {}
            for position, (schema, channel, message, _) in enumerate(messages):
                if channel.topic in self._topics:
                    key = (message.log_time, offset, position)
                    by_topic.setdefault(channel.topic, []).append(
                        (key, _Entry(schema, channel, message))
                    )
            self._chunks[offset] = chunk
            self._loaded[offset] = by_topic
        self._held.clear()


class WholeFileTimeline:
    """The sample topics' messages of a file without a chunk index, from one
    scan in file order.

    Each topic keeps only the messages its samples' steps can take
    (``_ScanKeep``), so what is held follows the task's samples, not the file.
    """

    def __init__(self, messages: Iterator[_Selected], keeps: Dict[str, "_ScanKeep"]):
        for ordinal, (schema, channel, message, _) in enumerate(messages):
            keep = keeps.get(channel.topic)
            if keep is not None:
                key = (message.log_time, ordinal)
                keep.offer(key, _Entry(schema, channel, message))
        self._held = {topic: SortedTimes(keep.kept()) for topic, keep in keeps.items()}

    def view(self, topic: str) -> SortedTimes[_Entry]:
        return self._held[topic]

    def read_spans(self, spans: Dict[str, Tuple[int, int]]) -> None:
        pass

    def forget_before(self, time: int) -> None:
        pass


# A message a scan keeps, with its key: its log time, then its place in the scan.
_Keyed = Tuple[Tuple[int, int], _Entry]


class _ScanKeep:
    """The messages of one topic that a scan keeps for a task's runs.

    ``resolve_steps`` asks only for messages inside a sample's ``bounds``:
    every one inside its reach, the ``count`` nearest before or after the
    reach, perhaps across a gap, and with ``pad="edge"`` the first or last one
    inside the bounds. So the scan keeps every message inside a run's span, the
    ``count`` nearest on each side of each span, and, in the gaps, the first
    message from the start of a sample's bounds and the last up to their end::

        bounds     [                                         ]
        spans               [=====]           [====]
        kept       f  ...  k[ all ]k  ...    k[all ]k  ...  l
    """

    def __init__(
        self,
        spans: List[Tuple[int, int]],
        count: int,
        edges: Iterable[Tuple[int, bool]],
    ):
        """``spans`` may overlap and come in any order. ``edges`` are the
        bounds' starts, with ``True``, and ends, with ``False``."""
        self._spans = _merge_spans(spans)
        self._lows = [low for low, _ in self._spans]
        self._count = count
        self._inside: List[_Keyed] = []
        # Gap ``g`` lies before span ``g``. Per gap, the ``count`` earliest
        # messages, as a heap of negated keys, and the ``count`` latest.
        self._earliest: List[List[Tuple[Tuple[int, int], _Keyed]]] = [
            [] for _ in range(len(self._spans) + 1)
        ]
        self._latest: List[List[_Keyed]] = [[] for _ in range(len(self._spans) + 1)]
        self._edges: Dict[int, List[Tuple[int, bool]]] = {}
        for edge in sorted(set(edges)):
            gap = self._gap(edge[0])
            if gap is not None:
                self._edges.setdefault(gap, []).append(edge)
        # Per bounds' start, the first message at or after it, and per end,
        # the last at or before it.
        self._at_edge: Dict[Tuple[int, bool], _Keyed] = {}

    @classmethod
    def of(
        cls, plan: TopicPlan, runs: Sequence["_Run"], samples: Sequence["_Sample"]
    ) -> "_ScanKeep":
        """What a scan keeps of ``plan``'s topic for ``runs`` of ``samples``."""
        counted = not plan.clip and plan.fps is None and plan.span_ns is None
        count = plan.num_steps if counted and plan.num_steps else 1
        edges = []
        for sample in samples:
            first, last = plan.bounds(sample.times)
            edges += [(first, True), (last, False)]
        return cls([run.spans[plan.topic] for run in runs], count, edges)

    @classmethod
    def everything(cls) -> "_ScanKeep":
        """Keeps every message, as a decoded topic needs: its decoder feeds the
        messages back to a keyframe, and between groups, not only its steps'."""
        return cls([(-1, 2**64)], 1, [])

    def offer(self, key: Tuple[int, int], entry: _Entry) -> None:
        """Keep the message if a step may take it."""
        time = key[0]
        gap = self._gap(time)
        if gap is None:
            self._inside.append((key, entry))
            return
        if gap > 0:
            negated = (-key[0], -key[1])
            _keep_largest(self._earliest[gap], (negated, (key, entry)), self._count)
        if gap < len(self._spans):
            _keep_largest(self._latest[gap], (key, entry), self._count)
        for edge, start in self._edges.get(gap, ()):
            best = self._at_edge.get((edge, start))
            if start and time >= edge and (best is None or key < best[0]):
                self._at_edge[(edge, start)] = (key, entry)
            if not start and time <= edge and (best is None or key > best[0]):
                self._at_edge[(edge, start)] = (key, entry)

    def kept(self) -> List[Tuple[int, _Entry]]:
        """The kept messages by log time, those of equal log time in scan order."""
        found = dict(self._inside)
        for latest in self._latest:
            found.update(latest)
        for earliest in self._earliest:
            found.update(keyed for _, keyed in earliest)
        found.update(self._at_edge.values())
        return [(key[0], found[key]) for key in sorted(found)]

    def _gap(self, time: int) -> Optional[int]:
        """The gap ``time`` lies in, or ``None`` inside a span."""
        gap = bisect.bisect_right(self._lows, time)
        if gap and time <= self._spans[gap - 1][1]:
            return None
        return gap


def _merge_spans(spans: Iterable[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """The union of the spans ``[low, high]``, as sorted spans that neither
    overlap nor touch.

    A run can stretch back over earlier runs, such as a long clip after
    short ones, so the runs' spans may overlap and come out of order.
    """
    merged: List[Tuple[int, int]] = []
    for low, high in sorted(spans):
        if merged and low <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], high))
        else:
            merged.append((low, high))
    return merged


def _keep_largest(heap: List[Any], item: Any, count: int) -> None:
    """Push ``item`` onto the min-heap ``heap``, keeping its ``count`` largest."""
    if len(heap) < count:
        heapq.heappush(heap, item)
    elif item > heap[0]:
        heapq.heapreplace(heap, item)


@dataclass(frozen=True)
class _Sample:
    """One sample of a task: its row in the file's manifest, and its times.

    ``where`` names the sample and its file.
    """

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


@dataclass
class _Run:
    """Samples of one file whose windows meet, each topic's span over them, and
    the bytes of the chunks the spans overlap (``chunk_bytes``)."""

    samples: List[_Sample] = field(default_factory=list)
    spans: Dict[str, Tuple[int, int]] = field(default_factory=dict)
    # The chunks' offsets, and the bytes they hold in all once read.
    chunks: Set[int] = field(default_factory=set)
    num_bytes: int = 0

    @property
    def low(self) -> int:
        return min(low for low, _ in self.spans.values())

    @property
    def high(self) -> int:
        return max(high for _, high in self.spans.values())

    def new_chunks(
        self, reached: Dict[str, Tuple[int, int]], chunk_bytes: Optional[_ChunkBytes]
    ) -> Dict[int, int]:
        """The chunks the spans would newly overlap if they stretched over
        ``reached``, with their bytes."""
        added: Dict[int, int] = {}
        if chunk_bytes is None:
            return added
        for topic, (low, high) in reached.items():
            old = self.spans.get(topic)
            regions = (
                [(low, high)]
                if old is None
                else [(low, old[0] - 1), (old[1] + 1, high)]
            )
            for region_low, region_high in regions:
                if region_low > region_high:
                    continue
                chunks = chunk_bytes(topic, region_low, region_high)
                for offset, size in chunks.items():
                    if offset not in self.chunks:
                        added[offset] = size
        return added

    def takes(
        self,
        reached: Dict[str, Tuple[int, int]],
        added: Dict[int, int],
        max_bytes: Optional[int],
    ) -> bool:
        """Whether a sample that reaches ``reached`` and adds the chunks
        ``added`` joins the run: its windows meet the run's, and the chunks stay
        within ``max_bytes``."""
        if min(low for low, _ in reached.values()) > self.high + 1:
            return False
        return max_bytes is None or self.num_bytes + sum(added.values()) <= max_bytes

    def add(
        self,
        sample: _Sample,
        reached: Dict[str, Tuple[int, int]],
        added: Dict[int, int],
    ) -> None:
        self.samples.append(sample)
        for topic, (low, high) in reached.items():
            old_low, old_high = self.spans.get(topic, (low, high))
            self.spans[topic] = (min(old_low, low), max(old_high, high))
        self.chunks.update(added)
        self.num_bytes += sum(added.values())


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


def _taken(resolved: _Resolved, topic: str) -> List[_Entry]:
    """The messages a decoded topic's steps take for one sample."""
    # Planning refuses all_in_step on a decoded topic, so its steps are plain.
    steps = cast(List[Step[_Entry]], resolved.steps.get(topic, []))
    return [step.message for step in steps if step.message is not None]


@dataclass(frozen=True)
class _FrameBudget:
    """The most bytes a group's decoded frames may hold.

    Each frame of a decoded topic counts ``frame_bytes(topic)``, which is
    ``None`` until the topic's frame size is known.
    """

    topics: Sequence[str]
    max_bytes: int
    frame_bytes: Callable[[str], Optional[int]]

    def fits(self, frames: Dict[str, Set[FrameKey]], resolved: _Resolved) -> bool:
        """Whether the group's ``frames`` and those ``resolved`` takes fit.

        Frames of a topic whose size is not known yet fit only a group of one
        sample, whose decode makes the size known.
        """
        total = 0
        for topic in self.topics:
            held = frames.get(topic, set())
            taken = {frame_key(e) for e in _taken(resolved, topic)}
            count = len(held) + len(taken - held)
            if count == 0:
                continue
            size = self.frame_bytes(topic)
            if size is None:
                return False
            total += count * size
        return total <= self.max_bytes


class _Group:
    """Resolved samples whose steps' messages meet, whose rows are built together.

    A group that ``decodes`` ends before the frames it decodes would pass its
    ``budget``. One that does not holds one sample, since its rows share no
    decode.
    """

    def __init__(self, *, decodes: bool, budget: Optional[_FrameBudget] = None):
        self.members: List[_Resolved] = []
        self.low: Optional[int] = None
        self._high: Optional[int] = None
        self._decodes = decodes
        self._budget = budget
        # The frames each decoded topic's steps take, by ``frame_key``.
        self._frames: Dict[str, Set[FrameKey]] = {}

    def joins(self, resolved: _Resolved) -> bool:
        """Whether ``resolved`` can join the group: it is empty, or the group
        decodes, the sample's messages start before the group's end and its
        frames fit."""
        if self._high is None:
            return True
        if not self._decodes or resolved.low > self._high:
            return False
        return self._budget is None or self._budget.fits(self._frames, resolved)

    def add(self, resolved: _Resolved) -> None:
        self.members.append(resolved)
        self.low = resolved.low if self.low is None else min(self.low, resolved.low)
        self._high = (
            resolved.high if self._high is None else max(self._high, resolved.high)
        )
        if self._budget is not None:
            for topic in self._budget.topics:
                self._frames.setdefault(topic, set()).update(
                    frame_key(e) for e in _taken(resolved, topic)
                )


def _first_entry(steps: _TopicSteps) -> Optional[_Entry]:
    """The first message that ``steps`` take, if any."""
    for step in steps:
        if isinstance(step, SlotStep):
            if step.messages:
                return step.messages[0]
        elif step.message is not None:
            return step.message
    return None


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


class TopicReach:
    """The span of log time each topic's steps of a sample are read over first,
    in one file.

    It is ``TopicPlan.reach``, with ``steps.last`` and ``steps.next`` stretched
    over the time the topic takes on average to log that many messages, from
    the summary's statistics, within the window's bounds. The reader groups
    samples into runs by it, and sizing read tasks counts the chunks it
    overlaps. A step past it is still read, chunk by chunk.
    """

    def __init__(self, summary: Optional["Summary"]):
        self._statistics = None if summary is None else summary.statistics
        self._channels = {} if summary is None else channel_ids_by_topic(summary)

    def channel_ids(self, topic: str) -> Set[int]:
        """The ids of the topic's channels in the file's summary."""
        return self._channels.get(topic, set())

    def of(self, plan: TopicPlan, sample: SampleTimes) -> Tuple[int, int]:
        """The span ``[low, high]`` the topic's steps of ``sample`` reach."""
        low, high = plan.reach(sample)
        span = self._counted_span(plan)
        if span is None:
            return low, high
        first, last = plan.bounds(plan.measured(sample))
        if plan.before:
            return max(low - span, first), high
        return low, min(high + span, last)

    def _counted_span(self, plan: TopicPlan) -> Optional[int]:
        """The mean time the topic takes to log ``steps.last`` or ``steps.next``'s
        messages, in nanoseconds, or ``None`` for other steps or without
        statistics."""
        counted = not plan.clip and plan.fps is None and plan.span_ns is None
        statistics = self._statistics
        if not counted or plan.num_steps is None or statistics is None:
            return None
        count = sum(
            statistics.channel_message_counts.get(channel_id, 0)
            for channel_id in self.channel_ids(plan.topic)
        )
        duration = statistics.message_end_time - statistics.message_start_time
        if count < 2 or duration <= 0:
            return None
        return duration * plan.num_steps // (count - 1)


def _runs(
    samples: List[_Sample],
    plans: Sequence[TopicPlan],
    reach: TopicReach,
    chunk_bytes: Optional[_ChunkBytes],
    max_bytes: Optional[int],
) -> Iterator[_Run]:
    """Group consecutive samples whose windows meet or touch, over every topic.

    Back-to-back clips touch: one ends the nanosecond before the next starts.
    A run is read in one pass, so such clips read like one long window. With
    ``max_bytes``, a run also ends before its chunks would hold more
    (``chunk_bytes``), so they can be let go. A sample whose chunks hold more on
    their own is a run alone.
    """
    run = _Run()
    for sample in samples:
        reached = {plan.topic: reach.of(plan, sample.times) for plan in plans}
        added = run.new_chunks(reached, chunk_bytes)
        if run.samples and not run.takes(reached, added, max_bytes):
            yield run
            run = _Run()
            added = run.new_chunks(reached, chunk_bytes)
        run.add(sample, reached, added)
    if run.samples:
        yield run


class SampleTableBuilder:
    """Accumulates the sample rows of one file and builds Arrow tables of them.

    A row is the sample's own columns, taken from its manifest row, then the
    wanted columns of each topic. A decoded topic has ``frames:`` in place of
    ``data:`` (``FrameColumns``), whose black frames take ``frame_size`` of the
    topic when a row has no frame. Only the wanted columns are built.
    """

    def __init__(
        self,
        samples: pa.Table,
        plans: Sequence[TopicPlan],
        wanted: Optional[Set[str]],
        *,
        decoded: Sequence[str] = (),
        resize: Optional[Tuple[int, int]] = None,
        frame_size: Optional[Callable[[str], Optional[Tuple[int, int]]]] = None,
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
            plan.topic: FrameColumns(plan, resize, frame_size)
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
                # its steps keep their padding flags.
                built = frames if self._wants(FRAMES_PREFIX, plan.topic) else None
                added += columns.add(plain, built, where)
            else:
                added += columns.add(topic_steps)
        self.estimated_bytes += (
            self._sample_row_bytes
            + added
            + sum(
                _STEP_BYTES * len(topic_steps) + _TOPIC_LIST_BYTES
                for topic_steps in steps.values()
            )
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
        self.step_is_pad: List[bool] = []

    def add(self, steps: _TopicSteps) -> int:
        """Add one row's steps; return the bytes of their payloads."""
        added = 0
        for step in steps:
            if isinstance(step, SlotStep):
                messages, times = step.messages, step.times
            else:
                messages, times = [step.message], [step.time]
            if self._with_data:
                for entry in messages:
                    payload = b"" if entry is None else entry.message.data
                    self.payloads.append(payload)
                    added += len(payload)
            self.step_times.extend(times)
            self.step_offsets.append(self.step_offsets[-1] + len(times))
            self.step_is_pad.append(step.is_pad)
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
            (IS_PAD_PREFIX, lambda: pa.array(self.step_is_pad, pa.bool_()), False),
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
    or frames pass ``max_row_bytes`` fails the read. A run of samples holds at
    most ``max_run_bytes`` of chunks (``_runs``), unless one sample needs more.
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
        max_run_bytes: Optional[int] = None,
        decoded_topics: Sequence[str] = (),
        resize: Optional[Tuple[int, int]] = None,
        max_lead_in_ns: int = 0,
        decodes_video: bool = False,
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
        # With ``video``, the topics left undecoded, checked for a video schema
        # planning did not see (``warn_undecoded_video``).
        self._undecoded = [
            p.topic
            for p in self._plans
            if decodes_video and p.topic not in self._decoded
        ]
        self._message_reader = message_reader
        self._finish = finish
        self._target_block_size = target_block_size
        self._max_row_bytes = max_row_bytes
        self._max_run_bytes = max_run_bytes
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
    ) -> Generator[pa.Table, None, None]:
        """Yield the rows of the samples of one open file, in anchor order.

        ``summary`` is the file's whole summary, or ``None`` when the file has
        no chunk index. The chunks of a run of samples are read together, and
        let go of once the reads to come start after them. A chunk that cannot
        be decoded fails the file.

        With decoding, samples whose steps' messages meet form a group, whose
        rows are built once its last sample is resolved, so each camera is
        decoded once per group. A group's decoded frames stay within
        ``FRAME_BUDGET_BLOCKS`` blocks (``_FrameBudget``). A camera's decoder
        may run on into the next group (``RunDecoder``).
        """
        timeline, runs = self._timeline_and_runs(f, file, summary)
        decoder = self._decoder(f, file.path, summary)
        pictures = PictureFilter() if self._decoded else None
        builder = SampleTableBuilder(
            file.samples,
            self._plans,
            self._wanted,
            decoded=self._decoded,
            resize=self._resize,
            frame_size=None if decoder is None else decoder.frame_size,
        )
        group = self._new_group(decoder)
        try:
            for i, run in enumerate(runs):
                timeline.read_spans(run.spans)
                for sample in run.samples:
                    resolved = self._resolve(timeline, sample, pictures, file.path)
                    if not group.joins(resolved):
                        yield from self._add_rows(builder, decoder, timeline, group)
                        group = self._new_group(decoder)
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

    def _resolve(
        self,
        timeline: _Timeline,
        sample: _Sample,
        pictures: Optional[PictureFilter],
        path: str,
    ) -> _Resolved:
        """Resolve one sample's steps."""
        steps = self._steps(timeline, sample, pictures)
        self._check_undecoded(steps, path)
        return _Resolved.of(sample, steps)

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

    def _new_group(self, decoder: Optional[RunDecoder]) -> "_Group":
        """An empty group. Without decoding it holds one sample. With decoding,
        its frames are bounded by ``FRAME_BUDGET_BLOCKS`` blocks."""
        if decoder is None:
            return _Group(decodes=False)
        if self._target_block_size is None:
            return _Group(decodes=True)
        frame_bytes = partial(self._frame_bytes, decoder)
        max_bytes = FRAME_BUDGET_BLOCKS * self._target_block_size
        budget = _FrameBudget(self._decodes, max_bytes, frame_bytes)
        return _Group(decodes=True, budget=budget)

    def _frame_bytes(self, decoder: RunDecoder, topic: str) -> Optional[int]:
        """The bytes of one RGB frame of ``topic``, at ``resize`` or at the size
        its frames decode to in the file so far; ``None`` before one decodes."""
        size = self._resize or decoder.frame_size(topic)
        return None if size is None else size[0] * size[1] * 3

    def _add_rows(
        self,
        builder: SampleTableBuilder,
        decoder: Optional[RunDecoder],
        timeline: Any,
        group: "_Group",
    ) -> Iterator[pa.Table]:
        """Decode a group's frames and add its rows, yielding each full table."""
        frames = None if decoder is None else self._frames(decoder, timeline, group)
        for i, resolved in enumerate(group.members):
            sample = resolved.sample
            built = None if frames is None else frames[i]
            added = builder.add_row(sample.index, resolved.steps, built, sample.where)
            if added > self._max_row_bytes:
                raise self._row_too_large(sample)
            if (
                self._target_block_size is not None
                and builder.estimated_bytes >= self._target_block_size
            ):
                yield self._finish(builder.build())

    def _row_too_large(self, sample: _Sample) -> ValueError:
        """The error that fails a read on a row past ``max_row_bytes``."""
        return ValueError(
            f"{sample.where}: its payloads and frames would exceed "
            f"{self._max_row_bytes} bytes (RAY_DATA_MCAP_MAX_ROW_BYTES). "
            "Shorten its windows, or shrink frames with "
            "VideoOptions(resize=(height, width))."
        )

    def _check_frame_bytes(self, decoder: RunDecoder, group: _Group) -> None:
        """Fail on a sample of ``group`` whose frames alone pass
        ``max_row_bytes``, at the frame sizes known so far."""
        for resolved in group.members:
            frame_bytes = sum(
                len(resolved.steps[topic]) * (self._frame_bytes(decoder, topic) or 0)
                for topic in self._decodes
            )
            if frame_bytes > self._max_row_bytes:
                raise self._row_too_large(resolved.sample)

    def _kept_from(
        self, next_run: _Run, group: "_Group", decoder: Optional[RunDecoder]
    ) -> int:
        """The earliest log time whose chunks the reads to come may need.

        The next run starts there, unless the group still to build starts
        earlier. A decoder looks back up to the look-back cap before it.
        """
        low = next_run.low if group.low is None else min(next_run.low, group.low)
        return low - (self._max_lead_in_ns if decoder is not None else 0)

    def _timeline_and_runs(
        self,
        f: Any,
        file: SampleFile,
        summary: Optional["Summary"],
    ) -> Tuple[_Timeline, List[_Run]]:
        """The file's topics and its runs of samples.

        With a chunk index the topics are read chunk by chunk, and runs end at
        ``max_run_bytes``. Without one, the file is scanned once for what the
        runs need.
        """
        samples = _samples_of(file)
        reach = TopicReach(summary)
        if summary is not None:
            topics = [plan.topic for plan in self._plans]
            chunked = ChunkTimeline(f, file.path, summary, self._message_reader, topics)
            sizes, max_bytes = chunked.chunk_bytes, self._max_run_bytes
            runs = list(_runs(samples, self._plans, reach, sizes, max_bytes))
            return chunked, runs
        from ray.data.context import DataContext

        runs = list(_runs(samples, self._plans, reach, None, None))
        keeps = {
            plan.topic: _ScanKeep.everything()
            if plan.topic in self._decodes
            else _ScanKeep.of(plan, runs, samples)
            for plan in self._plans
        }
        buffer_size = DataContext.get_current().streaming_read_buffer_size
        messages = self._message_reader.iter_unindexed(
            buffered(f, buffer_size), file.path, log_time_order=False
        )
        return WholeFileTimeline(messages, keeps), runs

    def _steps(
        self, timeline: _Timeline, sample: _Sample, pictures: Optional[PictureFilter]
    ) -> Dict[str, _TopicSteps]:
        """Each topic's steps for one sample. A decoded topic's steps match
        only the messages that hold a picture."""
        steps: Dict[str, _TopicSteps] = {}
        for plan in self._plans:
            resolve = resolve_slots if plan.all_in_step else resolve_steps
            times = timeline.view(plan.topic)
            if pictures is not None and plan.topic in self._decoded:
                times = pictures.times(times)
            steps[plan.topic] = resolve(plan, times, sample.times, sample.where)
        return steps

    def _check_undecoded(self, steps: Dict[str, _TopicSteps], path: str) -> None:
        """Warn about an undecoded topic of a video schema, once per topic: its
        first message in ``steps`` names the schema."""
        for topic in list(self._undecoded):
            entry = _first_entry(steps.get(topic, []))
            if entry is not None:
                warn_undecoded_video(topic, entry, path)
                self._undecoded.remove(topic)

    def _frames(
        self, decoder: RunDecoder, timeline: Any, group: "_Group"
    ) -> List[Dict[FrameKey, Any]]:
        """Each member's frames, by ``frame_key``: each camera is decoded once
        for the group.

        A row whose frames pass ``max_row_bytes`` fails before they decode, or
        as soon as its first frame shows their size.
        """
        self._check_frame_bytes(decoder, group)
        check = partial(self._check_frame_bytes, decoder, group)
        frames: List[Dict[FrameKey, Any]] = [{} for _ in group.members]
        for topic in self._decodes:
            taken = [_taken(resolved, topic) for resolved in group.members]
            if any(taken):
                decoded = decoder.frames(timeline.view(topic), taken, check)
                for member, member_frames in zip(frames, decoded):
                    member.update(member_frames)
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
        """The path and filesystem a read task opens ``path`` with.

        Raises ``ValueError`` naming ``path`` when it does not resolve, such as
        a malformed URI or a scheme whose filesystem is not installed.
        """
        from ray.data.datasource.path_util import _resolve_paths_and_filesystem

        parsed = urllib.parse.urlparse(path)
        root = (parsed.scheme, parsed.netloc)
        given = self._filesystem or self._by_root.get(root)
        # The helper logs a path it cannot resolve and returns no paths.
        resolved, filesystem = _resolve_paths_and_filesystem([path], given)
        if not resolved:
            raise ValueError(
                f"Cannot resolve the sample path {path!r}; see the preceding "
                "warning for the cause."
            )
        if self._filesystem is None:
            self._by_root.setdefault(root, filesystem)
        return resolved[0], filesystem
