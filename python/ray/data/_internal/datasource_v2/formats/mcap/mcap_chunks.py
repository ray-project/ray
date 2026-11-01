"""The reading of selected messages out of one MCAP file.

An indexed file is read chunk by chunk, in file order or merged by log time.
Chunks that are read one after another and lie close together in the file are
fetched in one ranged read (``mcap_io.read_chunks``). A file without a chunk
index is scanned from the start, and sorted in memory when ordered by log time.
A channel or schema that only an earlier chunk declares is read back from that
chunk.
"""

import heapq
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)

from ray.data._internal.datasource_v2.formats.mcap.mcap_io import (
    ChunkReads,
    OnCorrupt,
    read_chunk,
    read_chunks,
    read_message_index,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import MCAPSelection
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    message_row_id,
    unindexed_message_row_id,
)

if TYPE_CHECKING:
    from mcap.records import Channel, Chunk, ChunkIndex, Message, Schema
    from mcap.summary import Summary

# One selected message as the reader sees it: its schema (``None`` for a
# schema-less channel), its channel, the record, and its ``row_id``.
_Selected = Tuple[Optional["Schema"], "Channel", "Message", str]
# A message of a file without a chunk index, with its position in the file.
_Scanned = Tuple[Optional["Schema"], "Channel", "Message", int]


def _log_time(message: _Scanned) -> int:
    return message[2].log_time


def _merge_order(chunk_index: "ChunkIndex") -> Tuple[int, int]:
    """The order in which the log-time merge expands chunks."""
    return chunk_index.message_start_time, chunk_index.chunk_start_offset


@dataclass
class _Declared:
    """Channel and schema records known while reading one file.

    Starts as the summary's records and grows with those met inside chunks, so
    a channel declared only in an earlier chunk serves the later ones.
    ``scanned`` holds the offsets of the chunks already read back for records.
    ``file_summary`` returns the file's whole summary, whose chunk indexes say
    where to read back.
    """

    channels: Dict[int, "Channel"]
    schemas: Dict[int, "Schema"]
    scanned: Set[int]
    file_summary: Callable[[], "Summary"]


def _scan_declarations(f: Any, chunk_index: "ChunkIndex", declared: _Declared) -> None:
    """Add the channel and schema records of one chunk to ``declared``.

    A record already in ``declared`` is not replaced. The chunk is marked as
    scanned. A chunk that does not decode adds nothing, so the walk goes on to
    the next earlier chunk; the task that owns it reports it. An error reading
    ``f`` still raises.
    """
    from mcap.records import Channel, Schema
    from mcap.stream_reader import breakup_chunk

    declared.scanned.add(chunk_index.chunk_start_offset)
    try:
        records = list(breakup_chunk(read_chunk(f, chunk_index)))
    except OSError:
        raise
    except Exception:  # noqa: BLE001 - a corrupt chunk declares nothing here
        return
    for record in records:
        if isinstance(record, Channel):
            declared.channels.setdefault(record.id, record)
        elif isinstance(record, Schema):
            declared.schemas.setdefault(record.id, record)


def chunk_may_hold(
    f: Any, chunk: "ChunkIndex", channel_id: int, low: int, high: int
) -> bool:
    """Whether the chunk may hold a message of the channel in ``[low, high)``.

    The chunk's Message Index records answer without decompressing it. A chunk
    written without them may hold any channel.
    """
    if not chunk.message_index_offsets:
        return True
    return channel_id in chunk.message_index_offsets and any(
        low <= log_time < high
        for log_time, _ in read_message_index(f, chunk, channel_id)
    )


class SelectedMessageReader:
    """Reads the messages a selection keeps out of an open MCAP file.

    With ``log_time_order``, an indexed file's messages come out in ascending
    ``log_time`` order rather than in file order. ``chunk_reads`` decides which
    chunks share a ranged read.
    """

    def __init__(
        self,
        selection: MCAPSelection,
        log_time_order: bool,
        chunk_reads: ChunkReads = ChunkReads(),
    ):
        self._selection = selection
        self._log_time_order = log_time_order
        self._chunk_reads = chunk_reads

    def iter_chunks(
        self,
        f: Any,
        path: str,
        summary: "Summary",
        offsets: Optional[Set[int]],
        file_summary: Optional[Callable[[], "Summary"]] = None,
        on_corrupt: Optional[OnCorrupt] = None,
        *,
        chunk_indexes: Optional[List["ChunkIndex"]] = None,
        selected: Optional[Set[int]] = None,
        time_bounds: Optional[Tuple[Optional[int], Optional[int]]] = None,
        log_time_order: Optional[bool] = None,
    ) -> Iterator[_Selected]:
        """Yield the selected messages of the owned chunks of an indexed file.

        Chunks are read in file order, or merged by log time with
        ``log_time_order``. ``summary`` may index only some of the file's
        chunks, as a read task's records do (``mcap_task_summary``). Then
        ``file_summary`` returns the whole summary, which reading back a
        declaration needs.
        ``chunk_indexes``, ``selected``, ``time_bounds`` and ``log_time_order``
        override what the summary, the selection and the reader give. A
        decoding task uses them to read a video channel's messages back from
        before its own (``mcap_video_source``).

        With ``on_corrupt``, a chunk that cannot be decoded is handed to it and
        skipped, so the file's other chunks are still read. An error reading
        ``f`` still raises.
        """
        if selected is None:
            selected = self._selection.selected_channel_ids(
                summary.channels, summary.schemas
            )
        if chunk_indexes is None:
            chunk_indexes = self.candidate_chunks(summary, selected)
        if offsets is not None:
            chunk_indexes = [
                c for c in chunk_indexes if c.chunk_start_offset in offsets
            ]
        if time_bounds is None:
            time_bounds = (self._selection.start_time, self._selection.end_time)
        if log_time_order is None:
            log_time_order = self._log_time_order
        declared = _Declared(
            dict(summary.channels),
            dict(summary.schemas),
            set(),
            file_summary or (lambda: summary),
        )
        if not log_time_order:
            chunks = read_chunks(f, chunk_indexes, self._chunk_reads, on_corrupt)
            for chunk_index, chunk in chunks:
                yield from self._decoded_messages(
                    f,
                    path,
                    summary,
                    chunk_index,
                    chunk,
                    selected,
                    declared,
                    time_bounds,
                    on_corrupt,
                )
            return
        yield from self._merge_chunks_by_log_time(
            f, path, summary, chunk_indexes, selected, declared, time_bounds, on_corrupt
        )

    def iter_chunk_messages(
        self,
        f: Any,
        path: str,
        summary: "Summary",
        chunk_indexes: List["ChunkIndex"],
        file_summary: Optional[Callable[[], "Summary"]] = None,
        *,
        selected: Optional[Set[int]] = None,
    ) -> Iterator[Tuple["ChunkIndex", List[_Selected]]]:
        """Yield each chunk with its selected messages, in file order.

        Unlike ``iter_chunks``, a chunk's messages come together and are not
        cut to the time range, so a caller can keep or drop whole chunks.
        Chunks close together in the file still share a ranged read. A chunk
        that cannot be decoded raises.
        """
        if selected is None:
            selected = self._selection.selected_channel_ids(
                summary.channels, summary.schemas
            )
        declared = _Declared(
            dict(summary.channels),
            dict(summary.schemas),
            set(),
            file_summary or (lambda: summary),
        )
        ordered = sorted(chunk_indexes, key=lambda c: c.chunk_start_offset)
        for chunk_index, chunk in read_chunks(f, ordered, self._chunk_reads):
            assert chunk is not None, "without on_corrupt, a bad chunk raises"
            messages = self._chunk_messages(
                f, path, summary, chunk_index, chunk, selected, declared, (None, None)
            )
            yield chunk_index, list(messages)

    def candidate_chunks(
        self, summary: "Summary", selected: Set[int]
    ) -> List["ChunkIndex"]:
        """The file's chunks that may hold a selected message, in file order.

        The indexer lists exactly these, so the indexer and the reader agree
        on which chunks a selection reads.
        """
        # A summary that repeats no channel record says nothing about which
        # chunk holds a selected message. Every chunk in the time range is read,
        # and the channels declared inside them are filtered as they appear.
        channels_known = bool(summary.channels)
        return sorted(
            (
                c
                for c in summary.chunk_indexes
                if (
                    self._selection.chunk_may_match(c, selected)
                    if channels_known
                    else self._selection.overlaps(
                        c.message_start_time, c.message_end_time
                    )
                )
            ),
            key=lambda c: c.chunk_start_offset,
        )

    def _merge_chunks_by_log_time(
        self,
        f: Any,
        path: str,
        summary: "Summary",
        chunk_indexes: List["ChunkIndex"],
        selected: Set[int],
        declared: _Declared,
        time_bounds: Tuple[Optional[int], Optional[int]],
        on_corrupt: Optional[OnCorrupt],
    ) -> Iterator[_Selected]:
        """Yield the selected messages of the chunks in log-time order.

        One heap holds chunk indexes, keyed by their first log time, and
        messages, keyed by theirs. A chunk is decompressed only when it reaches
        the top, so only the chunks that overlap in time are held at once. A
        message is yielded once every chunk that could precede it is expanded.
        """
        # Heap entries: (log time, kind, chunk offset, index in chunk, message).
        # ``kind`` 0 is a chunk, 1 a message, so on a tied log time a chunk is
        # expanded before a message is yielded.
        heap: List[Tuple[int, int, int, int, Any]] = [
            (c.message_start_time, 0, c.chunk_start_offset, 0, None)
            for c in chunk_indexes
        ]
        heapq.heapify(heap)
        # Chunks leave the heap in ``_merge_order``, so they are read in it.
        chunks = read_chunks(
            f, sorted(chunk_indexes, key=_merge_order), self._chunk_reads, on_corrupt
        )
        while heap:
            _, kind, offset, _, item = heapq.heappop(heap)
            if kind == 0:
                chunk_index, chunk = next(chunks)
                assert chunk_index.chunk_start_offset == offset
                for index, selected_message in self._decoded_messages(
                    f,
                    path,
                    summary,
                    chunk_index,
                    chunk,
                    selected,
                    declared,
                    time_bounds,
                    on_corrupt,
                    with_index=True,
                ):
                    heapq.heappush(
                        heap,
                        (
                            selected_message[2].log_time,
                            1,
                            offset,
                            index,
                            selected_message,
                        ),
                    )
            else:
                yield item

    def _decoded_messages(
        self,
        f: Any,
        path: str,
        summary: "Summary",
        chunk_index: "ChunkIndex",
        chunk: Optional["Chunk"],
        selected: Set[int],
        declared: "_Declared",
        time_bounds: Tuple[Optional[int], Optional[int]],
        on_corrupt: Optional[OnCorrupt],
        with_index: bool = False,
    ) -> Iterator[Any]:
        """``_chunk_messages``, or the messages before a decoding error.

        ``chunk`` is ``None`` when its record did not parse, which
        ``read_chunks`` has already handed to ``on_corrupt``. A chunk that fails
        while it is decompressed or parsed stops there and goes to
        ``on_corrupt``. Without ``on_corrupt``, or for an error reading ``f``,
        the error raises.
        """
        if chunk is None:
            return
        try:
            yield from self._chunk_messages(
                f,
                path,
                summary,
                chunk_index,
                chunk,
                selected,
                declared,
                time_bounds,
                with_index,
            )
        except OSError:
            raise
        except Exception as e:  # noqa: BLE001 - a corrupt chunk costs only itself
            if on_corrupt is None:
                raise
            on_corrupt(chunk_index, e)

    def _chunk_messages(
        self,
        f: Any,
        path: str,
        summary: "Summary",
        chunk_index: "ChunkIndex",
        chunk: "Chunk",
        selected: Set[int],
        declared: "_Declared",
        time_bounds: Tuple[Optional[int], Optional[int]],
        with_index: bool = False,
    ) -> Iterator[Any]:
        """Decompress one chunk and yield its selected messages in file order.

        Channel and schema records inside the chunk take precedence over the
        summary's. ``row_id`` counts every message record of the chunk, so it
        does not depend on the selection. With ``with_index``, each message is
        yielded as ``(index in chunk, message)``.
        """
        from mcap.records import Channel, Message, Schema
        from mcap.stream_reader import breakup_chunk

        start_time, end_time = time_bounds
        index = -1
        for record in breakup_chunk(chunk):
            if isinstance(record, Message):
                index += 1
                channel, schema = self._channel_and_schema(
                    f, path, chunk_index, declared, record
                )
                if not self._channel_is_selected(channel, schema, summary, selected):
                    continue
                if start_time is not None and record.log_time < start_time:
                    continue
                if end_time is not None and record.log_time >= end_time:
                    continue
                row_id = message_row_id(path, chunk_index.chunk_start_offset, index)
                item = (schema, channel, record, row_id)
                yield (index, item) if with_index else item
            elif isinstance(record, Channel):
                declared.channels[record.id] = record
            elif isinstance(record, Schema):
                declared.schemas[record.id] = record

    def _channel_and_schema(
        self,
        f: Any,
        path: str,
        chunk_index: "ChunkIndex",
        declared: _Declared,
        message: "Message",
    ) -> Tuple["Channel", Optional["Schema"]]:
        """Look up the channel and schema of ``message``.

        A record not met yet is read back from the earlier chunks of the file.
        """
        channel = declared.channels.get(message.channel_id)
        if channel is None:
            self._find_in_earlier_chunks(
                f, chunk_index, declared, channel_id=message.channel_id
            )
            channel = declared.channels.get(message.channel_id)
        if channel is None:
            raise ValueError(
                f"MCAP file {path!r} has a message on channel "
                f"{message.channel_id}, which neither the summary nor any "
                "chunk up to this one declares."
            )
        if channel.schema_id and channel.schema_id not in declared.schemas:
            # The schema record may be only in an earlier chunk
            # (``repeat_schemas=False``). ``message_types`` and the schema
            # columns need it.
            self._find_in_earlier_chunks(
                f, chunk_index, declared, schema_id=channel.schema_id
            )
        schema = declared.schemas.get(channel.schema_id) if channel.schema_id else None
        return channel, schema

    def _channel_is_selected(
        self,
        channel: "Channel",
        schema: Optional["Schema"],
        summary: "Summary",
        selected: Set[int],
    ) -> bool:
        """Whether messages on ``channel`` pass the selection.

        ``selected`` was computed from the summary. A channel or schema that the
        summary does not list is checked here.
        """
        if channel.id not in selected:
            if channel.id in summary.channels:
                return False
            # A channel the summary does not list was unknown to the listing.
            return self._selection.accepts_channel(channel, schema)
        if channel.schema_id and channel.schema_id not in summary.schemas:
            # The summary lists the channel but not its schema, so the listing
            # could not apply ``message_types``.
            return self._selection.accepts_channel(channel, schema)
        return True

    def _find_in_earlier_chunks(
        self,
        f: Any,
        chunk_index: "ChunkIndex",
        declared: "_Declared",
        *,
        channel_id: Optional[int] = None,
        schema_id: Optional[int] = None,
    ) -> None:
        """Read back through the chunks before ``chunk_index`` for a declaration.

        A writer may declare a channel or schema only in the first chunk that
        uses it, so a task that owns only later chunks has to read back for it.
        Records met on the way are added to ``declared``, and each chunk is read
        back at most once per file. The walk stops once the wanted records are
        known or no earlier chunk is left.
        """
        earlier = sorted(
            (
                c
                for c in declared.file_summary().chunk_indexes
                if c.chunk_start_offset < chunk_index.chunk_start_offset
                and c.chunk_start_offset not in declared.scanned
            ),
            key=lambda c: c.chunk_start_offset,
        )
        for earlier_index in earlier:
            _scan_declarations(f, earlier_index, declared)
            if (channel_id is None or channel_id in declared.channels) and (
                schema_id is None or schema_id in declared.schemas
            ):
                break

    def iter_unindexed(
        self,
        f: Any,
        path: str,
        limit: Optional[int] = None,
        *,
        time_bounds: Optional[Tuple[Optional[int], Optional[int]]] = None,
        log_time_order: Optional[bool] = None,
    ) -> Iterator[_Selected]:
        """Yield the selected messages of a file without a chunk index.

        Without ``log_time_order``, they stream in file order. With it, the file
        can only be ordered whole, so its selected messages are held in memory
        and sorted by log time. The sort is stable: equal log times keep file
        order. With ``limit``, only the ``limit`` earliest are held.

        ``log_time_order`` overrides the reader's setting, and ``time_bounds``
        the selection's time range. Sample rows stream the whole file once and
        index what they collect themselves.
        """
        if log_time_order is None:
            log_time_order = self._log_time_order
        scanned: Iterable[_Scanned] = self._scan_unindexed(f, path, time_bounds)
        if log_time_order and limit is not None:
            scanned = heapq.nsmallest(limit, scanned, key=_log_time)
        elif log_time_order:
            scanned = sorted(scanned, key=_log_time)
        for schema, channel, message, ordinal in scanned:
            yield schema, channel, message, unindexed_message_row_id(path, ordinal)

    def _scan_unindexed(
        self,
        f: Any,
        path: str,
        time_bounds: Optional[Tuple[Optional[int], Optional[int]]],
    ) -> Iterator[_Scanned]:
        """Scan a file without a chunk index from the start, in file order.

        Each message comes with its position among all messages of the file, so
        a sort holds an integer per message rather than its ``row_id``.
        """
        from mcap.records import Channel, Message, Schema
        from mcap.stream_reader import StreamReader

        if time_bounds is None:
            time_bounds = (self._selection.start_time, self._selection.end_time)
        start_time, end_time = time_bounds
        f.seek(0)
        schemas: Dict[int, "Schema"] = {}
        channels: Dict[int, "Channel"] = {}
        ordinal = -1
        for record in StreamReader(f).records:
            if isinstance(record, Schema):
                schemas[record.id] = record
            elif isinstance(record, Channel):
                channels[record.id] = record
            elif isinstance(record, Message):
                ordinal += 1
                channel = channels.get(record.channel_id)
                if channel is None:
                    raise ValueError(
                        f"MCAP file {path!r} has a message on channel "
                        f"{record.channel_id} before that channel is declared."
                    )
                schema = schemas.get(channel.schema_id) if channel.schema_id else None
                if not self._selection.accepts_channel(channel, schema):
                    continue
                if start_time is not None and record.log_time < start_time:
                    continue
                if end_time is not None and record.log_time >= end_time:
                    continue
                yield schema, channel, record, ordinal
