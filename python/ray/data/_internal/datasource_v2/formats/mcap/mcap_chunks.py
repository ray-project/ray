"""The reading of selected messages out of one MCAP file.

An indexed file is read chunk by chunk, in file order or merged by log time. A
file without a chunk index is scanned from the start. A channel or schema that
only an earlier chunk declares is read back from that chunk.
"""

import heapq
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)

from ray.data._internal.datasource_v2.formats.mcap.mcap_options import MCAPSelection
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    message_row_id,
    unindexed_message_row_id,
)

if TYPE_CHECKING:
    from mcap.records import Channel, ChunkIndex, Message, Schema
    from mcap.summary import Summary

# One selected message as the reader sees it: its schema (``None`` for a
# schema-less channel), its channel, the record, and its ``row_id``.
_Selected = Tuple[Optional["Schema"], "Channel", "Message", str]


@dataclass
class _Declared:
    """Channel and schema records known while reading one file.

    Starts as the summary's records and grows with those met inside chunks, so
    a channel declared only in an earlier chunk serves the later ones.
    ``scanned`` holds the offsets of the chunks already read back for records.
    """

    channels: Dict[int, "Channel"]
    schemas: Dict[int, "Schema"]
    scanned: Set[int]


def _scan_declarations(f: Any, chunk_index: "ChunkIndex", declared: _Declared) -> None:
    """Add the channel and schema records of one chunk to ``declared``.

    A record already in ``declared`` is not replaced. The chunk is marked as
    scanned.
    """
    from mcap.data_stream import ReadDataStream
    from mcap.records import Channel, Chunk, Schema
    from mcap.stream_reader import breakup_chunk

    declared.scanned.add(chunk_index.chunk_start_offset)
    f.seek(chunk_index.chunk_start_offset + 1 + 8)
    for record in breakup_chunk(Chunk.read(ReadDataStream(f))):
        if isinstance(record, Channel):
            declared.channels.setdefault(record.id, record)
        elif isinstance(record, Schema):
            declared.schemas.setdefault(record.id, record)


class SelectedMessageReader:
    """Reads the messages a selection keeps out of an open MCAP file.

    With ``log_time_order``, an indexed file's messages come out in ascending
    ``log_time`` order rather than in file order.
    """

    def __init__(self, selection: MCAPSelection, log_time_order: bool):
        self._selection = selection
        self._log_time_order = log_time_order

    def iter_chunks(
        self,
        f: Any,
        path: str,
        summary: "Summary",
        offsets: Optional[Set[int]],
        *,
        chunk_indexes: Optional[List["ChunkIndex"]] = None,
        selected: Optional[Set[int]] = None,
        time_bounds: Optional[Tuple[Optional[int], Optional[int]]] = None,
        log_time_order: Optional[bool] = None,
    ) -> Iterator[_Selected]:
        """Yield the selected messages of the owned chunks of an indexed file.

        Chunks are read in file order, or merged by log time with
        ``log_time_order``. ``time_bounds`` overrides the selection's time
        range. Coarse rows use it to read a window's lead-in, which lies before
        the range.
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
        declared = _Declared(dict(summary.channels), dict(summary.schemas), set())
        if not log_time_order:
            for chunk_index in chunk_indexes:
                yield from self._read_chunk(
                    f, path, summary, chunk_index, selected, declared, time_bounds
                )
            return
        yield from self._merge_chunks_by_log_time(
            f, path, summary, chunk_indexes, selected, declared, time_bounds
        )

    def candidate_chunks(
        self, summary: "Summary", selected: Set[int]
    ) -> List["ChunkIndex"]:
        """The file's chunks that may hold a selected message, in file order.

        The indexer lists exactly these, so the indexer and the reader compute
        a window's owner over the same chunks.
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
    ) -> Iterator[_Selected]:
        """Yield the selected messages of the chunks in log-time order.

        One heap holds chunk indexes, keyed by their first log time, and
        messages, keyed by theirs. A chunk is decompressed only when it reaches
        the top, so only the chunks that overlap in time are held at once. A
        message is yielded once every chunk that could precede it is expanded.
        """
        # Heap entries: (log time, kind, chunk offset, index in chunk, item).
        # ``kind`` 0 is a chunk index, 1 a message, so on a tied log time a
        # chunk is expanded before a message is yielded.
        heap: List[Tuple[int, int, int, int, Any]] = [
            (c.message_start_time, 0, c.chunk_start_offset, 0, c) for c in chunk_indexes
        ]
        heapq.heapify(heap)
        while heap:
            _, kind, offset, _, item = heapq.heappop(heap)
            if kind == 0:
                for index, selected_message in self._read_chunk(
                    f,
                    path,
                    summary,
                    item,
                    selected,
                    declared,
                    time_bounds,
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

    def _read_chunk(
        self,
        f: Any,
        path: str,
        summary: "Summary",
        chunk_index: "ChunkIndex",
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
        from mcap.data_stream import ReadDataStream
        from mcap.records import Channel, Chunk, Message, Schema
        from mcap.stream_reader import breakup_chunk

        start_time, end_time = time_bounds
        # Skip the record's opcode (1 byte) and length (8 bytes).
        f.seek(chunk_index.chunk_start_offset + 1 + 8)
        chunk = Chunk.read(ReadDataStream(f))
        index = -1
        for record in breakup_chunk(chunk):
            if isinstance(record, Message):
                index += 1
                channel, schema = self._channel_and_schema(
                    f, path, summary, chunk_index, declared, record
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
        summary: "Summary",
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
                f, summary, chunk_index, declared, channel_id=message.channel_id
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
                f, summary, chunk_index, declared, schema_id=channel.schema_id
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
        summary: "Summary",
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
                for c in summary.chunk_indexes
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
        # Back to the chunk being read.
        f.seek(chunk_index.chunk_start_offset + 1 + 8)

    def iter_unindexed(
        self,
        f: Any,
        path: str,
        time_bounds: Optional[Tuple[Optional[int], Optional[int]]] = None,
        *,
        on_message: Optional[Callable[["Message"], None]] = None,
    ) -> Iterator[_Selected]:
        """Scan a file without a chunk index from the start, in file order.

        ``log_time_order`` is not honoured for message rows: ordering by log
        time would mean holding the whole file in memory. Coarse rows sort what
        they collect. ``on_message`` sees every message record, selected or not.
        It stands in for the file-wide statistics an indexed file carries.
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
                if on_message is not None:
                    on_message(record)
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
                yield schema, channel, record, unindexed_message_row_id(path, ordinal)
