"""The planned type of the ``data`` column: decoded JSON or bytes, never a mix.

Arrow has no column type that mixes the two. ``data`` holds decoded JSON only
when every selected channel is JSON-encoded, and every read task follows the
plan. Order of the checks::

    each sampled file, in listing order:
        summary settles the selection?
          yes -> a selected channel is not JSON: binary
                 no type yet: type from its first in-range chunk
          no  -> the same checks by a scan, for up to _MAX_SCANNED_FILES files
    no type yet -> the same from summaries of up to _MAX_WALKED_FILES more
                   listed files, until one types data or gives binary
    still none  -> binary, with a warning
"""

import itertools
import logging
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
    Sequence,
    Set,
    Tuple,
)

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_message_rows import (
    decode_payload,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import MCAPSelection
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import read_summaries
from ray.data._internal.tensor_extensions.arrow import convert_to_pyarrow_array
from ray.util.debug import log_once

if TYPE_CHECKING:
    from mcap.records import Channel, ChunkIndex, Message, Schema
    from mcap.summary import Summary
    from pyarrow.fs import FileSystem

logger = logging.getLogger(__name__)

# Sampled files whose summary cannot settle the selection are scanned whole, up
# to this many per read.
_MAX_SCANNED_FILES = 4
# Listed files past the sample that planning checks, by summary only, when no
# sampled file types ``data``.
_MAX_WALKED_FILES = 256


@dataclass(frozen=True)
class _DataSignature:
    """What one file, or a run of files, says about ``data``.

    Attributes:
        has_non_json: A selected channel is not JSON-encoded, so ``data`` holds
            the payload bytes.
        value_type: Type of the first selected message in ``time_range``, if
            one was decoded.
    """

    has_non_json: bool = False
    value_type: Optional[pa.DataType] = None


def infer_data_type(
    selection: MCAPSelection,
    filesystem: "FileSystem",
    sampled_paths: Sequence[str],
    list_paths: Callable[[], Iterable[str]],
) -> Optional[pa.DataType]:
    """Type of ``data`` when it holds decoded JSON; ``None`` when it is ``binary``.

    The type is that of one message. Blocks whose structs differ are unified
    downstream.

    Args:
        selection: The read's topic, schema and time filters.
        filesystem: The filesystem the files are on.
        sampled_paths: The files sampled for the schema, in listing order.
        list_paths: Lists the read's files in listing order, with its extension
            filter. Called only when no sampled file types ``data``.

    Returns:
        The type of one decoded JSON value, or ``None`` for ``binary``.
    """
    inspector = _FileInspector(selection, filesystem)
    signature = inspector.inspect_sample(sampled_paths)
    if signature.has_non_json or signature.value_type is not None:
        return signature.value_type
    walked = _paths_past_sample(list_paths(), set(sampled_paths))
    signature = inspector.walk(walked)
    if signature.value_type is None and not signature.has_non_json:
        if log_once("mcap_schema_sample_no_selected_message"):
            logger.warning(
                "None of the %d files sampled for the schema holds a selected "
                "message (inside time_range, if set), so the data column is "
                "planned as binary; pass fewer paths or paths that start "
                "with the selected topics to have JSON payloads decoded.",
                len(sampled_paths) + len(walked),
            )
    return signature.value_type


class _FileInspector:
    """Reads the encodings of files' selected channels, and the type of one value.

    Scans at most ``_MAX_SCANNED_FILES`` files whole.
    """

    def __init__(self, selection: MCAPSelection, filesystem: "FileSystem"):
        self._selection = selection
        self._filesystem = filesystem
        self._scans_left = _MAX_SCANNED_FILES

    def inspect_sample(self, paths: Sequence[str]) -> _DataSignature:
        """Check every sampled file's encodings, and type ``data`` from one.

        The type comes from the first file in listing order with a selected
        message in ``time_range``. Files after it get the encoding check only.
        Stops at a selected channel that is not JSON. A summary that cannot be
        read fails planning.
        """
        value_type: Optional[pa.DataType] = None
        for path, summary in _summaries(self._filesystem, paths, skip_unreadable=False):
            signature = self._inspect(
                path, summary, want_type=value_type is None, may_scan=True
            )
            if signature.has_non_json:
                return signature
            if value_type is None:
                value_type = signature.value_type
        return _DataSignature(value_type=value_type)

    def walk(self, paths: List[str]) -> _DataSignature:
        """Inspect files until one types ``data`` or has a non-JSON selected channel.

        Only files whose summary settles the selection are inspected, so no file
        is scanned whole. Files whose summary cannot be read are skipped.
        """
        for path, summary in _summaries(self._filesystem, paths, skip_unreadable=True):
            signature = self._inspect(path, summary, want_type=True, may_scan=False)
            if signature.has_non_json or signature.value_type is not None:
                return signature
        return _DataSignature()

    def _inspect(
        self,
        path: str,
        summary: Optional["Summary"],
        *,
        want_type: bool,
        may_scan: bool,
    ) -> _DataSignature:
        """Inspect one file from its summary, or by a scan while the budget lasts."""
        if summary is not None and self._settles(summary):
            return self._from_summary(path, summary, want_type)
        if not may_scan or self._scans_left == 0:
            return _DataSignature()
        self._scans_left -= 1
        with self._filesystem.open_input_file(path) as f:
            return self._by_scanning(f, path, want_type)

    def _settles(self, summary: "Summary") -> bool:
        """Whether the summary alone tells which channels are selected.

        It must index the chunks, repeat the channel records and hold every
        schema ``message_types`` must check.
        """
        return (
            bool(summary.chunk_indexes)
            and bool(summary.channels)
            and self._selection.schemas_settle(summary.channels, summary.schemas)
        )

    def _from_summary(
        self, path: str, summary: "Summary", want_type: bool
    ) -> _DataSignature:
        """Check the selected channels' encodings, then type ``data`` if wanted."""
        selected = self._selection.selected_channel_ids(
            summary.channels, summary.schemas
        )
        if any(summary.channels[cid].message_encoding != "json" for cid in selected):
            return _DataSignature(has_non_json=True)
        if not want_type or not selected:
            return _DataSignature()
        return _DataSignature(
            value_type=self._type_from_chunks(path, summary, selected)
        )

    def _type_from_chunks(
        self, path: str, summary: "Summary", selected: Set[int]
    ) -> Optional[pa.DataType]:
        """Type of the first selected message in ``time_range``.

        Only the chunks that may hold one are read, in order, until one does.
        """
        candidates = [
            chunk_index
            for chunk_index in summary.chunk_indexes
            if self._selection.chunk_may_match(chunk_index, selected)
        ]
        if not candidates:
            return None
        with self._filesystem.open_input_file(path) as f:
            for chunk_index in candidates:
                message = self._first_selected_message(f, chunk_index, selected)
                if message is not None:
                    channel = summary.channels[message.channel_id]
                    return _decoded_value_type(channel, message.data, path)
        return None

    def _first_selected_message(
        self, f: Any, chunk_index: "ChunkIndex", selected: Set[int]
    ) -> Optional["Message"]:
        """The chunk's first message on a ``selected`` channel in ``time_range``."""
        from mcap.data_stream import ReadDataStream
        from mcap.records import Chunk, Message
        from mcap.stream_reader import breakup_chunk

        # Skip the record's opcode (1 byte) and length (8 bytes).
        f.seek(chunk_index.chunk_start_offset + 1 + 8)
        for record in breakup_chunk(Chunk.read(ReadDataStream(f))):
            if (
                isinstance(record, Message)
                and record.channel_id in selected
                and self._selection.in_time_range(record.log_time)
            ):
                return record
        return None

    def _by_scanning(self, f: Any, path: str, want_type: bool) -> _DataSignature:
        """Scan the file's records, stopping at a selected channel that is not JSON.

        The scan goes past the first JSON message, because a selected channel
        that is not JSON may be declared after it. Schema records are read where
        the writer put them, so ``message_types`` selects the same channels as
        the reader. A message is decoded only with ``want_type``.
        """
        from mcap.records import Channel, Message, Schema
        from mcap.stream_reader import StreamReader

        value_type: Optional[pa.DataType] = None
        schemas: Dict[int, "Schema"] = {}
        channels: Dict[int, "Channel"] = {}
        for record in StreamReader(f).records:
            if isinstance(record, Schema):
                schemas[record.id] = record
            elif isinstance(record, Channel):
                channels[record.id] = record
                if record.message_encoding != "json" and self._accepts(record, schemas):
                    return _DataSignature(has_non_json=True)
            elif (
                isinstance(record, Message)
                and want_type
                and value_type is None
                and self._selection.in_time_range(record.log_time)
            ):
                # Only a message the read returns can type ``data``. One outside
                # ``time_range`` may be malformed, and the read never decodes it.
                channel = channels.get(record.channel_id)
                if channel is not None and self._accepts(channel, schemas):
                    value_type = _decoded_value_type(channel, record.data, path)
        return _DataSignature(value_type=value_type)

    def _accepts(self, channel: "Channel", schemas: Dict[int, "Schema"]) -> bool:
        """Whether the selection keeps ``channel``, judged with its schema record."""
        schema = schemas.get(channel.schema_id) if channel.schema_id else None
        return self._selection.accepts_channel(channel, schema)


def _summaries(
    filesystem: "FileSystem", paths: Iterable[str], *, skip_unreadable: bool
) -> Iterator[Tuple[str, Optional["Summary"]]]:
    """Read the summaries of ``paths`` several at a time, yielding them in order.

    With ``skip_unreadable``, a file whose summary cannot be read is skipped.
    Otherwise its error is raised.
    """
    for path, summary_read in read_summaries(filesystem, paths, lambda path: path):
        try:
            summary = summary_read.result()
        except Exception as exc:  # noqa: BLE001 - not an MCAP file
            if not skip_unreadable:
                raise
            logger.debug("Skipping %s for the schema: %s", path, exc)
            continue
        yield path, summary


def _paths_past_sample(listed: Iterable[str], sampled: Set[str]) -> List[str]:
    """The first ``_MAX_WALKED_FILES`` listed paths that were not sampled."""
    unsampled = (path for path in listed if path not in sampled)
    return list(itertools.islice(unsampled, _MAX_WALKED_FILES))


def _decoded_value_type(channel: "Channel", data: bytes, path: str) -> pa.DataType:
    """Decode a JSON payload sampled for the schema and return its Arrow type."""
    where = f"{path} (sampled for the schema)"
    return convert_to_pyarrow_array([decode_payload(channel, data, where)], "data").type
