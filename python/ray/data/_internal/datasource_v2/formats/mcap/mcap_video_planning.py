"""Video planning: which topics of a sample read with ``video`` are decoded.

``VideoPlanner`` reads the first message of each selected channel in the first
``_SCHEMA_SAMPLE_FILES`` sampled files. ``decoded_topics`` names the topics that
get ``frames:`` columns: the listed ``video_topics`` and the sampled video
topics. A sampled video topic needs a codec and its decoder::

    first message of each selected channel in the sample
      not video?              -> not decoded
      no codec found?         -> fail
      decoder not installed?  -> fail (Pillow for JPEG/PNG, av for the rest)
    decoded_topics: video_topics + the sampled video topics

Peeking stays cheap. Each sampled summary is read once. The search for first
messages skips each chunk whose message index names no channel still unseen. A
file without an index is scanned to its end, through a read buffer. Reading on
for a codec stops after ``_KEYFRAME_SCAN_MESSAGES`` of the channel's messages or
``_KEYFRAME_SCAN_RECORDS`` records. A sampled file whose summary or records
cannot be read is skipped, so it fails its own read task rather than planning.
"""

import itertools
import logging
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
)

from ray.data._internal.datasource_v2.formats.mcap.mcap_io import buffered, read_chunk
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import MCAPSelection
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import read_summaries
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    VideoCodec,
    channel_codec,
    is_video_channel,
)
from ray.data._internal.util import _check_import

if TYPE_CHECKING:
    from mcap.records import Channel, ChunkIndex, Message, Schema
    from mcap.summary import Summary
    from pyarrow.fs import FileSystem

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# Sampled files the planner opens. They settle which topics get frame columns.
_SCHEMA_SAMPLE_FILES = 4

# Scan budget of ``VideoPlanner._channel_payloads``: messages of the channel,
# and records of any kind. It keeps a sparse or absent channel in an unindexed
# file from turning planning into a scan of the whole file.
_KEYFRAME_SCAN_MESSAGES = 1_000
_KEYFRAME_SCAN_RECORDS = 50_000


class _UnreadableRecords(Exception):
    """A sampled file's chunk or record stream does not parse."""


class VideoPlanner:
    """Plans the decoded topics of a sample read with ``video`` from its sampled
    files.

    Args:
        selection: The read's topic filter.
        filesystem: The filesystem the files are on.
        video_topics: The topics ``read_mcap(video_topics=...)`` lists.
        owner: The object a missing decoder's ``ImportError`` names: the
            datasource.
    """

    def __init__(
        self,
        selection: MCAPSelection,
        filesystem: "FileSystem",
        video_topics: AbstractSet[str],
        owner: object,
    ):
        self._selection = selection
        self._filesystem = filesystem
        self._video_topics = video_topics
        self._owner = owner
        # The readable sampled files' summaries, read once each.
        self._summaries: Dict[str, Optional["Summary"]] = {}

    def decoded_topics(self, sampled_paths: Sequence[str]) -> Tuple[str, ...]:
        """The topics a decoded sample row gets frame columns for, sorted.

        They are the listed ``video_topics`` and the video topics of the sample
        files. Planning fails if one of the latter has no codec that its
        ``format`` or bytes name, or no importable decoder.
        """
        topics = set(self._video_topics)
        for path in self._readable(sampled_paths[:_SCHEMA_SAMPLE_FILES]):
            topics.update(self._unless_unreadable(path, self._file_video_topics))
        return tuple(sorted(topics))

    def _file_video_topics(self, path: str) -> List[str]:
        """The video topics of one sampled file, each checked to be decodable."""
        topics = []
        for channel, schema, message in self._first_messages(path):
            schema_name = schema.name if schema else None
            if is_video_channel(channel.topic, schema_name, self._video_topics):
                self._decodable_codec(path, channel, schema, message)
                topics.append(channel.topic)
        return topics

    def _readable(self, paths: Sequence[str]) -> Iterator[str]:
        """``paths`` less the files whose summary cannot be read.

        The summaries are read several at a time and kept for the reads that
        follow. An unreadable file is skipped, as ``inspect_sample`` skips it.
        """
        for path, summary_read in read_summaries(self._filesystem, paths, str):
            try:
                self._summaries[path] = summary_read.result()
            except Exception as exc:  # noqa: BLE001 - not a readable MCAP file
                logger.debug("Skipping %s for the video plan: %s", path, exc)
                continue
            yield path

    def _unless_unreadable(
        self, path: str, plan_file: Callable[[str], List[_T]]
    ) -> List[_T]:
        """``plan_file(path)``, or nothing if the file's records cannot be read."""
        try:
            return plan_file(path)
        except _UnreadableRecords as exc:
            logger.debug("Skipping %s for the video plan: %s", path, exc.__cause__)
            return []

    def _first_messages(
        self, path: str
    ) -> Iterator[Tuple["Channel", Optional["Schema"], "Message"]]:
        """Yield the first message in ``time_range`` of each selected channel.

        Only those messages are ever read, so a channel with none in
        ``time_range`` is not yielded. In an indexed file, chunks are read in
        order until every selected channel has been seen, skipping those whose
        message index names none still missing. A file whose summary lacks the
        schema records is scanned until then, and a file without an index is
        scanned to its end.
        """
        summary = self._summaries[path]
        seen: Set[int] = set()
        targets: Optional[Set[int]] = None
        with self._filesystem.open_input_file(path) as f:
            if summary is None or not summary.chunk_indexes or not summary.channels:
                # A scan learns the channels as it goes, so it cannot stop early.
                messages = _messages_by_scan(f, self._selection, skip=seen)
            else:
                targets = self._selection.selected_channel_ids(
                    summary.channels, summary.schemas
                )
                if _lacks_schemas(summary, targets):
                    # The schema records sit in the chunks, where a scan meets them.
                    messages = _messages_by_scan(f, self._selection, skip=seen)
                else:
                    messages = _messages_by_index(
                        f, summary, self._selection, targets, skip=seen
                    )
            for channel, schema, message in messages:
                seen.add(channel.id)
                yield channel, schema, message
                if targets is not None and seen >= targets:
                    return

    def _decodable_codec(
        self,
        path: str,
        channel: "Channel",
        schema: Optional["Schema"],
        message: "Message",
    ) -> VideoCodec:
        """A video channel's codec, checked to have an importable decoder.

        Fails when neither the ``format`` field nor the bytes name a codec. A
        VP9 or AV1 inter frame names none, so a file that starts mid-GOP is
        read on to its first keyframe.
        """
        codec = self._channel_codec(path, channel, schema, message.data)
        if codec is None:
            raise ValueError(
                f"Cannot decode topic {channel.topic!r} in {path!r}: its "
                "payloads are not JPEG, PNG, H.264/H.265 Annex-B, VP9 or AV1."
            )
        if codec.every_frame_is_a_keyframe:
            _check_import(self._owner, module="PIL", package="Pillow")
        else:
            _check_import(self._owner, module="av", package="av")
        return codec

    def _channel_payloads(self, path: str, channel_id: int) -> Iterator[bytes]:
        """The channel's payloads in file order.

        At most ``_KEYFRAME_SCAN_MESSAGES`` of the channel's messages and
        ``_KEYFRAME_SCAN_RECORDS`` records of any kind are read.
        """
        summary = self._summaries[path]
        messages_left = _KEYFRAME_SCAN_MESSAGES
        records_left = _KEYFRAME_SCAN_RECORDS
        with self._filesystem.open_input_file(path) as f:
            for record in _channel_records(f, summary, channel_id):
                records_left -= 1
                message = _channel_message(record, channel_id)
                if message is not None:
                    yield message.data
                    messages_left -= 1
                if messages_left <= 0 or records_left <= 0:
                    return

    def _channel_codec(
        self,
        path: str,
        channel: "Channel",
        schema: Optional["Schema"],
        first: bytes,
    ) -> Optional[VideoCodec]:
        """The codec of a video channel (:func:`channel_codec`).

        ``first`` is the channel's first payload. The file is read on, within
        the scan budget, only if neither its ``format`` nor its bytes settle
        the codec.
        """
        return channel_codec(
            schema.name if schema else None,
            channel.message_encoding,
            itertools.chain((first,), self._channel_payloads(path, channel.id)),
        )


def _messages_by_scan(
    f: Any, selection: MCAPSelection, skip: Set[int]
) -> Iterator[Tuple["Channel", Optional["Schema"], "Message"]]:
    """Yield the selected messages in ``time_range``, scanning ``f`` from its start.

    Schema and channel records are collected as they appear. Channels in
    ``skip`` are passed over; the caller may add to ``skip`` between messages.
    """
    from mcap.records import Channel, Message, Schema

    schemas: Dict[int, "Schema"] = {}
    channels: Dict[int, "Channel"] = {}
    for record in _scan(f):
        if isinstance(record, Schema):
            schemas[record.id] = record
        elif isinstance(record, Channel):
            channels[record.id] = record
        elif isinstance(record, Message):
            channel = channels.get(record.channel_id)
            if channel is None or channel.id in skip:
                continue
            if not selection.in_time_range(record.log_time):
                continue
            schema = schemas.get(channel.schema_id) if channel.schema_id else None
            if selection.accepts_channel(channel, schema):
                yield channel, schema, record


def _messages_by_index(
    f: Any,
    summary: "Summary",
    selection: MCAPSelection,
    targets: Set[int],
    skip: Set[int],
) -> Iterator[Tuple["Channel", Optional["Schema"], "Message"]]:
    """Yield the messages in ``time_range`` of ``targets`` channels not in ``skip``.

    Chunks are read in order. A chunk is read only if it overlaps
    ``time_range`` and its message index, when present, names a target not
    yet in ``skip``. The walk ends once every target is in ``skip``; the
    caller may add to ``skip`` between messages.
    """
    from mcap.records import Message

    for chunk_index in summary.chunk_indexes:
        remaining = targets - skip
        if not remaining:
            return
        if not selection.chunk_may_match(chunk_index, remaining):
            continue
        for record in _chunk_records(f, chunk_index):
            if (
                isinstance(record, Message)
                and record.channel_id in targets
                and record.channel_id not in skip
                and selection.in_time_range(record.log_time)
            ):
                channel = summary.channels[record.channel_id]
                schema = (
                    summary.schemas.get(channel.schema_id)
                    if channel.schema_id
                    else None
                )
                yield channel, schema, record


def _lacks_schemas(summary: "Summary", channel_ids: Set[int]) -> bool:
    """Whether the summary lacks the schema record of one of the channels.

    A writer with ``repeat_schemas=False`` leaves them in the chunks only.
    """
    return any(
        summary.channels[channel_id].schema_id
        and summary.channels[channel_id].schema_id not in summary.schemas
        for channel_id in channel_ids
    )


def _channel_records(
    f: Any, summary: Optional["Summary"], channel_id: int
) -> Iterator[Any]:
    """The records that may hold the channel's messages.

    A file without a chunk index is read from its start. Otherwise only the
    chunks whose message index, when present, names the channel are read, in
    order.
    """
    if summary is None or not summary.chunk_indexes:
        yield from _scan(f)
        return
    for chunk_index in summary.chunk_indexes:
        if chunk_index.message_index_offsets and channel_id not in (
            chunk_index.message_index_offsets
        ):
            continue
        yield from _chunk_records(f, chunk_index)


def _chunk_records(f: Any, chunk_index: "ChunkIndex") -> List[Any]:
    """The records of one chunk, read in one request."""
    from mcap.stream_reader import breakup_chunk

    try:
        return breakup_chunk(read_chunk(f, chunk_index))
    except OSError:
        raise  # an I/O failure, not a corrupt file
    except Exception as exc:
        raise _UnreadableRecords() from exc


def _scan(f: Any) -> Iterator[Any]:
    """The records of ``f`` from its start, read through a buffer of
    ``DataContext.streaming_read_buffer_size`` bytes."""
    from mcap.stream_reader import StreamReader

    from ray.data.context import DataContext

    f.seek(0)
    stream = buffered(f, DataContext.get_current().streaming_read_buffer_size)
    try:
        yield from StreamReader(stream).records
    except OSError:
        raise  # an I/O failure, not a corrupt file
    except Exception as exc:
        raise _UnreadableRecords() from exc


def _channel_message(record: Any, channel_id: int) -> Optional["Message"]:
    """``record`` if it is a message on ``channel_id``."""
    from mcap.records import Message

    if not isinstance(record, Message) or record.channel_id != channel_id:
        return None
    return record
