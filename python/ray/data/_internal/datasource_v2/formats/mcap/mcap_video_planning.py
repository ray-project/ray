"""Video planning: the frame type and the decoded topics of a read with ``video``.

``VideoPlanner`` reads the first in-range message of each selected channel in
the first ``_SCHEMA_SAMPLE_FILES`` sampled files. For ``message`` rows every
such channel must be video, and ``frame_type`` gives the ``frame`` column's
tensor type. For ``window`` rows, ``decoded_topics`` names the topics that get
frame columns. A sampled video channel needs a codec and its decoder::

    first in-range message of each selected channel in the sample
      not video?              -> message rows: fail, naming the topic and
                                 video_topics; window rows: not decoded
      no codec found?         -> fail
      decoder not installed?  -> fail (Pillow for JPEG/PNG, av for the rest)
    frame_type (message rows):
      resize                  -> fixed (h, w, 3)
      else decode one keyframe per channel:
        one size              -> fixed (h, w, 3)
        none, or sizes differ -> variable-shaped
    decoded_topics (window rows): video_topics + the sampled video topics

Peeking stays cheap. The search for first messages skips each chunk whose
message index names no channel still unseen. A file without an index is scanned
to its end. Reading on for a codec or a keyframe stops after
``_KEYFRAME_SCAN_MESSAGES`` of the channel's messages or
``_KEYFRAME_SCAN_RECORDS`` records.
"""

import itertools
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import decode_one
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    MCAPSelection,
    VideoOptions,
    max_lead_in_ns,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import read_summary
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    VideoCodec,
    carries_picture,
    channel_codec,
    is_keyframe,
    is_video_channel,
    require_video_channel,
)
from ray.data._internal.tensor_extensions.arrow import (
    ArrowTensorTypeV2,
    ArrowVariableShapedTensorType,
)
from ray.data._internal.util import _check_import

if TYPE_CHECKING:
    from mcap.records import Channel, Message, Schema
    from mcap.summary import Summary
    from pyarrow.fs import FileSystem

# Sampled files the planner opens. They settle the frame type, or at ``window``
# granularity which topics get frame columns.
_SCHEMA_SAMPLE_FILES = 4

# Scan budget of ``VideoPlanner._channel_payloads``: messages of the channel,
# and records of any kind. It keeps a sparse or absent channel in an unindexed
# file from turning planning into a scan of the whole file.
_KEYFRAME_SCAN_MESSAGES = 1_000
_KEYFRAME_SCAN_RECORDS = 50_000


@dataclass(frozen=True)
class _SampledVideoChannel:
    """A selected channel of a sample file, vetted as decodable video."""

    path: str
    channel_id: int
    topic: str
    codec: VideoCodec


class VideoPlanner:
    """Plans the decoded columns of a read with ``video`` from its sampled files.

    Args:
        selection: The read's topic, schema and time filters.
        filesystem: The filesystem the files are on.
        video: The read's video options.
        video_topics: The topics ``read_mcap(video_topics=...)`` lists.
        owner: The object a missing decoder's ``ImportError`` names: the
            datasource.
    """

    def __init__(
        self,
        selection: MCAPSelection,
        filesystem: "FileSystem",
        video: VideoOptions,
        video_topics: AbstractSet[str],
        owner: object,
    ):
        self._selection = selection
        self._filesystem = filesystem
        self._video = video
        self._video_topics = video_topics
        self._owner = owner

    def frame_type(self, sampled_paths: Sequence[str]) -> pa.DataType:
        """The tensor type of decoded frames, for ``message`` rows.

        Every selected channel of the sample must be decodable video, or the
        read fails here, naming the topic. The frame shape comes from
        ``resize`` when given. Otherwise it is the shape of the sampled
        channels' first keyframes when they agree. Cameras of different sizes,
        or no decodable keyframe, give a variable-shaped tensor.
        """
        channels = self._vet_video_channels(sampled_paths[:_SCHEMA_SAMPLE_FILES])
        if self._video.resize is not None:
            height, width = self._video.resize
            return ArrowTensorTypeV2((height, width, 3), pa.uint8())
        shapes = {
            shape for shape in map(self._keyframe_shape, channels) if shape is not None
        }
        if len(shapes) == 1:
            (shape,) = shapes
            return ArrowTensorTypeV2(shape, pa.uint8())
        return ArrowVariableShapedTensorType(pa.uint8(), 3)

    def decoded_topics(self, sampled_paths: Sequence[str]) -> Tuple[str, ...]:
        """The topics a decoded ``window`` row gets frame columns for, sorted.

        They are the listed ``video_topics`` and the video topics of the sample
        files. Planning fails if one of the latter has no codec that its
        ``format`` or bytes name, or no importable decoder.
        """
        topics = set(self._video_topics)
        for path in sampled_paths[:_SCHEMA_SAMPLE_FILES]:
            for channel, schema, message in self._first_messages(path):
                schema_name = schema.name if schema else None
                if is_video_channel(channel.topic, schema_name, self._video_topics):
                    self._decodable_codec(path, channel, schema, message)
                    topics.add(channel.topic)
        return tuple(sorted(topics))

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
        summary = read_summary(self._filesystem, path)
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

    def _keyframe_shape(
        self, channel: _SampledVideoChannel
    ) -> Optional[Tuple[int, ...]]:
        """The shape of the channel's first keyframe once decoded, or ``None``."""
        head = self._stream_head(channel.path, channel.channel_id, channel.codec)
        frame = decode_one(head, channel.codec, None) if head else None
        return tuple(frame.shape) if frame is not None else None

    def _vet_video_channels(self, paths: Sequence[str]) -> List[_SampledVideoChannel]:
        """Check that every selected channel of the sample files is decodable video.

        Returns the channels in the order found. Fails at the first one that is
        not, naming its topic.
        """
        channels: List[_SampledVideoChannel] = []
        for path in paths:
            for channel, schema, message in self._first_messages(path):
                schema_name = schema.name if schema else None
                require_video_channel(
                    channel.topic, schema_name, self._video_topics, path
                )
                codec = self._decodable_codec(path, channel, schema, message)
                channels.append(
                    _SampledVideoChannel(path, channel.id, channel.topic, codec)
                )
        return channels

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

    def _channel_payloads(
        self, path: str, channel_id: int, since: Optional[int] = None
    ) -> Iterator[Tuple[int, bytes]]:
        """The channel's ``(log_time, payload)`` pairs in file order.

        Messages logged before ``since`` are skipped, and so are the chunks
        that end before it. At most ``_KEYFRAME_SCAN_MESSAGES`` of the
        channel's messages and ``_KEYFRAME_SCAN_RECORDS`` records of any kind
        are read.
        """
        summary = read_summary(self._filesystem, path)
        messages_left = _KEYFRAME_SCAN_MESSAGES
        records_left = _KEYFRAME_SCAN_RECORDS
        with self._filesystem.open_input_file(path) as f:
            for record in _channel_records(f, summary, channel_id, since):
                records_left -= 1
                message = _channel_message(record, channel_id, since)
                if message is not None:
                    yield message.log_time, message.data
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
        payloads = (data for _, data in self._channel_payloads(path, channel.id))
        return channel_codec(
            schema.name if schema else None,
            channel.message_encoding,
            itertools.chain((first,), payloads),
        )

    def _stream_head(
        self, path: str, channel_id: int, codec: VideoCodec
    ) -> List[bytes]:
        """The payloads that decode the channel's first frame in ``time_range``.

        That frame comes from the last keyframe at or before it, within the
        look-back cap, or else from the first keyframe in the range. The head
        also holds the messages since the keyframe before, which may carry the
        parameter sets. Empty when no such keyframe is found within the scan
        budget.
        """
        start, end = self._selection.start_time, self._selection.end_time
        since = None if start is None else start - max_lead_in_ns()
        head: List[bytes] = []
        pending: List[bytes] = []
        for log_time, payload in self._channel_payloads(path, channel_id, since):
            if end is not None and log_time >= end:
                break
            in_range = start is None or log_time >= start
            keyframe = is_keyframe(payload, codec)
            if head and in_range and not keyframe and carries_picture(payload, codec):
                # The range's first frame decodes from the keyframe in ``head``.
                break
            pending.append(payload)
            if keyframe:
                head, pending = pending, []
                if in_range:
                    break
        return head


def _messages_by_scan(
    f: Any, selection: MCAPSelection, skip: Set[int]
) -> Iterator[Tuple["Channel", Optional["Schema"], "Message"]]:
    """Yield the selected messages in ``time_range``, scanning ``f`` from its start.

    Schema and channel records are collected as they appear. Channels in
    ``skip`` are passed over; the caller may add to ``skip`` between messages.
    """
    from mcap.records import Channel, Message, Schema
    from mcap.stream_reader import StreamReader

    schemas: Dict[int, "Schema"] = {}
    channels: Dict[int, "Channel"] = {}
    f.seek(0)
    for record in StreamReader(f).records:
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
    from mcap.data_stream import ReadDataStream
    from mcap.records import Chunk, Message
    from mcap.stream_reader import breakup_chunk

    for chunk_index in summary.chunk_indexes:
        remaining = targets - skip
        if not remaining:
            return
        if not selection.chunk_may_match(chunk_index, remaining):
            continue
        f.seek(chunk_index.chunk_start_offset + 1 + 8)
        for record in breakup_chunk(Chunk.read(ReadDataStream(f))):
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
    f: Any, summary: Optional["Summary"], channel_id: int, since: Optional[int]
) -> Iterator[Any]:
    """The records that may hold the channel's messages from ``since`` on.

    A file without a chunk index is read from its start. Otherwise only the
    chunks whose message index, when present, names the channel and that end
    at or after ``since`` are read, in order.
    """
    from mcap.data_stream import ReadDataStream
    from mcap.records import Chunk
    from mcap.stream_reader import StreamReader, breakup_chunk

    if summary is None or not summary.chunk_indexes:
        f.seek(0)
        yield from StreamReader(f).records
        return
    for chunk_index in summary.chunk_indexes:
        if chunk_index.message_index_offsets and channel_id not in (
            chunk_index.message_index_offsets
        ):
            continue
        if since is not None and chunk_index.message_end_time < since:
            continue
        f.seek(chunk_index.chunk_start_offset + 1 + 8)
        yield from breakup_chunk(Chunk.read(ReadDataStream(f)))


def _channel_message(
    record: Any, channel_id: int, since: Optional[int]
) -> Optional["Message"]:
    """``record`` if it is a message on ``channel_id`` logged at or after ``since``."""
    from mcap.records import Message

    if not isinstance(record, Message) or record.channel_id != channel_id:
        return None
    if since is not None and record.log_time < since:
        return None
    return record
