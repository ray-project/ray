"""Decoded message rows: one row per video frame, decoded in the read task.

Each video channel keeps one decoder for the whole task (``_FrameChannel``).
Before the channel's first owned message, and before each one that follows a
gap, the decoder is primed with the messages ``mcap_video_source`` reads back,
from their last keyframe, and their frames are dropped. An owned message that
is a keyframe needs none of them. Without a keyframe in reach, as in a lead-in
or a gap cut short by the look-back cap, the channel is cold: its pictures are
skipped until the next keyframe. Each frame the decoder releases becomes the
row of the owned message with its log time. Every other owned message that
carries a picture is a row too: one whose frame does not decode gets a null
``frame``. So the rows are the same with or without the ``frame`` column, and
without it nothing is decoded::

    what to read               decoder per camera            rows
    (mcap_video_source)        (_FrameChannel)               (_MessageTableBuilder)

    lead-in before the    ->   prime: decode from the
    first owned message        last keyframe, drop output
    owned messages, in    ->   feed -> FrameDecoder      ->  frame -> the row of the
    log-time order                                           message with its log time
    another task's        ->   prime again at each gap
    messages (a gap)
"""

import functools
import itertools
import logging
from dataclasses import dataclass, replace
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Generator,
    List,
    Optional,
    Sequence,
    Set,
)

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import (
    FrameDecoder,
    warn_cold_channel,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_message_rows import (
    _MessageTableBuilder,
    decoded_frame_type,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    MCAPSelection,
    VideoOptions,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    NalFormat,
    VideoCodec,
    carries_picture,
    channel_codec,
    is_keyframe,
    require_video_channel,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_video_source import (
    SelectedMessage,
    VideoMessageSource,
)
from ray.util.debug import log_once

if TYPE_CHECKING:
    from mcap.records import Message
    from mcap.summary import Summary

logger = logging.getLogger(__name__)

# Messages a channel holds for frames the decoder has not released. A decoder
# holds back a few frames. A message whose frame never comes, such as one the
# codec dropped without an error, gets a null frame once this many newer ones
# are held.
_MAX_OWED = 32


def last_keyframe(
    messages: Sequence[SelectedMessage], codec: VideoCodec
) -> Optional[int]:
    """The index of the last message a decoder can start on, if any."""
    for index in range(len(messages) - 1, -1, -1):
        if is_keyframe(messages[index].message.data, codec):
            return index
    return None


def look_back_start(
    messages: Sequence[SelectedMessage], first: "Message", codec: VideoCodec
) -> Optional[int]:
    """The index in ``messages`` where decoding starts so that ``first`` decodes.

    ``messages`` are the look-back before ``first``, the stream's next message.
    Decoding starts at the last keyframe at or before ``first``. When ``first``
    is a keyframe, as every still is, that is ``len(messages)``: the look-back
    needs no decoding. ``None`` if no keyframe is in reach.
    """
    if is_keyframe(first.data, codec):
        return len(messages)
    return last_keyframe(messages, codec)


@dataclass(frozen=True)
class DecodedRowSettings:
    """The reader options decoded message rows are built with. Each field is
    the ``MCAPReader`` argument of the same name."""

    selection: MCAPSelection
    video: VideoOptions
    video_topics: FrozenSet[str]
    include_metadata: bool
    include_row_id: bool
    columns: Optional[List[str]]
    target_block_size: Optional[int]
    max_lead_in_ns: int


def _take_pending(
    waiting: Dict[int, List[SelectedMessage]], log_time: int
) -> Optional[SelectedMessage]:
    """Pop the oldest message still owed a frame at ``log_time``, if any."""
    queue = waiting.get(log_time)
    if not queue:
        return None
    item = queue.pop(0)
    if not queue:
        del waiting[log_time]
    return item


def _take_oldest(waiting: Dict[int, List[SelectedMessage]]) -> SelectedMessage:
    """Pop the first-held message still owed a frame."""
    item = _take_pending(waiting, next(iter(waiting)))
    assert item is not None
    return item


@dataclass(frozen=True)
class _FrameSettings:
    """What every video channel of a decoded message task works with."""

    path: str
    video: VideoOptions
    video_topics: FrozenSet[str]
    # Whether frames are decoded. Without the ``frame`` column a channel only
    # builds the rows, without their frames.
    decode: bool
    max_lead_in_ns: int


class _FrameChannel:
    """One video channel of a decoded message task.

    The channel is cold, its pictures skipped, until a keyframe is in reach.
    Each decoded frame becomes the row of the owned message with its log time,
    whatever order the decoder releases frames in: B-frames come out in
    display order. An owned picture whose frame does not come, because the
    codec rejects it or the decoder never releases it, gets a null frame.
    Without ``decode`` no decoder is built, and each owned picture of a warm
    channel is a row, so both modes give the same rows.

    ``read_parameter_sets`` reads the stream's parameter sets where it starts
    (``StreamStart.read``), for a decoder whose first keyframe comes without
    them. Length-prefixed H.264 and H.265 payloads are read in Annex-B form
    (``NalFormat``).
    """

    def __init__(
        self,
        settings: _FrameSettings,
        rows: _MessageTableBuilder,
        read_parameter_sets: Callable[[VideoCodec], bytes],
        topic: str,
    ):
        self._settings = settings
        self._rows = rows
        self._topic = topic
        self._read_parameter_sets = read_parameter_sets
        self.codec: Optional[VideoCodec] = None
        # Gives the payloads as Annex-B access units, once the codec is known.
        self._nal_format: Optional[NalFormat] = None
        self.decoder: Optional[FrameDecoder] = None
        # Whether a decoder would have a keyframe to work from at this point.
        self.decodable = False
        # The task's messages whose frames have not come out yet, by log time
        # (a list, since two messages may share one), oldest first.
        self._owed: Dict[int, List[SelectedMessage]] = {}
        # Log times of the primed messages, whose frames are dropped if the
        # decoder releases them late.
        self._primed: Set[int] = set()
        # Log time of the last owned message, where a gap would start.
        self.last_time = 0

    @property
    def owed(self) -> int:
        """How many owned messages still wait for their frame."""
        return sum(len(queue) for queue in self._owed.values())

    def decoding_start(
        self, item: SelectedMessage, look_back: Sequence[SelectedMessage]
    ) -> Optional[int]:
        """The log time decoding starts at so that ``item`` decodes, given the
        ``look_back`` before it read so far, or ``None`` with no keyframe in
        reach (``VideoMessageSource.lead_in``)."""
        codec = self._find_codec(item, look_back)
        if codec is None:
            return None
        look_back = [self._as_annex_b(m) for m in look_back]
        start = look_back_start(look_back, self._as_annex_b(item).message, codec)
        if start is None:
            return None
        if start == len(look_back):
            return item.message.log_time
        return look_back[start].message.log_time

    def prime(
        self,
        item: SelectedMessage,
        primer: Sequence[SelectedMessage],
        *,
        gap: bool,
        clipped: bool = False,
    ) -> None:
        """Feed the decoder the messages before ``item``.

        ``primer`` is the lead-in before the channel's first owned message, or
        the skipped messages of a gap between two owned ones. Decoding starts
        at the last keyframe at or before ``item``, so a keyframe ``item``
        needs only the primer's parameter sets. A lead-in without a keyframe
        leaves the channel cold until its next keyframe, since frames decoded
        without their reference would be rejected or concealed. A gap without
        one is fed whole, so the decoder's references stay continuous, unless
        it is clipped: then the channel goes cold too. The primer's own frames
        are dropped, but a frame of ours that the decoder held back comes out
        and is kept.
        """
        codec = self._sniff_codec(item, primer)
        if codec is None:
            # No codec yet: cold until a payload names one.
            self.decodable = False
            return
        primer = [self._as_annex_b(m) for m in primer]
        keyframe_at = look_back_start(primer, self._as_annex_b(item).message, codec)
        self._update_decodable(
            item, primer, codec, keyframe_at, gap=gap, clipped=clipped
        )
        if self._settings.decode:
            self._feed_primer(primer, codec, keyframe_at, whole=gap and not clipped)

    def _update_decodable(
        self,
        item: SelectedMessage,
        primer: Sequence[SelectedMessage],
        codec: VideoCodec,
        keyframe_at: Optional[int],
        *,
        gap: bool,
        clipped: bool,
    ) -> None:
        """Settle whether the decoder has a keyframe to work from after ``primer``.

        A keyframe in the primer, or a codec of stills, makes the channel
        decodable. A lead-in without one leaves it cold, and so does a clipped
        gap. Either warns, unless a lead-in holds no picture, as at the start of
        a recording. Any other gap keeps the state.
        """
        if keyframe_at is not None or codec.every_frame_is_a_keyframe:
            self.decodable = True
        elif not gap or clipped:
            self.decodable = False
            if clipped or any(carries_picture(m.message.data, codec) for m in primer):
                warn_cold_channel(
                    item.channel.topic,
                    self._settings.path,
                    self._settings.max_lead_in_ns,
                    item.message.log_time,
                )

    def _feed_primer(
        self,
        primer: Sequence[SelectedMessage],
        codec: VideoCodec,
        keyframe_at: Optional[int],
        *,
        whole: bool,
    ) -> None:
        """Decode the primer to set up the decoder, dropping its own frames.

        Decoding starts at ``keyframe_at``. Without a keyframe the primer is fed
        whole if ``whole``, and otherwise only its parameter sets. Only frames
        of owed messages are converted to RGB.
        """
        decoder = self._decoder_for(codec)
        if keyframe_at is not None:
            start: Optional[int] = keyframe_at
        elif whole:
            start = 0
        else:
            start = None
        # Parameter sets written on their own are fed whatever the start, so
        # the keyframe that follows has them.
        fed = {
            index
            for index, m in enumerate(primer)
            if (start is not None and index >= start)
            or not carries_picture(m.message.data, codec)
        }
        self._primed.update(primer[index].message.log_time for index in fed)
        for index in sorted(fed):
            message = primer[index].message
            for log_time, frame in decoder.decode(message, self._owed.__contains__):
                self._emit(log_time, frame)

    def feed(self, item: SelectedMessage) -> bool:
        """Decode one owned message into the row of its frame.

        Without a decoder, the message is a row when it holds a picture.
        Returns ``False`` when the message is skipped because the channel is
        cold: its codec is unknown, or it is a picture with no keyframe to
        decode against.
        """
        codec = self._sniff_codec(item)
        if codec is None:
            return False
        item = self._as_annex_b(item)
        message = item.message
        has_picture = carries_picture(message.data, codec)
        # A cold channel skips pictures until its next keyframe. Its parameter
        # sets are still fed to the decoder.
        if has_picture and not self.decodable:
            if not is_keyframe(message.data, codec):
                return False
            self.decodable = True
        if not self._settings.decode:
            if has_picture:
                self._rows.add(item)
            return True
        decoder = self._decoder_for(codec)
        # Parameter sets written on their own yield no picture: no row, and no
        # claim on the frame of a keyframe stamped alike.
        if has_picture:
            self._owed.setdefault(message.log_time, []).append(item)
        for log_time, frame in decoder.decode(message):
            self._emit(log_time, frame)
        if has_picture and (codec.every_frame_is_a_keyframe or decoder.rejected_last):
            # A still comes out at once, and a rejected packet never does.
            self._give_up_on(item)
        self._give_up_beyond(_MAX_OWED)
        return True

    def flush(self) -> None:
        """Drain the decoder at the end of the task. The messages whose frames
        never came out get null frames."""
        if self.decoder is not None:
            for log_time, frame in self.decoder.flush():
                self._emit(log_time, frame)
        self._give_up_beyond(0)

    def _find_codec(
        self, item: SelectedMessage, context: Sequence[SelectedMessage] = ()
    ) -> Optional[VideoCodec]:
        """The channel's codec, from ``item``'s ``format`` or the bytes.

        A VP9 or AV1 inter frame does not name its codec in its bytes, so the
        ``context`` before it is tried too, latest first: the previous keyframe
        sits there.
        """
        if self.codec is not None:
            return self.codec
        schema, channel, message = item.schema, item.channel, item.message
        codec = channel_codec(
            schema.name if schema else None,
            channel.message_encoding,
            itertools.chain(
                (message.data,), (m.message.data for m in reversed(context))
            ),
        )
        if codec is not None:
            self.codec = codec
            self._nal_format = NalFormat(codec)
        return codec

    def _sniff_codec(
        self, item: SelectedMessage, context: Sequence[SelectedMessage] = ()
    ) -> Optional[VideoCodec]:
        """The channel's codec (``_find_codec``). A channel whose codec nothing
        names yet stays cold until a later payload, its next keyframe, does."""
        codec = self._find_codec(item, context)
        if codec is None and log_once(f"mcap_codec_pending:{item.channel.topic}"):
            logger.warning(
                "The codec of video topic %r in %r cannot be told from its "
                "first payloads; its frames are skipped until a keyframe "
                "identifies it.",
                item.channel.topic,
                self._settings.path,
            )
        return codec

    def _as_annex_b(self, item: SelectedMessage) -> SelectedMessage:
        """``item`` with its payload as an Annex-B access unit (``NalFormat``)."""
        assert self._nal_format is not None
        message = item.message
        data = self._nal_format.annex_b(message.data)
        if data is message.data:
            return item
        return item._replace(message=replace(message, data=data))

    def _decoder_for(self, codec: VideoCodec) -> FrameDecoder:
        """The channel's decoder, built on first use."""
        if self.decoder is None:
            self.decoder = FrameDecoder(
                codec,
                resize=self._settings.video.resize,
                read_parameter_sets=self._read_parameter_sets,
                topic=self._topic,
                path=self._settings.path,
            )
        return self.decoder

    def _emit(self, log_time: int, frame: Any) -> None:
        """Make a decoded frame the row of the owed message with its log time.

        A frame stamped with a primed time belongs to another task's row and
        is dropped. So is one whose message no longer waits, such as one that
        already got a null frame: it is never given to another message.
        """
        source = _take_pending(self._owed, log_time)
        if source is not None:
            self._rows.add(source, frame)
        elif log_time not in self._primed:
            _warn_unowed_frame(self._settings.path, log_time)

    def _give_up_on(self, item: SelectedMessage) -> None:
        """Give ``item`` a null frame if it still waits for one."""
        queue = self._owed.get(item.message.log_time, [])
        if any(held is item for held in queue):
            queue[:] = [held for held in queue if held is not item]
            if not queue:
                del self._owed[item.message.log_time]
            self._rows.add(item, None)

    def _give_up_beyond(self, limit: int) -> None:
        """Give the first-held messages null frames until at most ``limit`` wait."""
        excess = self.owed - limit
        for _ in range(excess):
            self._rows.add(_take_oldest(self._owed), None)


def _warn_unowed_frame(path: str, log_time: int) -> None:
    """Warn once that a decoded frame matched no message waiting for one."""
    if log_once("mcap_unowed_frame"):
        logger.warning(
            "A video frame decoded from %r at log_time %d matches no message "
            "waiting for a frame, so it is dropped. Its message's row, if any, "
            "has a null frame.",
            path,
            log_time,
        )


class _FrameChannels:
    """The video channels of a decoded message task, primed as they come up."""

    def __init__(
        self,
        settings: _FrameSettings,
        rows: _MessageTableBuilder,
        source: VideoMessageSource,
    ):
        self._settings = settings
        self._rows = rows
        self._source = source
        self._channels: Dict[int, _FrameChannel] = {}
        # The channels that have a decoder, in the order the decoders were
        # built, which is the order ``flush`` drains them in.
        self._decoding: Dict[int, _FrameChannel] = {}

    @property
    def owed(self) -> int:
        """How many owned messages still wait for their frame, in all channels."""
        return sum(channel.owed for channel in self._decoding.values())

    def feed(self, item: SelectedMessage) -> bool:
        """Feed an owned message to its channel, priming the channel where needed.

        A channel is primed with its lead-in at its first owned message. When
        another task's chunk may hold the channel between two owned messages,
        it is primed again with the messages it skipped, if any. Returns
        ``False`` when the message is skipped because the channel is cold. A
        channel that is not video fails the read, as it fails planning.
        """
        channel_id, log_time = item.channel.id, item.message.log_time
        channel = self._channels.get(channel_id)
        if channel is None:
            channel = self._channels[channel_id] = self._new_channel(item)
            lead_in = self._source.lead_in(
                channel_id, log_time, functools.partial(channel.decoding_start, item)
            )
            channel.prime(item, lead_in, gap=False)
        elif self._source.may_have_gap(channel_id, channel.last_time, log_time):
            gap = self._source.gap(
                channel_id,
                channel.last_time,
                log_time,
                functools.partial(channel.decoding_start, item),
            )
            if gap.messages or gap.clipped:
                channel.prime(item, gap.messages, gap=True, clipped=gap.clipped)
        channel.last_time = log_time
        fed = channel.feed(item)
        if channel.decoder is not None:
            self._decoding.setdefault(channel_id, channel)
        return fed

    def _new_channel(self, item: SelectedMessage) -> _FrameChannel:
        """A channel for ``item``'s, checked to be video."""
        schema, record = item.schema, item.channel
        require_video_channel(
            record.topic,
            schema.name if schema else None,
            self._settings.video_topics,
            self._settings.path,
        )
        return _FrameChannel(
            self._settings,
            self._rows,
            functools.partial(self._source.stream_start.read, record.id, record.topic),
            record.topic,
        )

    def flush(self) -> None:
        """Drain every decoder at the end of the task."""
        for channel in self._decoding.values():
            channel.flush()

    def close(self) -> None:
        """Release every decoder, also when the task stops early."""
        for channel in self._channels.values():
            if channel.decoder is not None:
                channel.decoder.close()


class DecodedMessageRows:
    """Builds the decoded frame rows of one read task, one row per frame."""

    def __init__(
        self, settings: DecodedRowSettings, message_reader: SelectedMessageReader
    ):
        self._settings = settings
        self._message_reader = message_reader

    def tables(
        self,
        f: Any,
        path: str,
        offsets: Optional[Set[int]],
        summary: Optional["Summary"],
        finish: Callable[[pa.Table, int], pa.Table],
        extra_row_bytes: int,
        limit: Optional[int] = None,
    ) -> Generator[pa.Table, None, None]:
        """Decode the task's video messages into one row per frame.

        A task rarely starts on a keyframe. Each channel's decoder is first fed
        the frames back to the last keyframe before its first message (the
        lead-in), unless that message is a keyframe, so every emitted frame is
        complete. The lead-in's frames are dropped before RGB conversion. The
        messages another task owns between two of ours are fed the same way.
        If ``frame`` is not projected, nothing is decoded, and the rows are
        those a decoded read gives.

        The video source plans with the file's whole summary, since the
        lead-ins and gaps lie in chunks the task does not own. ``summary`` is
        ``None`` for a file read without its index. ``offsets`` are the chunks
        the task owns, ``None`` for the whole file. ``finish`` completes each
        table, given the rows before it, and ``extra_row_bytes`` is what it
        adds per row. No message is fed once the rows built and the frames
        still owed reach ``limit``, though the last table may hold more rows.
        """
        wanted = (
            set(self._settings.columns) if self._settings.columns is not None else None
        )
        rows = self._new_builder(extra_row_bytes, wanted)
        source = VideoMessageSource(
            self._message_reader,
            self._settings.selection,
            self._settings.max_lead_in_ns,
            f,
            path,
            offsets,
            summary,
        )
        channels = _FrameChannels(self._frame_settings(path, wanted), rows, source)
        rows_before = 0
        try:
            for item in source.owned():
                if channels.feed(item) and self._fills_block(rows):
                    num_rows = rows.num_rows
                    # ``build`` empties the builder, so the yielded table is
                    # the only copy of its rows.
                    yield finish(rows.build(), rows_before)
                    rows_before += num_rows
                if limit is not None and (
                    rows_before + rows.num_rows + channels.owed >= limit
                ):
                    break
            channels.flush()
        finally:
            channels.close()
        if rows.num_rows > 0:
            yield finish(rows.build(), rows_before)

    def _new_builder(
        self, extra_row_bytes: int, wanted: Optional[Set[str]]
    ) -> _MessageTableBuilder:
        """A builder of decoded rows with the ``wanted`` columns, or all of them."""
        return _MessageTableBuilder(
            columns=wanted,
            include_metadata=self._settings.include_metadata,
            include_row_id=self._settings.include_row_id,
            decode_json=False,
            extra_row_bytes=extra_row_bytes,
            frame_type=decoded_frame_type(self._settings.video.resize),
        )

    def _frame_settings(self, path: str, wanted: Optional[Set[str]]) -> _FrameSettings:
        """What the task's video channels work with in the file at ``path``."""
        return _FrameSettings(
            path=path,
            video=self._settings.video,
            video_topics=self._settings.video_topics,
            decode=wanted is None or "frame" in wanted,
            max_lead_in_ns=self._settings.max_lead_in_ns,
        )

    def _fills_block(self, rows: _MessageTableBuilder) -> bool:
        """Whether the rows built so far fill a table of ``target_block_size``."""
        target = self._settings.target_block_size
        return (
            target is not None and rows.estimated_bytes >= target and rows.num_rows > 0
        )
