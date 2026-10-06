"""Decoded message rows: one row per video frame, decoded in the read task.

Each video channel keeps one decoder for the whole task (``_FrameChannel``).
Before the channel's first owned message, and before each one that follows a
gap, the decoder is primed with the messages ``mcap_video_source`` reads back,
and their frames are dropped. Without a keyframe in the lead-in, or in a gap cut
short by the look-back cap, the channel is cold: its pictures are skipped until
the next keyframe. Each frame the decoder releases becomes the row of the owned
message with its log time. Without the ``frame`` column nothing is decoded, and
a message is a row when a decoder would keep its frame::

    what to read               decoder per camera            rows
    (mcap_video_source)        (_FrameChannel)               (_MessageTableBuilder)

    lead-in before the    ->   prime: decode from the
    first owned message        last keyframe, drop output
    owned messages, in    ->   feed -> FrameDecoder      ->  frame -> the row of the
    log-time order                                           message with its log time
    another task's        ->   prime again at each gap
    messages (a gap)
    fps seed from the     ->   FrameThinner (epoch grid)
    message index
"""

import itertools
import logging
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    FrozenSet,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
)

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
    _Selected,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_coarse_rows import (
    FinishTable,
    RowSettings,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import (
    FrameDecoder,
    FrameThinner,
    warn_cold_channel,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_lead_in import last_keyframe
from ray.data._internal.datasource_v2.formats.mcap.mcap_message_rows import (
    _MessageTableBuilder,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import VideoOptions
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    VideoCodec,
    carries_picture,
    channel_codec,
    is_keyframe,
    require_video_channel,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_video_source import (
    VideoMessageSource,
)
from ray.util.debug import log_once

if TYPE_CHECKING:
    from mcap.summary import Summary

    from ray.data._internal.datasource_v2.formats.mcap.mcap_reader import _Assignment

logger = logging.getLogger(__name__)


def _take_pending(
    waiting: Dict[int, List[_Selected]], log_time: Optional[int]
) -> Optional[_Selected]:
    """Pop the oldest message still owed a frame at ``log_time``, if any."""
    if log_time is None:
        return None
    queue = waiting.get(log_time)
    if not queue:
        return None
    item = queue.pop(0)
    if not queue:
        del waiting[log_time]
    return item


@dataclass(frozen=True)
class _FrameSettings:
    """What every video channel of a decoded message task works with."""

    path: str
    video: VideoOptions
    video_topics: FrozenSet[str]
    # Whether frames are decoded. Without the ``frame`` column a channel only
    # counts the rows a decoder would emit.
    decode: bool
    max_lead_in_ns: int


class _FrameChannel:
    """One video channel of a decoded message task.

    The channel is cold, its pictures skipped, until a keyframe is in reach.
    Each decoded frame becomes the row of the owned message with its log time.
    Without ``decode`` no decoder is built, and a message is a row when a
    decoder would have kept its frame.

    Streams are taken to be free of B-frames, as the recorders that write MCAP
    video require: each message decodes to one frame, and frames come out of
    the decoder in log-time order.
    """

    def __init__(self, settings: _FrameSettings, rows: _MessageTableBuilder):
        self._settings = settings
        self._rows = rows
        self.codec: Optional[VideoCodec] = None
        self.decoder: Optional[FrameDecoder] = None
        self.thinner = FrameThinner(settings.video.fps_interval_ns)
        # Whether a decoder would have a keyframe to work from at this point.
        self.decodable = False
        # The task's messages whose frames have not come out yet, by log time
        # (a list, since two messages may share one).
        self._owed: Dict[int, List[_Selected]] = {}
        # Log times of the primed messages, whose frames are dropped if the
        # decoder releases them late.
        self._primed: Set[int] = set()
        # Log time of the last owned message, where a gap would start.
        self.last_time = 0

    def prime(
        self,
        item: _Selected,
        primer: List[_Selected],
        *,
        gap: bool,
        clipped: bool = False,
    ) -> None:
        """Feed the decoder and the thinning the messages before ``item``.

        ``primer`` is the lead-in before the channel's first owned message, or
        the skipped messages of a gap between two owned ones. Decoding starts
        at its last keyframe. A lead-in without one leaves the channel cold
        until its next keyframe, since frames decoded without their reference
        would be rejected or concealed. A gap without one is fed whole, so the
        decoder's references stay continuous, unless it is clipped: then the
        channel goes cold too. The primer's own frames are dropped, but a frame
        of ours that the decoder held back comes out and is kept.
        """
        codec = self._sniff_codec(item, primer)
        if codec is None:
            # No codec yet: cold until a payload names one.
            self.decodable = False
            for m in primer:
                self.thinner.observe(m[2].log_time)
            return
        keyframe_at = last_keyframe(primer, codec)
        self._update_decodable(
            item, primer, codec, keyframe_at, gap=gap, clipped=clipped
        )
        fed: Set[int] = set()
        if self._settings.decode:
            fed = self._feed_primer(
                primer, codec, keyframe_at, whole=gap and not clipped
            )
        self._observe_unfed(primer, codec, fed)

    def _update_decodable(
        self,
        item: _Selected,
        primer: List[_Selected],
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
            if clipped or any(carries_picture(m[2].data, codec) for m in primer):
                warn_cold_channel(
                    item[1].topic,
                    self._settings.path,
                    self._settings.max_lead_in_ns,
                    item[2].log_time,
                )

    def _feed_primer(
        self,
        primer: List[_Selected],
        codec: VideoCodec,
        keyframe_at: Optional[int],
        *,
        whole: bool,
    ) -> Set[int]:
        """Decode the primer to set up the decoder, dropping its own frames.

        Decoding starts at ``keyframe_at``. Without a keyframe the primer is fed
        whole if ``whole``, and otherwise only its parameter sets. Returns the
        indexes fed.
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
            or not carries_picture(m[2].data, codec)
        }
        self._primed.update(primer[index][2].log_time for index in fed)
        for index in sorted(fed):
            for log_time, frame in decoder.decode(primer[index][2]):
                self._emit(log_time, frame, None)
        return fed

    def _observe_unfed(
        self, primer: List[_Selected], codec: VideoCodec, fed: Set[int]
    ) -> None:
        """Note the primer's pictures that were not fed, for ``fps`` thinning.

        They count only for the frames after them: the decoder may still hold
        frames of ours from before the gap. Only pictures take an interval, and
        the decoder takes those of the fed pictures that decode.
        """
        for index, m in enumerate(primer):
            if index not in fed and carries_picture(m[2].data, codec):
                self.thinner.observe_unread(m[2].log_time)

    def feed(self, item: _Selected) -> bool:
        """Decode one owned message into the row of its frame.

        Without a decoder, the message is a row when a decoder would keep its
        frame. Returns ``False`` when the message is skipped because the
        channel is cold: its codec is unknown, or it is a picture with no
        keyframe to decode against.
        """
        message = item[2]
        codec = self._sniff_codec(item)
        if codec is None:
            # The skipped frame still takes its ``fps`` interval, so a split
            # read keeps the frames a whole-file read keeps.
            self.thinner.observe(message.log_time)
            return False
        has_picture = carries_picture(message.data, codec)
        # A cold channel skips pictures until its next keyframe. Its parameter
        # sets are still fed to the decoder. A skipped picture counts only for
        # the frames after it, since the decoder may still hold earlier ones.
        if has_picture and not self.decodable:
            if not is_keyframe(message.data, codec):
                self.thinner.observe_unread(message.log_time)
                return False
            self.decodable = True
        if not self._settings.decode:
            if has_picture and self.thinner.keep(message.log_time):
                self._rows.add(item)
            return True
        decoder = self._decoder_for(codec)
        # Parameter sets written on their own yield no picture: no row, and no
        # claim on the frame of a keyframe stamped alike.
        if has_picture:
            self._owed.setdefault(message.log_time, []).append(item)
        for log_time, frame in decoder.decode(message):
            self._emit(log_time, frame, item if has_picture else None)
        return True

    def flush(self) -> None:
        """Drain the decoder at the end of the task."""
        assert self.decoder is not None
        for log_time, frame in self.decoder.flush():
            source = _take_pending(self._owed, log_time)
            if source is None:
                if log_time in self._primed or not self._owed:
                    continue
                # A frame that matches no owed message, such as one without a
                # stamp, goes to the latest message still owed one.
                source = _take_pending(self._owed, max(self._owed))
                assert source is not None
            self._rows.add(source, frame)

    def _sniff_codec(
        self, item: _Selected, context: Sequence[_Selected] = ()
    ) -> Optional[VideoCodec]:
        """The channel's codec, from ``item``'s ``format`` or the bytes.

        A VP9 or AV1 inter frame does not name its codec in its bytes, so the
        lead-in is tried too: the previous keyframe sits there. A channel whose
        codec nothing names yet stays cold until a later payload, its next
        keyframe, does.
        """
        if self.codec is not None:
            return self.codec
        schema, channel, message, _ = item
        codec = channel_codec(
            schema.name if schema else None,
            channel.message_encoding,
            itertools.chain((message.data,), (m[2].data for m in reversed(context))),
        )
        if codec is not None:
            self.codec = codec
            return codec
        if log_once(f"mcap_codec_pending:{channel.topic}"):
            logger.warning(
                "The codec of video topic %r in %r cannot be told from its "
                "first payloads; its frames are skipped until a keyframe "
                "identifies it.",
                channel.topic,
                self._settings.path,
            )
        return None

    def _decoder_for(self, codec: VideoCodec) -> FrameDecoder:
        """The channel's decoder, built on first use and sharing its thinning."""
        if self.decoder is None:
            self.decoder = FrameDecoder(
                codec, resize=self._settings.video.resize, thinner=self.thinner
            )
        return self.decoder

    def _emit(self, log_time: int, frame: Any, fallback: Optional[_Selected]) -> None:
        """Attribute a decoded frame to the message that held it, if it is ours.

        A frame stamped with an owed message's log time is that message's. One
        stamped with a primed time belongs to another task's row and is
        dropped. Any other frame goes to ``fallback``, the message being
        decoded. While priming there is none, and the frame is dropped.
        """
        source = _take_pending(self._owed, log_time)
        if source is None:
            if log_time in self._primed or fallback is None:
                return
            source = fallback
        # Frames come out in log-time order, so a message older than this
        # frame will not be given one: stop holding it.
        for stale in list(itertools.takewhile(lambda t: t < log_time, self._owed)):
            del self._owed[stale]
        self._rows.add(source, frame)


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

    def feed(self, item: _Selected) -> bool:
        """Feed an owned message to its channel, priming the channel where needed.

        A channel is primed with its lead-in at its first owned message. When
        another task's chunk may hold the channel between two owned messages,
        it is primed again with the messages it skipped, if any. Both times its
        ``fps`` thinning is also seeded from the message index. Returns
        ``False`` when the message is skipped because the channel is cold. A
        channel that is not video fails the read, as it fails planning.
        """
        channel_id, log_time = item[1].id, item[2].log_time
        channel = self._channels.get(channel_id)
        if channel is None:
            schema, record = item[0], item[1]
            require_video_channel(
                record.topic,
                schema.name if schema else None,
                self._settings.video_topics,
                self._settings.path,
            )
            channel = self._channels[channel_id] = _FrameChannel(
                self._settings, self._rows
            )
            channel.prime(item, self._source.lead_in(channel_id, log_time), gap=False)
            self._source.seed_fps(channel.thinner, channel_id, log_time)
        elif self._source.may_have_gap(channel_id, channel.last_time, log_time):
            gap = self._source.gap(channel_id, channel.last_time, log_time)
            if gap.messages or gap.clipped:
                channel.prime(item, gap.messages, gap=True, clipped=gap.clipped)
            # Only a clipped gap has an unread part that may hold the channel.
            if gap.clipped:
                self._source.seed_fps(
                    channel.thinner, channel_id, log_time, channel.last_time
                )
        channel.last_time = log_time
        fed = channel.feed(item)
        if channel.decoder is not None:
            self._decoding.setdefault(channel_id, channel)
        return fed

    def flush(self) -> None:
        """Drain every channel's decoder at the end of the task."""
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
        self,
        settings: RowSettings,
        message_reader: SelectedMessageReader,
        finish: FinishTable,
    ):
        self._settings = settings
        self._message_reader = message_reader
        self._finish = finish

    def tables(
        self, f: Any, assignment: "_Assignment", summary: Optional["Summary"]
    ) -> Iterator[pa.Table]:
        """Decode the task's video messages into one row per frame.

        A task rarely starts on a keyframe. Each channel's decoder is first fed
        the frames back to the previous keyframe (the lead-in), whose output is
        discarded, so every emitted frame is complete. The messages another
        task owns between two of ours are fed the same way. ``fps`` thinning also
        observes the lead-in, and the message index back to the start of the
        task's first interval, so the kept frames do not depend on where the
        task starts. If ``frame`` is not projected, nothing is decoded and a row
        is a message a decoder would turn into a kept frame. A payload the codec
        rejects still counts as a row then.
        """
        assert self._settings.video is not None
        wanted = (
            set(self._settings.columns) if self._settings.columns is not None else None
        )
        rows = self._new_builder(wanted)
        source = VideoMessageSource(
            self._message_reader, self._settings, f, assignment, summary
        )
        channels = _FrameChannels(
            self._frame_settings(assignment.path, wanted), rows, source
        )
        rows_before = 0
        try:
            for item in source.owned():
                if channels.feed(item) and self._fills_block(rows):
                    yield self._finish(rows.build(), assignment, rows_before)
                    rows_before += rows.num_rows
                    rows.reset()
            channels.flush()
        finally:
            channels.close()
        if rows.num_rows > 0:
            yield self._finish(rows.build(), assignment, rows_before)

    def _new_builder(self, wanted: Optional[Set[str]]) -> _MessageTableBuilder:
        """A builder of decoded rows with the ``wanted`` columns, or all of them."""
        return _MessageTableBuilder(
            columns=wanted,
            include_metadata=self._settings.include_metadata,
            include_row_id=self._settings.include_row_id,
            decode_json=False,
            decoded=True,
        )

    def _frame_settings(self, path: str, wanted: Optional[Set[str]]) -> _FrameSettings:
        """What the task's video channels work with in the file at ``path``."""
        assert self._settings.video is not None
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
