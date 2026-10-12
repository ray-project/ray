"""Video decoding inside the read task (``VideoOptions``).

A compressed video frame usually needs the earlier frames of its group of
pictures to decode, and Ray Data cuts blocks without regard to those groups.
Decoding in the read task, which sees each channel's messages in order, makes
every row a self-contained frame.

Each channel keeps its own :class:`FrameDecoder` across the interleaved
messages of other topics. PyAV decodes H.264, H.265, VP9 and AV1, and Pillow
decodes JPEG and PNG. The codec comes from the message's ``format`` field or its
bytes (see ``mcap_video``).
Each packet is stamped with its message's ``log_time``, and the decoded frame
carries that stamp back. This attributes each frame to the message that held
it, even when the decoder reorders or delays its output.
"""

import io
import logging
from typing import TYPE_CHECKING, Any, Iterator, List, Optional, Tuple

from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    _ANNEXB_START,
    _JPEG_SOI,
    _PNG_SIGNATURE,
    VideoCodec,
    _embedded_frames,
    carries_picture,
)
from ray.util.debug import log_once

if TYPE_CHECKING:
    import numpy as np
    from mcap.records import Message

logger = logging.getLogger(__name__)

# Decoder names to try per codec, in order. FFmpeg's own decoder comes first,
# except for AV1, whose FFmpeg decoder works only with hardware acceleration.
_AV_DECODERS = {
    VideoCodec.H264: ("h264",),
    VideoCodec.H265: ("hevc",),
    VideoCodec.VP9: ("vp9", "libvpx-vp9"),
    VideoCodec.AV1: ("libdav1d", "av1", "libaom-av1"),
}


def _codec_context(codec: VideoCodec) -> Any:
    import av

    last: Optional[Exception] = None
    for name in _AV_DECODERS[codec]:
        try:
            return av.CodecContext.create(name, "r")
        except Exception as e:  # noqa: BLE001 - PyAV raises its own hierarchy
            last = e
    raise ValueError(
        f"No decoder for {codec.value} in this build of av: tried "
        f"{', '.join(_AV_DECODERS[codec])}"
    ) from last


def _frame_bytes(payload: bytes, codec: VideoCodec) -> bytes:
    """The encoded frame inside a message payload.

    Recorders wrap the frame in a serialized message, such as a CDR header. For
    JPEG, PNG, H.264 and H.265, the frame starts at the first signature or start
    code. VP9 and AV1 have neither, so their frame is found by parsing the
    message framing.
    """
    if codec is VideoCodec.JPEG:
        start = payload.find(_JPEG_SOI)
    elif codec is VideoCodec.PNG:
        start = payload.find(_PNG_SIGNATURE)
    elif not codec.is_annex_b:
        return _embedded_frames(payload)[0]
    else:
        start = payload.find(_ANNEXB_START)
        # A four-byte start code has a leading zero the search skipped.
        if start > 0 and payload[start - 1] == 0:
            start -= 1
    return payload if start <= 0 else payload[start:]


class FrameThinner:
    """Keeps at most one frame per interval of log time (the ``fps`` option).

    The intervals form a grid aligned to the epoch, not to a task's first
    frame. A task also observes the frames before its own: those it reads back
    for the decoder, and the latest earlier one in its first interval, which
    the file's message index gives. The kept frames then do not depend on how
    the read is split into tasks. Intervals only move forward: a frame the
    decoder releases late cannot reopen an interval that already kept a frame.
    The grid is over log time because an MCAP topic has no fixed frame rate.
    """

    def __init__(self, interval_ns: Optional[int]):
        self._interval_ns = interval_ns
        self._last_bucket: Optional[int] = None
        # Log times of the frames noted by ``observe_unread`` and not passed yet.
        self._unread: List[int] = []

    def interval_start(self, log_time: int) -> Optional[int]:
        """The start of the interval holding ``log_time``; ``None`` without ``fps``."""
        if self._interval_ns is None:
            return None
        return log_time - log_time % self._interval_ns

    def keep(self, log_time: int) -> bool:
        """Whether a frame at ``log_time`` opens a new interval."""
        if self._interval_ns is None:
            return True
        taken = self._taken_before(log_time)
        self._unread = [t for t in self._unread if t >= log_time]
        bucket = log_time // self._interval_ns
        if taken is not None and bucket <= taken:
            self._last_bucket = taken
            return False
        self._last_bucket = bucket
        return True

    def would_keep(self, log_time: int) -> bool:
        """``keep`` without the side effect, to decide before decoding."""
        if self._interval_ns is None:
            return True
        taken = self._taken_before(log_time)
        return taken is None or log_time // self._interval_ns > taken

    def observe(self, log_time: int) -> None:
        """Note a frame that precedes the task's own, without emitting it."""
        self.keep(log_time)

    def observe_unread(self, log_time: int) -> None:
        """Note a frame the decoder is never fed, without emitting it.

        Such a frame is known only from the message index, or is a picture the
        task skips: a primer picture left out of the feed, or one of a cold
        channel. It counts only for the frames after it. A frame from before it
        that the decoder releases late is thinned first, as in a whole-file read.
        """
        self._unread.append(log_time)

    def _taken_before(self, log_time: int) -> Optional[int]:
        """The last interval that kept a frame, as seen by a frame at ``log_time``."""
        assert self._interval_ns is not None
        buckets = [t // self._interval_ns for t in self._unread if t < log_time]
        if self._last_bucket is not None:
            buckets.append(self._last_bucket)
        return max(buckets, default=None)


class FrameDecoder:
    """Decodes one channel's payloads into RGB frames (``uint8`` arrays).

    Args:
        codec: The channel's codec (``mcap_video.channel_codec``).
        resize: ``(height, width)`` to scale frames to, or ``None``.
        min_interval_ns: The ``fps`` interval in nanoseconds of log time.
            ``None`` emits every frame.
        thinner: A :class:`FrameThinner` shared with the caller's count path,
            so both make the same ``fps`` decisions. Built from
            ``min_interval_ns`` when ``None``.
    """

    def __init__(
        self,
        codec: VideoCodec,
        *,
        resize: Optional[Tuple[int, int]] = None,
        min_interval_ns: Optional[int] = None,
        thinner: Optional[FrameThinner] = None,
    ):
        self.codec = codec
        self._codec = codec
        self._resize = resize
        self.thinner = thinner if thinner is not None else FrameThinner(min_interval_ns)
        self._context: Any = None
        if not codec.every_frame_is_a_keyframe:
            self._context = _codec_context(codec)
        # Log time of the last frame out of the decoder, kept or thinned. A
        # still is out as soon as it is fed.
        self.last_output: Optional[int] = None
        # Log time of the last packet fed.
        self._last_fed: Optional[int] = None
        # Whether the codec rejected the last packet fed. It then has no frame
        # to come out.
        self.rejected_last = False

    def decode(self, message: "Message") -> Iterator[Tuple[int, "np.ndarray"]]:
        """Yield ``(log_time, frame)`` for each frame that ``message`` completes.

        The log time is that of the message the frame came from. A decoder
        that holds frames back may yield a frame of an earlier message. The
        lead-in goes through here too: the caller drops frames stamped with a
        lead-in time and keeps any frame of its own that the lead-in flushes
        out.
        """
        if self._codec.every_frame_is_a_keyframe:
            self.last_output = message.log_time
            # Check the interval before decoding, so a dropped still is never
            # decoded. Take the interval only once the still decodes, so a
            # corrupt one does not cost the next good still its interval.
            if self.thinner.would_keep(message.log_time):
                still = self._decode_still_or_none(message.data, message.log_time)
                if still is not None:
                    self.thinner.keep(message.log_time)
                    yield message.log_time, still
            return
        self._last_fed = message.log_time
        for frame in self._decode_packet(message):
            log_time = frame.pts if frame.pts is not None else message.log_time
            self.last_output = log_time
            if self.thinner.keep(log_time):
                yield log_time, self._to_array(frame)

    def flush(self) -> Iterator[Tuple[Optional[int], "np.ndarray"]]:
        """Drain the frames the decoder still holds at the end of the task.

        The decoder cannot be used afterwards.
        """
        context, self._context = self._context, None
        if context is None:
            return
        try:
            frames = context.decode(None)
        except Exception:  # pragma: no cover - codec-specific teardown quirks
            return
        for frame in frames:
            # An unstamped frame takes the time of the last packet fed, as in
            # ``decode``, so it is thinned like any other.
            log_time = frame.pts if frame.pts is not None else self._last_fed
            if log_time is None or self.thinner.keep(log_time):
                yield log_time, self._to_array(frame)

    def close(self) -> None:
        """Drain the decoder and drop its frames, unless ``flush`` already ran.

        A read that stops early must call this. Freeing a libdav1d (AV1)
        context that still holds frames can hang the process.
        """
        context, self._context = self._context, None
        if context is None:
            return
        try:
            context.decode(None)
        except Exception:  # noqa: BLE001 - the frames are dropped anyway
            pass

    # -- internals ---------------------------------------------------------

    def _decode_packet(self, message: "Message") -> List[Any]:
        import av

        packet = av.Packet(_frame_bytes(message.data, self._codec))
        packet.pts = message.log_time
        self.rejected_last = False
        try:
            return self._context.decode(packet)
        except av.FFmpegError as e:
            self.rejected_last = True
            # A truncated or mid-GOP access unit, or one the codec rejects.
            # Skipping it keeps the rest of the topic readable. The warning is
            # logged once per error type, and not for a message holding only
            # parameter sets, which has no picture to output.
            if carries_picture(message.data, self._codec) and log_once(
                f"mcap_decode_error:{type(e).__name__}"
            ):
                logger.warning(
                    "Skipping an undecodable %s access unit at log_time %d (%s: %s); "
                    "further ones are skipped silently.",
                    self._codec.name,
                    message.log_time,
                    type(e).__name__,
                    e,
                )
            return []

    def _to_array(self, frame: Any) -> "np.ndarray":
        if self._resize is not None:
            height, width = self._resize
            # One libswscale pass does the colour conversion and the scale.
            return frame.reformat(
                width=width, height=height, format="rgb24"
            ).to_ndarray()
        return frame.to_ndarray(format="rgb24")

    def _decode_still_or_none(
        self, payload: bytes, log_time: int
    ) -> Optional["np.ndarray"]:
        """Decode a still, or skip it with a warning once per kind of error.

        A corrupt or truncated image loses only that frame, as a rejected
        access unit does on the video path.
        """
        try:
            return self._decode_still(payload)
        except Exception as e:  # Pillow raises OSError, SyntaxError, ValueError...
            if log_once(f"mcap_decode_error:{type(e).__name__}"):
                logger.warning(
                    "Skipping an undecodable %s image at log_time %d (%s: %s); "
                    "further ones are skipped silently.",
                    self._codec.name,
                    log_time,
                    type(e).__name__,
                    e,
                )
            return None

    def _decode_still(self, payload: bytes) -> "np.ndarray":
        import numpy as np
        from PIL import Image

        image = Image.open(io.BytesIO(_frame_bytes(payload, self._codec))).convert(
            "RGB"
        )
        if self._resize is not None:
            height, width = self._resize
            if image.size != (width, height):
                # Pillow before 9.1 has no ``Image.Resampling``: its filters sit
                # on ``Image`` itself.
                filters: Any = getattr(Image, "Resampling", Image)
                image = image.resize((width, height), filters.BILINEAR)
        return np.asarray(image)


def warn_cold_channel(topic: str, path: str, cap_ns: int, before: int) -> None:
    """Warn once per topic that a channel starts cold, so skipped frames are not silent.

    Callers do not warn for a stream with no picture before it within the cap:
    that is the recording's own start, not a split.
    """
    if log_once(f"mcap_cold_channel:{topic}"):
        logger.warning(
            "Video topic %r in %r has no keyframe within the %.3g s look-back cap "
            "before log_time %d: its frames are skipped until the next keyframe. "
            "Set RAY_DATA_MCAP_MAX_LEAD_IN_S to look further back.",
            topic,
            path,
            cap_ns / 1e9,
            before,
        )


def decode_one(
    payloads: List[bytes], codec: VideoCodec, resize: Optional[Tuple[int, int]]
) -> Optional["np.ndarray"]:
    """Decode the first frame of a stream's head, for schema inference.

    ``payloads`` are a channel's messages up to and including a keyframe, in
    order. The messages before the keyframe may carry parameter sets (SPS,
    PPS) that the decoder needs. Returns ``None`` when nothing
    decodes, so planning falls back to a variable-shaped tensor instead of
    failing on a stream the read task may still decode.
    """
    decoder = FrameDecoder(codec, resize=resize)
    if codec.every_frame_is_a_keyframe:
        return decoder._decode_still_or_none(payloads[-1], 0)
    try:
        frames = _decode_stream_head(decoder, payloads, codec)
        return decoder._to_array(frames[0]) if frames else None
    finally:
        decoder.close()


def _decode_stream_head(
    decoder: FrameDecoder, payloads: List[bytes], codec: VideoCodec
) -> List[Any]:
    """Feed ``payloads`` until a frame comes out; drain the decoder if none does."""
    import av

    frames: List[Any] = []
    for payload in payloads:
        try:
            frames.extend(
                decoder._context.decode(av.Packet(_frame_bytes(payload, codec)))
            )
        except av.FFmpegError as e:
            # A message holding only parameter sets can raise "no frame". The
            # keyframe after it still decodes, using those parameter sets.
            logger.debug("Planning-time decode of a stream-head packet failed: %s", e)
            continue
        if frames:
            return frames
    try:
        return list(decoder._context.decode(None))
    except av.FFmpegError:
        return []
