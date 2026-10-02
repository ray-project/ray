"""Decoding video payloads inside the read task (``VideoOptions.decode``).

A compressed video message is useless downstream without the group of
pictures it belongs to, and Ray Data cuts blocks at positions that have
nothing to do with those groups. Decoding where the reader holds the whole
stream of a channel makes every emitted row a self-contained frame.

One :class:`FrameDecoder` per channel: topics interleave in log-time order, so
the decoder for a camera has to survive IMU messages between two of its own
frames. H.264 and H.265 go through PyAV, JPEG and PNG through Pillow; the
codec is sniffed from the bytes (see ``mcap_video``). Every packet is stamped
with its message's ``log_time``, and a decoded frame carries that stamp back,
so a frame is attributed to the message that held it even when the decoder
reorders or delays its output.
"""

import io
import logging
from typing import TYPE_CHECKING, Any, Iterator, List, Optional, Tuple

from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    _ANNEXB_START,
    _JPEG_SOI,
    _PNG_SIGNATURE,
    VideoCodec,
    carries_picture,
)
from ray.util.debug import log_once

if TYPE_CHECKING:
    import numpy as np
    from mcap.records import Message

logger = logging.getLogger(__name__)

_AV_CODEC_NAMES = {VideoCodec.H264: "h264", VideoCodec.H265: "hevc"}


def _frame_bytes(payload: bytes, codec: VideoCodec) -> bytes:
    """The encoded frame inside a message payload.

    Recorders wrap the frame in a serialized message (a CDR or protobuf
    header, a format string, a length), so the decoder is handed the bytes
    from the first signature or start code on.
    """
    if codec is VideoCodec.JPEG:
        start = payload.find(_JPEG_SOI)
    elif codec is VideoCodec.PNG:
        start = payload.find(_PNG_SIGNATURE)
    else:
        start = payload.find(_ANNEXB_START)
        # A four-byte start code has a leading zero the search skipped.
        if start > 0 and payload[start - 1] == 0:
            start -= 1
    return payload if start <= 0 else payload[start:]


class FrameThinner:
    """Keeps at most one frame per interval of log time (the ``fps`` option).

    The intervals are a grid aligned to the epoch, not to the first frame a
    task sees, and a task first observes the frames just before its own, so
    which frames survive does not depend on how the read was split into
    tasks. Intervals only move forward: a frame the decoder releases late (a
    lead-in frame held back behind the task's own) cannot reopen an interval
    that already kept a frame. An MCAP topic has no fixed frame rate
    (messages are timestamped and can be irregular), so the grid is over log
    time rather than a frame index.
    """

    def __init__(self, interval_ns: Optional[int]):
        self._interval_ns = interval_ns
        self._last_bucket: Optional[int] = None

    def keep(self, log_time: int) -> bool:
        """Whether a frame at ``log_time`` opens a new interval."""
        if self._interval_ns is None:
            return True
        bucket = log_time // self._interval_ns
        if self._last_bucket is not None and bucket <= self._last_bucket:
            return False
        self._last_bucket = bucket
        return True

    def observe(self, log_time: int) -> None:
        """Note a frame that precedes the task's own, without emitting it."""
        self.keep(log_time)


class FrameDecoder:
    """Decodes one channel's payloads into RGB frames (``uint8`` arrays).

    Args:
        codec: The channel's codec, sniffed from its first payload.
        resize: ``(height, width)`` to scale frames to, or ``None``.
        min_interval_ns: Minimum log-time grid between emitted frames, the
            ``fps`` option; ``None`` emits every frame.
    """

    def __init__(
        self,
        codec: VideoCodec,
        *,
        resize: Optional[Tuple[int, int]] = None,
        min_interval_ns: Optional[int] = None,
    ):
        self.codec = codec
        self._codec = codec
        self._resize = resize
        self.thinner = FrameThinner(min_interval_ns)
        self._context: Any = None
        if not codec.every_frame_is_a_keyframe:
            import av

            self._context = av.CodecContext.create(_AV_CODEC_NAMES[codec], "r")

    def decode(self, message: "Message") -> Iterator[Tuple[int, "np.ndarray"]]:
        """Yield ``(log_time, frame)`` for the frames ``message`` completes.

        The log time is that of the message the frame came from, which with a
        decoder that holds frames back may be an earlier one than ``message``.
        A lead-in is fed through here too: the caller drops the frames whose
        log time it primed with, and keeps a held-back frame of its own that
        the lead-in flushed out.
        """
        if self._codec.every_frame_is_a_keyframe:
            if self.thinner.keep(message.log_time):
                still = self._decode_still_or_none(message.data, message.log_time)
                if still is not None:
                    yield message.log_time, still
            return
        for frame in self._decode_packet(message):
            log_time = frame.pts if frame.pts is not None else message.log_time
            if self.thinner.keep(log_time):
                yield log_time, self._to_array(frame)

    def flush(self) -> Iterator[Tuple[Optional[int], "np.ndarray"]]:
        """Drain the frames the decoder still holds at the end of the task."""
        if self._context is None:
            return
        try:
            frames = self._context.decode(None)
        except Exception:  # pragma: no cover - codec-specific teardown quirks
            return
        for frame in frames:
            if frame.pts is None or self.thinner.keep(frame.pts):
                yield frame.pts, self._to_array(frame)

    # -- internals ---------------------------------------------------------

    def _decode_packet(self, message: "Message") -> List[Any]:
        import av

        packet = av.Packet(_frame_bytes(message.data, self._codec))
        packet.pts = message.log_time
        try:
            return self._context.decode(packet)
        except av.FFmpegError as e:
            # A truncated or mid-GOP access unit (a task that found no keyframe
            # within its lead-in), or a packet the codec rejects. Skipping keeps
            # the rest of the topic readable; failing here would lose the whole
            # task. Said once per kind of error, so a corrupt recording is not
            # silent either. A message holding only parameter sets (SPS/PPS
            # written ahead of the keyframe) has no picture to output and is
            # not reported.
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

        A corrupt or truncated image fails the one frame, not the task, as a
        rejected access unit does on the video path.
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
                image = image.resize((width, height), Image.Resampling.BILINEAR)
        return np.asarray(image)


def decode_one(
    payloads: List[bytes], codec: VideoCodec, resize: Optional[Tuple[int, int]]
) -> Optional["np.ndarray"]:
    """Decode the first frame of a stream's head, for schema inference at planning.

    ``payloads`` are a channel's messages from its first through its first
    keyframe, in order: a recorder may put the parameter sets (SPS, PPS) in a
    message of their own ahead of the keyframe, and the decoder needs them.
    Returns ``None`` when nothing decodes, so planning can fall back to a
    variable-shaped tensor instead of failing on a stream the read task would
    decode by playing it in order.
    """
    decoder = FrameDecoder(codec, resize=resize)
    if codec.every_frame_is_a_keyframe:
        return decoder._decode_still_or_none(payloads[-1], 0)
    import av

    frames: List[Any] = []
    for payload in payloads:
        try:
            frames.extend(
                decoder._context.decode(av.Packet(_frame_bytes(payload, codec)))
            )
        except av.FFmpegError as e:
            # A parameter-set-only message yields "no frame"; the keyframe that
            # follows still decodes against the parameter sets it carried.
            logger.debug("Planning-time decode of a stream-head packet failed: %s", e)
            continue
        if frames:
            break
    if not frames:
        try:
            frames.extend(decoder._context.decode(None))
        except av.FFmpegError:
            return None
    return decoder._to_array(frames[0]) if frames else None
