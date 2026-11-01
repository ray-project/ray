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
it, even when the decoder reorders or delays its output. An H.264 or H.265 decoder whose first keyframe comes without the stream's
parameter sets gets them from where the stream starts (see
``mcap_video_source``).
"""

import io
import logging
from typing import TYPE_CHECKING, Any, Callable, Iterator, List, Optional, Set, Tuple

from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    _ANNEXB_START,
    _JPEG_SOI,
    _PNG_SIGNATURE,
    VideoCodec,
    _annex_b_data,
    _embedded_frames,
    carries_picture,
    is_keyframe,
    parameter_set_kinds,
    parameter_set_types,
    parameter_sets,
    with_parameter_sets,
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
    JPEG and PNG, the frame starts at the first signature. For the other codecs
    it is the message's ``data`` field (``_embedded_frames``), and an H.264 or
    H.265 frame starts at the first start code in it.
    """
    if codec is VideoCodec.JPEG:
        start = payload.find(_JPEG_SOI)
    elif codec is VideoCodec.PNG:
        start = payload.find(_PNG_SIGNATURE)
    elif not codec.is_annex_b:
        return _embedded_frames(payload)[0]
    else:
        payload = _annex_b_data(payload)
        start = payload.find(_ANNEXB_START)
        # A four-byte start code has a leading zero the search skipped.
        if start > 0 and payload[start - 1] == 0:
            start -= 1
    return payload if start <= 0 else payload[start:]


class FrameDecoder:
    """Decodes one channel's payloads into RGB frames (``uint8`` arrays).

    Args:
        codec: The channel's codec (``mcap_video.channel_codec``).
        resize: ``(height, width)`` to scale frames to, or ``None``.
        read_parameter_sets: Reads the parameter sets at the start of the
            stream, given the codec (``StreamStart.read``). Called at most
            once, for the first keyframe that comes without them. ``None``
            reads none.
    """

    def __init__(
        self,
        codec: VideoCodec,
        *,
        resize: Optional[Tuple[int, int]] = None,
        read_parameter_sets: Optional[Callable[[VideoCodec], bytes]] = None,
    ):
        self.codec = codec
        self._codec = codec
        self._resize = resize
        self._context: Any = None
        if not codec.every_frame_is_a_keyframe:
            self._context = _codec_context(codec)
        # The kinds of parameter set fed so far, and how to read the stream's
        # own (``_with_parameter_sets``).
        self._kinds_fed: Set[int] = set()
        self._read_parameter_sets = read_parameter_sets
        # Log time of the last frame out of the decoder. A still is out as
        # soon as it is fed.
        self.last_output: Optional[int] = None
        # Log time of the last packet fed.
        self._last_fed: Optional[int] = None
        # Whether the codec rejected the last packet fed. It then has no frame
        # to come out.
        self.rejected_last = False

    def decode(
        self, message: "Message", wanted: Optional[Callable[[int], bool]] = None
    ) -> Iterator[Tuple[int, "np.ndarray"]]:
        """Yield ``(log_time, frame)`` for each frame the decoder releases.

        The log time is that of the message the frame came from. A decoder may
        hold frames back, so this may yield frames of earlier messages. The
        look-back goes through here too, with ``wanted`` naming the log times
        whose frames the caller keeps. The other frames are not converted to
        RGB. ``None`` keeps all.
        """
        if self._codec.every_frame_is_a_keyframe:
            self.last_output = message.log_time
            yield from self._release(message.log_time, message.data, wanted)
            return
        self._last_fed = message.log_time
        for frame in self._decode_packet(message):
            log_time = frame.pts if frame.pts is not None else message.log_time
            self.last_output = log_time
            yield from self._release(log_time, frame, wanted)

    def flush(self) -> Iterator[Tuple[int, "np.ndarray"]]:
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
            # ``decode``.
            log_time = frame.pts if frame.pts is not None else self._last_fed
            if log_time is not None:
                self.last_output = log_time
                yield from self._release(log_time, frame, None)

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

    def _release(
        self, log_time: int, source: Any, wanted: Optional[Callable[[int], bool]]
    ) -> Iterator[Tuple[int, "np.ndarray"]]:
        """``source`` as an RGB array at ``log_time``, if ``wanted``: a decoded
        frame, or the payload of a still."""
        if wanted is not None and not wanted(log_time):
            return
        if not self._codec.every_frame_is_a_keyframe:
            yield log_time, self._to_array(source)
            return
        still = self._decode_still_or_none(source, log_time)
        if still is not None:
            yield log_time, still

    def _decode_packet(self, message: "Message") -> List[Any]:
        import av

        frame = _frame_bytes(message.data, self._codec)
        packet = av.Packet(self._with_parameter_sets(frame))
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

    def _with_parameter_sets(self, frame: bytes) -> bytes:
        """``frame``, led by the stream's parameter sets if the decoder lacks them.

        Only the first keyframe that comes without them gets them, read once
        from the start of the stream. It gets only the kinds that neither it
        nor an earlier packet holds, so those win.
        """
        needed = parameter_set_types(self._codec)
        if self._kinds_fed >= needed:
            return frame
        self._kinds_fed |= parameter_set_kinds(frame, self._codec)
        if self._kinds_fed >= needed or not is_keyframe(frame, self._codec):
            return frame
        read, self._read_parameter_sets = self._read_parameter_sets, None
        sets = b""
        if read is not None:
            sets = parameter_sets(read(self._codec), self._codec, self._kinds_fed)
        # Asked once: a stream whose start lacks them lacks them for good.
        self._kinds_fed |= needed
        return with_parameter_sets(frame, sets, self._codec) if sets else frame

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
