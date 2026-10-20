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
it, even when the decoder reorders or delays its output. ``fps`` keeps one
frame per slot of log time, chosen from the log times alone (``FrameThinner``).
An H.264 or H.265 decoder whose first keyframe comes without the stream's
parameter sets gets them from where the stream starts (see
``mcap_video_source``).
"""

import io
import logging
from collections import deque
from dataclasses import dataclass
from fractions import Fraction
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)

from ray.data._internal.datasource_v2.formats.mcap.mcap_options import _NS_PER_S
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


@dataclass
class _Decision:
    """Whether a noted picture's frame is kept; ``None`` while it waits."""

    log_time: int
    kept: Optional[bool]


class FrameThinner:
    """Keeps the frame nearest the centre of each ``fps`` slot (the ``fps`` option).

    A frame at log time ``t`` ns is in slot ``floor(t * fps / 1e9 + 1/2)``,
    computed exactly: slot ``k`` is centred on ``k / fps`` seconds since the
    epoch. Each slot keeps its frame nearest the centre, the earlier one on a
    tie. The choice uses only the log times of the channel's pictures, so
    nothing extra is decoded, and a kept frame that fails to decode leaves its
    slot empty. The grid is over log time because an MCAP topic has no fixed
    frame rate::

            slot k-1      |         slot k          |      slot k+1
        ----x--------x----|----x-------+-----x------|--x---------x---
                               a    k / fps  b
        a waits for the next picture, b. Nearer the centre, b takes the slot.

    The caller notes every picture of the channel in log-time order (``note``),
    fed to the decoder or not. Whether a frame is kept then depends only on the
    pictures just before and just after it. A frame before its slot's centre
    waits for the next picture: a delay of one frame per channel. Frames out of
    the decoder are held (``offer``) until their slot is decided (``release``).
    Where a task reads no further, the message index gives the next picture
    (``look_ahead``), or there is none (``settle``). So the kept frames do not
    depend on how the read is split into tasks.
    """

    def __init__(self, fps: Optional[Fraction]):
        self._fps = fps
        # The noted pictures' decisions, oldest first, until their frames come
        # out or a later frame does. A gap's pictures, which no frame of ours
        # matches, stay until the next frame out of the decoder.
        self._decisions: Deque[_Decision] = deque()
        # The last picture noted, and its decision while it waits for the next.
        self._last: Optional[int] = None
        self._waiting: Optional[_Decision] = None
        # The frames offered and not released yet, in order: (log time, item).
        self._held: Deque[Tuple[int, Any]] = deque()

    def slot_start(self, log_time: int) -> Optional[int]:
        """The first log time of the slot holding ``log_time``; ``None`` without
        ``fps``."""
        if self._fps is None:
            return None
        p, q = self._fps.numerator, self._fps.denominator
        # The least t with 2 * t * p + q * 1e9 >= 2 * k * q * 1e9.
        return -(-(2 * self._slot(log_time) - 1) * q * _NS_PER_S // (2 * p))

    def note(self, log_time: int) -> None:
        """Note the channel's next picture, in log-time order.

        It decides the frame waiting before it. A picture noted out of order is
        ignored, so its frame is not kept.
        """
        if self._fps is None or (self._last is not None and log_time < self._last):
            return
        waiting = self._waiting
        if waiting is not None and log_time > waiting.log_time:
            waiting.kept = not self._nearer(log_time, waiting.log_time)
            self._waiting = None
        decision = _Decision(log_time, self._decision_from_before(log_time))
        if decision.kept is None:
            self._waiting = decision
        self._decisions.append(decision)
        self._last = log_time

    def look_ahead(self) -> Optional[Tuple[int, int]]:
        """``[low, high)``: where a later picture would take the waiting frame's
        slot, being nearer its centre. ``None`` when no frame waits.

        The span ends at most half a slot past the centre.
        """
        if self._waiting is None:
            return None
        assert self._fps is not None
        p, q = self._fps.numerator, self._fps.denominator
        log_time = self._waiting.log_time
        # A later u is nearer while u * p - c < c - t * p, with c = k * q * 1e9.
        centre = self._slot(log_time) * q * _NS_PER_S
        return log_time + 1, -(-(2 * centre - log_time * p) // p)

    def settle(self) -> None:
        """No picture follows the waiting frame within its look-ahead: keep it."""
        if self._waiting is not None:
            self._waiting.kept = True
            self._waiting = None

    def offer(self, log_time: int, item: Any) -> None:
        """Hold ``item``, the frame at ``log_time``, until its slot is decided."""
        self._held.append((log_time, item))

    def release(self) -> Iterator[Tuple[int, Any, bool]]:
        """Yield each offered frame whose slot is decided, in order, with whether
        it is kept.

        A frame that matches no noted picture, such as one the decoder releases
        after a later frame, is not kept.
        """
        while self._held:
            log_time, item = self._held[0]
            kept = self._decision_for(log_time)
            if kept is None:
                return
            self._held.popleft()
            yield log_time, item, kept

    @property
    def held_from(self) -> Optional[int]:
        """The log time of the oldest frame held, if any."""
        return self._held[0][0] if self._held else None

    def _slot(self, log_time: int) -> int:
        """``floor(log_time * fps / 1e9 + 1/2)``, in integers."""
        assert self._fps is not None
        p, q = self._fps.numerator, self._fps.denominator
        return (2 * log_time * p + q * _NS_PER_S) // (2 * q * _NS_PER_S)

    def _offset(self, log_time: int, slot: int) -> int:
        """How far ``log_time`` lies from the centre of ``slot``, times ``fps``."""
        assert self._fps is not None
        p, q = self._fps.numerator, self._fps.denominator
        return log_time * p - slot * q * _NS_PER_S

    def _decision_from_before(self, log_time: int) -> Optional[bool]:
        """A new picture's decision, from the picture before it.

        It is dropped if that one is in its slot and as near the centre or
        nearer. Otherwise it is kept from the centre on, where every later
        picture is farther, and waits before the centre.
        """
        slot = self._slot(log_time)
        offset = self._offset(log_time, slot)
        last = self._last
        if (
            last is not None
            and self._slot(last) == slot
            and abs(self._offset(last, slot)) <= abs(offset)
        ):
            return False
        return True if offset >= 0 else None

    def _nearer(self, later: int, earlier: int) -> bool:
        """Whether ``later`` is in ``earlier``'s slot and strictly nearer its centre."""
        slot = self._slot(earlier)
        return self._slot(later) == slot and abs(self._offset(later, slot)) < abs(
            self._offset(earlier, slot)
        )

    def _decision_for(self, log_time: int) -> Optional[bool]:
        """The decision for a frame out of the decoder at ``log_time``.

        Frames come out in log-time order, so older decisions are passed.
        """
        if self._fps is None:
            return True
        decisions = self._decisions
        while decisions and decisions[0].log_time < log_time:
            decisions.popleft()
        if not decisions or decisions[0].log_time != log_time:
            return False
        kept = decisions[0].kept
        if kept is not None:
            decisions.popleft()
        return kept


class FrameDecoder:
    """Decodes one channel's payloads into RGB frames (``uint8`` arrays).

    Args:
        codec: The channel's codec (``mcap_video.channel_codec``).
        resize: ``(height, width)`` to scale frames to, or ``None``.
        fps: The ``fps`` option as an exact fraction. ``None`` emits every
            frame.
        thinner: A :class:`FrameThinner` shared with the caller, which notes
            the channel's pictures in it. Built from ``fps`` when ``None``.
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
        fps: Optional[Fraction] = None,
        thinner: Optional[FrameThinner] = None,
        read_parameter_sets: Optional[Callable[[VideoCodec], bytes]] = None,
    ):
        self.codec = codec
        self._codec = codec
        self._resize = resize
        self.thinner = thinner if thinner is not None else FrameThinner(fps)
        self._context: Any = None
        if not codec.every_frame_is_a_keyframe:
            self._context = _codec_context(codec)
        # The kinds of parameter set fed so far, and how to read the stream's
        # own (``_with_parameter_sets``).
        self._kinds_fed: Set[int] = set()
        self._read_parameter_sets = read_parameter_sets
        # Log time of the last frame out of the decoder, kept or thinned. A
        # still is out as soon as it is fed.
        self.last_output: Optional[int] = None
        # Log time of the last packet fed.
        self._last_fed: Optional[int] = None
        # Whether the codec rejected the last packet fed. It then has no frame
        # to come out.
        self.rejected_last = False

    def decode(
        self, message: "Message", wanted: Optional[Callable[[int], bool]] = None
    ) -> Iterator[Tuple[int, "np.ndarray"]]:
        """Yield ``(log_time, frame)`` for each kept frame whose slot is decided.

        The log time is that of the message the frame came from. A frame waits
        in the thinner until its slot is decided (``FrameThinner``), and a
        decoder may hold frames back, so this may yield frames of earlier
        messages. The caller notes the message's picture first. The look-back
        goes through here too, with ``wanted`` naming the log times whose
        frames the caller keeps. The other frames are not converted to RGB.
        ``None`` keeps all.
        """
        if self._codec.every_frame_is_a_keyframe:
            self.last_output = message.log_time
            # A still is decoded only once its slot keeps it.
            self.thinner.offer(message.log_time, (message.data, wanted))
            yield from self._release()
            return
        self._last_fed = message.log_time
        for frame in self._decode_packet(message):
            log_time = frame.pts if frame.pts is not None else message.log_time
            self.last_output = log_time
            self.thinner.offer(log_time, (frame, wanted))
        yield from self._release()

    def flush(self) -> Iterator[Tuple[int, "np.ndarray"]]:
        """Drain the frames the decoder and the thinner still hold at the end
        of the task. A frame that still waits for its slot is kept.

        The decoder cannot be used afterwards.
        """
        self.thinner.settle()
        yield from self._release()
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
            if log_time is not None:
                self.last_output = log_time
                self.thinner.offer(log_time, (frame, None))
        yield from self._release()

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

    def _release(self) -> Iterator[Tuple[int, "np.ndarray"]]:
        """The frames the thinner has decided, as RGB arrays if kept and wanted."""
        for log_time, (source, wanted), kept in self.thinner.release():
            if not kept or (wanted is not None and not wanted(log_time)):
                continue
            if not self._codec.every_frame_is_a_keyframe:
                yield log_time, self._to_array(source)
                continue
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


def decode_one(
    payloads: List[bytes],
    codec: VideoCodec,
    resize: Optional[Tuple[int, int]],
    read_parameter_sets: Optional[Callable[[VideoCodec], bytes]] = None,
) -> Optional["np.ndarray"]:
    """Decode the first frame of a stream's head, for schema inference.

    ``payloads`` are a channel's messages up to and including a keyframe, in
    order. The messages before the keyframe may carry the parameter sets (SPS,
    PPS) the decoder needs. When none does, ``read_parameter_sets`` reads them
    from the start of the stream (``FrameDecoder``). Returns ``None`` when
    nothing decodes, so planning falls back to a variable-shaped tensor
    instead of failing on a stream the read task may still decode.
    """
    decoder = FrameDecoder(
        codec, resize=resize, read_parameter_sets=read_parameter_sets
    )
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
        frame = decoder._with_parameter_sets(_frame_bytes(payload, codec))
        try:
            frames.extend(decoder._context.decode(av.Packet(frame)))
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
