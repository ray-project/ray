"""Video detection: video topics, their codecs, keyframes and parameter sets.

A decoded frame needs the frames back to the last keyframe
(``mcap_decoded_samples``). These rules decide which topics are video, and how
their codec and keyframes are found::

    video?    known video schema, or topic in video_topics?   no   -> not video
      | yes
    codec?    format field of a known schema names one?       yes  -> that codec
      | no
              a payload whose bytes name one?                 yes  -> that codec
              (JPEG/PNG signature, H.264/H.265 NAL headers,
               AV1 OBUs, VP9 key frame)
      | none
              no codec: the topic's frames are left empty and padded
    keyframe? from the payload bytes, per codec (is_keyframe)
    parameter sets? the H.264 and H.265 NAL units a decoder needs before its
              first picture (parameter_sets); other codecs' keyframes carry them

The known video schemas are ``VIDEO_SCHEMA_NAMES``. The recognised codecs are
JPEG and PNG (every image is a keyframe), H.264 and H.265, VP9 and AV1. A
payload wraps the frame in a serialized message, so signatures are searched for
rather than read at offset 0. The keyframe, picture and parameter-set rules
read a video frame from the ``data`` field when the payload parses as a
Foxglove ``CompressedVideo`` (``_embedded_frames``). A VP9 or AV1 frame has no
signature to search for, and around an H.264 or H.265 frame the message's own
bytes can read as a start code.

The rules read H.264 and H.265 in Annex-B form, where a start code opens each
NAL unit. Some recorders write each unit after its length instead, as MP4
stores them (AVCC). ``NalFormat`` tells the two apart once per channel and
gives the decoding paths Annex-B access units.
"""

import re
from enum import Enum
from typing import (
    AbstractSet,
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
)

_JPEG_SOI = b"\xff\xd8\xff"
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_ANNEXB_START = b"\x00\x00\x01"
# The four-byte form of the start code, which opens an access unit.
_ANNEXB_START_4 = b"\x00\x00\x00\x01"
# How far into a payload to search for a still-image signature. A ROS 2
# ``sensor_msgs/CompressedImage`` puts a header and a format string before it.
_SIGNATURE_SEARCH_WINDOW = 256

# H.264 (ITU-T H.264 table 7-1): nal_unit_type is the low five bits of the
# one-byte header. Type 5, an IDR slice, is the only slice a decoder can start
# on. H.265 (ITU-T H.265 table 7-1): nal_unit_type is bits 1-6 of a two-byte
# header. Types 16-23 are the IRAP pictures (BLA, IDR, CRA, reserved IRAP).
_H264_IDR = 5
_H265_IRAP_TYPES = range(16, 24)

# VP9 (bitstream spec 6.2): the uncompressed header opens with frame_marker
# ``0b10``, two profile bits, show_existing_frame and frame_type (0 = key
# frame). A key frame then carries frame_sync_code 0x49 0x83 0x42.
_VP9_SYNC_CODE = 0x498342

# AV1 (spec 5.3): an OBU header is a forbidden bit, a 4-bit obu_type, an
# extension flag, a has_size flag and a reserved bit. A temporal unit that
# holds a sequence header OBU is one a decoder can start on.
_AV1_SEQUENCE_HEADER = 1
_AV1_OBU_TYPES = frozenset({1, 2, 3, 4, 5, 6, 7, 8, 15})
# OBUs that carry picture data: a frame header, a tile group, a frame.
_AV1_PICTURE_OBUS = frozenset({3, 4, 6})

# Schema names whose channels carry video: Foxglove's ``CompressedVideo`` and
# ``CompressedImage`` in their ROS 2, ROS 1 and protobuf names, and ROS's
# ``sensor_msgs`` ``CompressedImage``.
VIDEO_SCHEMA_NAMES = frozenset(
    {
        "foxglove_msgs/msg/CompressedVideo",
        "foxglove_msgs/CompressedVideo",
        "foxglove.CompressedVideo",
        "foxglove_msgs/msg/CompressedImage",
        "foxglove_msgs/CompressedImage",
        "foxglove.CompressedImage",
        "sensor_msgs/msg/CompressedImage",
        "sensor_msgs/CompressedImage",
    }
)


class VideoCodec(str, Enum):
    JPEG = "jpeg"
    PNG = "png"
    H264 = "h264"
    H265 = "h265"
    VP9 = "vp9"
    AV1 = "av1"

    @property
    def every_frame_is_a_keyframe(self) -> bool:
        return self in (VideoCodec.JPEG, VideoCodec.PNG)

    @property
    def is_annex_b(self) -> bool:
        return self in (VideoCodec.H264, VideoCodec.H265)


# The parameter sets a decoder needs before its first picture, as NAL unit
# types: H.264's SPS (7) and PPS (8), H.265's VPS (32), SPS (33) and PPS (34).
# An access unit delimiter (H.264 9, H.265 35) comes before them in an access
# unit. The other codecs carry what a decoder needs in their keyframes.
_PARAMETER_SET_TYPES: Dict[VideoCodec, FrozenSet[int]] = {
    VideoCodec.H264: frozenset({7, 8}),
    VideoCodec.H265: frozenset({32, 33, 34}),
}
_DELIMITER_TYPES = {VideoCodec.H264: 9, VideoCodec.H265: 35}


# Words of a ``format`` string that name a codec, in lower case. Foxglove's
# messages hold just the word. ROS's ``sensor_msgs`` ``CompressedImage`` holds
# a phrase such as ``"bgr8; jpeg compressed bgr8"``.
_FORMAT_TOKENS: Dict[bytes, VideoCodec] = {
    b"h264": VideoCodec.H264,
    b"h265": VideoCodec.H265,
    b"hevc": VideoCodec.H265,
    b"vp9": VideoCodec.VP9,
    b"av1": VideoCodec.AV1,
    b"jpeg": VideoCodec.JPEG,
    b"jpg": VideoCodec.JPEG,
    b"png": VideoCodec.PNG,
}


def is_video_channel(
    topic: str, schema_name: Optional[str], video_topics: AbstractSet[str]
) -> bool:
    """Whether a channel carries video: a known video schema, or a listed topic."""
    return schema_name in VIDEO_SCHEMA_NAMES or topic in video_topics


def require_video_channel(
    topic: str, schema_name: Optional[str], video_topics: AbstractSet[str], path: str
) -> None:
    """Fail a decode request for a channel that is not video."""
    if not is_video_channel(topic, schema_name, video_topics):
        raise ValueError(
            f"Cannot decode topic {topic!r} in {path!r}: it is not a video topic. "
            f"Its schema {schema_name!r} is not a known video schema. List it in "
            "video_topics=[...] if it carries video, or pass topics=[...] to "
            "select only the video topics."
        )


# -- Annex-B (H.264 / H.265) ---------------------------------------------------


def _nal_headers(payload: bytes) -> Iterator[Tuple[int, int]]:
    """Yield the first two bytes after every Annex-B start code in ``payload``.

    A four-byte start code (``00 00 00 01``) contains the three-byte one, so
    searching for the latter finds both.
    """
    position = payload.find(_ANNEXB_START)
    while position != -1:
        header = position + len(_ANNEXB_START)
        if header + 1 < len(payload):
            yield payload[header], payload[header + 1]
        position = payload.find(_ANNEXB_START, header)


def _annex_b_codec(payload: bytes) -> Optional[VideoCodec]:
    """Tell H.264 from H.265 by the NAL header layout, or ``None`` if neither fits.

    An Annex-B frame opens with a start code, at most a short message header
    into the payload. So the first start code must lie within
    ``_SIGNATURE_SEARCH_WINDOW`` bytes; one deeper in is chance, as VP9 or AV1
    data can hold those bytes. The payload must hold a NAL header, and every
    one must fit the layout. An H.265 header is two bytes: a zero forbidden
    bit, a six-bit type, a six-bit layer id (zero in a single-layer stream) and
    a three-bit temporal id plus one (so at least one). An H.264 header is one
    byte: a zero forbidden bit, a two-bit reference indicator and a five-bit
    type, 1 to 23 for anything in a stream.
    """
    first = payload.find(_ANNEXB_START)
    if first == -1 or first >= _SIGNATURE_SEARCH_WINDOW:
        return None
    headers = list(_nal_headers(payload))
    if not headers:
        return None
    if _fits_h265(headers):
        # The second byte of an H.264 NAL is already payload (a profile, or
        # slice-header bits), which is rarely 1 to 7 in every NAL. So a payload
        # that fits the H.265 layout throughout is H.265, even if the H.264
        # layout fits too.
        return VideoCodec.H265
    if _fits_h264(headers):
        return VideoCodec.H264
    return None


def _fits_h265(headers: List[Tuple[int, int]]) -> bool:
    """Whether every NAL header fits the H.265 layout of a single-layer stream."""
    return all(
        (first & 0x80) == 0
        and (((first & 0x01) << 5) | (second >> 3)) == 0
        and (second & 0x07) >= 1
        for first, second in headers
    )


def _fits_h264(headers: List[Tuple[int, int]]) -> bool:
    """Whether every NAL header fits the H.264 layout, with a type of 1 to 23."""
    return all(
        (first & 0x80) == 0 and 1 <= (first & 0x1F) <= 23 for first, _ in headers
    )


def _nal_units(payload: bytes) -> List[Tuple[int, int]]:
    """``(start, header)`` of each Annex-B NAL unit in ``payload``.

    ``start`` is where the unit's start code begins, a four-byte one's leading
    zero included, and ``header`` where its header byte is. A unit runs to the
    next one's start, or to the end of ``payload``.
    """
    units = []
    position = payload.find(_ANNEXB_START)
    while position != -1:
        header = position + len(_ANNEXB_START)
        start = position
        if position > 0 and payload[position - 1] == 0:
            start -= 1
        if header < len(payload):
            units.append((start, header))
        position = payload.find(_ANNEXB_START, header)
    return units


def _nal_type(codec: VideoCodec, header: int) -> int:
    """The NAL unit type in the first header byte of an H.264 or H.265 unit."""
    return header & 0x1F if codec is VideoCodec.H264 else (header >> 1) & 0x3F


def _annex_b_data(payload: bytes) -> bytes:
    """The bytes of an H.264 or H.265 payload that hold its NAL units.

    That is the ``data`` field when the payload parses as a Foxglove
    ``CompressedVideo``, as for VP9 and AV1. A CDR header in front of it can
    read as a start code and a NAL header, and the ``format`` string follows
    it. Any other payload is read whole.
    """
    return _embedded_frames(payload)[0]


# -- length-prefixed H.264 / H.265 (AVCC) --------------------------------------


def _length_prefixed_units(
    data: bytes, codec: VideoCodec
) -> Optional[List[Tuple[int, int]]]:
    """``(start, end)`` of each NAL unit when ``data`` holds them length-prefixed.

    Each unit then follows its length as a 4-byte big-endian integer. ``None``
    unless those lengths tile ``data`` exactly and each unit opens with a NAL
    header that fits the codec's layout.
    """
    is_h264 = codec is VideoCodec.H264
    header_bytes = 1 if is_h264 else 2
    units: List[Tuple[int, int]] = []
    position = 0
    while position < len(data):
        start = position + 4
        end = start + int.from_bytes(data[position:start], "big")
        if start > len(data) or end - start < header_bytes or end > len(data):
            return None
        header = [(data[start], data[start + 1] if start + 1 < end else 0)]
        if not (_fits_h264(header) if is_h264 else _fits_h265(header)):
            return None
        units.append((start, end))
        position = end
    return units or None


class NalFormat:
    """Gives one H.264 or H.265 channel's payloads as Annex-B access units.

    The channel's first payload decides whether its NAL units are
    length-prefixed (AVCC): they are when the lengths tile its ``data`` field.
    A four-byte start code would read as a length of 1, which opens no access
    unit, so a payload that opens with one is Annex B. A length-prefixed
    payload then gets a start code in place of each length. Annex-B payloads,
    and those of other codecs, pass through unchanged.
    """

    def __init__(self, codec: VideoCodec):
        self._codec = codec
        # ``None`` until the first payload decides.
        self._length_prefixed: Optional[bool] = None if codec.is_annex_b else False

    def annex_b(self, payload: bytes) -> bytes:
        """``payload``, or its access unit in Annex-B form if the channel's NAL
        units are length-prefixed. A payload whose lengths do not tile its
        ``data`` passes through, for the decoder to reject."""
        if self._length_prefixed is False:
            return payload
        data = _annex_b_data(payload)
        units = _length_prefixed_units(data, self._codec)
        if self._length_prefixed is None:
            self._length_prefixed = units is not None and not data.startswith(
                _ANNEXB_START_4
            )
        if not self._length_prefixed or units is None:
            return payload
        return b"".join(_ANNEXB_START_4 + data[start:end] for start, end in units)


# -- message framing (where the frame bytes sit) ------------------------------


def _varint(data: bytes, pos: int) -> Tuple[Optional[int], int]:
    """Read the protobuf varint at ``pos``; the value is ``None`` if malformed."""
    value, shift = 0, 0
    while pos < len(data) and shift <= 63:
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7
    return None, pos


def _protobuf_field(payload: bytes, number: int) -> Optional[bytes]:
    """Value of length-delimited field ``number`` if ``payload`` parses as protobuf."""
    pos, size = 0, len(payload)
    while pos < size:
        tag, pos = _varint(payload, pos)
        if tag is None:
            return None
        field, wire = tag >> 3, tag & 0x07
        if wire == 0:
            value, pos = _varint(payload, pos)
            if value is None:
                return None
        elif wire == 1:
            pos += 8
        elif wire == 2:
            length, pos = _varint(payload, pos)
            if length is None or pos + length > size:
                return None
            if field == number:
                return payload[pos : pos + length]
            pos += length
        elif wire == 5:
            pos += 4
        else:
            return None
    return None


# The leading fields of the known ROS video messages, as their ``.msg`` files
# declare them. ``seq`` is one 4-byte integer and ``stamp`` two. Every other
# field is a string or ``uint8[]``: a uint32 length, then that many bytes.
_SENSOR_MSGS_ROS2 = ("stamp", "frame_id", "format", "data")
_SENSOR_MSGS_ROS1 = ("seq", "stamp", "frame_id", "format", "data")
_FOXGLOVE_ROS = ("stamp", "frame_id", "data", "format")
_ROS_WORDS = {"seq": 1, "stamp": 2}

# The layout of each known ROS video schema, by schema name and message encoding.
_ROS_LAYOUTS: Dict[Tuple[str, str], Tuple[str, ...]] = {
    ("sensor_msgs/msg/CompressedImage", "cdr"): _SENSOR_MSGS_ROS2,
    ("sensor_msgs/CompressedImage", "ros1"): _SENSOR_MSGS_ROS1,
    ("foxglove_msgs/msg/CompressedVideo", "cdr"): _FOXGLOVE_ROS,
    ("foxglove_msgs/msg/CompressedImage", "cdr"): _FOXGLOVE_ROS,
    ("foxglove_msgs/CompressedVideo", "ros1"): _FOXGLOVE_ROS,
    ("foxglove_msgs/CompressedImage", "ros1"): _FOXGLOVE_ROS,
}
# The field number of ``format`` in Foxglove's protobuf messages.
_PROTOBUF_FORMAT_FIELDS = {"foxglove.CompressedVideo": 4, "foxglove.CompressedImage": 3}


class _RosReader:
    """Reads the fields of a ROS 1 or ROS 2 (CDR) message from its start.

    ROS 1 packs fields little-endian with no padding. ROS 2 CDR opens with a
    4-byte encapsulation header that gives the byte order, and pads to 4 bytes
    before each 4-byte integer, an array's length included. A CDR string's
    length counts its NUL terminator.
    """

    def __init__(
        self, payload: bytes, pos: int, order: Literal["little", "big"], cdr: bool
    ):
        self._payload = payload
        self._pos = pos
        self._order: Literal["little", "big"] = order
        self._cdr = cdr

    @classmethod
    def open(cls, payload: bytes, message_encoding: str) -> Optional["_RosReader"]:
        """A reader at the first field, or ``None`` for another encoding."""
        if message_encoding == "ros1":
            return cls(payload, 0, "little", cdr=False)
        # CDR_BE is 00 00 and CDR_LE is 00 01, then two bytes of options.
        if message_encoding == "cdr" and payload[:2] in (b"\x00\x00", b"\x00\x01"):
            return cls(payload, 4, "little" if payload[1] else "big", cdr=True)
        return None

    def skip_words(self, count: int) -> bool:
        """Skip ``count`` 4-byte integers; ``False`` if the payload ends first."""
        for _ in range(count):
            if self._word() is None:
                return False
        return True

    def array(self) -> Optional[bytes]:
        """The next string or ``uint8[]``, or ``None`` if the payload ends first."""
        length = self._word()
        if length is None or self._pos + length > len(self._payload):
            return None
        start, self._pos = self._pos, self._pos + length
        return self._payload[start : self._pos]

    def at_end(self) -> bool:
        """Whether the payload is read to its end, CDR's alignment padding aside."""
        rest = self._payload[self._pos :]
        return not rest or (self._cdr and len(rest) < 4 and not any(rest))

    def _word(self) -> Optional[int]:
        if self._cdr:
            self._pos = (self._pos + 3) // 4 * 4
        end = self._pos + 4
        if end > len(self._payload):
            return None
        word = int.from_bytes(self._payload[self._pos : end], self._order)
        self._pos = end
        return word


def _ros_field(
    payload: bytes, message_encoding: str, layout: Tuple[str, ...], name: str
) -> Optional[bytes]:
    """Field ``name`` of a ROS 1 or ROS 2 (CDR) message laid out as ``layout``.

    ``None`` if the payload is not in that encoding or ends before the field.
    """
    reader = _RosReader.open(payload, message_encoding)
    if reader is None:
        return None
    for field in layout:
        if field in _ROS_WORDS:
            if not reader.skip_words(_ROS_WORDS[field]):
                return None
            continue
        value = reader.array()
        if value is None or field == name:
            return value
    return None


def _embedded_frames(payload: bytes) -> List[bytes]:
    """Bytes that may hold a VP9 or AV1 frame: the ``data`` field, then the payload.

    ``data`` is read as Foxglove's ``CompressedVideo`` lays it out: in protobuf
    or ROS 2 CDR when the payload opens like one, and in ROS 1. A layout counts
    only when the whole payload parses as that message, since a bare AV1
    temporal unit also parses as protobuf and bare frame bytes can open like a
    ROS 1 message.
    """
    fields: List[Optional[bytes]] = []
    if payload[:1] in (b"\x0a", b"\x12", b"\x1a", b"\x22"):
        fields.append(_protobuf_video_data(payload))
    elif payload[:1] == b"\x00":
        fields.append(_ros_video_data(payload, "cdr"))
    fields.append(_ros_video_data(payload, "ros1"))
    return [data for data in fields if data] + [payload]


def _protobuf_video_data(payload: bytes) -> Optional[bytes]:
    """``data`` of a protobuf ``foxglove.CompressedVideo``, or ``None`` if it is not one.

    Its fields are ``timestamp`` (1), ``frame_id`` (2), ``data`` (3) and
    ``format`` (4), all length-delimited. A bare AV1 temporal unit reads as
    such fields too, its OBU types as field numbers, but a sequence header is
    not a ``Timestamp`` and a tile group is not a printable ``format``. A
    ``foxglove.CompressedImage`` reads alike, but its field 3 is the printable
    ``format``.
    """
    fields = _length_delimited_fields(payload)
    if fields is None or not set(fields) <= {1, 2, 3, 4} or 3 not in fields:
        return None
    if 1 in fields and not _is_timestamp(fields[1]):
        return None
    if 4 in fields and not _is_printable(fields[4]):
        return None
    if _is_printable(fields[3]):
        return None
    return fields[3]


def _length_delimited_fields(payload: bytes) -> Optional[Dict[int, bytes]]:
    """The fields of a protobuf message whose fields are all length-delimited.

    ``None`` if the payload does not parse that way to its end, or repeats a field.
    """
    fields: Dict[int, bytes] = {}
    pos = 0
    while pos < len(payload):
        tag, pos = _varint(payload, pos)
        if tag is None or tag & 0x07 != 2 or tag >> 3 in fields:
            return None
        length, pos = _varint(payload, pos)
        if length is None or pos + length > len(payload):
            return None
        fields[tag >> 3] = payload[pos : pos + length]
        pos += length
    return fields


def _is_timestamp(value: bytes) -> bool:
    """Whether ``value`` parses as a protobuf ``Timestamp``: varint fields 1 and 2."""
    pos = 0
    while pos < len(value):
        tag, pos = _varint(value, pos)
        if tag not in (0x08, 0x10):
            return False
        number, pos = _varint(value, pos)
        if number is None:
            return False
    return True


def _is_printable(value: bytes) -> bool:
    return value.isascii() and value.decode("ascii").isprintable()


def _ros_video_data(payload: bytes, message_encoding: str) -> Optional[bytes]:
    """``data`` of a ROS 1 or ROS 2 (CDR) Foxglove ``CompressedVideo``, or ``None``.

    The payload must end with its printable ``format`` string, so bare frame
    bytes that open like the message by chance do not count.
    """
    reader = _RosReader.open(payload, message_encoding)
    if reader is None:
        return None
    values: Dict[str, bytes] = {}
    for field in _FOXGLOVE_ROS:
        if field in _ROS_WORDS:
            if not reader.skip_words(_ROS_WORDS[field]):
                return None
            continue
        value = reader.array()
        if value is None:
            return None
        values[field] = value
    # A CDR string ends with a NUL that its length counts.
    if not reader.at_end() or not _is_printable(values["format"].rstrip(b"\x00")):
        return None
    return values["data"]


def _format_codec(
    payload: bytes, schema_name: Optional[str], message_encoding: str
) -> Optional[VideoCodec]:
    """The codec that a known video schema's ``format`` field names, or ``None``.

    The field is read at the schema's layout for ``message_encoding``. ``None``
    too when the payload does not parse that way or the format names no
    recognised codec.
    """
    value: Optional[bytes] = None
    if message_encoding == "protobuf":
        number = _PROTOBUF_FORMAT_FIELDS.get(schema_name or "")
        if number is not None:
            value = _protobuf_field(payload, number)
    else:
        layout = _ROS_LAYOUTS.get((schema_name or "", message_encoding))
        if layout is not None:
            value = _ros_field(payload, message_encoding, layout, "format")
    return _codec_named_by(value) if value else None


def _codec_named_by(value: bytes) -> Optional[VideoCodec]:
    """The codec that a ``format`` string names, ignoring case.

    The first word that names one decides.
    """
    for word in re.split(rb"[^a-z0-9]+", value.lower()):
        codec = _FORMAT_TOKENS.get(word)
        if codec is not None:
            return codec
    return None


# -- VP9 ---------------------------------------------------------------------


class _Bits:
    def __init__(self, data: bytes):
        self._data = data
        self._pos = 0

    def read(self, count: int) -> Optional[int]:
        if self._pos + count > len(self._data) * 8:
            return None
        value = 0
        for _ in range(count):
            byte = self._data[self._pos >> 3]
            value = (value << 1) | ((byte >> (7 - (self._pos & 7))) & 1)
            self._pos += 1
        return value


def _vp9_keyframe(frame: bytes, strict: bool) -> Optional[bool]:
    """Whether ``frame`` is a VP9 key frame, or ``None`` if it is not VP9.

    With ``strict``, only a key frame counts as VP9. Its 24-bit sync code
    rarely matches by chance, while an inter frame header is just two bits and
    a few flags.
    """
    bits = _Bits(frame)
    if bits.read(2) != 0b10:
        return None
    low, high = bits.read(1), bits.read(1)
    if low is None or high is None:
        return None
    profile = (high << 1) | low
    if profile == 3 and bits.read(1) != 0:
        return None
    show_existing = bits.read(1)
    if show_existing is None:
        return None
    if show_existing:
        return None if strict else False
    frame_type = bits.read(1)
    if frame_type is None:
        return None
    if frame_type == 0:
        bits.read(2)  # show_frame, error_resilient_mode
        if bits.read(24) != _VP9_SYNC_CODE:
            return None
        return True
    return None if strict else False


# -- AV1 ---------------------------------------------------------------------


def _leb128(data: bytes, pos: int) -> Tuple[Optional[int], int]:
    value = 0
    for index in range(8):
        if pos >= len(data):
            return None, pos
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << (7 * index)
        if not byte & 0x80:
            return value, pos
    return None, pos


def _av1_obu_types(frame: bytes) -> Optional[List[int]]:
    """The OBU types of an AV1 temporal unit, or ``None`` if it does not parse.

    Tries the low-overhead format, where the last OBU may omit its size as the
    ISO-BMFF binding allows, then the Annex-B length-delimited format.
    """
    types = _av1_sized_obus(frame)
    if types is None:
        types = _av1_annex_b_obus(frame)
    return types or None


def _av1_obu_header(data: bytes, pos: int) -> Tuple[Optional[int], bool, int]:
    """``(obu_type, has_size, position after the header)``; type ``None`` if invalid."""
    if pos >= len(data):
        return None, False, pos
    header = data[pos]
    obu_type = (header >> 3) & 0x0F
    if header & 0x80 or header & 0x01 or obu_type not in _AV1_OBU_TYPES:
        return None, False, pos
    pos += 1 + ((header >> 2) & 1)
    # The extension byte the header announces must be there.
    if pos > len(data):
        return None, False, pos
    return obu_type, bool((header >> 1) & 1), pos


def _av1_sized_obus(frame: bytes) -> Optional[List[int]]:
    pos, types = 0, []
    while pos < len(frame):
        obu_type, has_size, pos = _av1_obu_header(frame, pos)
        if obu_type is None:
            return None
        types.append(obu_type)
        if not has_size:
            # Only the last OBU of a unit may run to the end.
            return types
        size, pos = _leb128(frame, pos)
        if size is None or pos + size > len(frame):
            return None
        pos += size
    return types


def _av1_annex_b_obus(frame: bytes) -> Optional[List[int]]:
    unit_size, pos = _leb128(frame, 0)
    if unit_size is None or pos + unit_size != len(frame):
        return None
    types = []
    while pos < len(frame):
        frame_size, pos = _leb128(frame, pos)
        if frame_size is None or pos + frame_size > len(frame):
            return None
        frame_end = pos + frame_size
        while pos < frame_end:
            obu_size, pos = _leb128(frame, pos)
            if obu_size is None or pos + obu_size > frame_end:
                return None
            obu_type, _, _ = _av1_obu_header(frame, pos)
            if obu_type is None:
                return None
            types.append(obu_type)
            pos += obu_size
    return types


def _av1_keyframe(frame: bytes) -> Optional[bool]:
    """Whether ``frame`` is an AV1 temporal unit a decoder can start on.

    ``None`` if it does not parse as AV1.
    """
    types = _av1_obu_types(frame)
    if types is None:
        return None
    return _AV1_SEQUENCE_HEADER in types


# -- public ------------------------------------------------------------------


def detect_codec(payload: bytes) -> Optional[VideoCodec]:
    """Guess a payload's codec from its bytes, or ``None`` if nothing is recognised.

    Still images are found by signature, and H.264 and H.265 by the NAL
    headers of the message's ``data`` field (``_annex_b_data``). VP9 and AV1
    are parsed from the frame bytes inside the message. VP9 needs a key frame
    (its sync code), since an inter frame header is too short to be telling.
    """
    head = payload[:_SIGNATURE_SEARCH_WINDOW]
    if _JPEG_SOI in head:
        return VideoCodec.JPEG
    if _PNG_SIGNATURE in head:
        return VideoCodec.PNG
    codec = _annex_b_codec(_annex_b_data(payload))
    if codec is not None:
        return codec
    for frame in _embedded_frames(payload):
        if _av1_keyframe(frame) is not None:
            return VideoCodec.AV1
        if _vp9_keyframe(frame, strict=True) is not None:
            return VideoCodec.VP9
    return None


class CodecSearch:
    """Finds a video channel's codec from its payloads, fed one at a time.

    The rules are :func:`channel_codec`'s. ``found`` turns true once no later
    payload can change the answer: the first payload's ``format`` field, or a
    payload whose bytes name one codec. A payload that fits both Annex-B
    layouts counts only if no payload names a codec, so that answer waits for
    the last payload.
    """

    def __init__(self, schema_name: Optional[str], message_encoding: str):
        self._schema_name = schema_name
        self._message_encoding = message_encoding
        self._fed_first = False
        self._codec: Optional[VideoCodec] = None
        self._fallback: Optional[VideoCodec] = None

    @property
    def found(self) -> bool:
        return self._codec is not None

    def feed(self, payload: bytes) -> None:
        if self._codec is not None:
            return
        if not self._fed_first:
            self._fed_first = True
            self._codec = _format_codec(
                payload, self._schema_name, self._message_encoding
            )
            if self._codec is not None:
                return
        codec = detect_codec(payload)
        headers = list(_nal_headers(_annex_b_data(payload)))
        if codec is VideoCodec.H265 and _fits_h264(headers):
            self._fallback = self._fallback or codec
        elif codec is not None:
            self._codec = codec

    def result(self) -> Optional[VideoCodec]:
        """The codec, given every payload fed so far was the channel's last."""
        return self._codec or self._fallback


def channel_codec(
    schema_name: Optional[str], message_encoding: str, payloads: Iterable[bytes]
) -> Optional[VideoCodec]:
    """The codec of a video channel, or ``None`` if nothing names one.

    A known video schema names it in the first message's ``format`` field.
    Otherwise, or when that field is missing or names no recognised codec, the
    payloads' bytes decide: a payload that fits both the H.264 and the H.265
    layout, such as a lone SEI NAL, is named H.265 only when no other payload
    names a codec (``CodecSearch``).
    """
    search = CodecSearch(schema_name, message_encoding)
    for payload in payloads:
        search.feed(payload)
        if search.found:
            break
    return search.result()


def carries_picture(payload: bytes, codec: VideoCodec) -> bool:
    """Whether the payload holds coded picture data, not only parameter sets.

    A recorder may write the parameter sets (SPS, PPS, VPS) in a message of
    their own ahead of a keyframe. A decoder fed such a message has nothing to
    output, which is not an error. A VP9 frame is always a picture. An AV1
    temporal unit is one when it holds a frame or tile group OBU.
    """
    if codec.every_frame_is_a_keyframe or codec is VideoCodec.VP9:
        return True
    if codec is VideoCodec.H264:
        headers = _nal_headers(_annex_b_data(payload))
        return any(1 <= (first & 0x1F) <= 5 for first, _ in headers)
    if codec is VideoCodec.H265:
        headers = _nal_headers(_annex_b_data(payload))
        return any(((first >> 1) & 0x3F) <= 31 for first, _ in headers)
    for frame in _embedded_frames(payload):
        types = _av1_obu_types(frame)
        if types is not None:
            return bool(_AV1_PICTURE_OBUS & set(types))
    return True


def is_keyframe(payload: bytes, codec: VideoCodec) -> bool:
    """Whether a decoder can start on this payload."""
    if codec.every_frame_is_a_keyframe:
        return True
    if codec is VideoCodec.H264:
        headers = _nal_headers(_annex_b_data(payload))
        return any((first & 0x1F) == _H264_IDR for first, _ in headers)
    if codec is VideoCodec.H265:
        headers = _nal_headers(_annex_b_data(payload))
        return any(((first >> 1) & 0x3F) in _H265_IRAP_TYPES for first, _ in headers)
    for frame in _embedded_frames(payload):
        found = (
            _vp9_keyframe(frame, strict=False)
            if codec is VideoCodec.VP9
            else _av1_keyframe(frame)
        )
        if found is not None:
            return found
    return False


def parameter_set_types(codec: VideoCodec) -> FrozenSet[int]:
    """The kinds of parameter set, as NAL unit types, that a decoder needs
    before its first picture. Empty for a codec whose keyframes carry them."""
    return _PARAMETER_SET_TYPES.get(codec, frozenset())


def parameter_set_kinds(payload: bytes, codec: VideoCodec) -> FrozenSet[int]:
    """The kinds of parameter set that ``payload`` holds."""
    types = parameter_set_types(codec)
    if not types:
        return frozenset()
    data = _annex_b_data(payload)
    kinds = (_nal_type(codec, data[header]) for _, header in _nal_units(data))
    return frozenset(kind for kind in kinds if kind in types)


def parameter_sets(
    payload: bytes, codec: VideoCodec, skip: AbstractSet[int] = frozenset()
) -> bytes:
    """The parameter-set NAL units of ``payload``, start codes included, in order.

    Kinds in ``skip`` are left out. A unit runs to the next start code, or to
    the end of the ``data`` field.
    """
    types = parameter_set_types(codec) - skip
    if not types:
        return b""
    data = _annex_b_data(payload)
    units = _nal_units(data)
    ends = [start for start, _ in units[1:]] + [len(data)]
    return b"".join(
        data[start:end]
        for (start, header), end in zip(units, ends)
        if _nal_type(codec, data[header]) in types
    )


def with_parameter_sets(frame: bytes, sets: bytes, codec: VideoCodec) -> bytes:
    """The access unit ``frame`` with ``sets`` put first, after its delimiter if
    it opens with one."""
    units = _nal_units(frame)
    at = units[0][0] if units else 0
    if units and _nal_type(codec, frame[units[0][1]]) == _DELIMITER_TYPES.get(codec):
        at = units[1][0] if len(units) > 1 else len(frame)
    return frame[:at] + sets + frame[at:]
