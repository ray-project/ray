"""Recognising video topics and keyframes from MCAP payload bytes.

A window row has to carry, for every video topic, the frames back to the last
keyframe before the window starts, or the clip cannot be decoded. MCAP is
codec-agnostic, so the codec is sniffed from the bytes: a JPEG or PNG
signature (every still image is a keyframe), or H.264 / H.265 NAL units in
Annex-B byte-stream form, the format most recorders write.

Payloads are the serialized message (CDR, protobuf, ...) with the frame bytes
embedded after a small header, so signatures are searched for rather than
expected at offset 0. Length-prefixed streams (AVCC / HVCC) carry no start
codes and are not recognised; ``VideoOptions.lead_in_s`` covers them.
"""

from enum import Enum
from typing import Iterator, Optional, Tuple

_JPEG_SOI = b"\xff\xd8\xff"
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_ANNEXB_START = b"\x00\x00\x01"
# How far into a payload a still-image signature may sit: a ROS 2
# ``sensor_msgs/CompressedImage`` puts a header and a format string before it.
_SIGNATURE_SEARCH_WINDOW = 256

# H.264 (ITU-T H.264 table 7-1): nal_unit_type is the low five bits of the
# one-byte header; 5 is an IDR slice, the only slice type a decoder can start
# on. H.265 (ITU-T H.265 table 7-1): nal_unit_type is bits 1-6 of a two-byte
# header; 16-23 are the IRAP pictures (BLA, IDR, CRA and reserved IRAP types).
_H264_IDR = 5
_H265_IRAP_TYPES = range(16, 24)

# Schema names under which recorders commonly log compressed video or images.
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

    @property
    def every_frame_is_a_keyframe(self) -> bool:
        return self in (VideoCodec.JPEG, VideoCodec.PNG)


def is_video_schema(schema_name: Optional[str]) -> bool:
    """Whether a channel with this schema name carries compressed video or images."""
    return schema_name is not None and schema_name in VIDEO_SCHEMA_NAMES


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


def detect_codec(payload: bytes) -> Optional[VideoCodec]:
    """Guess a payload's codec from its bytes, or ``None`` if nothing is recognised.

    H.264 and H.265 share the Annex-B framing and are told apart by their NAL
    header layouts. An H.265 header is two bytes: a zero forbidden bit, a
    six-bit type, a six-bit layer id (zero in a single-layer stream) and a
    three-bit temporal id plus one (so at least one). An H.264 header is one
    byte: a zero forbidden bit, a two-bit reference indicator and a five-bit
    type between 1 and 23 for anything carried in a stream.
    """
    head = payload[:_SIGNATURE_SEARCH_WINDOW]
    if _JPEG_SOI in head:
        return VideoCodec.JPEG
    if _PNG_SIGNATURE in head:
        return VideoCodec.PNG
    headers = list(_nal_headers(payload))
    if not headers:
        return None
    hevc_like = all(
        (first & 0x80) == 0
        and (((first & 0x01) << 5) | (second >> 3)) == 0
        and (second & 0x07) >= 1
        for first, second in headers
    )
    if hevc_like:
        # The second byte of an H.264 NAL is payload: a profile for a parameter
        # set, slice-header bits for a slice, both practically never 1..7 for
        # every NAL of a stream. A stream that fits the H.265 layout throughout
        # is H.265, even where the H.264 layout would also fit.
        return VideoCodec.H265
    if all((first & 0x80) == 0 and 1 <= (first & 0x1F) <= 23 for first, _ in headers):
        return VideoCodec.H264
    return None


def is_keyframe(payload: bytes, codec: VideoCodec) -> bool:
    """Whether a decoder can start on this payload."""
    if codec.every_frame_is_a_keyframe:
        return True
    if codec is VideoCodec.H264:
        return any((first & 0x1F) == _H264_IDR for first, _ in _nal_headers(payload))
    return any(
        ((first >> 1) & 0x3F) in _H265_IRAP_TYPES for first, _ in _nal_headers(payload)
    )
