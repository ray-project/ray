"""Unit tests for telling a video payload's codec and keyframes from its bytes.

The payloads are synthetic: valid headers around filler bytes, built from named
parts, bare or inside a CDR, ROS 1 or protobuf message.
"""

import importlib.util
import struct

import pytest

from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    VideoCodec,
    channel_codec,
    detect_codec,
    is_keyframe,
)
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    H264_KEYFRAME,
    H264_PFRAME,
    H264_SLICE,
    START_CODE,
    VP9_KEYFRAME,
    VP9_PFRAME,
    h264_nal,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)

H264_SEI = 6  # H.264 NAL unit type of supplemental enhancement information

# H.265 NAL unit types.
H265_TRAIL_R = 1  # a slice of an inter frame
H265_IDR_W_RADL = 19
H265_VPS = 32
H265_SPS = 33
H265_PPS = 34


def h265_nal(nal_type: int, payload: bytes) -> bytes:
    """An Annex-B H.265 NAL unit: the start code, a two-byte header (layer 0,
    temporal id 0), ``payload``."""
    return START_CODE + bytes([nal_type << 1, 0x01]) + payload


H265_KEYFRAME = (
    h265_nal(H265_VPS, b"\x0c")
    + h265_nal(H265_SPS, b"\x01")
    + h265_nal(H265_PPS, b"\xc1")
    + h265_nal(H265_IDR_W_RADL, b"\xaf")
)
H265_PFRAME = h265_nal(H265_TRAIL_R, b"\xd0")

# AV1 OBU types.
AV1_SEQUENCE_HEADER = 1
AV1_TEMPORAL_DELIMITER = 2
AV1_FRAME_HEADER = 3
AV1_TILE_GROUP = 4
AV1_FRAME = 6


def av1_obu(obu_type: int, payload: bytes) -> bytes:
    """An AV1 OBU in the low-overhead format: a header byte, then the size of
    ``payload`` (under 128 bytes) and ``payload``."""
    return bytes([obu_type << 3 | 0b010, len(payload)]) + payload


# A temporal unit opens with a temporal delimiter. A key frame's also carries a
# sequence header.
AV1_KEYFRAME = (
    av1_obu(AV1_TEMPORAL_DELIMITER, b"")
    + av1_obu(AV1_SEQUENCE_HEADER, bytes(4))
    + av1_obu(AV1_FRAME, bytes(5))
)
AV1_PFRAME = av1_obu(AV1_TEMPORAL_DELIMITER, b"") + av1_obu(AV1_FRAME, bytes(5))
# A key unit whose frame is a frame header and a tile group, as some encoders
# write it. Its bytes also parse as protobuf, with the frame header as field 3.
AV1_SPLIT_KEYFRAME = (
    av1_obu(AV1_TEMPORAL_DELIMITER, b"")
    + av1_obu(AV1_SEQUENCE_HEADER, bytes(4))
    + av1_obu(AV1_FRAME_HEADER, b"\x10\x00")
    + av1_obu(AV1_TILE_GROUP, bytes(3))
)
JPEG = b"\xff\xd8\xff\xe0\x00\x10JFIF" + bytes(32)
PNG = b"\x89PNG\r\n\x1a\n" + bytes(32)
# Bytes that name no codec, even after a length field.
OPAQUE = b"\x11\x22\x33\x44" * 4


def cdr_video(frame: bytes, fmt: str) -> bytes:
    """A CDR-encoded Foxglove ``CompressedVideo`` message around ``frame``."""
    body = bytes(8) + (4).to_bytes(4, "little") + b"cam\x00"
    body += len(frame).to_bytes(4, "little") + frame
    body += bytes(-len(body) % 4)
    token = fmt.encode() + b"\x00"
    return b"\x00\x01\x00\x00" + body + len(token).to_bytes(4, "little") + token


def protobuf_video(frame: bytes, fmt: str) -> bytes:
    """A protobuf-encoded Foxglove ``CompressedVideo`` message around ``frame``."""
    return (
        b"\x0a\x02\x08\x01"  # timestamp
        + b"\x12\x03cam"  # frame_id
        + b"\x1a"
        + bytes([len(frame)])
        + frame  # data
        + b"\x22"
        + bytes([len(fmt)])
        + fmt.encode()  # format
    )


def protobuf_image(data: bytes, fmt: str) -> bytes:
    """A protobuf-encoded Foxglove ``CompressedImage``: data is field 2, format 3."""
    return (
        b"\x0a\x02\x08\x01"  # timestamp
        + b"\x12"
        + bytes([len(data)])
        + data  # data
        + b"\x1a"
        + bytes([len(fmt)])
        + fmt.encode()  # format
        + b"\x22\x03cam"  # frame_id
    )


def ros2_message(*fields, big_endian=False) -> bytes:
    """A CDR-encoded ROS 2 message of uint32 fields (``int``), strings (``str``)
    and ``uint8[]`` (``bytes``)."""
    order = ">" if big_endian else "<"
    body = b""
    for value in fields:
        body += bytes(-len(body) % 4)
        if isinstance(value, int):
            body += struct.pack(order + "I", value)
            continue
        raw = value.encode() + b"\x00" if isinstance(value, str) else value
        body += struct.pack(order + "I", len(raw)) + raw
    return (b"\x00\x00" if big_endian else b"\x00\x01") + b"\x00\x00" + body


def ros1_message(*fields) -> bytes:
    """A ROS 1 message of uint32 fields (``int``), strings (``str``) and
    ``uint8[]`` (``bytes``)."""
    out = b""
    for value in fields:
        if isinstance(value, int):
            out += struct.pack("<I", value)
            continue
        raw = value.encode() if isinstance(value, str) else value
        out += struct.pack("<I", len(raw)) + raw
    return out


# A VP9 key frame whose bytes after the sync code read as a ROS 1 message's
# first two arrays, the second opening like a VP9 inter frame.
VP9_KEYFRAME_LIKE_ROS1 = (
    VP9_KEYFRAME[:4] + bytes(4) + ros1_message(b"\x00", b"\x86\x00") + bytes(8)
)


@pytest.mark.parametrize(
    "payload, codec, keyframe",
    [
        (H264_KEYFRAME, VideoCodec.H264, True),
        (H264_PFRAME, VideoCodec.H264, False),
        (H265_KEYFRAME, VideoCodec.H265, True),
        (H265_PFRAME, VideoCodec.H265, False),
        (b"\x00\x00\x00\x08cdr-head" + JPEG, VideoCodec.JPEG, True),
        (PNG, VideoCodec.PNG, True),
        # VP9 and AV1 key frames are recognised bare or inside the message
        # framing.
        (VP9_KEYFRAME, VideoCodec.VP9, True),
        (cdr_video(VP9_KEYFRAME, "vp9"), VideoCodec.VP9, True),
        (ros1_message(1, 2, "cam", VP9_KEYFRAME, "vp9"), VideoCodec.VP9, True),
        (AV1_KEYFRAME, VideoCodec.AV1, True),
        (protobuf_video(AV1_KEYFRAME, "av1"), VideoCodec.AV1, True),
        (cdr_video(AV1_PFRAME, "av1"), VideoCodec.AV1, False),
        (protobuf_video(AV1_PFRAME, "av1"), VideoCodec.AV1, False),
        (cdr_video(H264_PFRAME, "h264"), VideoCodec.H264, False),
        # A bare key frame whose bytes also read as part of a message is still
        # a key frame: a message must fill the whole payload.
        (AV1_SPLIT_KEYFRAME, VideoCodec.AV1, True),
        (protobuf_video(AV1_SPLIT_KEYFRAME, "av1"), VideoCodec.AV1, True),
        (VP9_KEYFRAME_LIKE_ROS1, VideoCodec.VP9, True),
    ],
)
def test_detect_codec_and_keyframes(payload, codec, keyframe):
    """A payload's bytes name its codec and whether it is a keyframe."""
    assert detect_codec(payload) is codec
    assert is_keyframe(payload, codec) is keyframe


def test_stream_codec_skips_a_payload_that_fits_both_annex_b_layouts():
    """A lone SEI fits both the H.264 and the H.265 layout and is named H.265, so
    a stream's codec comes from the first payload that only one layout fits."""
    from ray.data._internal.datasource_v2.formats.mcap.mcap_video import stream_codec

    sei_only = h264_nal(H264_SEI, 0, bytes([0x05, 0x10]) + b"\x33" * 8 + b"\x80")
    assert detect_codec(sei_only) is VideoCodec.H265

    codec_of = stream_codec
    assert codec_of([sei_only, H264_KEYFRAME, H264_PFRAME]) is VideoCodec.H264
    assert codec_of([sei_only, H265_KEYFRAME]) is VideoCodec.H265
    assert codec_of([sei_only]) is VideoCodec.H265
    assert codec_of([VP9_PFRAME, VP9_KEYFRAME]) is VideoCodec.VP9


def test_a_start_code_deep_in_a_frame_does_not_name_annex_b():
    """VP9 or AV1 data can hold ``00 00 01`` by chance. Only a start code near the
    payload's start names H.264 or H.265, so such a stream keeps its codec."""
    from ray.data._internal.datasource_v2.formats.mcap.mcap_video import stream_codec

    noisy_pframe = VP9_PFRAME + bytes(300) + h264_nal(H264_SLICE, 2, b"\x9a\x02")
    assert detect_codec(noisy_pframe) is None
    assert stream_codec([noisy_pframe, VP9_KEYFRAME]) is VideoCodec.VP9
    # A frame behind a short message header is still found.
    assert detect_codec(b"header-bytes" + H264_KEYFRAME) is VideoCodec.H264


def test_a_start_code_with_no_nal_header_names_no_codec():
    """A start code at the end of a payload has no NAL header after it, so it
    names neither H.264 nor H.265, and a VP9 key frame ending in one is VP9."""
    from ray.data._internal.datasource_v2.formats.mcap.mcap_video import stream_codec

    assert detect_codec(START_CODE) is None
    keyframe = VP9_KEYFRAME + b"\x00\x00\x01"
    assert detect_codec(keyframe) is VideoCodec.VP9
    assert stream_codec([keyframe, VP9_PFRAME]) is VideoCodec.VP9


def test_detect_codec_unknown():
    """Bytes that fit no codec name none, and so does a VP9 inter frame, whose
    header is too short to tell."""
    assert detect_codec(b"\x01\x02\x03\x04" * 8) is None
    assert detect_codec(b"") is None
    # Without a matching format string, a VP9 inter frame stays unrecognised.
    assert detect_codec(VP9_PFRAME) is None
    assert detect_codec(cdr_video(VP9_PFRAME, "h264")) is None
    assert detect_codec(AV1_PFRAME) is VideoCodec.AV1  # opens with a delimiter


@pytest.mark.parametrize(
    "schema_name, encoding, payload, codec, keyframe",
    [
        (
            "sensor_msgs/msg/CompressedImage",
            "cdr",
            ros2_message(1, 2, "cam", "bgr8; jpeg compressed bgr8", OPAQUE),
            VideoCodec.JPEG,
            True,
        ),
        (
            "sensor_msgs/msg/CompressedImage",
            "cdr",
            ros2_message(
                1, 2, "cam", "16UC1; compressedDepth PNG", OPAQUE, big_endian=True
            ),
            VideoCodec.PNG,
            True,
        ),
        (
            "sensor_msgs/CompressedImage",
            "ros1",
            ros1_message(7, 1, 2, "cam", "rgb8; png compressed rgb8", OPAQUE),
            VideoCodec.PNG,
            True,
        ),
        (
            "foxglove_msgs/msg/CompressedVideo",
            "cdr",
            cdr_video(VP9_PFRAME, "vp9"),
            VideoCodec.VP9,
            False,
        ),
        (
            "foxglove_msgs/msg/CompressedImage",
            "cdr",
            cdr_video(OPAQUE, "JPEG"),
            VideoCodec.JPEG,
            True,
        ),
        (
            "foxglove_msgs/CompressedVideo",
            "ros1",
            ros1_message(1, 2, "cam", VP9_PFRAME, "vp9"),
            VideoCodec.VP9,
            False,
        ),
        (
            "foxglove_msgs/CompressedImage",
            "ros1",
            ros1_message(1, 2, "cam", OPAQUE, "png"),
            VideoCodec.PNG,
            True,
        ),
        (
            "foxglove.CompressedVideo",
            "protobuf",
            protobuf_video(VP9_PFRAME, "vp9"),
            VideoCodec.VP9,
            False,
        ),
        (
            "foxglove.CompressedImage",
            "protobuf",
            protobuf_image(OPAQUE, "jpeg"),
            VideoCodec.JPEG,
            True,
        ),
    ],
)
def test_codec_from_the_format_field(schema_name, encoding, payload, codec, keyframe):
    """A known video schema's codec comes from its ``format`` field, read at the
    schema's layout for the encoding, where the bytes alone name no codec or the
    wrong one."""
    assert detect_codec(payload) is not codec
    assert channel_codec(schema_name, encoding, [payload]) is codec
    assert is_keyframe(payload, codec) is keyframe


@pytest.mark.parametrize(
    "schema_name, encoding, payloads, codec",
    [
        # A missing, empty or unknown format: the bytes decide.
        (
            "foxglove.CompressedImage",
            "protobuf",
            [protobuf_image(JPEG, "")],
            VideoCodec.JPEG,
        ),
        (
            "foxglove_msgs/msg/CompressedImage",
            "cdr",
            [cdr_video(JPEG, "")],
            VideoCodec.JPEG,
        ),
        (
            "foxglove_msgs/msg/CompressedImage",
            "cdr",
            [cdr_video(PNG, "webp")],
            VideoCodec.PNG,
        ),
        # A payload that does not parse at the layout: the bytes decide, looking
        # past VP9 inter frames to a key frame.
        ("foxglove_msgs/msg/CompressedVideo", "cdr", [H264_KEYFRAME], VideoCodec.H264),
        (
            "foxglove_msgs/CompressedVideo",
            "ros1",
            [VP9_PFRAME, VP9_KEYFRAME],
            VideoCodec.VP9,
        ),
        # A schema read_mcap does not know, or a JSON message: the format is not read.
        ("my_msgs/msg/Frame", "cdr", [cdr_video(H264_PFRAME, "vp9")], VideoCodec.H264),
        (
            "foxglove.CompressedVideo",
            "json",
            [b'{"format": "h264", "data": "AAAB"}'],
            None,
        ),
        # Neither the format nor the bytes name a codec.
        (
            "foxglove_msgs/msg/CompressedVideo",
            "cdr",
            [cdr_video(OPAQUE, "theora")],
            None,
        ),
        ("foxglove_msgs/msg/CompressedVideo", "cdr", [], None),
    ],
)
def test_codec_falls_back_to_the_bytes(schema_name, encoding, payloads, codec):
    """Without a ``format`` that names a codec, the payloads' bytes decide."""
    assert channel_codec(schema_name, encoding, payloads) is codec


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
