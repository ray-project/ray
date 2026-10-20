"""Unit tests for telling a video payload's codec and keyframes from its bytes.

The payloads are synthetic: valid headers around filler bytes, built from named
parts, bare or inside a CDR, ROS 1 or protobuf message.
"""

import importlib.util
import struct

import pytest

from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    NalFormat,
    VideoCodec,
    carries_picture,
    channel_codec,
    detect_codec,
    is_keyframe,
    parameter_set_kinds,
    parameter_sets,
    with_parameter_sets,
)
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    H264_KEYFRAME,
    H264_PFRAME,
    H264_SLICE,
    SECOND,
    START_CODE,
    START_CODE_SECOND,
    VP9_KEYFRAME,
    VP9_PFRAME,
    cdr_video,
    h264_nal,
    length_prefixed,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)

H264_SEI = 6  # H.264 NAL unit type of supplemental enhancement information
H264_AUD = 9  # H.264 NAL unit type of an access unit delimiter

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
        # A protobuf CompressedImage holds its data in field 2, and its format
        # in field 3, where a CompressedVideo holds its data.
        (protobuf_image(H264_KEYFRAME, "h264"), VideoCodec.H264, True),
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


def test_a_codec_search_skips_a_payload_that_fits_both_annex_b_layouts():
    """A lone SEI fits both the H.264 and the H.265 layout and is named H.265, so
    a stream's codec comes from the first payload that only one layout fits."""
    sei_only = h264_nal(H264_SEI, 0, bytes([0x05, 0x10]) + b"\x33" * 8 + b"\x80")
    assert detect_codec(sei_only) is VideoCodec.H265

    def codec_of(payloads):
        return channel_codec(None, "cdr", payloads)

    assert codec_of([sei_only, H264_KEYFRAME, H264_PFRAME]) is VideoCodec.H264
    assert codec_of([sei_only, H265_KEYFRAME]) is VideoCodec.H265
    assert codec_of([sei_only]) is VideoCodec.H265
    assert codec_of([VP9_PFRAME, VP9_KEYFRAME]) is VideoCodec.VP9


def test_codec_search_settles_once_no_later_payload_can_change_it():
    """A payload that fits both Annex-B layouts names H.265 only if no other
    payload names a codec, so the search stays open after it, and settles on
    the next payload that names one."""
    from ray.data._internal.datasource_v2.formats.mcap.mcap_video import CodecSearch

    sei_only = h264_nal(H264_SEI, 0, bytes([0x05, 0x10]) + b"\x33" * 8 + b"\x80")
    search = CodecSearch(schema_name=None, message_encoding="cdr")

    search.feed(sei_only)
    assert not search.found and search.result() is VideoCodec.H265
    search.feed(H264_PFRAME)

    assert search.found and search.result() is VideoCodec.H264


def test_a_start_code_deep_in_a_frame_does_not_name_annex_b():
    """VP9 or AV1 data can hold ``00 00 01`` by chance. Only a start code near the
    payload's start names H.264 or H.265, so such a stream keeps its codec."""
    noisy_pframe = VP9_PFRAME + bytes(300) + h264_nal(H264_SLICE, 2, b"\x9a\x02")
    assert detect_codec(noisy_pframe) is None
    assert channel_codec(None, "cdr", [noisy_pframe, VP9_KEYFRAME]) is VideoCodec.VP9
    # A frame behind a short message header is still found.
    assert detect_codec(b"header-bytes" + H264_KEYFRAME) is VideoCodec.H264


def test_a_start_code_with_no_nal_header_names_no_codec():
    """A start code at the end of a payload has no NAL header after it, so it
    names neither H.264 nor H.265, and a VP9 key frame ending in one is VP9."""
    assert detect_codec(START_CODE) is None
    keyframe = VP9_KEYFRAME + b"\x00\x00\x01"
    assert detect_codec(keyframe) is VideoCodec.VP9
    assert channel_codec(None, "cdr", [keyframe, VP9_PFRAME]) is VideoCodec.VP9


def test_detect_codec_unknown():
    """Bytes that fit no codec name none, and so does a VP9 inter frame, whose
    header is too short to tell."""
    assert detect_codec(b"\x01\x02\x03\x04" * 8) is None
    assert detect_codec(b"") is None
    # Without a matching format string, a VP9 inter frame stays unrecognised.
    assert detect_codec(VP9_PFRAME) is None
    assert detect_codec(cdr_video(VP9_PFRAME, "h264")) is None
    assert detect_codec(AV1_PFRAME) is VideoCodec.AV1  # opens with a delimiter
    # A temporal delimiter, then a sequence header OBU whose header announces
    # an extension byte the payload lacks.
    assert detect_codec(b"\x12\x00\x0c") is None


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


@pytest.mark.parametrize(
    "keyframe, codec, kinds",
    [
        (H264_KEYFRAME, VideoCodec.H264, {7, 8}),
        (H265_KEYFRAME, VideoCodec.H265, {H265_VPS, H265_SPS, H265_PPS}),
    ],
    ids=["h264", "h265"],
)
def test_parameter_sets_come_out_and_go_back_first(keyframe, codec, kinds):
    """A keyframe's parameter sets come out with their start codes, in order,
    also from inside a message. Put back into the bare picture, they rebuild the
    access unit as the encoder wrote it."""
    sets = parameter_sets(keyframe, codec)
    picture = keyframe[len(sets) :]

    assert parameter_set_kinds(keyframe, codec) == kinds
    assert parameter_set_kinds(picture, codec) == frozenset()
    assert keyframe.startswith(sets) and sets
    assert parameter_sets(cdr_video(keyframe, codec.value), codec) == sets
    assert with_parameter_sets(picture, sets, codec) == keyframe
    # A kind already at hand is left out.
    first = min(kinds)
    assert parameter_set_kinds(parameter_sets(keyframe, codec, {first}), codec) == (
        kinds - {first}
    )


def test_parameter_sets_go_after_an_access_unit_delimiter():
    """An access unit that opens with a delimiter keeps it first, as H.264
    requires, with the parameter sets right after it."""
    delimiter = h264_nal(H264_AUD, 0, bytes([0xF0]))
    sets = parameter_sets(H264_KEYFRAME, VideoCodec.H264)
    picture = H264_KEYFRAME[len(sets) :]

    rebuilt = with_parameter_sets(delimiter + picture, sets, VideoCodec.H264)

    assert rebuilt == delimiter + H264_KEYFRAME


def test_h264_rules_read_only_the_data_field():
    """A CDR header can read as a start code and an IDR slice header, and the
    ``format`` string trails the data. The H.264 rules read only the ``data``
    field, so a P-frame is no keyframe and a message of parameter sets holds no
    picture and no bytes after them."""
    stamp = START_CODE_SECOND * SECOND
    sets = parameter_sets(H264_KEYFRAME, VideoCodec.H264)
    pframe = cdr_video(H264_PFRAME, "h264", stamp_ns=stamp)
    sets_only = cdr_video(sets, "h264", stamp_ns=stamp)
    # Read whole, each header holds a start code and an IDR slice header.
    assert b"\x00\x00\x01\x25" in pframe[: pframe.index(H264_PFRAME)]

    assert not is_keyframe(pframe, VideoCodec.H264)
    assert not carries_picture(sets_only, VideoCodec.H264)
    assert parameter_sets(sets_only, VideoCodec.H264) == sets


@pytest.mark.parametrize(
    "frame, codec",
    [(H264_KEYFRAME, VideoCodec.H264), (H265_KEYFRAME, VideoCodec.H265)],
    ids=["h264", "h265"],
)
def test_codec_detection_reads_only_the_data_field(frame, codec):
    """A CDR header can read as a start code and a NAL header that fits neither
    layout. Without a ``format``, the bytes of the ``data`` field alone name the
    codec."""
    payload = cdr_video(frame, "", stamp_ns=SECOND)
    # Read whole, the header holds a start code and the NAL header 00 00.
    assert b"\x00\x00\x01\x00\x00" in payload[: payload.index(frame)]

    assert detect_codec(payload) is codec
    assert channel_codec("foxglove_msgs/msg/CompressedVideo", "cdr", [payload]) is codec


@pytest.mark.parametrize(
    "keyframe, pframe, codec",
    [
        (H264_KEYFRAME, H264_PFRAME, VideoCodec.H264),
        (H265_KEYFRAME, H265_PFRAME, VideoCodec.H265),
    ],
    ids=["h264", "h265"],
)
def test_length_prefixed_nal_units_come_out_as_annex_b(keyframe, pframe, codec):
    """A channel whose first payload holds length-prefixed NAL units (AVCC) gets
    each payload with start codes in their place, where the keyframe rules find
    them. A payload whose lengths do not tile its data passes through."""
    nal_format = NalFormat(codec)
    avcc_keyframe = cdr_video(length_prefixed(keyframe), codec.value)
    cut_short = cdr_video(length_prefixed(pframe)[:-1], codec.value)

    assert not is_keyframe(avcc_keyframe, codec)
    assert nal_format.annex_b(avcc_keyframe) == keyframe
    assert nal_format.annex_b(cdr_video(length_prefixed(pframe), codec.value)) == pframe
    assert nal_format.annex_b(cut_short) == cut_short


# Two one-byte NAL units, each after a four-byte start code that also reads as a
# length of 1, so the start codes tile the bytes as lengths.
TILES_AS_LENGTHS = (START_CODE + bytes([H264_AUD])) * 2


@pytest.mark.parametrize(
    "first",
    [H264_KEYFRAME, bytes(2) + H264_KEYFRAME, TILES_AS_LENGTHS],
    ids=["annex-b", "leading-zero-bytes", "tiles-as-lengths"],
)
def test_a_stream_that_looks_length_prefixed_stays_annex_b(first):
    """The channel's first payload decides, so a later payload that tiles as
    lengths by chance passes through. A first payload that opens with zero
    bytes, as Annex B allows, is not length-prefixed either. One that opens with
    a four-byte start code is Annex B even when it tiles: no access unit opens
    with a NAL unit of one byte."""
    nal_format = NalFormat(VideoCodec.H264)
    payloads = [first, TILES_AS_LENGTHS, length_prefixed(H264_PFRAME)]

    assert [nal_format.annex_b(p) for p in payloads] == payloads


def test_codecs_without_parameter_sets_have_none_to_take():
    """VP9 keyframes carry what a decoder needs, so there is nothing to take."""
    assert parameter_set_kinds(VP9_KEYFRAME, VideoCodec.VP9) == frozenset()
    assert parameter_sets(VP9_KEYFRAME, VideoCodec.VP9) == b""


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
