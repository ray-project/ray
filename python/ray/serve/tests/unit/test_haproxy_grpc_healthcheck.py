"""Unit tests for build_grpc_healthcheck_request_hex().

The function returns a hex string passed verbatim to HAProxy's
`tcp-check send-binary`. It must encode a complete, valid HTTP/2 client
message stream for a unary gRPC `Healthz` call:

  1. H2 connection preface
  2. empty SETTINGS frame
  3. HEADERS frame (HPACK, END_HEADERS only) carrying :method, :scheme,
     :path, :authority, te, content-type
  4. DATA frame carrying a 5-byte empty gRPC message and END_STREAM

This test verifies all four pieces by parsing the bytes back out.
"""

from typing import Tuple

import pytest

from ray.serve._private.haproxy import build_grpc_healthcheck_request_hex

H2_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

# HTTP/2 frame type codes (RFC 7540 §6).
FRAME_DATA = 0x0
FRAME_HEADERS = 0x1
FRAME_SETTINGS = 0x4

# HTTP/2 frame flags relevant to this test.
FLAG_END_STREAM = 0x1
FLAG_END_HEADERS = 0x4


def _parse_frame(buf: bytes, offset: int) -> Tuple[int, int, int, bytes, int]:
    """Parse a single HTTP/2 frame starting at offset.

    Returns (frame_type, flags, stream_id, payload, next_offset).
    """
    length = int.from_bytes(buf[offset : offset + 3], "big")
    frame_type = buf[offset + 3]
    flags = buf[offset + 4]
    stream_id = int.from_bytes(buf[offset + 5 : offset + 9], "big") & 0x7FFFFFFF
    payload = buf[offset + 9 : offset + 9 + length]
    assert len(payload) == length, "frame truncated"
    return frame_type, flags, stream_id, payload, offset + 9 + length


def _parse_hpack_literals(block: bytes) -> dict:
    """Parse an HPACK block built by build_grpc_healthcheck_request_hex.

    The producer only emits 0x00-prefixed literal-without-indexing entries
    with new names, so we don't need a real HPACK decoder. Every entry is:
      0x00 <name_len> <name bytes> <value_len> <value bytes>
    All lengths are < 127 (single byte). Returns {name: value}.
    """
    out: dict = {}
    i = 0
    while i < len(block):
        assert (
            block[i] == 0x00
        ), f"expected literal-no-index marker at {i}, got {block[i]:#x}"
        i += 1
        name_len = block[i]
        assert name_len < 128, "unexpected huffman / long-length encoding"
        i += 1
        name = block[i : i + name_len].decode()
        i += name_len
        value_len = block[i]
        assert value_len < 128
        i += 1
        value = block[i : i + value_len].decode()
        i += value_len
        out[name] = value
    return out


@pytest.fixture
def healthz_bytes() -> bytes:
    hex_str = build_grpc_healthcheck_request_hex(
        "/ray.serve.RayServeAPIService/Healthz"
    )
    # Sanity: result is a hex string, decodes cleanly.
    return bytes.fromhex(hex_str)


def test_starts_with_h2_preface(healthz_bytes: bytes):
    """Every HTTP/2 client connection must start with the 24-byte preface."""
    assert healthz_bytes.startswith(H2_PREFACE)


def test_emits_empty_settings_then_headers_then_data(healthz_bytes: bytes):
    """Frame sequence must be SETTINGS (empty) → HEADERS → DATA."""
    offset = len(H2_PREFACE)

    ftype, flags, stream_id, payload, offset = _parse_frame(healthz_bytes, offset)
    assert ftype == FRAME_SETTINGS
    assert payload == b"", "client SETTINGS must be empty (no settings sent)"
    assert stream_id == 0

    ftype, flags, _, headers_payload, offset = _parse_frame(healthz_bytes, offset)
    assert ftype == FRAME_HEADERS

    ftype, flags, _, data_payload, offset = _parse_frame(healthz_bytes, offset)
    assert ftype == FRAME_DATA

    # No trailing bytes after the DATA frame.
    assert offset == len(
        healthz_bytes
    ), f"unexpected trailing bytes: {healthz_bytes[offset:]!r}"


def test_headers_frame_carries_required_grpc_pseudo_headers(healthz_bytes: bytes):
    """The Python gRPC server rejects requests missing :authority with a
    Trailers-Only error before the handler runs. Verify every required
    pseudo-header (and te/content-type) is present.
    """
    offset = len(H2_PREFACE)
    _, _, _, _, offset = _parse_frame(healthz_bytes, offset)  # skip SETTINGS
    ftype, flags, stream_id, payload, _ = _parse_frame(healthz_bytes, offset)
    assert ftype == FRAME_HEADERS

    # HEADERS frame must end the headers block but NOT the stream, since the
    # gRPC message is still coming in the following DATA frame.
    assert flags & FLAG_END_HEADERS, "END_HEADERS must be set"
    assert not (
        flags & FLAG_END_STREAM
    ), "END_STREAM on HEADERS would suppress the DATA frame"
    assert stream_id == 1, "client stream ids start at 1"

    headers = _parse_hpack_literals(payload)
    assert headers[":method"] == "POST"
    assert headers[":scheme"] == "http"
    assert headers[":path"] == "/ray.serve.RayServeAPIService/Healthz"
    # `:authority` is mandatory; without it, the server returns Trailers-Only
    # "Missing :authority header" and the check never matches.
    assert headers[":authority"], ":authority must be present"
    assert headers["content-type"] == "application/grpc"
    assert headers["te"] == "trailers"


def test_data_frame_is_empty_grpc_message_with_end_stream(healthz_bytes: bytes):
    """The DATA frame must carry exactly the 5-byte empty gRPC frame
    (uncompressed flag + 4-byte length=0) so the server's framing layer
    deserializes an empty HealthzRequest and runs the handler. END_STREAM
    must be on this frame, not on HEADERS.
    """
    offset = len(H2_PREFACE)
    _, _, _, _, offset = _parse_frame(healthz_bytes, offset)  # SETTINGS
    _, _, _, _, offset = _parse_frame(healthz_bytes, offset)  # HEADERS
    ftype, flags, stream_id, payload, _ = _parse_frame(healthz_bytes, offset)

    assert ftype == FRAME_DATA
    assert stream_id == 1
    assert flags & FLAG_END_STREAM, "DATA frame must close the request stream"
    assert (
        payload == b"\x00\x00\x00\x00\x00"
    ), f"expected empty gRPC frame, got {payload.hex()}"


@pytest.mark.parametrize(
    "path",
    [
        "/ray.serve.RayServeAPIService/Healthz",
        "/grpc.health.v1.Health/Check",
        "/a/b",
    ],
)
def test_path_is_threaded_through_to_pseudo_header(path: str):
    """The :path pseudo-header must reflect the argument verbatim — the
    template renders one healthcheck per backend, all with the same
    health_check_path, and rendering the wrong path would silently route
    the check away from the handler."""
    raw = bytes.fromhex(build_grpc_healthcheck_request_hex(path))
    offset = len(H2_PREFACE)
    _, _, _, _, offset = _parse_frame(raw, offset)  # SETTINGS
    _, _, _, headers_payload, _ = _parse_frame(raw, offset)
    headers = _parse_hpack_literals(headers_payload)
    assert headers[":path"] == path


def test_output_is_lowercase_hex_with_no_separators():
    """`tcp-check send-binary` accepts a hex string with no separators;
    HAProxy will reject the config if we emit anything else. Guard against
    accidental formatting changes (uppercase, spaces, 0x-prefixes)."""
    out = build_grpc_healthcheck_request_hex("/x")
    assert out == out.lower(), "hex must be lowercase"
    assert all(
        c in "0123456789abcdef" for c in out
    ), f"non-hex character in output: {out!r}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
