"""Unit / integration tests for the external-shuffle runtime primitives.

Covers the Arrow Flight fetch transport, ``ShuffleFileServer`` actor lifecycle,
the shard codec, prefetch layout, and error classification.
"""

import contextlib
import os
import socket
import struct
import threading

import pytest

from ray.data._internal.execution.operators.shuffle_operators.external_shuffle_runtime import (  # noqa: E501
    ShuffleDiskError,
    ShuffleFileServer,
    ShuffleFileServerAnomalyError,
    _chunk_members_by_bytes,
    _compute_prefetch_layout,
    _encode_shard,
    _FileRanges,
    _group_by_server,
    _make_flight_server,
    _NodeGroup,
    _PwriteSink,
    _read_ipc,
    _SourceRef,
    _stream_members_flight,
)
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.tests.conftest import *  # noqa: F401, F403


# ---------------------------------------------------------------------- helpers
def _make_file_server(tmp_path, token="test-token"):
    """Create a ShuffleFileServer actor rooted at tmp_path, return (actor, endpoint)."""
    import ray

    actor = ShuffleFileServer.remote(str(tmp_path), token)
    endpoint = ray.get(actor.endpoint.remote())
    return actor, endpoint


@contextlib.contextmanager
def _running_flight_server(base_dir, token):
    """Start a bare Flight server (no Ray) on loopback; yield its (host, port)."""
    srv = _make_flight_server("127.0.0.1", str(base_dir), token)
    endpoint = ("127.0.0.1", srv.port)
    t = threading.Thread(target=srv.serve, daemon=True)
    t.start()
    try:
        yield endpoint
    finally:
        srv.shutdown()


def _open_sink(tmp_path, size=64):
    """Open a fresh pwrite sink over a ``size``-byte file at offset 0."""
    fd = os.open(str(tmp_path / "sink.bin"), os.O_RDWR | os.O_CREAT, 0o644)
    os.ftruncate(fd, size)
    return fd, _PwriteSink(fd, base_offset=0)


# --------------------------------------------------------- _PwriteSink testers
def test_pwrite_sink_write(tmp_path):
    path = tmp_path / "sink.bin"
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        os.ftruncate(fd, 32)
        sink = _PwriteSink(fd, base_offset=8)
        n = sink.write(b"hello")
        assert n == 5
        # Reading confirms the write landed at base_offset, not 0.
        assert os.pread(fd, 5, 8) == b"hello"
        assert os.pread(fd, 5, 0) == b"\x00" * 5
    finally:
        os.close(fd)


def test_pwrite_sink_reset(tmp_path):
    path = tmp_path / "sink.bin"
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        os.ftruncate(fd, 32)
        sink = _PwriteSink(fd, base_offset=4)
        sink.write(b"first")
        sink.reset()
        sink.write(b"AGAIN")  # overwrites the same region
        assert os.pread(fd, 5, 4) == b"AGAIN"
    finally:
        os.close(fd)


# ----------------------------------------------- ShuffleFileServer actor lifecycle
def test_shuffle_file_server_lifecycle(ray_start_regular_shared_2_cpus, tmp_path):
    # Creating a file server should (1) mkdir the base_dir on disk, (2) return a
    # well-formed endpoint, and (3) leave the Flight endpoint reachable via TCP.
    sub = tmp_path / "does-not-exist-yet"
    _actor, (host, port) = _make_file_server(sub)

    assert sub.exists() and sub.is_dir()
    assert isinstance(host, str) and host
    assert isinstance(port, int) and 1024 < port < 65536

    s = socket.create_connection((host, port), timeout=5)
    s.close()


# ------------------------------------------------ Arrow Flight fetch transport
def test_flight_fetch_wire_format(tmp_path):
    # do_action streams each range as [u32 len][frame bytes] into the sink,
    # back-to-back, in request order.
    payload = b"HELLO_WORLD_" * 4  # 48 bytes
    (tmp_path / "s.bin").write_bytes(payload)

    fd, sink = _open_sink(tmp_path)
    try:
        with _running_flight_server(tmp_path, "t") as endpoint:
            _stream_members_flight(
                endpoint,
                "t",
                [_FileRanges(path="s.bin", ranges=[(0, 12), (12, 12)])],
                max_bytes=1 << 20,
                sink=sink,
            )
        # Record 1: u32(12) + payload[0:12]
        assert struct.unpack(">I", os.pread(fd, 4, 0))[0] == 12
        assert os.pread(fd, 12, 4) == payload[0:12]
        # Record 2: u32(12) + payload[12:24] immediately after
        assert struct.unpack(">I", os.pread(fd, 4, 16))[0] == 12
        assert os.pread(fd, 12, 20) == payload[12:24]
    finally:
        os.close(fd)


def test_flight_auth_fail_is_terminal(tmp_path):
    # Wrong token -> server rejects -> PermissionError (terminal, not retryable).
    (tmp_path / "s.bin").write_bytes(b"x" * 16)
    fd, sink = _open_sink(tmp_path)
    try:
        with _running_flight_server(tmp_path, "correct") as endpoint:
            with pytest.raises(PermissionError):
                _stream_members_flight(
                    endpoint,
                    "wrong",
                    [_FileRanges(path="s.bin", ranges=[(0, 4)])],
                    max_bytes=1 << 20,
                    sink=sink,
                )
    finally:
        os.close(fd)


def test_flight_unreachable_is_connection_error(tmp_path):
    # Bind a port with nothing listening -> connect refused -> ConnectionError
    # (retryable transport fault, distinct from the terminal auth failure).
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]  # bound but never listen()ed
        fd, sink = _open_sink(tmp_path)
        try:
            with pytest.raises(ConnectionError):
                _stream_members_flight(
                    ("127.0.0.1", port),
                    "t",
                    [_FileRanges(path="x.bin", ranges=[(0, 4)])],
                    max_bytes=1 << 20,
                    sink=sink,
                )
        finally:
            os.close(fd)
    finally:
        s.close()


def test_flight_short_read_fails(tmp_path):
    # File shorter than the requested range: the header already promised `length`
    # bytes, so the server MUST fail the stream rather than truncate. A short send
    # silently desyncs every later frame at the client. SPARK-34534: response and
    # request correspondence is lost and data is silently mis-associated.
    (tmp_path / "s.bin").write_bytes(b"only8byt")  # 8 bytes
    fd, sink = _open_sink(tmp_path)
    try:
        with _running_flight_server(tmp_path, "t") as endpoint:
            with pytest.raises(ConnectionError, match="short read"):
                _stream_members_flight(
                    endpoint,
                    "t",
                    [_FileRanges(path="s.bin", ranges=[(0, 64)])],  # asks for 64
                    max_bytes=1 << 20,
                    sink=sink,
                )
    finally:
        os.close(fd)


# --------------------------- reducer helpers (no Ray and no sockets)
def test_chunk_members_by_bytes():
    # Cover the three interesting shapes: fits-in-one, split-across-batches,
    # and single-range-larger-than-budget (must stand alone as its own batch).
    m1 = _FileRanges(path="a", ranges=[(0, 10), (10, 10)])  # 20 total
    m2 = _FileRanges(path="b", ranges=[(0, 30)])  # 30 total
    m3 = _FileRanges(path="c", ranges=[(0, 5), (5, 5)])  # 10 total

    # Budget 100 -> everything fits in one batch.
    batches = list(_chunk_members_by_bytes([m1, m2, m3], max_bytes=100))
    assert len(batches) == 1 and batches[0] == [m1, m2, m3]

    # Budget 25 -> splits: {m1(20)}, {m2's 30-byte range alone}, {m3(10)}.
    # Single range never splits, even if it exceeds the budget.
    batches = list(_chunk_members_by_bytes([m1, m2, m3], max_bytes=25))
    assert len(batches) == 3
    assert batches[0] == [m1]
    assert batches[1] == [_FileRanges(path="b", ranges=[(0, 30)])]
    assert batches[2] == [m3]

    # Empty input -> no batches.
    assert list(_chunk_members_by_bytes([], max_bytes=100)) == []


def test_group_by_server():
    # Same (shuffle_id, node_id) collapses; distinct pairs stay separate.
    s0 = _SourceRef("sh", "n1", "tok", _FileRanges("a", [(0, 4)]))
    s1 = _SourceRef("sh", "n2", "tok", _FileRanges("b", [(0, 8)]))
    s2 = _SourceRef("sh", "n1", "tok", _FileRanges("c", [(0, 2)]))

    groups = _group_by_server([s0, s1, s2])
    by_node = {g.node_id: g for g in groups}
    assert set(by_node) == {"n1", "n2"}

    # Members within a group are in original input order.
    assert [m.path for m in by_node["n1"].members] == ["a", "c"]
    assert [m.path for m in by_node["n2"].members] == ["b"]


def test_compute_prefetch_layout():
    # Each range contributes 4 (u32 len prefix) + range_length to the group's
    # size. base_offsets are the running cumulative sum.
    g0 = _NodeGroup(
        "sh",
        "n1",
        "tok",
        members=[
            _FileRanges(path="a", ranges=[(0, 10), (10, 10)]),  # (4+10)*2 = 28
        ],
    )
    g1 = _NodeGroup(
        "sh",
        "n2",
        "tok",
        members=[
            _FileRanges(path="b", ranges=[(0, 100)]),  # 4+100 = 104
        ],
    )
    total, base_offsets, sizes = _compute_prefetch_layout([g0, g1])
    assert sizes == [28, 104]
    assert base_offsets == [0, 28]
    assert total == 132

    # Empty layout.
    assert _compute_prefetch_layout([]) == (0, [], [])


# -------------------------------------------------------- shard codec roundtrip
def test_encode_read_ipc_roundtrip():
    import pyarrow as pa

    # Multi-chunk table to exercise combine_chunks in _encode_shard.
    t = pa.concat_tables(
        [
            pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}),
            pa.table({"a": [4, 5], "b": ["p", "q"]}),
        ]
    )
    buf = _encode_shard(t)
    assert _read_ipc(buf).equals(t.combine_chunks())


# --------------------------------------------------- error class sanity checks
def test_shuffle_disk_error_is_runtime_error_subclass():
    assert issubclass(ShuffleDiskError, RuntimeError)
    assert not issubclass(ShuffleDiskError, ShuffleFileServerAnomalyError)


def test_shuffle_file_server_anomaly_error_is_runtime_error_subclass():
    assert issubclass(ShuffleFileServerAnomalyError, RuntimeError)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
