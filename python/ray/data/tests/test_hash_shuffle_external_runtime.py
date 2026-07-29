"""Unit / integration tests for the external-shuffle runtime primitives.

Covers the Arrow Flight transport (server + client fetch, auth, error
classification), ``ShuffleManager`` actor lifecycle, prefetch layout, the
shard codec, and the error hierarchy.
"""

import os
import socket
import struct
import threading
import time

import pytest

from ray.data._internal.execution.operators.shuffle_operators.external_shuffle_runtime import (  # noqa: E501
    ShuffleDiskError,
    ShuffleManager,
    ShuffleManagerAnomalyError,
    _chunk_members_by_bytes,
    _compute_prefetch_layout,
    _encode_shard,
    _group_by_manager,
    _make_flight_server,
    _NodeGroup,
    _NodeMember,
    _PwriteSink,
    _read_ipc,
    _SourceRef,
    _stream_members_flight,
)
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.tests.conftest import *  # noqa: F401, F403


# ---------------------------------------------------------------------- helpers
def _make_manager(tmp_path, token="test-token"):
    """Create a ShuffleManager actor rooted at tmp_path, return (actor, endpoint)."""
    import ray

    actor = ShuffleManager.remote(str(tmp_path), token)
    endpoint = ray.get(actor.endpoint.remote())
    return actor, endpoint


def _serve_flight(tmp_path, token="t"):
    """Start an in-process Flight server on tmp_path; return (server, endpoint)."""
    srv = _make_flight_server("127.0.0.1", str(tmp_path), token)
    threading.Thread(target=srv.serve, daemon=True).start()
    time.sleep(0.3)
    return srv, ("127.0.0.1", srv.port)


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


# ----------------------------------------------- ShuffleManager actor lifecycle
def test_shuffle_manager_lifecycle(ray_start_regular_shared_2_cpus, tmp_path):
    # Creating a manager should (1) mkdir the base_dir on disk, (2) return a
    # well-formed endpoint, and (3) leave the gRPC endpoint reachable via TCP.
    sub = tmp_path / "does-not-exist-yet"
    _actor, (host, port) = _make_manager(sub)

    assert sub.exists() and sub.is_dir()
    assert isinstance(host, str) and host
    assert isinstance(port, int) and 1024 < port < 65536

    s = socket.create_connection((host, port), timeout=5)
    s.close()


# --------------------------------------------- Flight transport: fetch + errors
def test_flight_fetch_wire_format(tmp_path):
    # _stream_members_flight writes a flat (u32 len + bytes)* stream to the sink,
    # same layout the reducer decodes.
    srv, endpoint = _serve_flight(tmp_path, token="t")
    payload = b"HELLO_WORLD_" * 4  # 48 bytes
    (tmp_path / "s.bin").write_bytes(payload)

    fd = os.open(str(tmp_path / "sink.bin"), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        os.ftruncate(fd, 128)
        sink = _PwriteSink(fd, base_offset=0)
        members = [_NodeMember(path="s.bin", ranges=[(0, 12), (12, 12)])]
        _stream_members_flight(endpoint, "t", members, 1 << 20, sink)

        # Record 1: u32(12) + payload[0:12]
        assert struct.unpack(">I", os.pread(fd, 4, 0))[0] == 12
        assert os.pread(fd, 12, 4) == payload[0:12]
        # Record 2: u32(12) + payload[12:24] immediately after
        assert struct.unpack(">I", os.pread(fd, 4, 16))[0] == 12
        assert os.pread(fd, 12, 20) == payload[12:24]
    finally:
        os.close(fd)
        srv.shutdown()


def test_flight_auth_fail_is_terminal(tmp_path):
    # Wrong token -> PermissionError (terminal, not a retryable transport fault).
    srv, endpoint = _serve_flight(tmp_path, token="correct")
    (tmp_path / "s.bin").write_bytes(b"X" * 100)
    fd = os.open(str(tmp_path / "sink.bin"), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        sink = _PwriteSink(fd, base_offset=0)
        members = [_NodeMember(path="s.bin", ranges=[(0, 100)])]
        with pytest.raises(PermissionError):
            _stream_members_flight(endpoint, "wrong", members, 1 << 20, sink)
    finally:
        os.close(fd)
        srv.shutdown()


def test_flight_unreachable_is_connection_error(tmp_path):
    # Reserve a port with no Flight server behind it -> the fetch surfaces a
    # retryable ConnectionError (mapped from the Flight transport failure).
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    fd = os.open(str(tmp_path / "sink.bin"), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        sink = _PwriteSink(fd, base_offset=0)
        members = [_NodeMember(path="x", ranges=[(0, 4)])]
        with pytest.raises((ConnectionError, OSError)):
            _stream_members_flight(("127.0.0.1", port), "t", members, 1 << 20, sink)
    finally:
        os.close(fd)


# --------------------------- reducer helpers (no Ray and no sockets)
def test_chunk_members_by_bytes():
    # Cover the three interesting shapes: fits-in-one, split-across-batches,
    # and single-range-larger-than-budget (must stand alone as its own batch).
    m1 = _NodeMember(path="a", ranges=[(0, 10), (10, 10)])  # 20 total
    m2 = _NodeMember(path="b", ranges=[(0, 30)])  # 30 total
    m3 = _NodeMember(path="c", ranges=[(0, 5), (5, 5)])  # 10 total

    # Budget 100 -> everything fits in one batch.
    batches = list(_chunk_members_by_bytes([m1, m2, m3], max_bytes=100))
    assert len(batches) == 1 and batches[0] == [m1, m2, m3]

    # Budget 25 -> splits: {m1(20)}, {m2's 30-byte range alone}, {m3(10)}.
    # Single range never splits, even if it exceeds the budget.
    batches = list(_chunk_members_by_bytes([m1, m2, m3], max_bytes=25))
    assert len(batches) == 3
    assert batches[0] == [m1]
    assert batches[1] == [_NodeMember(path="b", ranges=[(0, 30)])]
    assert batches[2] == [m3]

    # Empty input -> no batches.
    assert list(_chunk_members_by_bytes([], max_bytes=100)) == []


def test_group_by_manager():
    # Same (shuffle_id, node_id) collapses; distinct pairs stay separate.
    s0 = _SourceRef("sh", "n1", "tok", "a", [(0, 4)])
    s1 = _SourceRef("sh", "n2", "tok", "b", [(0, 8)])
    s2 = _SourceRef("sh", "n1", "tok", "c", [(0, 2)])

    groups = _group_by_manager([s0, s1, s2])
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
            _NodeMember(path="a", ranges=[(0, 10), (10, 10)]),  # (4+10)*2 = 28
        ],
    )
    g1 = _NodeGroup(
        "sh",
        "n2",
        "tok",
        members=[
            _NodeMember(path="b", ranges=[(0, 100)]),  # 4+100 = 104
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
    assert not issubclass(ShuffleDiskError, ShuffleManagerAnomalyError)


def test_shuffle_manager_anomaly_error_is_runtime_error_subclass():
    assert issubclass(ShuffleManagerAnomalyError, RuntimeError)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
