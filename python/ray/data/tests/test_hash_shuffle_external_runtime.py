"""Unit / integration tests for the external-shuffle runtime primitives.

Covers the wire protocol, ``ShuffleManager`` actor lifecycle, connection
handshake, fetch semantics, and error classification.
"""
import os
import socket
import struct
import threading

import pytest

from ray.data._internal.execution.operators.shuffle_operators.external_shuffle_runtime import (  # noqa: E501
    ShuffleDiskError,
    ShuffleManager,
    ShuffleManagerAnomalyError,
    _FetchHandler,
    _NodeGroup,
    _NodeMember,
    _PwriteSink,
    _SourceRef,
    _ThreadingServer,
    _ThreadingServerV6,
    _chunk_members_by_bytes,
    _compute_prefetch_layout,
    _group_by_manager,
    _threading_server_for,
    open_shuffle_connection,
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


def _write_source_file(tmp_path, name, payload: bytes) -> str:
    """Write a raw byte blob into the manager's base_dir and return abs path."""
    p = tmp_path / name
    p.write_bytes(payload)
    return str(p.resolve())


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
    # well-formed endpoint, and (3) leave the endpoint reachable via TCP.
    sub = tmp_path / "does-not-exist-yet"
    _actor, (host, port) = _make_manager(sub)

    assert sub.exists() and sub.is_dir()
    assert isinstance(host, str) and host
    assert isinstance(port, int) and 1024 < port < 65536

    s = socket.create_connection((host, port), timeout=5)
    s.close()


# ----------------------------------------------------------------- IPv6 support
def test_threading_server_for_picks_family():
    # IPv4 literals + hostnames + non-IP strings → default V4 class.
    assert _threading_server_for("127.0.0.1") is _ThreadingServer
    assert _threading_server_for("192.168.1.1") is _ThreadingServer
    assert _threading_server_for("localhost") is _ThreadingServer
    assert _threading_server_for("not-an-ip") is _ThreadingServer
    # IPv6 literals → V6 subclass with AF_INET6.
    assert _threading_server_for("::1") is _ThreadingServerV6
    assert _threading_server_for("2001:db8::1") is _ThreadingServerV6


def test_ipv6_server_binds_and_reachable(tmp_path):
    try:
        probe = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        probe.bind(("::1", 0))
        probe.close()
    except OSError:
        pytest.skip("IPv6 not available on this host")

    server = _ThreadingServerV6(("::1", 0), _FetchHandler)
    server.token = "unused"
    server.base_dir = str(tmp_path)
    host, port = server.server_address[:2]
    assert host == "::1"
    assert 1024 < port < 65536

    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        s = socket.create_connection((host, port), timeout=5)
        s.close()
    finally:
        server.shutdown()
        server.server_close()


# --------------------------------------------------------------------- handshake
def test_handshake_token_validation(ray_start_regular_shared_2_cpus, tmp_path):
    _actor, endpoint = _make_manager(tmp_path, token="correct")
    # Good token yields a live connection.
    with open_shuffle_connection(endpoint, "correct") as conn:
        assert conn is not None
    # Bad token is terminal (PermissionError, not retryable).
    with pytest.raises(PermissionError):
        open_shuffle_connection(endpoint, "wrong")


def test_handshake_unreachable_endpoint_raises_connection_error():
    # Bind a socket without listen()ing so the kernel returns ECONNREFUSED deterministically
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        with pytest.raises((ConnectionError, OSError)):
            open_shuffle_connection(("127.0.0.1", port), "any-token")
    finally:
        s.close()


# --------------------------------------------- fetch_into (streaming to sink)
def test_fetch_error_paths(ray_start_regular_shared_2_cpus, tmp_path):
    # Missing path → FileNotFoundError; path outside base_dir → PermissionError.
    # Errors fire on the response status byte, before anything hits the sink.
    _actor, endpoint = _make_manager(tmp_path, token="t")

    sink_path = tmp_path / "sink.bin"
    fd = os.open(str(sink_path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        os.ftruncate(fd, 16)
        sink = _PwriteSink(fd, base_offset=0)

        with open_shuffle_connection(endpoint, "t") as conn:
            with pytest.raises(FileNotFoundError):
                conn.fetch_into(
                    [(str(tmp_path / "missing.bin"), [(0, 4)])], sink
                )

        outside = tmp_path.parent / "outside.bin"
        outside.write_bytes(b"secret")
        try:
            with open_shuffle_connection(endpoint, "t") as conn:
                with pytest.raises(PermissionError):
                    conn.fetch_into([(str(outside), [(0, 6)])], sink)
        finally:
            outside.unlink(missing_ok=True)
    finally:
        os.close(fd)



def test_fetch_into_wire_format(ray_start_regular_shared_2_cpus, tmp_path):
    # fetch_into writes a flat (u32 len + bytes)* stream to the sink.
    _actor, endpoint = _make_manager(tmp_path, token="t")
    payload = b"HELLO_WORLD_" * 4  # 48 bytes
    path = _write_source_file(tmp_path, "s.bin", payload)

    sink_path = tmp_path / "sink.bin"
    fd = os.open(str(sink_path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        os.ftruncate(fd, 64)
        sink = _PwriteSink(fd, base_offset=0)
        with open_shuffle_connection(endpoint, "t") as conn:
            conn.fetch_into([(path, [(0, 12), (12, 12)])], sink)

        # Record 1: u32(12) + payload[0:12]
        len0 = struct.unpack(">I", os.pread(fd, 4, 0))[0]
        assert len0 == 12
        assert os.pread(fd, 12, 4) == payload[0:12]

        # Record 2: u32(12) + payload[12:24] immediately after
        len1 = struct.unpack(">I", os.pread(fd, 4, 16))[0]
        assert len1 == 12
        assert os.pread(fd, 12, 20) == payload[12:24]
    finally:
        os.close(fd)


def test_fetch_into_reset_and_retry(ray_start_regular_shared_2_cpus, tmp_path):
    # Simulate the retry path in _prefetch_node_into: partial write, reset,
    # write again, then data at the base offset should be the second attempt.
    _actor, endpoint = _make_manager(tmp_path, token="t")
    path_a = _write_source_file(tmp_path, "a.bin", b"AAAAAA")
    path_b = _write_source_file(tmp_path, "b.bin", b"BBBBBB")

    sink_path = tmp_path / "sink.bin"
    fd = os.open(str(sink_path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        os.ftruncate(fd, 128)
        sink = _PwriteSink(fd, base_offset=0)
        with open_shuffle_connection(endpoint, "t") as conn:
            conn.fetch_into([(path_a, [(0, 6)])], sink)
        sink.reset()
        with open_shuffle_connection(endpoint, "t") as conn:
            conn.fetch_into([(path_b, [(0, 6)])], sink)

        # Expect u32(6) + b"BBBBBB" at offset 0.
        len0 = struct.unpack(">I", os.pread(fd, 4, 0))[0]
        assert len0 == 6
        assert os.pread(fd, 6, 4) == b"BBBBBB"
    finally:
        os.close(fd)


# --------------------------- reducer helpers (no Ray and no sockets)
def test_chunk_members_by_bytes():
    # Cover the three interesting shapes: fits-in-one, split-across-batches,
    # and single-range-larger-than-budget (must stand alone as its own batch).
    m1 = _NodeMember(path="a", ranges=[(0, 10), (10, 10)])   # 20 total
    m2 = _NodeMember(path="b", ranges=[(0, 30)])             # 30 total
    m3 = _NodeMember(path="c", ranges=[(0, 5), (5, 5)])      # 10 total

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
    g0 = _NodeGroup("sh", "n1", "tok", members=[
        _NodeMember(path="a", ranges=[(0, 10), (10, 10)]),  # (4+10)*2 = 28
    ])
    g1 = _NodeGroup("sh", "n2", "tok", members=[
        _NodeMember(path="b", ranges=[(0, 100)]),           # 4+100 = 104
    ])
    total, base_offsets, sizes = _compute_prefetch_layout([g0, g1])
    assert sizes == [28, 104]
    assert base_offsets == [0, 28]
    assert total == 132

    # Empty layout.
    assert _compute_prefetch_layout([]) == (0, [], [])


# --------------------------------------------------- error class sanity checks
def test_shuffle_disk_error_is_runtime_error_subclass():
    assert issubclass(ShuffleDiskError, RuntimeError)
    assert not issubclass(ShuffleDiskError, ShuffleManagerAnomalyError)


def test_shuffle_manager_anomaly_error_is_runtime_error_subclass():
    assert issubclass(ShuffleManagerAnomalyError, RuntimeError)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
