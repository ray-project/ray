"""Unit / integration tests for the disk-shuffle runtime primitives.

Covers the Arrow Flight fetch transport, ``ShuffleFileServer`` actor lifecycle,
the shard codec, prefetch layout, and error classification.
"""

import contextlib
import os
import socket
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from ray.actor import ActorClass, ActorHandle
from ray.data._internal.execution.operators.shuffle_operators import (
    disk_shuffle_runtime as _runtime,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_runtime import (  # noqa: E501
    _ENDPOINT_CACHE,
    _ENDPOINT_CACHE_LOCK,
    _RECORD_HEADER,
    _SHUFFLE_FILE_SERVER_NAMESPACE,
    ShuffleDiskError,
    ShuffleFileServer,
    ShuffleFileServerAnomalyError,
    _append_record,
    _compute_prefetch_layout,
    _counts_to_array,
    _decode_records,
    _encode_shard,
    _Endpoint,
    _fetch_from_file_server,
    _file_server_name,
    _FileRanges,
    _group_by_server,
    _make_flight_server,
    _NodeGroup,
    _order_by_map_id,
    _partition_input,
    _PartitionWriter,
    _PwriteSink,
    _read_ipc,
    _SourceRef,
    _stream_members_flight,
)
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.tests.conftest import *  # noqa: F401, F403


# ---------------------------------------------------------------------- helpers
def _make_file_server(tmp_path, shuffle_id="shuffle-0", node_id="node-1"):
    """Create a named (not detached) ShuffleFileServer; return (actor, endpoint)."""
    import ray

    actor_cls = cast(ActorClass[Any], ShuffleFileServer)
    actor = cast(
        ActorHandle[Any],
        actor_cls.options(
            name=_file_server_name(shuffle_id, node_id),
            namespace=_SHUFFLE_FILE_SERVER_NAMESPACE,
        ).remote(str(tmp_path)),
    )
    endpoint = cast(_Endpoint, ray.get(actor.endpoint.remote()))
    return actor, endpoint


def _clear_endpoint_cache(shuffle_id, node_id):
    with _ENDPOINT_CACHE_LOCK:
        _ENDPOINT_CACHE.pop(_file_server_name(shuffle_id, node_id), None)


@contextlib.contextmanager
def _running_flight_server(base_dir):
    """Start a bare Flight server (no Ray) on loopback; yield its
    (host, port, incarnation) endpoint (incarnation is a fixed test sentinel).
    """
    srv = _make_flight_server("127.0.0.1", str(base_dir))
    endpoint = _Endpoint("127.0.0.1", srv.port, "test-incarnation")
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
        sink.write(b"AGAIN")
        assert os.pread(fd, 5, 4) == b"AGAIN"
    finally:
        os.close(fd)


# ----------------------------------------------- ShuffleFileServer actor lifecycle
def test_shuffle_file_server_lifecycle(ray_start_regular_shared_2_cpus, tmp_path):
    # Creating a file server should (1) mkdir the base_dir on disk, (2) return a
    # well-formed endpoint, and (3) leave the Flight endpoint reachable via TCP.
    sub = tmp_path / "does-not-exist-yet"
    _actor, (host, port, incarnation) = _make_file_server(sub)

    assert sub.exists() and sub.is_dir()
    assert isinstance(host, str) and host
    assert isinstance(port, int) and 1024 < port < 65536
    assert isinstance(incarnation, str) and incarnation

    s = socket.create_connection((host, port), timeout=5)
    s.close()


def test_fetch_unregistered_name_is_anomaly(ray_start_regular_shared_2_cpus, tmp_path):
    # Cache miss + no named actor in the shuffle namespace → _resolve raises.
    shuffle_id, node_id = "unregistered", "node-1"
    _clear_endpoint_cache(shuffle_id, node_id)
    fd, sink = _open_sink(tmp_path)
    try:
        with pytest.raises(ShuffleFileServerAnomalyError, match="not found"):
            _fetch_from_file_server(
                sink,
                shuffle_id,
                node_id,
                [_FileRanges(path="x.bin", ranges=[(0, 4)])],
            )
    finally:
        os.close(fd)


def test_fetch_killed_actor_is_anomaly(ray_start_regular_shared_2_cpus, tmp_path):
    # After ray.kill(no_restart=True) the name is gone, so _resolve's get_actor
    # raises ValueError (same mapping as unregistered, not ActorDiedError).
    import ray
    from ray._common.test_utils import wait_for_condition

    shuffle_id, node_id = "killed", "node-1"
    key = _file_server_name(shuffle_id, node_id)
    actor, _endpoint = _make_file_server(tmp_path, shuffle_id, node_id)
    ray.kill(actor, no_restart=True)

    def _name_gone():
        try:
            ray.get_actor(key, namespace=_SHUFFLE_FILE_SERVER_NAMESPACE)
            return False
        except ValueError:
            return True

    wait_for_condition(_name_gone)
    _clear_endpoint_cache(shuffle_id, node_id)
    fd, sink = _open_sink(tmp_path)
    try:
        with pytest.raises(ShuffleFileServerAnomalyError):
            _fetch_from_file_server(
                sink,
                shuffle_id,
                node_id,
                [_FileRanges(path="x.bin", ranges=[(0, 4)])],
            )
    finally:
        os.close(fd)


def test_fetch_restart_retry(ray_start_regular_shared_2_cpus, tmp_path):
    # Seed cache with the live endpoint, SIGKILL the actor process (max_restarts=-1
    # restarts it), then fetch: first stream hits the dead port, _resolve sees a
    # new incarnation and retries.
    import signal

    from ray._common.test_utils import wait_for_condition
    from ray._private.test_utils import wait_for_pid_to_exit
    from ray.util.state import list_actors

    shuffle_id, node_id = "restart", "node-1"
    payload = b"SHARD_" * 8
    (tmp_path / "s.bin").write_bytes(payload)
    _actor, endpoint = _make_file_server(tmp_path, shuffle_id, node_id)
    key = _file_server_name(shuffle_id, node_id)

    def _pid():
        actors = list_actors(filters=[("name", "=", key)])
        return actors[0].pid if actors and actors[0].pid else None

    wait_for_condition(lambda: _pid() is not None)
    pid = _pid()
    assert pid is not None
    os.kill(pid, signal.SIGKILL)
    wait_for_pid_to_exit(pid)
    with _ENDPOINT_CACHE_LOCK:
        _ENDPOINT_CACHE[key] = endpoint

    fd, sink = _open_sink(tmp_path, size=64)
    try:
        _fetch_from_file_server(
            sink,
            shuffle_id,
            node_id,
            [_FileRanges(path="s.bin", ranges=[(0, 24), (24, 24)])],
        )
        assert os.pread(fd, 48, 0) == payload
    finally:
        _clear_endpoint_cache(shuffle_id, node_id)
        os.close(fd)


def test_fetch_same_incarnation_escalate(tmp_path):
    # Actor still reachable via Ray (same incarnation) but Flight keeps failing
    # → retry once, then ShuffleFileServerAnomalyError. Cannot SIGKILL this:
    # kill either drops the name or changes incarnation.
    import pyarrow.flight as flight

    shuffle_id, node_id = "mock-shuffle", "node-1"
    _clear_endpoint_cache(shuffle_id, node_id)
    stream_calls = {"n": 0}

    def fake_stream(endpoint, members, sink):
        stream_calls["n"] += 1
        raise flight.FlightUnavailableError("down")

    def fake_ray_get(_ref):
        return _Endpoint("127.0.0.1", 1, "A")

    fd, sink = _open_sink(tmp_path)
    members = [_FileRanges(path="x.bin", ranges=[(0, 4)])]
    try:
        with (
            patch.object(_runtime, "_stream_members_flight", fake_stream),
            patch.object(_runtime.ray, "get_actor", return_value=MagicMock()),
            patch.object(_runtime.ray, "get", side_effect=fake_ray_get),
            patch.object(_runtime.time, "sleep"),
        ):
            with pytest.raises(ShuffleFileServerAnomalyError):
                _fetch_from_file_server(sink, shuffle_id, node_id, members)
        assert stream_calls["n"] == 2
    finally:
        _clear_endpoint_cache(shuffle_id, node_id)
        os.close(fd)


# ------------------------------------------------ Arrow Flight fetch transport
@pytest.mark.parametrize(
    "members_spec",
    [
        [("s.bin", [(0, 12)])],
        [("s.bin", [(0, 12), (12, 12)])],
        [("s.bin", [(0, 0)])],
        [("s.bin", [(0, 0), (0, 8), (8, 0)])],
        [("a.bin", [(0, 10)]), ("b.bin", [(0, 20)])],
    ],
)
def test_layout_vs_wire(tmp_path, members_spec):
    # _compute_prefetch_layout predicts the bytes written into a group's
    # region; stream the same group via Flight and assert bytes-written matches
    # (and that the wire bytes are the requested ranges verbatim, in request
    # order: the files' records are self-framed, so nothing is added).
    file_bytes = {}
    members = []
    for path, ranges in members_spec:
        need = max((off + length for off, length in ranges), default=0)
        payload = bytes((i % 256) for i in range(need))
        (tmp_path / path).write_bytes(payload)
        file_bytes[path] = payload
        members.append(_FileRanges(path=path, ranges=list(ranges)))

    group = _NodeGroup("shuffle-0", "node-1", members=members)
    total, _base_offsets, sizes = _compute_prefetch_layout([group])
    expected = b"".join(
        file_bytes[path][off : off + length]
        for path, ranges in members_spec
        for off, length in ranges
    )
    assert len(expected) == sizes[0] == total

    fd, sink = _open_sink(tmp_path, size=max(total, 1))
    try:
        with _running_flight_server(tmp_path) as endpoint:
            _stream_members_flight(
                endpoint,
                members,
                sink=sink,
            )
        written = sink._pos - sink._base_offset
        assert written == sizes[0] == len(expected)
        assert os.pread(fd, written, sink._base_offset) == expected
    finally:
        os.close(fd)


def test_flight_unreachable_raises_transport_error(tmp_path):
    # Bind a port with nothing listening -> connect refused. _stream_members_flight
    # does NOT translate; it raises the raw pyarrow FlightError (a retryable
    # transport fault that _fetch_from_file_server classifies).
    import pyarrow.flight as flight

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]  # bound but never listen()ed
        fd, sink = _open_sink(tmp_path)
        try:
            with pytest.raises(flight.FlightError):
                _stream_members_flight(
                    _Endpoint("127.0.0.1", port, "test-incarnation"),
                    [_FileRanges(path="x.bin", ranges=[(0, 4)])],
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
    import pyarrow as pa

    (tmp_path / "s.bin").write_bytes(b"only8byt")  # 8 bytes
    fd, sink = _open_sink(tmp_path)
    try:
        with _running_flight_server(tmp_path) as endpoint:
            # Server-side short-read raises mid-stream; the client surfaces it as a
            # raw ArrowInvalid (no translation), with the message preserved.
            with pytest.raises(pa.ArrowInvalid, match="short read"):
                _stream_members_flight(
                    endpoint,
                    [_FileRanges(path="s.bin", ranges=[(0, 64)])],  # asks for 64
                    sink=sink,
                )
    finally:
        os.close(fd)


def test_fetch_from_file_server(tmp_path):
    # _fetch_from_file_server resolves the file-server endpoint and streams every
    # member's ranges into the sink over one Flight client. Seed the endpoint
    # cache so _resolve returns without a Ray RPC (no Ray needed here), then
    # assert the shard bytes land in the sink.
    payload = b"SHARD_" * 8  # 48 bytes
    (tmp_path / "s.bin").write_bytes(payload)
    fd, sink = _open_sink(tmp_path)
    shuffle_id, node_id = "shuffle-0", "node-1"
    key = _file_server_name(shuffle_id, node_id)
    try:
        with _running_flight_server(tmp_path) as (host, port, incarnation):
            with _ENDPOINT_CACHE_LOCK:
                _ENDPOINT_CACHE[key] = _Endpoint(host, port, incarnation)
            try:
                _fetch_from_file_server(
                    sink,
                    shuffle_id,
                    node_id,
                    [_FileRanges(path="s.bin", ranges=[(0, 24), (24, 24)])],
                )
            finally:
                with _ENDPOINT_CACHE_LOCK:
                    _ENDPOINT_CACHE.pop(key, None)
        # The two ranges land back to back, verbatim.
        assert os.pread(fd, 48, 0) == payload
    finally:
        os.close(fd)


def test_fetch_server_error_is_terminal(tmp_path):
    # A server-side error (here a missing file) surfaces as FlightServerError, a
    # FlightError subclass. It must be terminal, not retried as a transport fault:
    # _fetch_from_file_server re-raises it.
    import pyarrow.flight as flight

    fd, sink = _open_sink(tmp_path)
    shuffle_id, node_id = "shuffle-0", "node-1"
    key = _file_server_name(shuffle_id, node_id)
    try:
        with _running_flight_server(tmp_path) as (host, port, incarnation):
            with _ENDPOINT_CACHE_LOCK:
                _ENDPOINT_CACHE[key] = _Endpoint(host, port, incarnation)
            try:
                with pytest.raises(flight.FlightServerError):
                    _fetch_from_file_server(
                        sink,
                        shuffle_id,
                        node_id,
                        [_FileRanges(path="missing.bin", ranges=[(0, 4)])],
                    )
            finally:
                with _ENDPOINT_CACHE_LOCK:
                    _ENDPOINT_CACHE.pop(key, None)
    finally:
        os.close(fd)


# --------------------------- reducer helpers (no Ray and no sockets)
def test_group_by_server():
    # Same (shuffle_id, node_id) collapses; distinct pairs stay separate.
    s0 = _SourceRef("shuffle-0", "node-1", _FileRanges("a", [(0, 4)]))
    s1 = _SourceRef("shuffle-0", "node-2", _FileRanges("b", [(0, 8)]))
    s2 = _SourceRef("shuffle-0", "node-1", _FileRanges("c", [(0, 2)]))

    groups = _group_by_server([s0, s1, s2])
    by_node = {g.node_id: g for g in groups}
    assert set(by_node) == {"node-1", "node-2"}

    # Members within a group are in original input order.
    assert [m.path for m in by_node["node-1"].members] == ["a", "c"]
    assert [m.path for m in by_node["node-2"].members] == ["b"]


def test_order_by_map_id_undoes_node_grouping():
    import pyarrow as pa

    # Tasks 0 and 2 landed on node-1, task 1 on node-2: the fetch groups decode
    # as [t2, t0], [t1] (append order within a node file is arbitrary) but the
    # reducer must see map task order.
    t0, t1, t2 = [pa.table({"v": [i]}) for i in range(3)]

    ordered = _order_by_map_id([[(2, t2), (0, t0)], [(1, t1)]])

    assert ordered == [t0, t1, t2]


def test_compute_prefetch_layout():
    # Each range contributes its length to the group's size (records are
    # streamed verbatim). base_offsets are the running cumulative sum.
    g0 = _NodeGroup(
        "shuffle-0",
        "node-1",
        members=[
            _FileRanges(path="a", ranges=[(0, 10), (10, 10)]),  # 20
        ],
    )
    g1 = _NodeGroup(
        "shuffle-0",
        "node-2",
        members=[
            _FileRanges(path="b", ranges=[(0, 100)]),  # 100
        ],
    )
    total, base_offsets, sizes = _compute_prefetch_layout([g0, g1])
    assert sizes == [20, 100]
    assert base_offsets == [0, 20]
    assert total == 120

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


def _read_record(prefix, partition_id, off, length, map_id=None):
    """Return the IPC frame of the record at ``off`` (``_RECORD_HEADER``)."""
    with open(f"{prefix}_p{partition_id}.shf", "rb") as f:
        f.seek(off)
        record = f.read(length)
    frame_len, rec_map_id, _attempt_token = _RECORD_HEADER.unpack(
        record[: _RECORD_HEADER.size]
    )
    assert frame_len == length - _RECORD_HEADER.size
    if map_id is not None:
        assert rec_map_id == map_id
    return record[_RECORD_HEADER.size :]


def test_partition_writer_combine_path(tmp_path):
    # Writer decides combine path once per map from the schema: native (fast) for
    # plain columns, extension-safe (transform) when any column is an extension.
    import numpy as np
    import pyarrow as pa

    from ray.data.extensions.tensor_extension import ArrowTensorArray

    prefix = str(tmp_path / "node")
    # (1) non-extension -> native; frame round-trips to the combined table.
    w = _PartitionWriter(prefix, map_id=0, attempt_token=11, compression="lz4")
    w.add_shard(0, pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))
    w.add_shard(0, pa.table({"a": [4, 5], "b": ["p", "q"]}))
    w.flush_all()
    assert w._combine_native_ok is True
    off, length = w.index[0][0]
    expected = pa.concat_tables(
        [
            pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}),
            pa.table({"a": [4, 5], "b": ["p", "q"]}),
        ]
    ).combine_chunks()
    assert _read_ipc(_read_record(prefix, 0, off, length)).equals(expected)

    # (2) extension (tensor) column -> transform-safe path, still writes a frame.
    ext = pa.table({"t": ArrowTensorArray.from_numpy(np.arange(12.0).reshape(3, 4))})
    w2 = _PartitionWriter(prefix, map_id=1, attempt_token=12, compression="zstd")
    w2.add_shard(0, ext)
    w2.add_shard(0, ext)
    w2.flush_all()
    assert w2._combine_native_ok is False
    assert w2.index[0][0][1] > 0


def test_decoded_stats_tracks_rows_when_nbytes_zero(tmp_path):
    """A null-typed table has rows with tbl.nbytes == 0. num_rows must
    record those rows; decoded_bytes stays nbytes-only.
    """
    import pyarrow as pa

    tbl = pa.table({"k": pa.nulls(10)})
    assert tbl.num_rows == 10
    assert tbl.nbytes == 0

    w = _PartitionWriter(
        str(tmp_path / "node"), map_id=0, attempt_token=0, compression="none"
    )
    w.add_shard(0, tbl)
    w.flush_all()

    rows = _counts_to_array(w.rows_per_partition, num_partitions=4)
    nbytes = _counts_to_array(w.decoded_bytes_per_partition, num_partitions=4)
    assert rows.shape == (4,)
    assert rows[0] == 10
    assert not rows[1:].any()
    assert nbytes.shape == (4,)
    assert nbytes[0] == 0
    assert not nbytes[1:].any()


def test_append_record_concurrent_writers_stay_contiguous(tmp_path):
    """Map tasks on one node append to the same partition file concurrently.
    Every (offset, length) a writer gets back must cover exactly its own
    record: no interleaving, no two writers sharing bytes, and the records
    pack the file with no gaps."""
    from concurrent.futures import ThreadPoolExecutor

    import pyarrow as pa

    prefix = str(tmp_path / "node")
    n_writers, n_records = 8, 40

    def write_all(writer_id):
        out = []
        for i in range(n_records):
            w = _PartitionWriter(
                prefix, map_id=writer_id, attempt_token=i, compression="lz4"
            )
            # Varying sizes so records do not line up by accident.
            w.add_shard(0, pa.table({"w": [writer_id] * (i + 1), "i": [i] * (i + 1)}))
            w.flush_all()
            out.append((writer_id, i, w.index[0][0]))
        return out

    with ThreadPoolExecutor(n_writers) as ex:
        records = [r for rs in ex.map(write_all, range(n_writers)) for r in rs]

    size = os.path.getsize(f"{prefix}_p0.shf")
    ranges = sorted(rng for _, _, rng in records)
    assert ranges[0][0] == 0
    assert all(sum(a) == b[0] for a, b in zip(ranges, ranges[1:]))
    assert sum(ranges[-1]) == size
    for writer_id, i, (off, length) in records:
        tbl = _read_ipc(_read_record(prefix, 0, off, length, map_id=writer_id))
        assert tbl.column("w").to_pylist() == [writer_id] * (i + 1)
        assert tbl.column("i").to_pylist() == [i] * (i + 1)


def test_append_record_offsets_are_record_relative(tmp_path):
    path = str(tmp_path / "node_p3.shf")
    import pyarrow as pa

    r0 = _append_record(path, 4, 100, pa.py_buffer(b"abc"))
    r1 = _append_record(path, 9, 200, pa.py_buffer(b"defgh"))
    assert r0 == (0, _RECORD_HEADER.size + 3)
    assert r1 == (r0[1], _RECORD_HEADER.size + 5)
    with open(path, "rb") as f:
        assert f.read() == (
            _RECORD_HEADER.pack(3, 4, 100)
            + b"abc"
            + _RECORD_HEADER.pack(5, 9, 200)
            + b"defgh"
        )


def test_decode_records_keeps_committed_attempts_in_file_order(tmp_path):
    """A node file can hold records of a map task attempt that died after
    appending; only the attempt the regroup published is decoded."""
    import pyarrow as pa

    path = str(tmp_path / "node_p0.shf")
    frames = {
        (1, 10): pa.table({"v": [1]}),
        (0, 20): pa.table({"v": [0]}),  # dead attempt of map 0
        (0, 21): pa.table({"v": [100]}),  # its retry
    }
    for (map_id, attempt_token), tbl in frames.items():
        _append_record(path, map_id, attempt_token, _encode_shard(tbl, "lz4"))
    size = os.path.getsize(path)

    fd = os.open(path, os.O_RDONLY)
    try:
        records = _decode_records(fd, 0, size, {0: 21, 1: 10}, "lz4")
        assert [(m, t.column("v").to_pylist()) for m, t in records] == [
            (1, [1]),
            (0, [100]),
        ]
        # A region ending inside a record is a torn write: fail, never desync.
        with pytest.raises(RuntimeError, match="torn record"):
            _decode_records(fd, 0, size - 1, {0: 21, 1: 10}, "lz4")
        with pytest.raises(RuntimeError, match="torn record header"):
            _decode_records(fd, 0, _RECORD_HEADER.size - 1, {}, "lz4")
    finally:
        os.close(fd)


def test_partition_input():
    """One source per node with records for this partition, naming the node
    file of that partition and its whole size; empty files are skipped."""
    import numpy as np

    table = {
        "nodes": [("s", "node-1", "/a/node"), ("s", "node-2", "/b/node")],
        "committed_attempts": {0: 5, 1: 6},
    }
    entry = {
        "node_table": "ref",
        "sizes": np.array([26, 0], dtype=np.int64),
        "schema": "schema",
    }
    with patch.object(_runtime.ray, "get", side_effect=lambda ref: table):
        inp = _partition_input(entry, partition_id=7)
    assert inp.schema == "schema"
    assert inp.committed_attempts == {0: 5, 1: 6}
    assert [(s.node_id, s.file.path, s.file.ranges) for s in inp.sources] == [
        ("node-1", "/a/node_p7.shf", [(0, 26)]),
    ]


# --------------------------------------------------- error class sanity checks
def test_shuffle_disk_error_is_runtime_error_subclass():
    assert issubclass(ShuffleDiskError, RuntimeError)
    assert not issubclass(ShuffleDiskError, ShuffleFileServerAnomalyError)


def test_shuffle_file_server_anomaly_error_is_runtime_error_subclass():
    assert issubclass(ShuffleFileServerAnomalyError, RuntimeError)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
