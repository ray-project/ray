"""Disk-shuffle file-transport runtime: Arrow Flight transport, per-node
ShuffleFileServer actor, prefetch layout, error hierarchy. Imported by the
map/reduce task bodies in ``disk_shuffle_tasks``."""

import errno
import json
import logging
import os
import pickle
import struct
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Tuple,
    TypedDict,
    Union,
    cast,
)

import numpy as np
import pyarrow as pa

import ray
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.util import MiB
from ray.data._internal.utils.transform_pyarrow import _is_pa_extension_type
from ray.exceptions import ActorDiedError, ActorUnschedulableError
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

# ShuffleFileServer actor identity. Name is deterministic in (shuffle_id, node_id).
_SHUFFLE_FILE_SERVER_NAMESPACE = "ray_data_shuffle_disk"

# pyarrow-supported shard codecs (``"none"``/``None`` = uncompressed). Rides
# ``data_context.shuffle_compression``.
Compression = Optional[
    Literal[
        "none", "gzip", "bz2", "brotli", "lz4", "lz4_frame", "lz4_raw", "zstd", "snappy"
    ]
]


# =============================================================================
# SHARED: shard codec, ShuffleFileServer identity/lookup, the
# ShuffleHandle type, and the error hierarchy, which are used by both map and reduce.
# =============================================================================
# Each range's payload length is framed as a u64 in the sink, so a single
# range/IPC frame may be up to 16 EiB. Checked at mapper write time.
_MAX_RANGE_BYTES: int = (1 << 64) - 1


# ----------------------------------------------------------------- Arrow IPC
# Shard wire format: [u64 uncompressed_size][codec(whole IPC stream)]
_WF_HEADER = struct.Struct("<Q")


def _codec_for(compression: Optional[str]) -> Optional["pa.Codec"]:
    """Codec name -> pa.Codec; None/"none" -> None (pyarrow has no "none" codec)."""
    if not compression or compression.lower() == "none":
        return None
    return pa.Codec(compression)


def _encode_shard(
    table: pa.Table, compression: Optional[str] = "lz4", combine_native: bool = False
) -> pa.Buffer:
    """Encode a partition shard as a whole-frame blob (one codec frame per shard,
    vs Arrow's per-buffer IPC compression)."""
    if table.num_columns > 0:
        if combine_native:
            try:
                table = table.combine_chunks()
            except pa.ArrowInvalid:
                # >2 GiB string offsets overflow int32 during native combine
                # (esp. nested in struct/list); use the extension-safe path.
                table = transform_pyarrow.combine_chunks(table)
        else:
            table = transform_pyarrow.combine_chunks(table)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as w:  # uncompressed inner IPC
        for batch in table.to_batches():
            w.write_batch(batch)
    raw = sink.getvalue()
    codec = _codec_for(compression)
    out = pa.BufferOutputStream()
    out.write(_WF_HEADER.pack(raw.size))
    out.write(raw if codec is None else codec.compress(raw))
    return out.getvalue()


def _read_ipc(
    buf: Union[bytes, "pa.Buffer", memoryview], compression: Optional[str] = "lz4"
) -> pa.Table:
    """Decode a whole-frame shard: read the u64 size header, one decompress of
    the rest (per ``compression``, same value the map encoded with), then read
    the (uncompressed) inner IPC stream."""
    src = buf if isinstance(buf, pa.Buffer) else pa.py_buffer(buf)
    n = _WF_HEADER.unpack_from(memoryview(src))[0]
    body = src.slice(_WF_HEADER.size)
    codec = _codec_for(compression)
    raw = body if codec is None else codec.decompress(body, decompressed_size=n)
    with pa.ipc.open_stream(raw) as r:
        return r.read_all()


def _file_server_name(shuffle_id: str, node_id: str) -> str:
    return f"shuffle_file_server:{shuffle_id}:{node_id}"


class ShuffleHandle(TypedDict, total=False):
    """Handle written by each mapper task.

    Fetch/runtime fields (``path``, ``index_ranges``, ``shuffle_id``,
    ``node_id``) are consumed by reducers. Driver bookkeeping fields
    (``num_rows``, ``decoded_bytes``, ``schema``, ``num_partitions``,
    ``total_bytes``, ``compression``) are consumed by the map operator
    when emitting partition wrappers.
    """

    # Node-file prefix: this task's record for partition ``p`` is in
    # ``{path}_p{p}.shf``, shared by every map task on the node.
    path: str
    # Index of the map task; stamped into each record so the reducer can
    # restore map task order whatever the append order.
    map_id: int
    # Random 64-bit token of this task attempt, stamped into each record.
    # The regroup commits the token of the attempt that returned this
    # handle; the reducer skips records with any other token (a dead
    # attempt's).
    attempt_token: int
    # Dense per-partition range index (see _build_range_index): row ``p`` is
    # ``(offset, length)`` of this task's record (header included) in
    # ``{path}_p{p}.shf``.
    # ``length == 0`` => this mapper wrote no frame for ``p`` (reducer skips
    # the fetch). Not the same as decoded ``nbytes`` or ``num_rows``.
    index_ranges: "np.ndarray"  # shape [num_partitions, 2], int64
    # Dense per-partition row counts (see _counts_to_array). Empty-partition
    # gating uses this, not ``decoded_bytes``: a ``null``-typed table can
    # have ``num_rows > 0`` with ``tbl.nbytes == 0``.
    num_rows: "np.ndarray"  # shape [num_partitions], int64
    # Dense per-partition decoded-byte counts (see _counts_to_array):
    # pre-compression ``tbl.nbytes``. Used for reduce memory estimates.
    decoded_bytes: "np.ndarray"  # shape [num_partitions], int64
    shuffle_id: str
    node_id: str
    schema: Optional["pa.Schema"]
    # Extra mapper bookkeeping (not required for fetch).
    num_partitions: int
    total_bytes: int
    compression: Optional[str]


class _Endpoint(NamedTuple):
    """A file server's Flight endpoint. ``host``/``port`` are where to connect;
    ``incarnation`` identifies the server process so a restart is detectable."""

    host: str
    port: int
    incarnation: str


class ShuffleDiskError(RuntimeError):
    """Terminal: reducer's local disk exhausted (ENOSPC / EDQUOT).
    Retrying doesn't reclaim space."""


# errno values that indicate the reducer's local disk is exhausted.
# EDQUOT is glibc's quota-exceeded error; not all platforms expose it.
_DISK_EXHAUSTED_ERRNOS = frozenset(
    e
    for e in (
        errno.ENOSPC,
        getattr(errno, "EDQUOT", None),
    )
    if e is not None
)


def _is_disk_exhausted(exc: BaseException) -> bool:
    return isinstance(exc, OSError) and exc.errno in _DISK_EXHAUSTED_ERRNOS


class ShuffleFileServerAnomalyError(RuntimeError):
    """Terminal shuffle file-server failure: a driver-level retry cannot fix it.

    The server is a named actor owned by the driver-side regroup operator,
    created with ``max_restarts=-1``, ``max_task_retries=-1`` and
    ``NodeAffinitySchedulingStrategy(node_id, soft=False)``. Ray auto-restarts
    it on any mid-life crash and holds ``endpoint()`` calls until the restart
    completes, so a transient ``ActorUnavailableError`` never reaches us. The
    anomalous states we key off of are:

    - ``ActorDiedError``          -> ``__init__`` raised, the owning driver
                                    exited, or ``ray.kill(actor, no_restart=True)``
                                    (the regroup operator's teardown).
    - ``ActorUnschedulableError`` -> pinned node is gone; ``soft=False`` blocks
                                    relocation. Terminal at the shuffle layer;
                                    an upstream lineage layer must re-execute
                                    the mapper on healthy capacity.
    - ``ValueError`` (from ``ray.get_actor``) -> actor name is not registered
                                                (never created or gc'd).
    - The Flight fetch failed but the server's incarnation is unchanged and Ray
      RPC still works, which is often a network-configuration problem
      (``NetworkPolicy``, firewall, routing).
    """


# =============================================================================
# MAP SIDE (map tasks): buffer per-partition shards, seal them to one file, and
# build the ShuffleHandle's dense range index plus per-partition row/byte
# counts. What this writes is exactly what the SERVER serves and the FETCH
# side reads.
# =============================================================================
def _build_range_index(index, num_partitions):
    """Dense per-partition (offset, length) index: one row per partition.

    Row ``p`` is ``(offset, length)`` of partition ``p``'s on-disk IPC frame;
    mappers that wrote nothing for ``p`` stay ``(0, 0)`` (``length == 0`` =>
    the reducer skips the fetch).
    """
    ranges = np.zeros((num_partitions, 2), dtype=np.int64)
    for partition_id, frame_range in index.items():
        ranges[partition_id] = frame_range[0]
    return ranges


def _counts_to_array(counts: Dict[int, int], num_partitions: int) -> "np.ndarray":
    """Dense per-partition int64 counts, indexed by partition_id.

    Absent partitions stay 0. Used for both ``num_rows`` (``tbl.num_rows``)
    and ``decoded_bytes`` (``tbl.nbytes``).
    """
    arr = np.zeros(num_partitions, dtype=np.int64)
    for partition_id, n in counts.items():
        arr[partition_id] = n
    return arr


# Record header in a node partition file: (frame length excluding the
# header, map task id, attempt token), three big-endian u64. The map id
# orders records in the reducer; the token picks the committed attempt's.
_RECORD_HEADER = struct.Struct(">QQQ")


def _append_record(
    path: str, map_id: int, attempt_token: int, buf: "pa.Buffer"
) -> Tuple[int, int]:
    """Append one ``[header][frame]`` record to ``path`` and return its
    ``(offset, length)``, header included.

    - No coordination between writers: ``O_APPEND`` makes the kernel place
      each ``writev`` at end-of-file under the inode lock, and the offset is
      the descriptor's position after the write minus the record length.
      This holds on a local filesystem (Ray's temp dir), not NFS.
    - A short write (disk full, or a frame over the 2 GiB one ``writev``
      takes) raises, so the task fails instead of publishing a truncated
      record; nothing references the partial bytes.
    """
    size = _RECORD_HEADER.size + buf.size
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        written = os.writev(
            fd, [_RECORD_HEADER.pack(buf.size, map_id, attempt_token), memoryview(buf)]
        )
        end = os.lseek(fd, 0, os.SEEK_CUR)
    finally:
        os.close(fd)
    if written != size:
        raise RuntimeError(f"short write to {path}: {written} of {size} bytes")
    return end - size, size


def _decode_records(
    fd: int,
    base: int,
    size: int,
    committed_attempts: Mapping[int, int],
    compression: Optional[str],
) -> List[Tuple[int, pa.Table]]:
    """Return ``(map_id, table)`` for every committed record in
    ``[base, base + size)`` of the file behind ``fd``, in file order.

    - A record is kept iff ``committed_attempts[map_id]`` equals its attempt
      token. Other records are a dead attempt's (its retry appended again)
      and are skipped without reading their frame.
    - A record running past ``base + size`` raises: it is a torn write,
      which only a disk error leaves behind, and decoding on would desync.
    """
    out: List[Tuple[int, pa.Table]] = []
    pos, end = base, base + size
    header_size = _RECORD_HEADER.size
    while pos < end:
        if pos + header_size > end:
            raise RuntimeError(f"torn record header at {pos} (region ends at {end})")
        length, map_id, attempt_token = _RECORD_HEADER.unpack(
            os.pread(fd, header_size, pos)
        )
        if pos + header_size + length > end:
            raise RuntimeError(
                f"torn record at {pos}: {length} bytes run past region end {end}"
            )
        if committed_attempts.get(map_id) == attempt_token:
            ipc_buf = os.pread(fd, length, pos + header_size)
            out.append((map_id, _read_ipc(ipc_buf, compression)))
        pos += header_size + length
    return out


class _PartitionWriter:
    """Buffer shards per partition and seal each partition to disk as one
    record.

    ``add_shard`` buffers; ``flush_all`` concatenates each partition's
    shards, encodes one IPC frame, appends it to ``{node_prefix}_p{p}.shf``
    (``_append_record``) and stores the record's ``(offset, length)`` in
    ``index``. Frames are written as soon as encoded, so at most one is
    held in memory.
    """

    __slots__ = (
        "_node_prefix",
        "_map_id",
        "_attempt_token",
        "_compression",
        "_staging",
        "_index",
        "_rows_per_partition",
        "_decoded_bytes_per_partition",
        "_combine_native_ok",
    )

    def __init__(
        self,
        node_prefix: str,
        map_id: int,
        attempt_token: int,
        compression: Optional[str],
    ):
        self._node_prefix = node_prefix
        self._map_id = map_id
        self._attempt_token = attempt_token
        # Codec from data_context.shuffle_compression (same field the reduce reads).
        self._compression = compression
        self._staging: Dict[int, List[pa.Table]] = {}
        self._index: Dict[int, List[Tuple[int, int]]] = {}
        self._rows_per_partition: Dict[int, int] = {}
        self._decoded_bytes_per_partition: Dict[int, int] = {}
        # Native combine is safe when there are no extension columns.
        self._combine_native_ok: Optional[bool] = None

    def _flush(self, partition_id: int) -> None:
        shards = self._staging.get(partition_id)
        if not shards:
            return
        # Promote compatible schemas (e.g. null vs int64 column) like the
        # object-store map task; plain ``pa.concat_tables`` would reject them.
        tbl = (
            transform_pyarrow.concat(shards, promote_types=True, preserve_order=True)
            if len(shards) > 1
            else shards[0]
        )
        if self._combine_native_ok is None:
            # Once per map: concat above guarantees a uniform schema, so native
            # combine is safe iff no column is an extension type. Avoids the
            # per-shard extension-safe combine (which re-scans + does per-column
            # dispatch + nbytes on every shard).
            self._combine_native_ok = not any(
                _is_pa_extension_type(f.type) for f in tbl.schema
            )
        self._rows_per_partition[partition_id] = tbl.num_rows
        self._decoded_bytes_per_partition[partition_id] = tbl.nbytes
        buf = _encode_shard(tbl, self._compression, self._combine_native_ok)
        if buf.size > _MAX_RANGE_BYTES:
            raise RuntimeError(
                f"map task {self._map_id} partition {partition_id}: IPC frame is "
                f"{buf.size} bytes, exceeding the u64 wire-protocol "
                f"per-range limit ({_MAX_RANGE_BYTES}). Increase "
                f"``num_partitions`` or reduce the upstream block size."
            )
        path = f"{self._node_prefix}_p{partition_id}.shf"
        self._index[partition_id] = [
            _append_record(path, self._map_id, self._attempt_token, buf)
        ]
        self._staging[partition_id] = []

    def add_shard(self, partition_id: int, shard: pa.Table) -> None:
        if not shard.num_rows:
            return
        self._staging.setdefault(partition_id, []).append(shard)

    def flush_all(self) -> None:
        for partition_id in list(self._staging.keys()):
            self._flush(partition_id)

    @property
    def index(self) -> Dict[int, List[Tuple[int, int]]]:
        return self._index

    @property
    def rows_per_partition(self) -> Dict[int, int]:
        return self._rows_per_partition

    @property
    def decoded_bytes_per_partition(self) -> Dict[int, int]:
        return self._decoded_bytes_per_partition


# =============================================================================
# SERVER SIDE (created by map tasks; serves reduce fetches). Arrow Flight over
# gRPC: DoAction streams opaque byte-ranges (the whole-frame shard blob).
# =============================================================================
# Per-Result body size. Each flight.Result buffer is materialized whole in RAM
# when sent, so chunking bounds the ShuffleFileServer actor's memory.
_FLIGHT_CHUNK = MiB


# Flight fetch request (Action body): JSON ``{"s": [[path, [[off, len], ...]], ...]}``.


def _grpc_location(host: str, port) -> str:
    """gRPC URI for host:port. IPv6 literals must be bracketed (grpc://[::1]:0);
    a bare ``:`` in the host is the IPv6 signal."""
    h = f"[{host}]" if ":" in host else host
    return f"grpc://{h}:{port}"


def _make_flight_server(host: str, base_dir: str):
    """Build the Arrow Flight server that streams byte ranges of this node's
    partition files on ``DoAction("fetch")``.

    Ranges are served verbatim (records are self-framed, ``_RECORD_HEADER``)
    and packed into ``Result``s of about ``_FLIGHT_CHUNK`` bytes. A range
    the file cannot fill raises instead of sending short: a short body
    would desync every later record at the client.
    """
    import pyarrow.flight as flight

    class _ShuffleFlightServer(flight.FlightServerBase):
        def do_action(self, context, action):
            req = json.loads(action.body.to_pybytes())
            out = bytearray()
            for path, ranges in req["s"]:
                fpath = os.path.join(base_dir, os.path.basename(path))
                fd = os.open(fpath, os.O_RDONLY)
                try:
                    for off, length in ranges:
                        pos, end = off, off + length
                        while pos < end:
                            buf = os.pread(fd, min(end - pos, _FLIGHT_CHUNK), pos)
                            if not buf:
                                # Never send short: the client would desync
                                # on every later record.
                                raise ValueError(
                                    f"short read: {fpath} @{off}+{length}, "
                                    f"got {pos - off}"
                                )
                            pos += len(buf)
                            out += buf
                            # Bound the server's per-Result memory.
                            if len(out) >= _FLIGHT_CHUNK:
                                yield flight.Result(pa.py_buffer(bytes(out)))
                                out.clear()
                finally:
                    os.close(fd)
            if out:
                yield flight.Result(pa.py_buffer(bytes(out)))

    return _ShuffleFlightServer(_grpc_location(host, 0))


@ray.remote(num_cpus=0, max_restarts=-1, max_task_retries=-1)
class ShuffleFileServer:
    """Per-node file fetch service: owns an Arrow Flight server that serves
    byte-ranges of local shuffle files to remote reducers.

    One per (shuffle, node), pinned with NodeAffinity. Created and owned by
    the driver-side ``DiskHashShuffleRegroupOp`` when the first mapper handle
    from that node arrives, and killed in its teardown; survives individual
    map/reduce workers but not the driver."""

    def __init__(
        self,
        base_dir: str,
    ):
        self.base_dir = os.path.realpath(base_dir)
        os.makedirs(self.base_dir, exist_ok=True)
        ip = ray.util.get_node_ip_address()
        self._server = _make_flight_server(ip, self.base_dir)
        self._host, self._port = ip, self._server.port
        # Unique per actor process; Ray re-runs __init__ on every restart, so
        # this changes on restart. Reducers compare it to detect a restart.
        self._incarnation = uuid.uuid4().hex

    def endpoint(self) -> _Endpoint:
        return _Endpoint(self._host, self._port, self._incarnation)


@ray.remote(num_cpus=0)
def _cleanup_shuffle_dir(map_dir: str, reduce_dir: str) -> None:
    """Best-effort ``rmtree`` of this shuffle's map + reduce staging dirs. Pinned
    to its node with ``NodeAffinity(soft=False)``: on a live node it runs there;
    if the node is gone the task just fails."""
    import shutil

    shutil.rmtree(map_dir, ignore_errors=True)
    shutil.rmtree(reduce_dir, ignore_errors=True)


def teardown_disk_shuffle(shuffle_nodes: Iterable[Tuple[str, str]]) -> None:
    """Remove the shuffle directories of every ``(shuffle_id, node_id)`` in
    ``shuffle_nodes`` on that node. Called once from the regroup operator's
    teardown, when no reducer can still need the files. The node's
    ``ShuffleFileServer`` is owned by the regroup operator and killed there;
    serving a removed directory is harmless, so the two need no ordering.
    """
    cleanup_refs = []
    for shuffle_id, node_id in shuffle_nodes:
        try:
            map_dir, reduce_dir = disk_shuffle_dirs(shuffle_id)
            cleanup_refs.append(
                _cleanup_shuffle_dir.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id, soft=False
                    ),
                ).remote(map_dir, reduce_dir)
            )
        except Exception:
            pass

    if cleanup_refs:
        ray.wait(cleanup_refs, num_returns=len(cleanup_refs), timeout=5.0)


# =============================================================================
# FETCH SIDE (reduce tasks): resolve file-server endpoints, stream shards from each
# over a Flight client into the prefetch sink at disjoint offsets, then decode.
# =============================================================================
# Process-global endpoint cache, reused across reducers running on the same
# worker. Popped on fetch failure (re-resolved next call); the lock guards the
# concurrent fetch threads.
_ENDPOINT_CACHE: Dict[str, _Endpoint] = {}
_ENDPOINT_CACHE_LOCK = threading.Lock()

# --------------------------------------------------------- fetch routing types
# Named containers for reducer fetch orchestration. ``slots=True`` keeps
# per-instance memory small since we allocate one per source/group/member.


@dataclass(slots=True, frozen=True)
class _FileRanges:
    """A shuffle file and the byte ranges to read from it."""

    path: str
    ranges: List[Tuple[int, int]]


@dataclass(slots=True, frozen=True)
class _SourceRef:
    """A ``_FileRanges`` tagged with its ShuffleFileServer identity.

    Built once per input handle for a given partition_id at reducer start.
    The (shuffle_id, node_id) pair is the file server's named-actor identity.
    """

    shuffle_id: str
    node_id: str
    file: _FileRanges


@dataclass(slots=True)
class _NodeGroup:
    """All sources on one ShuffleFileServer, grouped so we open ONE Flight
    connection per file server. Sources collapse to the same group when their
    ``(shuffle_id, node_id)`` (i.e., the file server's named-actor identity) matches.
    """

    shuffle_id: str
    node_id: str
    members: List[_FileRanges] = field(default_factory=list)


class _PwriteSink:
    """A write-only file-like that ``os.pwrite``s sequentially from a
    fixed base offset on a shared fd. Multiple sinks (one per fetch thread) at
    DISJOINT base regions write the same fd concurrently without lock.

    ``reset()`` rewinds ``_pos`` to the base offset so a fetch attempt that
    partially wrote can be retried in-place and subsequent ``os.pwrite``s
    overwrite the partial data (same offsets, idempotent write)."""

    __slots__ = ("_fd", "_base_offset", "_pos")

    def __init__(self, fd: int, base_offset: int):
        self._fd = fd
        self._base_offset = base_offset
        self._pos = base_offset

    def reset(self) -> None:
        self._pos = self._base_offset

    def write(self, data: Union[bytes, bytearray, memoryview, "pa.Buffer"]) -> int:
        mv = memoryview(data)
        bytes_written = 0
        nbytes = len(mv)
        while bytes_written < nbytes:
            n = os.pwrite(self._fd, mv[bytes_written:], self._pos + bytes_written)
            if n <= 0:
                # POSIX pwrite of a nonzero buffer should return >0 or raise;
                # 0 can happen on some FUSE / network filesystems and would
                # spin this loop forever.
                raise OSError(
                    f"pwrite returned {n} on fd {self._fd} "
                    f"(offset={self._pos + bytes_written}, "
                    f"remaining={nbytes - bytes_written})"
                )
            bytes_written += n
        self._pos += bytes_written
        return bytes_written


def _stream_members_flight(
    endpoint: _Endpoint,
    members: List[_FileRanges],
    sink: _PwriteSink,
) -> None:
    """Fetch ``members`` from ``endpoint`` in one Flight ``DoAction``,
    writing the result bodies straight into ``sink``.

    Bodies are the files' self-framed records, so nothing is re-framed. The
    server streams them in ``_FLIGHT_CHUNK`` pieces, so request size does
    not bound memory on either side.
    """
    import pyarrow.flight as flight

    host, port, _incarnation = endpoint
    sources = [(m.path, m.ranges) for m in members]
    body = json.dumps({"s": sources}).encode("utf-8")
    with flight.connect(_grpc_location(host, port)) as client:
        for result in client.do_action(flight.Action("fetch", body)):
            sink.write(result.body)


def _fetch_from_file_server(
    out_file_obj: "_PwriteSink",
    shuffle_id: str,
    node_id: str,
    members: List[_FileRanges],
) -> None:
    """Stream every member's records into ``out_file_obj`` over ONE Flight
    client in one DoAction request.

    Retry only transport faults; propagate everything else. The retriable errors
    are all FlightError and share the single ``except flight.FlightError`` (so
    ``FlightTimedOutError`` and friends need no clause of their own); the terminal
    ones are either split out ahead of it or are not a FlightError at all.

    Retried (transport faults, caught by ``except flight.FlightError``):
      * ``FlightUnavailableError`` -- server down / restarting.
      * ``FlightTimedOutError`` -- hung.
      * ``FlightCancelledError`` -- dropped mid-call.
      * ``FlightInternalError`` and any other transport ``FlightError``.

    Terminal (propagate without retry):
      * ``flight.FlightServerError`` -- server alive but refused the request
        (missing/unreadable file, server-side OSError, or bug). It IS a
        FlightError, so it is re-raised ahead of the broad catch.
      * ``pa.lib.ArrowInvalid`` / ``ArrowKeyError`` -- corrupt / short frame, or a
        server-side ValueError / KeyError. Not a FlightError; propagates as-is.
      * sink ``OSError`` (local write failed, e.g. disk full or I/O error) and
        ``_resolve``'s ``ShuffleFileServerAnomalyError`` (dead / unschedulable /
        unregistered actor). Propagate as-is.

    On a transport fault, re-resolve and compare incarnations (not host:port -- a
    restart can rebind the port). A changed incarnation means the server restarted:
    reset the sink and retry in place. An unchanged incarnation means the process
    stayed up but the fetch still failed: retry once (a blip ``_resolve`` may have
    waited through), then escalate to a terminal ``ShuffleFileServerAnomalyError``
    (likely a network block on the Flight port). Flight error classes / gRPC codes:
    https://arrow.apache.org/docs/python/api/flight.html
    https://grpc.io/docs/guides/status-codes/
    """
    import pyarrow.flight as flight

    key = _file_server_name(shuffle_id, node_id)

    def _resolve() -> _Endpoint:
        # Endpoint cache avoids a blocking Ray RPC per fetch. On miss, ask
        # Ray for actor state and route by outcome (see docstring above).
        with _ENDPOINT_CACHE_LOCK:
            ep = _ENDPOINT_CACHE.get(key)
        if ep is not None:
            return ep
        try:
            server = ray.get_actor(
                _file_server_name(shuffle_id, node_id),
                namespace=_SHUFFLE_FILE_SERVER_NAMESPACE,
            )
        except ValueError as e:
            # Actor name isn't registered (never created or cleaned up)
            raise ShuffleFileServerAnomalyError(
                f"ShuffleFileServer on node {node_id} not found in namespace: {e}"
            ) from e
        try:
            ep = cast(_Endpoint, ray.get(server.endpoint.remote()))
        except ActorDiedError as e:
            # Ray restarts the actor on mid-life death, so an ActorDiedError
            # reaching us means init failure or external ray.kill.
            raise ShuffleFileServerAnomalyError(
                f"ShuffleFileServer on node {node_id} is dead: {e}"
            ) from e
        except ActorUnschedulableError as e:
            # Pinned node is gone; soft=False can't relocate the actor.
            raise ShuffleFileServerAnomalyError(
                f"ShuffleFileServer on node {node_id} is unschedulable "
                f"(node likely dead): {e}"
            ) from e
        with _ENDPOINT_CACHE_LOCK:
            _ENDPOINT_CACHE[key] = ep
        return ep

    same_incarnation_retried = False
    while True:
        endpoint = _resolve()
        try:
            _stream_members_flight(endpoint, members, out_file_obj)
            return
        except flight.FlightServerError:
            raise  # terminal: server alive, refused the request (see docstring)
        except flight.FlightError as e:
            # any other FlightError is a transport fault (see docstring)
            out_file_obj.reset()
            with _ENDPOINT_CACHE_LOCK:
                _ENDPOINT_CACHE.pop(key, None)
            fresh = _resolve()

            if fresh.incarnation != endpoint.incarnation:
                same_incarnation_retried = False
                logger.warning(f"node {node_id}: file server restarted, retrying")
                continue

            if not same_incarnation_retried:
                same_incarnation_retried = True
                logger.warning(
                    f"node {node_id}: Flight fetch failed, retrying once ({e})"
                )
                time.sleep(0.5)
                continue

            raise ShuffleFileServerAnomalyError(
                f"node {node_id}: Flight fetch still failing but file server "
                f"reachable via Ray; likely a network block on the Flight port "
                f"(NetworkPolicy/firewall/routing)."
            ) from e


def disk_shuffle_dirs(shuffle_id: str) -> Tuple[str, str]:
    prefix = os.path.join(tempfile.gettempdir(), f"ray_shuffle_disk_{shuffle_id}")
    return f"{prefix}_map", f"{prefix}_reduce"


_HANDLE_COLUMN = "__disk_shuffle_handle__"


def handle_to_block(handle: "ShuffleHandle") -> pa.Table:
    """Wrap a ``ShuffleHandle`` in a one-row Arrow block."""
    return pa.table({_HANDLE_COLUMN: pa.array([pickle.dumps(handle)], pa.binary())})


def handle_from_block(block: pa.Table) -> "ShuffleHandle":
    """Inverse of ``handle_to_block``; raises if ``block`` isn't a handle block."""
    if not (
        isinstance(block, pa.Table)
        and block.column_names == [_HANDLE_COLUMN]
        and block.num_rows == 1
    ):
        raise ValueError(
            "Expected a one-row disk-shuffle handle block, got "
            f"{type(block).__name__} with columns "
            f"{getattr(block, 'column_names', None)}"
        )
    return pickle.loads(block.column(0)[0].as_py())


class _PartitionInput(NamedTuple):
    """One upstream input's share of a partition, from its node table."""

    # One source per node with records for the partition: its whole file.
    sources: List[_SourceRef]
    # map_id -> attempt token the regroup committed; ``_decode_records``
    # keeps only records carrying it.
    committed_attempts: Mapping[int, int]
    schema: Optional[pa.Schema]


def _partition_input(table: Dict[str, Any], partition_id: int) -> _PartitionInput:
    """Slice ``partition_id`` out of an input's node table: its sources,
    committed attempts and schema.

    The table is one object per input, shared by every partition: ``nodes``
    names each node's file server and file prefix, ``committed_attempts``
    maps map id to the committed attempt token, and ``sizes`` is ``[N, P]``,
    the size of each node's ``node_p{p}.shf`` up to its last committed
    record. A node with size 0 wrote nothing for ``p`` and gets no source.
    """
    nodes = table["nodes"]
    sizes = table["sizes"][:, partition_id]
    sources: List[_SourceRef] = []
    for i in np.flatnonzero(sizes > 0).tolist():
        shuffle_id, node_id, prefix = nodes[i]
        sources.append(
            _SourceRef(
                shuffle_id=shuffle_id,
                node_id=node_id,
                file=_FileRanges(
                    path=f"{prefix}_p{partition_id}.shf",
                    ranges=[(0, int(sizes[i]))],
                ),
            )
        )
    return _PartitionInput(sources, table["committed_attempts"], table.get("schema"))


def _group_by_server(sources: List[_SourceRef]) -> List[_NodeGroup]:
    """Collapse sources by file server so each file server gets ONE Flight connection."""
    by_key: Dict[Tuple[str, str], _NodeGroup] = {}
    for source in sources:
        key = (source.shuffle_id, source.node_id)
        group = by_key.get(key)
        if group is None:
            group = _NodeGroup(
                shuffle_id=source.shuffle_id,
                node_id=source.node_id,
                members=[],
            )
            by_key[key] = group
        group.members.append(source.file)
    return list(by_key.values())


def _order_by_map_id(
    records_by_group: Iterable[List[Tuple[int, pa.Table]]],
) -> List[pa.Table]:
    """Return every group's decoded tables sorted by map id.

    Fetch groups follow scheduler placement and records within a file follow
    append order, so without this sort the reduce input would differ run to
    run.
    """
    records = [rec for group in records_by_group for rec in group]
    records.sort(key=lambda rec: rec[0])
    return [table for _, table in records]


def _compute_prefetch_layout(
    groups: List[_NodeGroup],
) -> Tuple[int, List[int], List[int]]:
    """Assign each group a contiguous byte region in the reducer's prefetch file.

    Returns ``(total_size, base_offsets, per_group_sizes)``: a group's size
    is the byte total of its ranges, and base offsets are the running sum,
    so fetch threads pwrite at disjoint offsets without locking.
    """
    sizes = [
        sum(length for m in g.members for (_off, length) in m.ranges) for g in groups
    ]
    base_offsets: List[int] = []
    acc = 0
    for sz in sizes:
        base_offsets.append(acc)
        acc += sz
    return acc, base_offsets, sizes
