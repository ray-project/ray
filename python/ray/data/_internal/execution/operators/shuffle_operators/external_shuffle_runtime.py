"""External-shuffle file-transport runtime: Arrow Flight transport, per-node
ShuffleFileServer actor, prefetch layout, error hierarchy. Imported by the
map/reduce task bodies in ``external_shuffle_tasks``."""

import errno
import json
import logging
import os
import struct
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import (
    BinaryIO,
    Dict,
    Iterable,
    List,
    Literal,
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
from ray.exceptions import (
    ActorDiedError,
    ActorUnavailableError,
    ActorUnschedulableError,
)

logger = logging.getLogger(__name__)

# ShuffleFileServer actor identity. Name is deterministic in (shuffle_id, node_id).
_SHUFFLE_FILE_SERVER_NAMESPACE = "ray_data_shuffle_external"

# Endpoint resolution polls while the actor restarts (Ray raises
# ActorUnavailableError): the poll interval, and the warn cadence (in polls) so
# a stuck fetch stays visible.
_ENDPOINT_POLL_INTERVAL_S = 2.0
_ENDPOINT_POLL_WARN_EVERY = 15  # ~30s at the interval above

# Materialized handle metadata per reduce resolve batch.
_DEFAULT_HANDLE_BATCH_BYTES = 64 * MiB

# pyarrow-supported shard codecs (``"none"``/``None`` = uncompressed). Rides
# ``data_context.hash_shuffle_compression``.
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
# Shard wire format: [u64 uncompressed_size][zstd(whole IPC stream)]
_WF_HEADER = struct.Struct("<Q")


def _codec_for(compression: Compression) -> Optional["pa.Codec"]:
    """Codec name -> pa.Codec; None/"none" (any case) -> None (pyarrow has no
    "none" codec). pa.Codec is itself case-insensitive for real codec names, so
    routing both the map (encode) and reduce (decode) sides through here makes
    them agree on the codec no matter how ``hash_shuffle_compression`` is cased.
    """
    if not compression or compression.lower() == "none":
        return None
    return pa.Codec(compression)


def _encode_shard(
    table: pa.Table, compression: Compression = "zstd", combine_native: bool = False
) -> pa.Buffer:
    """Encode a partition shard as a whole-frame blob (one codec frame per shard,
    vs Arrow's per-buffer IPC compression). ``compression`` comes from
    ``data_context.hash_shuffle_compression``. When the caller has confirmed there
    are no extension columns, ``combine_native`` uses PyArrow's native combine,
    which skips the per-buffer ``nbytes`` accounting the extension-safe
    ``transform_pyarrow`` path does; that only costs at large partition counts
    (each shard has many small chunks)."""
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
    buf: Union[bytes, "pa.Buffer", memoryview], compression: Compression = "zstd"
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
    """Handle written by each mapper task, consumed by reducer.

    Only the fields the runtime consumes are declared; the mapper task can
    add producer-side bookkeeping (byte counts, schema, etc.) as extra keys.
    """

    path: str
    # Dense per-partition range index (see _build_range_index): row ``p`` is
    # ``(offset, length)`` of partition ``p``'s frame; ``length == 0`` => the
    # partition is absent/empty.
    index_ranges: "np.ndarray"  # shape [num_partitions, 2], int64
    shuffle_id: str
    node_id: str
    schema: Optional["pa.Schema"]


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

    We run with ``max_restarts=-1``, ``lifetime="detached"``,
    ``NodeAffinitySchedulingStrategy(node_id, soft=False)``, this means Ray auto-restarts
    the actor on any mid-life crash (os.exit) and surfaces ``ActorUnavailableError``
    during the restart window. So the anomalous states we key off of are:

    - ``ActorDiedError``          -> ``__init__`` raised, or external
                                    ``ray.kill(actor, no_restart=True)``.
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
# build the ShuffleHandle's dense byte index. What this writes is exactly what
# the SERVER serves and the FETCH side reads.
# =============================================================================
def _build_range_index(index, num_partitions):
    """Dense per-partition (offset, length) index: one row per partition.

    Row ``p`` is ``(offset, length)`` of partition ``p``'s IPC frame; absent or
    empty partitions stay ``(0, 0)`` (``length == 0`` => the reducer skips them).
    """
    ranges = np.zeros((num_partitions, 2), dtype=np.int64)
    for partition_id, frame_range in index.items():
        # (offset, length); one frame per partition
        ranges[partition_id] = frame_range[0]
    return ranges


def _decoded_to_array(decoded, num_partitions):
    """Dense per-partition decoded-byte counts (was a Dict[partition_id,int] in every
    handle — a second O(partitions) bloat). One int64 array indexed by partition_id."""
    arr = np.zeros(num_partitions, dtype=np.int64)
    for partition_id, nbytes in decoded.items():
        arr[partition_id] = nbytes
    return arr


class _PartitionWriter:
    """Per-partition shard buffer, sealed to disk one IPC frame per partition.

    ``add_shard(partition_id, shard)`` buffers a shard into partition ``partition_id``'s list;
    ``flush_all()`` concatenates each partition's shards, IPC-encodes them
    into one whole-frame, writes it to the output file, and records the
    (offset, length) range in ``index``.
    """

    __slots__ = (
        "_out_file",
        "_map_id",
        "_compression",
        "_staging",
        "_index",
        "_decoded_bytes_per_partition",
        "_combine_native_ok",
    )

    def __init__(
        self,
        out_file: BinaryIO,
        map_id: int,
        compression: Compression,
    ):
        self._out_file = out_file
        self._map_id = map_id
        # Codec from data_context.hash_shuffle_compression (same field the reduce reads).
        self._compression = compression
        self._staging: Dict[int, List[pa.Table]] = {}
        self._index: Dict[int, List[Tuple[int, int]]] = {}
        self._decoded_bytes_per_partition: Dict[int, int] = {}
        # Whether native combine_chunks is safe (no extension columns). Computed
        # once on the first flush; every shard of this map shares one schema.
        self._combine_native_ok: Optional[bool] = None

    def _flush(self, partition_id: int) -> None:
        shards = self._staging.get(partition_id)
        if not shards:
            return
        tbl = pa.concat_tables(shards) if len(shards) > 1 else shards[0]
        if self._combine_native_ok is None:
            # Once per map: concat above guarantees a uniform schema, so native
            # combine is safe iff no column is an extension type. Avoids the
            # per-shard extension-safe combine (which re-scans + does per-column
            # dispatch + nbytes on every shard).
            self._combine_native_ok = not any(
                _is_pa_extension_type(f.type) for f in tbl.schema
            )
        # ``tbl.nbytes`` is the decoded (pre-IPC, pre-compression) byte count.
        self._decoded_bytes_per_partition[partition_id] = tbl.nbytes
        buf = _encode_shard(  # whole-frame codec (see _encode_shard)
            tbl, self._compression, self._combine_native_ok
        )
        # Refuse frames the u64 response-wire encoding can't represent.
        if buf.size > _MAX_RANGE_BYTES:
            raise RuntimeError(
                f"map_{self._map_id}.shf partition {partition_id}: IPC frame is "
                f"{buf.size} bytes, exceeding the u64 wire-protocol "
                f"per-range limit ({_MAX_RANGE_BYTES}). Increase "
                f"``num_partitions`` or reduce the upstream block size."
            )
        off = self._out_file.tell()
        self._out_file.write(memoryview(buf))
        self._index.setdefault(partition_id, []).append((off, buf.size))
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
    """Build (not start) an Arrow Flight server serving shuffle byte-ranges via
    DoAction. Each range is framed as ``[u64 length][frame bytes]``."""
    import pyarrow.flight as flight

    class _ShuffleFlightServer(flight.FlightServerBase):
        def do_action(self, context, action):
            req = json.loads(action.body.to_pybytes())
            for path, ranges in req["s"]:
                fpath = os.path.join(base_dir, os.path.basename(path))
                with open(fpath, "rb") as f:
                    for off, length in ranges:
                        # Empty ranges still emit a u64(0) header so the client's
                        # framed stream stays aligned with _compute_prefetch_layout
                        # (which reserves 8 bytes per range including length==0).
                        if length == 0:
                            yield flight.Result(pa.py_buffer(struct.pack(">Q", 0)))
                            continue
                        f.seek(off)
                        # Chunk the frame so no single Result materializes a whole
                        # large frame in the server's RAM. The u64 length header
                        # rides in the first chunk only (its own Result would pay a
                        # full gRPC message's overhead for 8 bytes).
                        remaining = length
                        first = True
                        while remaining:
                            buf = f.read(min(remaining, _FLIGHT_CHUNK))
                            if not buf:
                                # File shorter than the index says (truncated /
                                # stale offset). A short send silently desyncs
                                # every later frame at the client (SPARK-34534).
                                raise ValueError(
                                    f"short read: {fpath} @{off}+{length}, "
                                    f"got {length - remaining}"
                                )
                            remaining -= len(buf)
                            if first:
                                buf = struct.pack(">Q", length) + buf
                                first = False
                            yield flight.Result(pa.py_buffer(buf))

    return _ShuffleFlightServer(_grpc_location(host, 0))


@ray.remote(num_cpus=0, max_restarts=-1)
class ShuffleFileServer:
    """Per-node file fetch service: owns an Arrow Flight server that serves
    byte-ranges of local shuffle files to remote reducers. Survives individual
    map/reduce workers; in a real cluster, one per node (NodeAffinity)."""

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
        t = threading.Thread(target=self._run_server, daemon=True)
        t.start()

    def _run_server(self) -> None:
        # If the server loop ever returns/raises, the endpoint is dead but the
        # actor process would keep answering RPCs. Kill the process so Ray
        # restarts the actor (max_restarts=-1).
        try:
            self._server.serve()  # gRPC Flight server; blocks until shutdown
        except BaseException:
            logger.exception("ShuffleFileServer server loop crashed; exiting actor")
        else:
            logger.error("ShuffleFileServer server loop returned; exiting actor")
        os._exit(1)

    def endpoint(self) -> _Endpoint:
        # (host, port) to connect; incarnation to detect a restart.
        return _Endpoint(self._host, self._port, self._incarnation)


@ray.remote(num_cpus=0)
def _cleanup_shuffle_dir(map_dir: str, reduce_dir: str) -> None:
    """Best-effort ``rmtree`` of this shuffle's map + reduce staging dirs. Pinned
    to its node with ``NodeAffinity(soft=False)``: on a live node it runs there;
    if the node is gone the task just fails."""
    import shutil

    shutil.rmtree(map_dir, ignore_errors=True)
    shutil.rmtree(reduce_dir, ignore_errors=True)


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
    max_bytes: int,
    sink: _PwriteSink,
) -> None:
    """Arrow Flight DoAction: one client, batched fetch requests. Response
    ``Result`` bodies carry ``[u64 len][frame]`` framing, so they stream verbatim
    into the sink. This does NOT classify failures: it lets the raw transport
    error (pyarrow ``FlightError``) or the sink's ``OSError`` propagate, and
    ``_fetch_from_file_server`` decides terminal-vs-retryable in one place."""
    import pyarrow.flight as flight

    host, port, _incarnation = endpoint
    with flight.connect(_grpc_location(host, port)) as client:
        for batch in _chunk_members_by_bytes(members, max_bytes):
            sources = [(m.path, m.ranges) for m in batch]
            body = json.dumps({"s": sources}).encode("utf-8")
            for result in client.do_action(flight.Action("fetch", body)):
                sink.write(result.body)


def _fetch_from_file_server(
    out_file_obj: "_PwriteSink",
    shuffle_id: str,
    node_id: str,
    members: List[_FileRanges],
    max_bytes_per_fetch: int,
) -> None:
    """Stream every member's shards into ``out_file_obj`` over ONE Flight
    client, batched into DoAction requests of ≤ ``max_bytes_per_fetch``.

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
    polled through), then escalate to a terminal ``ShuffleFileServerAnomalyError``
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
        poll_count = 0
        while True:
            try:
                ep = cast(_Endpoint, ray.get(server.endpoint.remote()))
                break
            except ActorUnavailableError:
                poll_count += 1
                if poll_count % _ENDPOINT_POLL_WARN_EVERY == 0:
                    logger.warning(
                        f"ShuffleFileServer on node {node_id} unavailable "
                        f"(~{poll_count * _ENDPOINT_POLL_INTERVAL_S:.0f}s), "
                        f"still polling..."
                    )
                time.sleep(_ENDPOINT_POLL_INTERVAL_S)
            except ActorDiedError as e:
                # With max_restarts=-1, Ray auto-restarts on mid-life death and
                # surfaces ActorUnavailableError during the restart. So an
                # ActorDiedError reaching us means init failure or external
                # ray.kill
                raise ShuffleFileServerAnomalyError(
                    f"ShuffleFileServer on node {node_id} is dead: {e}"
                ) from e
            except ActorUnschedulableError as e:
                # Pinned node is gone; soft=False can't relocate the actor.
                # Ray transitions from ActorUnavailableError to this ~10s after
                # heartbeat loss.
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
            _stream_members_flight(endpoint, members, max_bytes_per_fetch, out_file_obj)
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


def _chunk_members_by_bytes(
    members: List[_FileRanges],
    max_bytes: int,
) -> Iterable[List[_FileRanges]]:
    """Yield sub-batches of members whose total requested bytes ≤ ``max_bytes``.

    A source's ranges MAY be split across batches: the source appears as
    multiple pseudo-members with the same ``path`` but disjoint range
    subsets, in the original range order. Individual ranges are NEVER
    split as each range is one Arrow IPC frame at the mapper, so a
    sub-range cut would break the reducer's decode.
    """
    batch: List[_FileRanges] = []
    batch_bytes = 0
    for member in members:
        pending: List[Tuple[int, int]] = []
        for off, length in member.ranges:
            if (batch or pending) and batch_bytes + length > max_bytes:
                if pending:
                    batch.append(_FileRanges(path=member.path, ranges=pending))
                    pending = []
                yield batch
                batch, batch_bytes = [], 0
            pending.append((off, length))
            batch_bytes += length
        if pending:
            batch.append(_FileRanges(path=member.path, ranges=pending))
    if batch:
        yield batch


# fetch helpers
def _handle_batch_size(handles, batch_bytes):
    """#handles to resolve per batch so materialized metadata stays ≈ batch_bytes.

    Peeks one handle for ``num_partitions`` (each handle ≈ num_partitions × 16
    bytes of CSR arrays), so in-flight handle memory stays constant regardless
    of #mappers/#partitions.
    """
    if not handles:
        return 1
    probe = handles[0]
    if not isinstance(probe, dict):
        probe = cast(dict, ray.get(probe))
    try:
        npart = int(probe.get("num_partitions") or len(probe["index_ranges"]))
    except Exception:
        npart = 1
    per_handle = max(1, npart * 16)
    return max(1, min(len(handles), batch_bytes // per_handle))


def _handles_to_sources(
    handles: List["ShuffleHandle"],
    partition_id: int,
    batch_bytes: int = _DEFAULT_HANDLE_BATCH_BYTES,
) -> Tuple[List[_SourceRef], Optional[pa.Schema]]:
    """Extract per-partition source refs from a reducer's input handles.

    Resolves handle refs in ≈``batch_bytes`` batches, reads this partition's row
    out of each handle's dense range index, then frees the batch — so in-flight
    handle memory stays constant, not O(maps × partitions). Skips handles with
    zero bytes for this partition; picks the first non-None schema.
    """
    sources: List[_SourceRef] = []
    output_schema: Optional[pa.Schema] = None
    if not handles:
        return sources, output_schema

    batch_size = _handle_batch_size(handles, batch_bytes)
    for start in range(0, len(handles), batch_size):
        batch = handles[start : start + batch_size]
        refs = [handle for handle in batch if not isinstance(handle, dict)]
        # pyrefly: ignore[no-matching-overload]
        vals = iter(ray.get(refs)) if refs else iter(())
        resolved = [
            handle if isinstance(handle, dict) else next(vals) for handle in batch
        ]
        for handle in resolved:
            if output_schema is None:
                output_schema = handle.get("schema")
            # Dense index: partition_id is the row; only this row materializes
            # as a Python range, the rest stay as (zero-copy) numpy buffers.
            off, length = handle["index_ranges"][partition_id]
            length = int(length)
            if length > 0:
                sources.append(
                    _SourceRef(
                        shuffle_id=handle["shuffle_id"],
                        node_id=handle["node_id"],
                        file=_FileRanges(
                            path=handle["path"],
                            ranges=[(int(off), length)],
                        ),
                    )
                )
        # Free this batch's resolved handles before resolving the next, so
        # in-flight handle memory stays ≈ batch_bytes regardless of #mappers.
        del resolved, vals
    return sources, output_schema


def _group_by_server(sources: List[_SourceRef]) -> List[_NodeGroup]:
    """Collapse sources by file server so each file server gets ONE Flight connection.

    Sources on the same file server share a ``(shuffle_id, node_id)`` which is
    used as the collapse key.
    """
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


def _compute_prefetch_layout(
    groups: List[_NodeGroup],
) -> Tuple[int, List[int], List[int]]:
    """Assign each group a contiguous byte region in the reducer's prefetch file.

    Returns ``(total_size, base_offsets, per_group_sizes)`` where sizes are the
    ``8 + length`` framed byte totals (u64 len prefix + IPC bytes per range),
    base offsets are running cumulative sums. Fetch threads then pwrite each
    group's fetched frames at DISJOINT offsets.
    """
    sizes = [
        sum(8 + length for m in g.members for (_off, length) in m.ranges)
        for g in groups
    ]
    base_offsets: List[int] = []
    acc = 0
    for sz in sizes:
        base_offsets.append(acc)
        acc += sz
    return acc, base_offsets, sizes
