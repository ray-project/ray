"""External-shuffle file-transport runtime: Arrow Flight transport, per-node
ShuffleManager actor, prefetch layout, error hierarchy. Imported by the
map/reduce task bodies in ``external_shuffle_tasks``."""

import errno
import json
import logging
import os
import struct
import threading
import time
from dataclasses import dataclass, field
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    TypedDict,
    Union,
)

import pyarrow as pa

import ray
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.exceptions import (
    ActorDiedError,
    ActorUnavailableError,
    ActorUnschedulableError,
)

logger = logging.getLogger(__name__)


# Each range's payload length is framed as a u32 in the sink, so no single
# range/IPC frame may exceed 4 GiB - 1. Checked at mapper write time so an
# oversized IPC buffer fails at the mapper task.
_MAX_RANGE_BYTES: int = (1 << 32) - 1


# ----------------------------------------------------------------- Arrow IPC
# Shard wire format: [u64 uncompressed_size][zstd(whole IPC stream)] -- ONE zstd
# frame per shard, vs Arrow's per-buffer IPC compression (~40 zstd blobs/shard).
# One decompress + one alloc per shard at the reducer; inner IPC is uncompressed
# so buffers read zero-copy.
_WF_HEADER = struct.Struct("<Q")


def _codec_for(compression: Optional[str]) -> Optional["pa.Codec"]:
    """Codec name -> pa.Codec; None/"none" -> None (pyarrow has no "none" codec)."""
    if not compression or compression == "none":
        return None
    return pa.Codec(compression)


def _encode_shard(table: pa.Table, compression: Optional[str] = "zstd") -> pa.Buffer:
    """Encode a partition shard as a whole-frame blob (one codec frame per shard,
    vs Arrow's per-buffer IPC compression). ``compression`` comes from
    ``data_context.hash_shuffle_compression``."""
    if table.num_columns > 0:
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
    buf: Union[bytes, "pa.Buffer", memoryview], compression: Optional[str] = "zstd"
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


# Linux-only; macOS lacks POSIX_FADV_DONTNEED. Probed once at import time so
# the hot path is a constant-time attribute check, not a try/except per range.
_HAS_FADV_DONTNEED = hasattr(os, "posix_fadvise") and hasattr(os, "POSIX_FADV_DONTNEED")


def _drop_pagecache(fd: int, offset: int, length: int) -> None:
    """Hint the kernel to drop ``[offset, offset+length)`` of ``fd`` from the
    page cache. Effective on clean pages; on dirty pages the hint is recorded
    and eviction happens on next writeback."""
    if not _HAS_FADV_DONTNEED or length <= 0:
        return
    try:
        os.posix_fadvise(fd, offset, length, os.POSIX_FADV_DONTNEED)
    except OSError:
        # Best-effort: any failure is silently ignored, cause the worst case is the
        # kernel keeps the pages longer.
        pass


# ShuffleManager actor identity. Name is deterministic in (shuffle_id, node_id)
_SHUFFLE_MANAGER_NAMESPACE = "ray_data_shuffle_external"


def _manager_name(shuffle_id: str, node_id: str) -> str:
    return f"shuffle_mgr:{shuffle_id}:{node_id}"


def _lookup_manager(shuffle_id: str, node_id: str) -> "ray.actor.ActorHandle":
    """Locate the ShuffleManager for this shuffle+node in the named-actor
    namespace. Raises ``ValueError`` if the actor has never been registered
    (or has been terminated and garbage-collected)."""
    return ray.get_actor(
        _manager_name(shuffle_id, node_id),
        namespace=_SHUFFLE_MANAGER_NAMESPACE,
    )


# =============================================================================
# Arrow Flight transport: server streams shuffle byte-ranges via DoAction;
# reducer fetches them over a Flight client. Payload is opaque bytes (the
# whole-frame shard blob), so no RecordBatch (de)serialization on the path.
# =============================================================================
_FLIGHT_CHUNK = 1 << 20  # Flight Result body size; keep under gRPC's ~4 MiB frame


# Flight fetch request (Action body): JSON ``{"t": token, "s": [[path, [[off, len], ...]], ...]}``.


def _make_flight_server(host: str, base_dir: str, token: str):
    """Build (not start) an Arrow Flight server serving shuffle byte-ranges via
    DoAction. Each range is framed as ``[u32 length][frame bytes]`` — the layout
    the reducer's sink expects, so the decode path is unchanged."""
    import pyarrow.flight as flight

    class _ShuffleFlightServer(flight.FlightServerBase):
        def do_action(self, context, action):
            req = json.loads(action.body.to_pybytes())
            tok, sources = req["t"], req["s"]
            if tok != token:
                raise ValueError("AUTH_FAIL")  # surfaces as FlightError client-side
            for path, ranges in sources:
                fpath = os.path.join(base_dir, os.path.basename(path))
                with open(fpath, "rb") as f:
                    for off, length in ranges:
                        # u32 length header, then the frame bytes (sink layout).
                        yield flight.Result(pa.py_buffer(struct.pack(">I", length)))
                        f.seek(off)
                        remaining = length
                        while remaining:
                            buf = f.read(min(remaining, _FLIGHT_CHUNK))
                            if not buf:
                                break
                            remaining -= len(buf)
                            yield flight.Result(pa.py_buffer(buf))

    return _ShuffleFlightServer(f"grpc://{host}:0")


@ray.remote
class ShuffleManager:
    """Per-node file fetch service: owns an Arrow Flight server that serves
    byte-ranges of local shuffle files to remote reducers. Survives individual
    map/reduce workers; in a real cluster, one per node (NodeAffinity)."""

    def __init__(
        self,
        base_dir: str,
        token: str,
    ):
        self.base_dir = os.path.realpath(base_dir)
        os.makedirs(self.base_dir, exist_ok=True)
        self.token = token
        ip = ray.util.get_node_ip_address()
        self._server = _make_flight_server(ip, self.base_dir, token)
        self._host, self._port = ip, self._server.port
        t = threading.Thread(target=self._run_server, daemon=True)
        t.start()

    def _run_server(self) -> None:
        # If the server loop ever returns/raises, the endpoint is dead but the
        # actor process would keep answering RPCs — a false-positive that breaks
        # the "actor alive ⇒ server alive" invariant reducers rely on. Kill the
        # process so Ray restarts the actor (max_restarts=-1).
        try:
            self._server.serve()  # gRPC Flight server; blocks until shutdown
        except BaseException:
            logger.exception("ShuffleManager server loop crashed; exiting actor")
        else:
            logger.error("ShuffleManager server loop returned; exiting actor")
        os._exit(1)

    def endpoint(self) -> Tuple[str, int]:
        return (self._host, self._port)


@ray.remote(num_cpus=0)
def _cleanup_shuffle_dir(map_dir: str, reduce_dir: str, expected_node_id: str) -> None:
    """Best-effort ``rmtree`` of this shuffle's map + reduce staging dirs.
    NodeAffinity(soft=True) may land us off-target; no-op then."""
    if ray.get_runtime_context().get_node_id() != expected_node_id:
        return
    import shutil

    shutil.rmtree(map_dir, ignore_errors=True)
    shutil.rmtree(reduce_dir, ignore_errors=True)


class ShuffleHandle(TypedDict, total=False):
    """Handle written by each mapper task, consumed by reducer.

    Only the fields the runtime consumes are declared; the mapper task can
    add producer-side bookkeeping (byte counts, schema, etc.) as extra keys.
    """

    path: str
    # CSR per-partition range index (3 int64 arrays; see _index_to_csr).
    # Partition ``p``: ``zip(index_off[a:b], index_len[a:b])``,
    # ``a=index_part_start[p]``, ``b=index_part_start[p+1]``.
    index_off: "np.ndarray"
    index_len: "np.ndarray"
    index_part_start: "np.ndarray"
    shuffle_id: str
    node_id: str
    token: str
    schema: Optional["pa.Schema"]


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


class ShuffleManagerAnomalyError(RuntimeError):
    """Terminal shuffle-manager failure: a driver-level retry cannot fix it.

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
    - The Flight fetch failed but the endpoint is unchanged and Ray RPC still
      works, which is often a network-configuration problem (``NetworkPolicy``,
      firewall, routing).
    """


# Process-global cache of ShuffleManager endpoints: {actor_name: (ip, port)}.
# Stale entries are popped by ``_prefetch_node_into`` on fetch failure; the next
# ``_resolve()`` call re-queries the actor. The lock guards concurrent access
# from reducer fetch threads.
_ENDPOINT_CACHE: Dict[str, Tuple[str, int]] = {}
_ENDPOINT_CACHE_LOCK = threading.Lock()

# --------------------------------------------------------- fetch routing types
# Named containers for reducer fetch orchestration. ``slots=True`` keeps
# per-instance memory small since we allocate one per source/group/member.


@dataclass(slots=True, frozen=True)
class _SourceRef:
    """One (mapper file, partition slice) the reducer needs to pull.

    Built once per input handle for a given partition_id at reducer start.
    The (shuffle_id, node_id) pair is the manager's named-actor identity;
    """

    shuffle_id: str
    node_id: str
    token: str
    path: str
    ranges: List[Tuple[int, int]]


@dataclass(slots=True, frozen=True)
class _NodeMember:
    """A source's ranges as they appear within a per-node fetch group."""

    path: str
    ranges: List[Tuple[int, int]]


@dataclass(slots=True)
class _NodeGroup:
    """All sources on one ShuffleManager, grouped so we open ONE Flight
    connection per manager. Sources collapse to the same group when their
    ``(shuffle_id, node_id)`` (i.e., the manager's named-actor identity) matches.
    """

    shuffle_id: str
    node_id: str
    token: str
    members: List[_NodeMember] = field(default_factory=list)


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

    def write(self, data) -> int:
        mv = memoryview(data)
        total = 0
        n_total = len(mv)
        while total < n_total:
            n = os.pwrite(self._fd, mv[total:], self._pos + total)
            if n <= 0:
                # POSIX pwrite of a nonzero buffer should return >0 or raise;
                # 0 can happen on some FUSE / network filesystems and would
                # spin this loop forever.
                raise OSError(
                    f"pwrite returned {n} on fd {self._fd} "
                    f"(offset={self._pos + total}, remaining={n_total - total})"
                )
            total += n
        self._pos += total
        return total


def _stream_members_flight(endpoint, token, members, max_bytes, sink) -> None:
    """Arrow Flight DoAction: one client, batched fetch requests. Response
    ``Result`` bodies carry ``[u32 len][frame]`` framing, so they stream
    verbatim into the sink. Transport failures map to
    ``ConnectionError`` (so _prefetch_node_into's resolve+retry handles them);
    auth failure maps to ``PermissionError`` (terminal); disk ``OSError`` from
    the sink propagates unchanged (so a full disk isn't mistaken for a retryable
    network fault)."""
    import pyarrow.flight as flight

    host, port = endpoint
    client = flight.connect(f"grpc://{host}:{port}")
    try:
        for batch in _chunk_members_by_bytes(members, max_bytes):
            sources = [(m.path, m.ranges) for m in batch]
            body = json.dumps({"t": token, "s": sources}).encode("utf-8")
            try:
                for result in client.do_action(flight.Action("fetch", body)):
                    sink.write(result.body)
            except OSError:
                # Sink pwrite failure (e.g. disk full): NOT a transport fault, so
                # don't remap it — let _prefetch_node_into's OSError handler
                # classify it (ShuffleDiskError). Must not become a retryable
                # ConnectionError or a full disk would spin forever.
                raise
            except Exception as e:
                # pyarrow surfaces our server-side token check as ArrowInvalid
                # (NOT a FlightError), so match on the message, not the type.
                if "AUTH_FAIL" in str(e):
                    raise PermissionError(f"ShuffleManager auth failed: {e}") from e
                # Any other fetch failure (unavailable / timeout / server error):
                # retryable transport fault. _prefetch_node_into re-resolves the
                # endpoint and, if unchanged, escalates to a terminal
                # ShuffleManagerAnomalyError (no infinite retry).
                raise ConnectionError(
                    f"flight fetch from {host}:{port} failed: {e}"
                ) from e
    finally:
        try:
            client.close()
        except Exception:
            pass


def _prefetch_node_into(
    out_file_obj: "_PwriteSink",
    shuffle_id: str,
    node_id: str,
    token: str,
    members: List[_NodeMember],
    max_bytes_per_fetch: int,
) -> None:
    """Stream every member's shards into ``out_file_obj`` over ONE Flight
    client, batched into DoAction requests of ≤ ``max_bytes_per_fetch``.

    Actor state drives the recovery policy:
      * Dead (init fail/ray.kill)     -> ``ShuffleManagerAnomalyError`` (terminal)
      * Unschedulable (node lost)     -> ``ShuffleManagerAnomalyError`` (terminal)
      * Unavailable (restarting)      -> poll until Ray resolves
      * conn dead, endpoint changed   -> reset sink, reconnect, retry in-place
      * conn dead, endpoint unchanged -> ``ShuffleManagerAnomalyError`` (network
                                           config problem, terminal)
    """
    key = _manager_name(shuffle_id, node_id)

    def _resolve() -> Tuple[str, int]:
        # Endpoint cache avoids a blocking Ray RPC per fetch. On miss, ask
        # Ray for actor state and route by outcome (see docstring above).
        with _ENDPOINT_CACHE_LOCK:
            ep = _ENDPOINT_CACHE.get(key)
        if ep is not None:
            return ep
        try:
            manager = _lookup_manager(shuffle_id, node_id)
        except ValueError as e:
            # Actor name isn't registered (never created or cleaned up)
            raise ShuffleManagerAnomalyError(
                f"ShuffleManager on node {node_id} not found in namespace: {e}"
            ) from e
        poll_count = 0
        while True:
            try:
                ep = ray.get(manager.endpoint.remote())
                break
            except ActorUnavailableError:
                poll_count += 1
                # Surface a warning every ~30s so a stuck fetch is visible.
                if poll_count % 15 == 0:
                    logger.warning(
                        f"ShuffleManager on node {node_id} unavailable "
                        f"(~{poll_count * 2}s), still polling..."
                    )
                time.sleep(2.0)
            except ActorDiedError as e:
                # With max_restarts=-1, Ray auto-restarts on mid-life death and
                # surfaces ActorUnavailableError during the restart. So an
                # ActorDiedError reaching us means init failure or external
                # ray.kill
                raise ShuffleManagerAnomalyError(
                    f"ShuffleManager on node {node_id} is dead: {e}"
                ) from e
            except ActorUnschedulableError as e:
                # Pinned node is gone; soft=False can't relocate the actor.
                # Ray transitions from ActorUnavailableError to this ~10s after
                # heartbeat loss.
                raise ShuffleManagerAnomalyError(
                    f"ShuffleManager on node {node_id} is unschedulable "
                    f"(node likely dead): {e}"
                ) from e
        with _ENDPOINT_CACHE_LOCK:
            _ENDPOINT_CACHE[key] = ep
        return ep

    while True:
        try:
            endpoint = _resolve()
            _stream_members_flight(
                endpoint, token, members, max_bytes_per_fetch, out_file_obj
            )
            return
        except PermissionError:
            raise
        except (ConnectionError, TimeoutError) as e:
            # If _resolve() returns, the actor is alive; endpoint compare tells us
            # whether the manager restarted (retry in-place) or the reducer-manager
            # network path is broken (terminal).
            out_file_obj.reset()
            with _ENDPOINT_CACHE_LOCK:
                _ENDPOINT_CACHE.pop(key, None)
            fresh = _resolve()
            if fresh == endpoint:
                # Endpoint unchanged: actor is alive but the connection is blocked.
                # Most likely a network configuration issue (NetworkPolicy,
                # firewall, routing); retrying to the same manager won't help.
                raise ShuffleManagerAnomalyError(
                    f"Flight fetch from node {node_id} failed ({e}) but "
                    f"ShuffleManager at {fresh} is still reachable via Ray. "
                    f"Likely a network configuration issue (NetworkPolicy, "
                    f"firewall, routing) between reducer and manager. "
                    f"Check the network config."
                ) from e
            logger.warning(
                f"Flight fetch from node {node_id} failed ({e}); ShuffleManager "
                f"restarted (endpoint {endpoint} → {fresh}). Retrying in place."
            )
            continue
        except OSError as e:
            if _is_disk_exhausted(e):
                raise ShuffleDiskError(
                    f"Disk exhausted writing prefetch for node {node_id}: {e}"
                ) from e
            raise


def _chunk_members_by_bytes(
    members: List[_NodeMember],
    max_bytes: int,
) -> Iterable[List[_NodeMember]]:
    """Yield sub-batches of members whose total requested bytes ≤ ``max_bytes``.

    A source's ranges MAY be split across batches: the source appears as
    multiple pseudo-members with the same ``path`` but disjoint range
    subsets, in the original range order. Individual ranges are NEVER
    split as each range is one Arrow IPC frame at the mapper, so a
    sub-range cut would break the reducer's decode.
    """
    batch: List[_NodeMember] = []
    batch_bytes = 0
    for member in members:
        pending: List[Tuple[int, int]] = []
        for off, length in member.ranges:
            if (batch or pending) and batch_bytes + length > max_bytes:
                if pending:
                    batch.append(_NodeMember(path=member.path, ranges=pending))
                    pending = []
                yield batch
                batch, batch_bytes = [], 0
            pending.append((off, length))
            batch_bytes += length
        if pending:
            batch.append(_NodeMember(path=member.path, ranges=pending))
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
        probe = ray.get(probe)
    try:
        npart = int(probe.get("num_partitions") or len(probe["index_part_start"]) - 1)
    except Exception:
        npart = 1
    per_handle = max(1, npart * 16)
    return max(1, min(len(handles), batch_bytes // per_handle))


def _handles_to_sources(
    handles: List["ShuffleHandle"],
    partition_id: int,
    batch_bytes: int = 64 * 1024 * 1024,
) -> Tuple[List[_SourceRef], Optional[pa.Schema]]:
    """Extract per-partition source refs from a reducer's input handles.

    Resolves handle refs in ≈``batch_bytes`` batches, slices this partition's
    ranges out of each handle's CSR arrays, then frees the batch — so in-flight
    handle memory stays constant, not O(maps × partitions). Skips handles with
    zero bytes for this partition; picks the first non-None schema.
    """
    sources: List[_SourceRef] = []
    output_schema: Optional[pa.Schema] = None
    if not handles:
        return sources, output_schema

    k = _handle_batch_size(handles, batch_bytes)
    for start in range(0, len(handles), k):
        batch = handles[start : start + k]
        refs = [h for h in batch if not isinstance(h, dict)]
        vals = iter(ray.get(refs)) if refs else iter(())
        resolved = [h if isinstance(h, dict) else next(vals) for h in batch]
        for h in resolved:
            if output_schema is None:
                output_schema = h.get("schema")
            # CSR slice: only this partition's ranges materialize as Python
            # objects; the rest stay as (zero-copy) numpy buffers.
            ps = h["index_part_start"]
            s = int(ps[partition_id])
            e = int(ps[partition_id + 1])
            if e > s:
                offs = h["index_off"][s:e].tolist()
                lens = h["index_len"][s:e].tolist()
                sources.append(
                    _SourceRef(
                        shuffle_id=h["shuffle_id"],
                        node_id=h["node_id"],
                        token=h["token"],
                        path=h["path"],
                        ranges=list(zip(offs, lens)),
                    )
                )
        # Free this batch's resolved handles before resolving the next, so
        # in-flight handle memory stays ≈ batch_bytes regardless of #mappers.
        del resolved, vals
    return sources, output_schema


def _group_by_manager(sources: List[_SourceRef]) -> List[_NodeGroup]:
    """Collapse sources by manager so each manager gets ONE Flight connection.

    Sources on the same manager share a ``(shuffle_id, node_id)`` which is
    used as the collapse key.
    """
    by_key: Dict[Tuple[str, str], _NodeGroup] = {}
    for s in sources:
        key = (s.shuffle_id, s.node_id)
        group = by_key.get(key)
        if group is None:
            group = _NodeGroup(
                shuffle_id=s.shuffle_id,
                node_id=s.node_id,
                token=s.token,
                members=[],
            )
            by_key[key] = group
        group.members.append(_NodeMember(path=s.path, ranges=s.ranges))
    return list(by_key.values())


def _compute_prefetch_layout(
    groups: List[_NodeGroup],
) -> Tuple[int, List[int], List[int]]:
    """Assign each group a contiguous byte region in the reducer's prefetch file.

    Returns ``(total_size, base_offsets, per_group_sizes)`` where sizes are the
    ``4 + length`` framed byte totals (u32 len prefix + IPC bytes per range),
    base offsets are running cumulative sums. Fetch threads then pwrite each
    group's fetched frames at DISJOINT offsets.
    """
    sizes = [
        sum(4 + length for m in g.members for (_off, length) in m.ranges)
        for g in groups
    ]
    base_offsets: List[int] = []
    acc = 0
    for sz in sizes:
        base_offsets.append(acc)
        acc += sz
    return acc, base_offsets, sizes
