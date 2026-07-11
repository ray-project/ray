"""External-shuffle task bodies (map + reduce).

- each MAP task writes ONE file (all its partitions, Arrow IPC) and returns ONE
  small handle (path + per-partition offset index + the source node's fetch
  endpoint + a per-shuffle auth token). Driver tracks O(N) handles; bulk data
  stays on local disk and never enters Ray's object store.
- a per-node ``ShuffleManager`` Ray actor runs its OWN socket server that
  ``pread``s requested byte-ranges and streams them back. The REDUCE task is a
  client of that server.
  Cross-node this is the real out-of-band transport; single-node it is a
  loopback socket (still the real code path, not a direct ``open``).

Uses the standard ``PartitionFn`` / ``ReduceFn`` contracts, so group-by /
sort / aggregate / join factories compose unchanged.
"""

import os
import pickle
import struct
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Union,
)

import pyarrow as pa

import ray
from ray._raylet import (
    StreamingGeneratorStats,  # pyrefly: ignore[missing-module-attribute]
)
from ray.data._internal.output_buffer import (
    BlockOutputBuffer,
    OutputBlockSizeOption,
)
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    TaskExecWorkerStats,
)
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

# ReduceFn/PartitionFn are shared with the in-memory variant. External is
# single-input, so ReduceFn's outer list always has length 1.
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (  # noqa: E402,E501
    PartitionFn,
    ReduceFn,
)
from ray.data._internal.execution.operators.shuffle_operators.external_shuffle_runtime import (  # noqa: E402,E501
    _MAX_RANGE_BYTES,
    _SHUFFLE_MANAGER_NAMESPACE,
    _compute_prefetch_layout,
    _drop_pagecache,
    _group_by_manager,
    _handles_to_sources,
    _ipc_buffer,
    _is_disk_exhausted,
    _manager_name,
    _prefetch_node_into,
    _PwriteSink,
    _read_ipc,
    ShuffleDiskError,
    ShuffleHandle,
    ShuffleManager,
)


_DEFAULT_MAX_BYTES_PER_FETCH = 256 * 1024 * 1024  # 256 MiB per FETCH frame
_DEFAULT_FETCH_THREADS = 32  # concurrent per-node fetch threads at the reducer


@ray.remote
def external_hash_shuffle_map_task(
    *blocks: Block,
    partition_fn: PartitionFn,
    num_partitions: int,
    out_dir: str,
    map_id: int,
    shuffle_id: str,
    token: str,
    map_op_name: str = "ExternalHashShuffleMap",
    pool_budget_bytes: int = 16 * 1024 * 1024,
    compression: Optional[str] = None,
    fsync_on_close: bool = True,
) -> ShuffleHandle:
    """Streaming write with a shared, byte-accounted staging pool, sealed
    via atomic ``rename``.

    ``pool_budget_bytes`` bounds both ends of the map:

    - **Output**: all post-hash partition buckets share one pool of that
      size. On overflow the LARGEST bucket is spilled — total staging is
      bounded independent of the partition count M.
    - **Input**: ``PartitionFn(Table → Dict[pid, Table])`` materializes all
      M shards at once (~2× copy of its input), so we feed it in row-batches
      sized ``pool_budget_bytes / avg_row_bytes`` to keep the transient spike
      ~pool-bounded. If the whole block already fits the pool, we skip
      batching.

    Net: **map peak ≈ input block + O(pool_budget_bytes)**. Blocking
    ``f.write`` gives natural OS backpressure on slow disks.

    Sealed via atomic ``rename``: writes go to ``map_{i}.shf.tmp``; after a
    final ``f.flush()`` + optional ``os.fsync`` + size sanity check against
    the index, we ``os.rename`` to the published path. Readers therefore
    see either no file or a complete, size-validated one — catches
    truncated files earlier than Arrow IPC's per-shard magic would.

    ``fsync_on_close=True`` gives durability against node crash. Readers go
    through the manager's page-cache-backed sendfile serve, so this sync
    only matters if node-reboot recovery is later added to the FT model.
    """
    node_id = ray.get_runtime_context().get_node_id()
    # Ensure the manager actor exists (get_if_exists=True → reuse across
    # mappers on the same node). We don't need to keep the handle: reducers
    # will look the manager up by name via ``_lookup_manager``.
    ShuffleManager.options(
        name=_manager_name(shuffle_id, node_id),
        namespace=_SHUFFLE_MANAGER_NAMESPACE,
        get_if_exists=True,
        lifetime="detached",
        max_restarts=-1,
        scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
        num_cpus=0,
    ).remote(out_dir, token)

    os.makedirs(out_dir, exist_ok=True)
    final_path = os.path.join(out_dir, f"map_{map_id}.shf")
    # Write to a temp file first; only ``rename`` once we've verified the full file.
    tmp_path = final_path + ".tmp"
    # index = {partition id, {offset, length}}
    index: Dict[int, List[Tuple[int, int]]] = {}
    staging: Dict[int, List[pa.Table]] = {}
    staging_bytes: Dict[int, int] = {}
    peak_inflight = 0  # max bytes of partition output held at once (excludes input)
    decoded_bytes_per_partition: Dict[int, int] = {} # Decoded bytes per partition
    output_schema: Optional[pa.Schema] = None

    def _partition_units(blk):
        """Yield (pid, shard).
        yield whole-block when the block already fits the pool (no overhead),
        else split into pool-sized row-batches (which bounds the 2×S partition
        spike to S + O(pool))."""
        if blk.num_rows == 0:
            return
        avg_row = max(1, blk.nbytes // blk.num_rows)
        batch_rows = max(1, pool_budget_bytes // avg_row)
        if blk.num_rows <= batch_rows:
            # block's partition spike is already ≤ pool → whole-block, no overhead
            for pid, shard in partition_fn(blk).items():
                yield pid, shard
        else:
            for batch in blk.to_batches(max_chunksize=batch_rows):
                bt = pa.Table.from_batches([batch], schema=blk.schema)
                for pid, shard in partition_fn(bt).items():
                    yield pid, shard

    final_size_on_close = -1
    try:
        with open(tmp_path, "wb") as f:

            def flush(pid: int):
                shards = staging.get(pid)
                if not shards:
                    return
                tbl = pa.concat_tables(shards) if len(shards) > 1 else shards[0]
                # ``tbl.nbytes`` is the decoded (pre-IPC, pre-compression) byte
                # count of this shard
                decoded_bytes_per_partition[pid] = (
                    decoded_bytes_per_partition.get(pid, 0) + tbl.nbytes
                )

                buf = _ipc_buffer(tbl, compression=compression)
                # Refuse frames the u32 response-wire encoding can't
                # represent (see top-of-file spec).
                if buf.size > _MAX_RANGE_BYTES:
                    raise RuntimeError(
                        f"map_{map_id}.shf partition {pid}: IPC frame is "
                        f"{buf.size} bytes, exceeding the u32 wire-protocol "
                        f"per-range limit ({_MAX_RANGE_BYTES}). Reduce "
                        f"``pool_budget_bytes`` or the upstream block size."
                    )
                off = f.tell()
                f.write(memoryview(buf))
                index.setdefault(pid, []).append((off, buf.size))
                staging[pid] = []
                staging_bytes[pid] = 0

            def pool_size() -> int:
                return sum(staging_bytes.values())

            for blk in blocks:
                # Accept any Ray Data Block (Arrow / pandas / ...) at the
                # boundary and normalize to ``pa.Table`` here. Downstream
                # (partition_fn, IPC serialize) is Arrow-only. No-op when
                # already Arrow.
                if not isinstance(blk, pa.Table):
                    blk = BlockAccessor.for_block(blk).to_arrow()
                if output_schema is None:
                    # First-seen schema; reducer uses it to type empty
                    # partitions (ShuffleHandle["schema"]).
                    output_schema = getattr(blk, "schema", None)
                for pid, shard in _partition_units(blk):
                    if not shard.num_rows:
                        continue
                    staging.setdefault(pid, []).append(shard)
                    staging_bytes[pid] = staging_bytes.get(pid, 0) + shard.nbytes
                    peak_inflight = max(peak_inflight, pool_size())
                    # Spill LARGEST bucket(s) on overflow to bound total
                    # staging to pool_budget_bytes.
                    while pool_size() >= pool_budget_bytes:
                        victim = max(staging_bytes, key=staging_bytes.get)
                        if staging_bytes[victim] == 0:
                            break
                        flush(victim)
            for pid in list(staging.keys()):
                flush(pid)

            # userspace --flush-→ page cache --fsync-→ disk, then sanity-check the file
            # size matches the index. Mismatch = logic bug or silent short
            # write; refuse to publish (the except below unlinks tmp).
            f.flush()
            final_size_on_close = f.tell()
            if fsync_on_close:
                os.fsync(f.fileno())
                # Drop the just-written pages so we don't hold GBs of
                # warm cache per mapper.
                _drop_pagecache(f.fileno(), 0, final_size_on_close)
            if index:
                expected_size = max(
                    off + length for ranges in index.values() for off, length in ranges
                )
            else:
                expected_size = 0
            if final_size_on_close != expected_size:
                raise RuntimeError(
                    f"external_hash_shuffle_map_task: file size mismatch — wrote "
                    f"{final_size_on_close} bytes, index implies "
                    f"{expected_size}. Refusing to publish corrupt file."
                )

        # Atomic publish: .tmp → .shf.
        os.rename(tmp_path, final_path)
    except Exception:
        # Don't leak a half-written .tmp in out_dir; Ray retries the task.
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return {
        "path": os.path.realpath(final_path),
        "index": index,
        # ShuffleManager identity: reducers rebuild the actor name from
        # (shuffle_id, node_id) and call ``_lookup_manager`` when they need
        # the handle.
        "shuffle_id": shuffle_id,
        "node_id": node_id,
        "token": token,
        "num_partitions": num_partitions,
        "peak_inflight_bytes": peak_inflight,
        # Total bytes written to the output file, post-seal.
        "total_bytes": final_size_on_close,
        "compression": compression,
        "decoded_bytes": decoded_bytes_per_partition,
        "schema": output_schema,
    }


@ray.remote
def external_hash_shuffle_reduce_task(
    handles: List[ShuffleHandle],
    partition_id: int,
    reduce_fn: ReduceFn,
    prefetch_dir: Optional[str] = None,
    max_bytes_per_fetch: int = _DEFAULT_MAX_BYTES_PER_FETCH,
    fetch_threads: int = _DEFAULT_FETCH_THREADS,
    target_max_block_size: Optional[int] = None,
    map_transformer: Optional[Any] = None,
    map_task_context: Optional[Any] = None,
) -> Generator[Union[Block, bytes], None, None]:
    """Fetch one partition's shards and stream ``reduce_fn`` output as
    ``(block, pickled metadata)`` pairs. Shuffle bytes stay out of the
    Ray object store — they flow directly from the socket into the
    reducer's user-space accumulator.

    Fetch + decode are pipelined: one thread per ShuffleManager pwrites
    response frames into a shared ``prefetch.bin`` at pre-assigned offsets,
    and this generator mmap-decodes each region as its future completes.

    The reducer always runs in blocking mode — accumulate the partition,
    reduce once, then finalize. Repartition needs "one partition = one
    block", so incremental flushing would be dead code. Output is reshaped
    to ``target_max_block_size`` via ``BlockOutputBuffer`` (a no-op
    passthrough when ``target_max_block_size`` is None).

    ``map_transformer`` runs a fused downstream map (typically Write)
    inline on each emitted block before yielding.
    """
    start_time_s = time.perf_counter()

    # Pull per-partition source refs + an output schema for the empty-partition
    # fallback path (so the N-block contract still emits a typed 0-row block
    # when no mapper produced any data for this partition_id).
    sources, output_schema = _handles_to_sources(handles, partition_id)

    def _yield_with_stats(block: Block):
        """Yield ``block`` then its pickled metadata. The two-yield protocol
        lets the executor slot ``StreamingGeneratorStats`` in between for
        accurate ``block_ser_time_s``."""
        exec_stats_builder = BlockExecStats.builder()
        exec_stats_builder.finish()
        gen_stats: StreamingGeneratorStats = yield block
        exec_stats = exec_stats_builder.build(
            block_ser_time_s=(gen_stats.object_creation_dur_s if gen_stats else None),
        )
        yield pickle.dumps(
            BlockMetadataWithSchema.from_block(
                block,
                block_exec_stats=exec_stats,
                task_exec_stats=TaskExecWorkerStats(
                    task_wall_time_s=time.perf_counter() - start_time_s,
                ),
            )
        )

    def _emit(block: Block):
        if map_transformer is None:
            yield from _yield_with_stats(block)
            return
        for out_block in map_transformer.apply_transform(
            iter([block]), map_task_context
        ):
            yield from _yield_with_stats(out_block)

    # Empty-input shortcut: no shards for this partition, hand [] to
    # reduce_fn and emit whatever it yields (may be nothing). Wrap in a
    # 1-element list to match reduce_fn's tables_by_input signature — we
    # are single-input (external mirrors ShuffleMap→ShuffleReduce, no
    # multi-input joins today).
    if not sources:
        for block in reduce_fn(partition_id, [[]]):
            yield from _emit(block)
        return

    # Decide where the prefetch file lives, and whether we own the cleanup.
    owns_dir = prefetch_dir is None
    if owns_dir:
        prefetch_dir = tempfile.mkdtemp(prefix=f"ray_shuffle_p{partition_id}_")
    else:
        os.makedirs(prefetch_dir, exist_ok=True)
    assert prefetch_dir is not None
    staging_dir: str = prefetch_dir
    prefetch_file = os.path.join(staging_dir, "prefetch.bin")

    groups = _group_by_manager(sources)

    try:
        # Fetch each source region in parallel, pwrite at pre-assigned offsets
        # into one prefetch.bin (disjoint → lock-free). Buffered pwrite lands
        # in page cache so the decode-side mmap reads hit cache.
        total_size, base_offsets, node_sizes = _compute_prefetch_layout(groups)

        # Accumulator for the final reduce.
        accum_tables: List[pa.Table] = []
        accum_bytes: int = 0
        output_buffer: Optional[BlockOutputBuffer] = None

        def _flush(tables: List[pa.Table]):
            """Call reduce_fn on ``tables`` and yield reshaped output."""
            nonlocal output_buffer
            if output_buffer is None and target_max_block_size is not None:
                output_buffer = BlockOutputBuffer(
                    OutputBlockSizeOption.of(
                        target_max_block_size=target_max_block_size,
                    )
                )
            # Wrap in a 1-element list — external is single-input, but
            # reduce_fn's signature is ``(partition_id, tables_by_input)``.
            for block in reduce_fn(partition_id, [tables]):
                if output_buffer is None:
                    # target_max_block_size=None: emit blocks as-is.
                    yield from _emit(block)
                else:
                    output_buffer.add_block(block)
                    while output_buffer.has_next():
                        yield from _emit(output_buffer.next())

        # O_RDWR: same fd serves ``os.pwrite`` from fetch threads AND
        # ``os.pread`` from decode. Sticking with plain file I/O (no
        # ``pa.memory_map``) keeps the per-region ``fdatasync +
        # posix_fadvise DONTNEED`` below straightforward.
        fd = os.open(prefetch_file, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            if total_size > 0:
                try:
                    os.posix_fallocate(fd, 0, total_size)  # Linux only
                except AttributeError:
                    # posix_fallocate not on this platform (macOS /
                    # Windows / minimal Python). Fall back to sparse
                    # ftruncate; pwrite allocates blocks on demand and
                    # will surface a real ENOSPC if the disk fills.
                    os.ftruncate(fd, total_size)
                except OSError as e:
                    if _is_disk_exhausted(e):
                        raise ShuffleDiskError(
                            f"Disk exhausted pre-allocating {total_size} "
                            f"bytes for prefetch.bin: {e}"
                        ) from e
                    raise

            def _fetch_one(args):
                base, size, group = args
                _prefetch_node_into(
                    _PwriteSink(fd, base),
                    group.shuffle_id,
                    group.node_id,
                    group.token,
                    group.members,
                    max_bytes_per_fetch,
                )
                return base, size

            n_threads = min(len(groups), max(1, fetch_threads))
            work = list(zip(base_offsets, node_sizes, groups))
            # Rotate submission order by partition_id to spread simultaneous
            # fan-in across all managers (avoids every reducer hitting the same
            # first N managers when n_threads < #managers).
            if work:
                _rot = partition_id % len(work)
                work = work[_rot:] + work[:_rot]

            def _decode_region(base: int, size: int):
                """Walk frames in [base, base+size), accumulate for the
                final reduce, then fdatasync + fadvise DONTNEED so page
                cache stays bounded by the currently-decoding region +
                the accumulator."""
                nonlocal accum_bytes
                pos = base
                end = base + size
                while pos < end:
                    length = struct.unpack(">I", os.pread(fd, 4, pos))[0]
                    ipc_buf = os.pread(fd, length, pos + 4)
                    pos += 4 + length
                    table = _read_ipc(ipc_buf)
                    accum_tables.append(table)
                    accum_bytes += table.nbytes
                # fdatasync turns this fd's dirty pwrite'd pages clean,
                # so the DONTNEED that follows actually evicts them.
                os.fdatasync(fd)
                _drop_pagecache(fd, base, size)

            with ThreadPoolExecutor(max_workers=n_threads) as ex:
                futs = [ex.submit(_fetch_one, w) for w in work]
                for fut in as_completed(futs):
                    base, size = fut.result()
                    if size > 0:
                        _decode_region(base, size)

            # Drain the accumulator tail.
            if accum_tables:
                yield from _flush(accum_tables)
                accum_tables = []
            if output_buffer is not None:
                output_buffer.finalize()
                while output_buffer.has_next():
                    yield from _emit(output_buffer.next())
        finally:
            os.close(fd)
    finally:
        # One file, one unlink. Idempotent on partial-failure paths.
        try:
            os.unlink(prefetch_file)
        except OSError:
            pass
        if owns_dir:
            try:
                os.rmdir(prefetch_dir)
            except OSError:
                pass
