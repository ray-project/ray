import os
import pickle
import random
import struct
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import pyarrow as pa

import ray
from ray._raylet import (
    StreamingGeneratorStats,  # pyrefly: ignore[missing-module-attribute]
)
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_runtime import (  # noqa: E402,E501
    _SHUFFLE_FILE_SERVER_NAMESPACE,
    ShuffleDiskError,
    ShuffleFileServer,
    ShuffleHandle,
    _build_range_index,
    _compute_prefetch_layout,
    _counts_to_array,
    _encode_shard,
    _fetch_from_file_server,
    _file_server_name,
    _group_by_server,
    _handles_to_sources,
    _is_disk_exhausted,
    _KeyRangeSources,
    _order_tables_by_source,
    _PartitionSources,
    _PartitionWriter,
    _PwriteSink,
    _read_ipc,
    disk_shuffle_dirs,
    handle_to_block,
)

# PartitionFn/ReduceFn/BlockTransformer contracts are shared with the
# object-store variant.
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (  # noqa: E402,E501
    BlockTransformer,
    PartitionFn,
    ReduceFn,
    TaskScopedPartitioner,
    resolve_partition_fn,
)
from ray.data._internal.output_buffer import (
    BlockOutputBuffer,
    OutputBlockSizeOption,
)
from ray.data._internal.table_block import TableBlockAccessor
from ray.data._internal.utils.transform_pyarrow import (
    _create_empty_table,
    _is_pa_extension_type,
)
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    BlockType,
    TaskExecWorkerStats,
)
from ray.data.context import DataContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if TYPE_CHECKING:
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

_DEFAULT_MAX_BYTES_PER_FETCH = 256 * 1024 * 1024  # 256 MiB per FETCH frame
# CAP on fetch connections per reducer: n_threads = min(#file-servers, this).
_DEFAULT_FETCH_THREADS = 16

# A sorted run is written in chunks so a reducer can fetch just the chunks
# that overlap its key range. The chunks a reducer fetches but only partly
# needs (at most two per run) are its over-fetch, so a run is cut into about
# this many chunks per partition, bounded by a floor (IPC and fetch overhead
# per chunk) and the target block size.
_SORT_RUN_CHUNKS_PER_PARTITION = 8
_SORT_RUN_MIN_CHUNK_BYTES = 64 * 1024


def _write_disk_shuffle_file(
    blocks: Iterable[Block],
    *,
    partition_fn: Union[PartitionFn, TaskScopedPartitioner],
    num_partitions: int,
    out_dir: str,
    map_id: int,
    shuffle_id: str,
    compression: Optional[str],
    block_transformer: Optional[BlockTransformer],
) -> ShuffleHandle:
    """Partition ``blocks`` and seal them into ``out_dir/map_{map_id}.shf``,
    returning the ``ShuffleHandle`` reducers fetch from (on-disk path +
    ``index_ranges`` + per-partition ``num_rows`` / ``decoded_bytes`` +
    ``shuffle_id`` / ``node_id``). Shuffle bytes never enter the Ray object
    store.

    The output file is sealed via atomic ``rename``: writes land in
    ``map_{i}.shf.tmp``, then flush + size sanity check against the index,
    then ``os.rename`` to the published path. Readers see either no file or
    a complete, size-validated one.

    Args:
        blocks: Input blocks to partition.
        partition_fn: Hash partitioner returning ``Dict[partition_id, Table]``.
        num_partitions: Total downstream partitions M.
        out_dir: Directory to write ``map_{i}.shf`` into.
        map_id: This map task's index.
        shuffle_id: Unique per-shuffle id; part of the file server's actor name.
        compression: Arrow IPC codec name (e.g. "lz4", "zstd") or None.
        block_transformer: Optional transform applied to the concatenation of
            all input blocks before partitioning (e.g. map-side partial
            aggregation). May change the block schema.

    Returns:
        ShuffleHandle describing the on-disk shards for reducers to fetch.
    """
    if block_transformer is not None:
        # The transformer (map-side combiner) needs the whole input at once.
        blocks = list(blocks)
        arrow_inputs = [
            TableBlockAccessor.try_convert_block_type(block, block_type=BlockType.ARROW)
            for block in blocks
            if BlockAccessor.for_block(block).num_rows() > 0
        ] or [
            # Run on an empty typed block so a global aggregate over empty
            # input still produces its identity row.
            TableBlockAccessor.try_convert_block_type(block, block_type=BlockType.ARROW)
            for block in blocks[:1]
        ]
        if arrow_inputs:
            combined_input = transform_pyarrow.concat(arrow_inputs, promote_types=True)
            blocks = (block_transformer(combined_input),)
    node_id = _ensure_file_server(shuffle_id, out_dir)
    output_schema: Optional[pa.Schema] = None
    writer: Optional[_PartitionWriter] = None

    def _write(out_file) -> int:
        nonlocal output_schema, writer
        writer = _PartitionWriter(out_file, map_id, compression)
        bound_partition_fn = resolve_partition_fn(partition_fn, map_id)
        for block in blocks:
            block = _to_arrow(block)
            if output_schema is None:
                # First-seen schema; reducer uses it to type empty
                # partitions (ShuffleHandle["schema"]).
                output_schema = getattr(block, "schema", None)
            if block.num_rows == 0:
                continue
            for partition_id, shard in bound_partition_fn(block).items():
                writer.add_shard(partition_id, shard)
        writer.flush_all()
        if not writer.index:
            return 0
        return max(
            off + length for ranges in writer.index.values() for off, length in ranges
        )

    final_path, final_size_on_close = _publish_shuffle_file(out_dir, map_id, _write)
    assert writer is not None

    _idx_ranges = _build_range_index(writer.index, num_partitions)
    return {
        "path": final_path,
        # Dense per-partition range index (see _build_range_index): row ``p`` is
        # ``(offset, length)`` of partition ``p``'s frame.
        "index_ranges": _idx_ranges,
        # ShuffleFileServer identity: reducers rebuild the actor name from
        # (shuffle_id, node_id) and call ``ray.get_actor`` when they need
        # the handle.
        "shuffle_id": shuffle_id,
        "node_id": node_id,
        "map_id": map_id,
        "num_partitions": num_partitions,
        # Total bytes written to the output file, post-seal.
        "total_bytes": final_size_on_close,
        "compression": compression,
        # Dense [num_partitions] row counts; empty-gating uses this, not nbytes.
        "num_rows": _counts_to_array(writer.rows_per_partition, num_partitions),
        # Dense [num_partitions] decoded tbl.nbytes; reduce memory estimates.
        "decoded_bytes": _counts_to_array(
            writer.decoded_bytes_per_partition, num_partitions
        ),
        "schema": output_schema,
    }


def _to_arrow(block: Block) -> pa.Table:
    """Accept any Ray Data Block (Arrow / pandas / ...) at the boundary and
    normalize to ``pa.Table``; downstream (partitioning, sorting, IPC
    serialize) is Arrow-only. No-op when already Arrow."""
    if isinstance(block, pa.Table):
        return block
    return BlockAccessor.for_block(block).to_arrow()


def _ensure_file_server(shuffle_id: str, out_dir: str) -> str:
    """Create this node's file-server actor if it does not exist yet
    (``get_if_exists=True`` reuses it across mappers on the node) and return
    the node id. The handle is not kept: reducers look the server up by name
    via ``ray.get_actor``."""
    node_id = ray.get_runtime_context().get_node_id()
    ShuffleFileServer.options(  # pyrefly: ignore[missing-attribute]
        name=_file_server_name(shuffle_id, node_id),
        namespace=_SHUFFLE_FILE_SERVER_NAMESPACE,
        get_if_exists=True,
        lifetime="detached",
        max_restarts=-1,
        scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
        num_cpus=0,
    ).remote(out_dir)
    return node_id


def _publish_shuffle_file(
    out_dir: str, map_id: int, write: Callable[[Any], int]
) -> Tuple[str, int]:
    """Run ``write`` against ``out_dir/map_{map_id}.shf.tmp`` and seal the file
    via atomic ``rename``: flush, sanity-check the size against the byte
    count ``write`` returns from its index (a mismatch is a logic bug or a
    silent short write, so the file is not published), then rename to the
    final path. Readers see either no file or a complete, size-validated one.
    Returns the real final path and the sealed size."""
    os.makedirs(out_dir, exist_ok=True)
    final_path = os.path.join(out_dir, f"map_{map_id}.shf")
    tmp_path = final_path + ".tmp"
    try:
        with open(tmp_path, "wb") as out_file:
            expected_size = write(out_file)
            # Flush userspace to page cache before measuring.
            out_file.flush()
            final_size_on_close = out_file.tell()
            assert final_size_on_close == expected_size, (
                f"map_{map_id}.shf: file size mismatch, wrote "
                f"{final_size_on_close} bytes, index implies {expected_size}"
            )
        os.rename(tmp_path, final_path)
    except Exception:
        # Don't leak a half-written .tmp in out_dir; Ray retries the task.
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    return os.path.realpath(final_path), final_size_on_close


def _sort_run_chunk_rows(
    num_rows: int, nbytes: int, num_partitions: int, max_chunk_bytes: Optional[int]
) -> int:
    """Rows per chunk of a sorted run (see ``_SORT_RUN_CHUNKS_PER_PARTITION``)."""
    if num_rows == 0 or nbytes == 0:
        return max(1, num_rows)
    chunk_bytes = nbytes / (num_partitions * _SORT_RUN_CHUNKS_PER_PARTITION)
    chunk_bytes = max(chunk_bytes, _SORT_RUN_MIN_CHUNK_BYTES)
    if max_chunk_bytes is not None:
        chunk_bytes = min(chunk_bytes, max_chunk_bytes)
    return max(1, int(chunk_bytes * num_rows / nbytes))


def _write_disk_sort_run(
    blocks: Iterable[Block],
    *,
    sort_key: "SortKey",
    num_partitions: int,
    sample_rows: int,
    max_chunk_bytes: Optional[int],
    out_dir: str,
    map_id: int,
    shuffle_id: str,
    compression: Optional[str],
) -> ShuffleHandle:
    """Sort ``blocks`` by ``sort_key`` and seal the sorted run into
    ``out_dir/map_{map_id}.shf`` as consecutive IPC chunks, returning the
    ``ShuffleHandle`` reducers fetch from.

    Unlike ``_write_disk_shuffle_file`` the run is not partitioned: the range
    boundaries are not known when the task runs. The handle instead carries a
    chunk index with each chunk's first key, so a reducer can later fetch the
    chunks overlapping its key range (``_KeyRangeSources``), and a key sample
    (every k-th key of the run, ``sample_rows`` of them at most) from which
    the regroup computes the boundaries.

    Args:
        blocks: Input blocks; sorted together as one run.
        sort_key: Sort columns and directions.
        num_partitions: Number of reducers, which sizes the chunks.
        sample_rows: Upper bound on the number of sampled keys.
        max_chunk_bytes: Upper bound on a chunk's decoded size, or None for
            no bound.
        out_dir: Directory to write ``map_{i}.shf`` into.
        map_id: This map task's index.
        shuffle_id: Unique per-shuffle id; part of the file server's actor name.
        compression: Arrow IPC codec name (e.g. "lz4", "zstd") or None.

    Returns:
        ShuffleHandle describing the on-disk run for reducers to fetch.
    """
    node_id = _ensure_file_server(shuffle_id, out_dir)
    output_schema: Optional[pa.Schema] = None
    tables: List[pa.Table] = []
    for block in blocks:
        block = _to_arrow(block)
        if output_schema is None:
            output_schema = getattr(block, "schema", None)
        if block.num_rows > 0:
            tables.append(block)

    key_columns = sort_key.get_columns()
    if tables:
        # Promote compatible schemas (e.g. null vs int64 column) as the
        # partitioned writer does; plain ``pa.concat_tables`` would reject them.
        run = (
            transform_pyarrow.concat(tables, promote_types=True)
            if len(tables) > 1
            else tables[0]
        )
        del tables
        run = BlockAccessor.for_block(run).sort(sort_key)
        output_schema = run.schema
        keys = run.select(key_columns)
    else:
        run = None
        # An empty input may have no columns at all (validate_schema allows it).
        keys = (
            _create_empty_table(output_schema).select(key_columns)
            if output_schema is not None
            and set(key_columns) <= set(output_schema.names)
            else pa.table({})
        )

    chunk_index: List[Tuple[int, int]] = []
    chunk_starts: List[int] = []
    chunk_num_rows: List[int] = []
    chunk_decoded_bytes: List[int] = []

    def _write(out_file) -> int:
        if run is None:
            return 0
        rows_per_chunk = _sort_run_chunk_rows(
            run.num_rows, run.nbytes, num_partitions, max_chunk_bytes
        )
        # Once per run: native combine is safe iff no column is an extension type.
        combine_native_ok = not any(_is_pa_extension_type(f.type) for f in run.schema)
        for start in range(0, run.num_rows, rows_per_chunk):
            chunk = run.slice(start, rows_per_chunk)
            buf = _encode_shard(chunk, compression, combine_native_ok)
            off = out_file.tell()
            out_file.write(memoryview(buf))
            chunk_index.append((off, buf.size))
            chunk_starts.append(start)
            chunk_num_rows.append(chunk.num_rows)
            chunk_decoded_bytes.append(chunk.nbytes)
        return out_file.tell()

    final_path, final_size_on_close = _publish_shuffle_file(out_dir, map_id, _write)

    num_rows = run.num_rows if run is not None else 0
    if num_rows > 0:
        step = max(1, -(-num_rows // max(1, sample_rows)))
        key_sample = transform_pyarrow.take_table(keys, np.arange(0, num_rows, step))
        chunk_first_keys = transform_pyarrow.take_table(keys, np.asarray(chunk_starts))
    else:
        key_sample = keys
        chunk_first_keys = keys
    return {
        "path": final_path,
        "shuffle_id": shuffle_id,
        "node_id": node_id,
        "map_id": map_id,
        "num_partitions": num_partitions,
        "total_bytes": final_size_on_close,
        "compression": compression,
        "schema": output_schema,
        "chunk_index": np.asarray(chunk_index, dtype=np.int64).reshape(-1, 2),
        "chunk_first_keys": chunk_first_keys,
        "chunk_num_rows": np.asarray(chunk_num_rows, dtype=np.int64),
        "chunk_decoded_bytes": np.asarray(chunk_decoded_bytes, dtype=np.int64),
        "key_sample": key_sample,
        "run_num_rows": num_rows,
        "run_decoded_bytes": run.nbytes if run is not None else 0,
    }


class DiskShuffleWriteFn:
    """Block transform that closes a disk-shuffle map task: partition the
    task's blocks, seal them into one ``map_{task_idx}.shf`` file, and yield
    the handle block (``handle_to_block``).
    """

    def __init__(
        self,
        *,
        partition_fn: Union[PartitionFn, TaskScopedPartitioner],
        num_partitions: int,
        out_dir: str,
        shuffle_id: str,
        compression: Optional[str],
        block_transformer: Optional[BlockTransformer],
    ):
        self._partition_fn = partition_fn
        self._num_partitions = num_partitions
        self._out_dir = out_dir
        self._shuffle_id = shuffle_id
        # Raw ``DataContext.shuffle_compression``, as the object-store
        # ShuffleMapOp passes it. Normalization (casing and the "none"
        # sentinel) lives in ``_codec_for``, which both the map (encode) and
        # reduce (decode) sides go through, so they can't disagree on the codec.
        self._compression = compression
        self._block_transformer = block_transformer

    def __call__(self, blocks: Iterable[Block], ctx: "TaskContext") -> Iterator[Block]:
        handle = _write_disk_shuffle_file(
            blocks,
            partition_fn=self._partition_fn,
            num_partitions=self._num_partitions,
            out_dir=self._out_dir,
            map_id=ctx.task_idx,
            shuffle_id=self._shuffle_id,
            compression=self._compression,
            block_transformer=self._block_transformer,
        )
        yield handle_to_block(handle)


class DiskSortRunWriteFn:
    """Block transform that closes a disk sort map task: sort the task's
    blocks, seal the run into one ``map_{task_idx}.shf`` file in chunks, and
    yield the handle block (``handle_to_block``). See ``_write_disk_sort_run``.
    """

    def __init__(
        self,
        *,
        sort_key: "SortKey",
        num_partitions: int,
        sample_rows: int,
        max_chunk_bytes: Optional[int],
        out_dir: str,
        shuffle_id: str,
        compression: Optional[str],
    ):
        self._sort_key = sort_key
        self._num_partitions = num_partitions
        self._sample_rows = sample_rows
        self._max_chunk_bytes = max_chunk_bytes
        self._out_dir = out_dir
        self._shuffle_id = shuffle_id
        self._compression = compression

    def __call__(self, blocks: Iterable[Block], ctx: "TaskContext") -> Iterator[Block]:
        handle = _write_disk_sort_run(
            blocks,
            sort_key=self._sort_key,
            num_partitions=self._num_partitions,
            sample_rows=self._sample_rows,
            max_chunk_bytes=self._max_chunk_bytes,
            out_dir=self._out_dir,
            map_id=ctx.task_idx,
            shuffle_id=self._shuffle_id,
            compression=self._compression,
        )
        yield handle_to_block(handle)


@ray.remote  # pyrefly: ignore[no-matching-overload]
def _disk_shuffle_reduce_task(
    *handles_by_input: List["ray.ObjectRef"],
    partition_id: int,
    source_selector: Union[_PartitionSources, _KeyRangeSources],
    reduce_fn: ReduceFn,
    max_bytes_per_fetch: int,
    fetch_threads: int,
    target_max_block_size: Optional[int],
    map_transformer: Optional[Any],
    map_task_context: Optional["TaskContext"],
    data_context: Optional["DataContext"],
    emit_empty_partition: bool = True,
) -> Generator[Union[Block, bytes], None, None]:
    """Reduce stage: fetch this partition's shards from every mapper over Arrow
    Flight, run ``reduce_fn`` on the accumulated tables, and yield ``(block,
    pickled metadata)`` pairs. Shuffle bytes stay out of the Ray object store:
    Flight streams into a per-partition local staging file, then this task
    decodes from that file.

    Fetch + decode are pipelined: one thread per ShuffleFileServer pwrites
    response frames into this partition's ``reduce_p{partition_id}.bin`` at
    pre-assigned offsets, and this generator ``pread``-decodes each region as
    its future completes.

    Args:
        *handles_by_input: One handle list per upstream input (one for
            repartition/sort/aggregate, two for join), each holding one
            ``ShuffleHandle`` per mapper. ``tables_by_input`` passed to
            ``reduce_fn`` is aligned with this order.
        partition_id: Partition this reducer owns.
        source_selector: Picks the byte ranges of this partition out of each
            mapper's file and trims the decoded tables to it (a no-op for
            hash-partitioned files).
        reduce_fn: User-supplied reduce callable.
        max_bytes_per_fetch: Per-FETCH-frame payload cap.
        fetch_threads: Concurrent per-file-server fetch threads.
        target_max_block_size: Output block size cap. None emits as-is.
        map_transformer: Fused downstream map applied to the full reduce
            output stream (or None).
        map_task_context: TaskContext for the fused map, or None.
        data_context: DataContext to install for the fused map, or None.
        emit_empty_partition: If True (default), a single-input partition with
            no shards yields one schema-typed empty block. Aggregations pass
            False (the mappers' pre-finalize schema would conflict with
            finalized partitions). Multi-input reduces always run reduce_fn.
    """
    start_time_s = time.perf_counter()

    num_inputs = len(handles_by_input)
    sources_by_input: List[List[Any]] = []
    schemas_by_input: List[Optional[pa.Schema]] = []
    for handles in handles_by_input:
        sources, schema = _handles_to_sources(handles, source_selector)
        sources_by_input.append(sources)
        schemas_by_input.append(schema)
    output_schema: Optional[pa.Schema] = next(
        (s for s in schemas_by_input if s is not None), None
    )

    def _with_typed_empty_inputs(
        tables_by_input: List[List[pa.Table]],
    ) -> List[List[pa.Table]]:
        if num_inputs == 1:
            return tables_by_input
        out: List[List[pa.Table]] = []
        for tables, schema in zip(tables_by_input, schemas_by_input):
            if not tables and schema is not None:
                tables = [_create_empty_table(schema)]
            out.append(tables)
        return out

    exec_stats_builder = BlockExecStats.builder()

    def _yield_with_stats(block: Block):
        """Yield ``block`` then its pickled metadata. The two-yield protocol
        lets the executor slot ``StreamingGeneratorStats`` in between for
        accurate ``block_ser_time_s``."""
        nonlocal exec_stats_builder
        exec_stats_builder.finish()
        gen_stats: StreamingGeneratorStats = yield block
        exec_stats = exec_stats_builder.build(
            block_ser_time_s=(gen_stats.object_creation_dur_s if gen_stats else None),
            task_idx=partition_id,
        )
        exec_stats_builder = BlockExecStats.builder()
        yield pickle.dumps(
            BlockMetadataWithSchema.from_block(
                block,
                block_exec_stats=exec_stats,
                task_exec_stats=TaskExecWorkerStats(
                    task_wall_time_s=time.perf_counter() - start_time_s,
                ),
            )
        )

    def _emit_blocks(blocks):
        """Apply fused map (if any) over the full reduce stream, then yield."""
        if map_transformer is None:
            for block in blocks:
                yield from _yield_with_stats(block)
            return
        assert map_task_context is not None and data_context is not None
        with DataContext.current(data_context), TaskContext.current(map_task_context):
            from ray.data._internal.execution.operators.map_transformer import (
                TransformClock,
            )

            map_transformer.override_target_max_block_size(
                map_task_context.target_max_block_size_override
            )
            for out_block in map_transformer.apply_transform(
                blocks, map_task_context, clock=TransformClock()
            ):
                yield from _yield_with_stats(out_block)

    if num_inputs == 1:

        def _no_input_blocks():
            if not emit_empty_partition:
                return
            assert output_schema is not None
            yield _create_empty_table(output_schema)

    else:

        def _no_input_blocks():
            yield from reduce_fn(
                partition_id,
                _with_typed_empty_inputs([[] for _ in range(num_inputs)]),
            )

    if not any(sources_by_input):
        yield from _emit_blocks(_no_input_blocks())
    else:
        shuffle_id = next(s for srcs in sources_by_input for s in srcs).shuffle_id
        _, staging_dir = disk_shuffle_dirs(shuffle_id)
        os.makedirs(staging_dir, exist_ok=True)
        prefetch_file = os.path.join(staging_dir, f"reduce_p{partition_id}.bin")

        groups_by_input = [_group_by_server(srcs) for srcs in sources_by_input]
        flat_groups = [group for groups in groups_by_input for group in groups]

        try:
            # Fetch each source region in parallel, pwrite at pre-assigned offsets
            # into this partition's staging file (disjoint → lock-free). Buffered
            # pwrite lands in page cache so the decode-side ``pread``s hit cache.
            total_size, base_offsets, node_sizes = _compute_prefetch_layout(flat_groups)

            # One slot per fetch group, holding one table list per member, so
            # regions decode as they land and _order_tables_by_source restores
            # map task order afterwards.
            tables_by_group: List[List[List[pa.Table]]] = [[] for _ in flat_groups]
            output_buffer: Optional[BlockOutputBuffer] = None
            # Codec from data_context.shuffle_compression (same field the map used).
            _compression = (
                data_context if data_context is not None else DataContext.get_current()
            ).shuffle_compression

            def _flush(tables_by_input: List[List[pa.Table]]):
                """Call reduce_fn on ``tables_by_input`` and yield reshaped raw blocks.

                Fused map / stats are applied downstream of ``_reduce_output_blocks``.
                """
                nonlocal output_buffer
                if output_buffer is None and target_max_block_size is not None:
                    output_buffer = BlockOutputBuffer(
                        OutputBlockSizeOption.of(
                            target_max_block_size=target_max_block_size,
                        )
                    )
                tables_by_input = _with_typed_empty_inputs(tables_by_input)
                for block in reduce_fn(partition_id, tables_by_input):
                    if output_buffer is None:
                        # target_max_block_size=None: emit blocks as-is.
                        yield block
                    else:
                        output_buffer.add_block(block)
                        while output_buffer.has_next():
                            yield output_buffer.next()

            def _reduce_output_blocks():
                # O_RDWR: same fd serves ``os.pwrite`` from fetch threads AND
                # ``os.pread`` from decode. Plain file I/O (no ``pa.memory_map``).
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
                                    f"bytes for {prefetch_file}: {e}"
                                ) from e
                            raise

                    n_threads = min(len(flat_groups), max(1, fetch_threads))
                    work = list(
                        zip(
                            range(len(flat_groups)),
                            base_offsets,
                            node_sizes,
                            flat_groups,
                        )
                    )
                    # Randomize submission order per reducer (seeded by partition_id →
                    # deterministic/retry-stable) so concurrent fan-in spreads evenly
                    # across file servers. Disjoint offsets make reordering safe.
                    if work:
                        random.Random(partition_id).shuffle(work)

                    def _decode_region(
                        base: int, size: int, group, accum: List[List[pa.Table]]
                    ):
                        """Decode one staging-file region's IPC frames, one
                        member (mapper file) at a time, into ``accum``."""
                        pos = base
                        for member in group.members:
                            tables: List[pa.Table] = []
                            for _ in member.ranges:
                                length = struct.unpack(">Q", os.pread(fd, 8, pos))[0]
                                ipc_buf = os.pread(fd, length, pos + 8)
                                pos += 8 + length
                                if length > 0:
                                    tables.append(_read_ipc(ipc_buf, _compression))
                            accum.append(source_selector.trim(tables))
                        assert pos == base + size, (pos, base, size)

                    with ThreadPoolExecutor(max_workers=n_threads) as ex:
                        futs = {
                            ex.submit(
                                _fetch_from_file_server,
                                _PwriteSink(fd, base),
                                group.shuffle_id,
                                group.node_id,
                                group.members,
                                max_bytes_per_fetch,
                            ): (base, size, group_idx)
                            for group_idx, base, size, group in work
                        }
                        for fut in as_completed(futs):
                            fut.result()
                            base, size, group_idx = futs[fut]
                            if size > 0:
                                _decode_region(
                                    base,
                                    size,
                                    flat_groups[group_idx],
                                    tables_by_group[group_idx],
                                )

                    accum_tables_by_input: List[List[pa.Table]] = []
                    start = 0
                    for groups in groups_by_input:
                        end = start + len(groups)
                        accum_tables_by_input.append(
                            _order_tables_by_source(groups, tables_by_group[start:end])
                        )
                        start = end
                    # Fetched chunks may trim to nothing (a key range between
                    # two runs' keys), which is an empty partition like one
                    # with no sources.
                    if any(accum_tables_by_input):
                        yield from _flush(accum_tables_by_input)
                    else:
                        yield from _no_input_blocks()
                    if output_buffer is not None:
                        output_buffer.finalize()
                        yield from output_buffer.iter_ready_blocks()
                finally:
                    os.close(fd)

            yield from _emit_blocks(_reduce_output_blocks())
        finally:
            # Unlink this reducer's own file.
            try:
                os.unlink(prefetch_file)
            except OSError:
                pass
