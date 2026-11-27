import os
import pickle
import random
import secrets
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    Iterator,
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
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_runtime import (  # noqa: E402,E501
    ShuffleDiskError,
    ShuffleHandle,
    _build_range_index,
    _compute_prefetch_layout,
    _counts_to_array,
    _decode_records,
    _fetch_from_file_server,
    _group_by_server,
    _is_disk_exhausted,
    _order_by_map_id,
    _partition_input,
    _PartitionWriter,
    _PwriteSink,
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
    cap_polars_threads,
    resolve_partition_fn,
)
from ray.data._internal.output_buffer import (
    BlockOutputBuffer,
    OutputBlockSizeOption,
)
from ray.data._internal.table_block import TableBlockAccessor
from ray.data._internal.utils.transform_pyarrow import _create_empty_table
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    BlockType,
    TaskExecWorkerStats,
)
from ray.data.context import DataContext

# CAP on fetch connections per reducer: n_threads = min(#file-servers, this).
_DEFAULT_FETCH_THREADS = 16


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
    """Partition ``blocks``, append each partition's IPC frame to this node's
    file for it (``out_dir/node_p{p}.shf``) and return the ``ShuffleHandle``
    reducers fetch from. Shuffle bytes never enter the object store.

    - Every map task on a node appends to the same per-partition files, in
      one ``O_APPEND`` ``writev`` per record (``_append_record``), so there
      is no coordinator and a reducer fetches one file per node.
    - Records carry the map id and a per-attempt token. A failed attempt's
      records stay in the files with a token the regroup never commits;
      reducers skip them.
    - Files are removed with the shuffle directory in
      ``teardown_disk_shuffle``.

    Args:
        blocks: Input blocks to partition.
        partition_fn: Hash partitioner returning ``Dict[partition_id, Table]``.
        num_partitions: Total downstream partitions M.
        out_dir: This node's directory for the shuffle's ``node_p{p}.shf`` files.
        map_id: This map task's index.
        shuffle_id: Unique per-shuffle id; part of the file server's actor name.
        compression: Arrow IPC codec name (e.g. "lz4", "zstd") or None.
        block_transformer: Optional transform applied to the concatenation of
            all input blocks before partitioning (e.g. map-side partial
            aggregation). May change the block schema.

    Returns:
        ShuffleHandle describing the on-disk records for reducers to fetch.
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
    node_id = ray.get_runtime_context().get_node_id()

    os.makedirs(out_dir, exist_ok=True)
    prefix = os.path.join(out_dir, "node")
    # Tells this attempt's records from those a dead earlier attempt of the
    # same map task left in the files.
    attempt_token = secrets.randbits(64)
    writer = _PartitionWriter(prefix, map_id, attempt_token, compression)
    partition_fn = resolve_partition_fn(partition_fn, map_id)
    output_schema: Optional[pa.Schema] = None
    for block in blocks:
        # Accept any Ray Data Block (Arrow / pandas / ...) at the boundary and
        # normalize to ``pa.Table`` here. Downstream (partition_fn, IPC
        # serialize) is Arrow-only. No-op when already Arrow.
        if not isinstance(block, pa.Table):
            block = BlockAccessor.for_block(block).to_arrow()
        if output_schema is None:
            # First-seen schema; reducer uses it to type empty partitions
            # (ShuffleHandle["schema"]).
            output_schema = getattr(block, "schema", None)
        if block.num_rows == 0:
            continue
        for partition_id, shard in partition_fn(block).items():
            writer.add_shard(partition_id, shard)
    writer.flush_all()

    return {
        "path": prefix,
        # Dense per-partition range index (see _build_range_index): row ``p`` is
        # ``(offset, length)`` of this task's record in ``node_p{p}.shf``.
        "index_ranges": _build_range_index(writer.index, num_partitions),
        "shuffle_id": shuffle_id,
        "node_id": node_id,
        "map_id": map_id,
        "attempt_token": attempt_token,
        "num_partitions": num_partitions,
        # Total bytes this task appended across its partition files.
        "total_bytes": sum(
            length for ranges in writer.index.values() for _, length in ranges
        ),
        "compression": compression,
        # Dense [num_partitions] row counts; empty-gating uses this, not nbytes.
        "num_rows": _counts_to_array(writer.rows_per_partition, num_partitions),
        # Dense [num_partitions] decoded tbl.nbytes; reduce memory estimates.
        "decoded_bytes": _counts_to_array(
            writer.decoded_bytes_per_partition, num_partitions
        ),
        "schema": output_schema,
    }


class DiskShuffleWriteFn:
    """Block transform that closes a disk-shuffle map task: partition the
    task's blocks, append them to the node's per-partition ``node_p{p}.shf``
    files, and yield the handle block (``handle_to_block``).
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
        # Set in the task, not through a runtime env, which would make the
        # writer unfusable with the upstream read or map.
        cap_polars_threads()
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


@ray.remote  # pyrefly: ignore[no-matching-overload]
def _disk_shuffle_reduce_task(
    *entries_by_input: Dict[str, Any],
    partition_id: int,
    reduce_fn: ReduceFn,
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
        *entries_by_input: One regroup entry per upstream input (one for
            repartition/sort/aggregate, two for join): the input's shared
            node table plus this partition's per-node file sizes (see
            ``_partition_input``). ``tables_by_input`` passed to
            ``reduce_fn`` is aligned with this order.
        partition_id: Partition this reducer owns.
        reduce_fn: User-supplied reduce callable.
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

    num_inputs = len(entries_by_input)
    inputs = [_partition_input(entry, partition_id) for entry in entries_by_input]
    sources_by_input: List[List[Any]] = [inp.sources for inp in inputs]
    schemas_by_input: List[Optional[pa.Schema]] = [inp.schema for inp in inputs]
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

    if not any(sources_by_input):
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

        yield from _emit_blocks(_no_input_blocks())
    else:
        shuffle_id = next(s for srcs in sources_by_input for s in srcs).shuffle_id
        _, staging_dir = disk_shuffle_dirs(shuffle_id)
        os.makedirs(staging_dir, exist_ok=True)
        prefetch_file = os.path.join(staging_dir, f"reduce_p{partition_id}.bin")

        groups_by_input = [_group_by_server(srcs) for srcs in sources_by_input]
        flat_groups = [group for groups in groups_by_input for group in groups]
        # Committed attempt token per map id, per fetch group's input.
        committed_attempts_by_group = [
            inp.committed_attempts
            for inp, groups in zip(inputs, groups_by_input)
            for _ in groups
        ]

        try:
            # Fetch each source region in parallel, pwrite at pre-assigned offsets
            # into this partition's staging file (disjoint → lock-free). Buffered
            # pwrite lands in page cache so the decode-side ``pread``s hit cache.
            total_size, base_offsets, node_sizes = _compute_prefetch_layout(flat_groups)

            # One slot per fetch group so regions decode as they land;
            # _order_by_map_id restores map task order afterwards.
            records_by_group: List[List[Tuple[int, pa.Table]]] = [
                [] for _ in flat_groups
            ]
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
                            os.posix_fallocate(  # pyrefly: ignore[missing-attribute]
                                fd, 0, total_size
                            )  # Linux only
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

                    with ThreadPoolExecutor(max_workers=n_threads) as ex:
                        futs = {
                            ex.submit(
                                _fetch_from_file_server,
                                _PwriteSink(fd, base),
                                group.shuffle_id,
                                group.node_id,
                                group.members,
                            ): (base, size, group_idx)
                            for group_idx, base, size, group in work
                        }
                        for fut in as_completed(futs):
                            fut.result()
                            base, size, group_idx = futs[fut]
                            if size > 0:
                                records_by_group[group_idx] = _decode_records(
                                    fd,
                                    base,
                                    size,
                                    committed_attempts_by_group[group_idx],
                                    _compression,
                                )

                    accum_tables_by_input: List[List[pa.Table]] = []
                    start = 0
                    for groups in groups_by_input:
                        end = start + len(groups)
                        accum_tables_by_input.append(
                            _order_by_map_id(records_by_group[start:end])
                        )
                        start = end
                    if any(accum_tables_by_input):
                        yield from _flush(accum_tables_by_input)
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
