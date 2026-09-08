import os
import pickle
import random
import struct
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import (
    Any,
    Generator,
    List,
    Optional,
    Union,
)

import pyarrow as pa

import ray
from ray._raylet import (
    StreamingGeneratorStats,  # pyrefly: ignore[missing-module-attribute]
)
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.shuffle_operators.external_shuffle_runtime import (  # noqa: E402,E501
    _SHUFFLE_FILE_SERVER_NAMESPACE,
    ShuffleDiskError,
    ShuffleFileServer,
    ShuffleHandle,
    _build_range_index,
    _compute_prefetch_layout,
    _counts_to_array,
    _fetch_from_file_server,
    _file_server_name,
    _group_by_server,
    _handles_to_sources,
    _is_disk_exhausted,
    _PartitionWriter,
    _PwriteSink,
    _read_ipc,
)

# PartitionFn/ReduceFn/BlockTransformer contracts are shared with the
# object-store variant.
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (  # noqa: E402,E501
    BlockTransformer,
    PartitionFn,
    ReduceFn,
)
from ray.data._internal.output_buffer import (
    BlockOutputBuffer,
    OutputBlockSizeOption,
)
from ray.data._internal.table_block import TableBlockAccessor
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

_DEFAULT_MAX_BYTES_PER_FETCH = 256 * 1024 * 1024  # 256 MiB per FETCH frame
# CAP on fetch connections per reducer: n_threads = min(#file-servers, this).
_DEFAULT_FETCH_THREADS = 16


@ray.remote  # pyrefly: ignore[no-matching-overload]
def _external_shuffle_map_task(
    *blocks: Block,
    partition_fn: PartitionFn,
    num_partitions: int,
    out_dir: str,
    map_id: int,
    shuffle_id: str,
    compression: Optional[str] = None,
    block_transformer: Optional[BlockTransformer] = None,
) -> ShuffleHandle:
    """Map stage: partition input blocks, write them to a single file under
    ``out_dir``, and return a ``ShuffleHandle`` (on-disk path +
    ``index_ranges`` + per-partition ``num_rows`` / ``decoded_bytes`` +
    ``shuffle_id`` / ``node_id``). Shuffle bytes never enter the Ray object
    store.

    The output file is sealed via atomic ``rename``: writes land in
    ``map_{i}.shf.tmp``, then flush + size sanity check against the index,
    then ``os.rename`` to the published path. Readers see either no file or
    a complete, size-validated one.

    Args:
        *blocks: Input blocks to partition.
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
        arrow_inputs = [
            TableBlockAccessor.try_convert_block_type(block, block_type=BlockType.ARROW)
            for block in blocks
            if BlockAccessor.for_block(block).num_rows() > 0
        ] or [
            TableBlockAccessor.try_convert_block_type(
                blocks[0], block_type=BlockType.ARROW
            )
        ]
        combined_input = transform_pyarrow.concat(arrow_inputs, promote_types=True)
        blocks = (block_transformer(combined_input),)
    node_id = ray.get_runtime_context().get_node_id()
    # Ensure the file-server actor exists (get_if_exists=True → reuse across
    # mappers on the same node). We don't need to keep the handle: reducers
    # will look the file server up by name via ``ray.get_actor``.
    ShuffleFileServer.options(  # pyrefly: ignore[missing-attribute]
        name=_file_server_name(shuffle_id, node_id),
        namespace=_SHUFFLE_FILE_SERVER_NAMESPACE,
        get_if_exists=True,
        lifetime="detached",
        max_restarts=-1,
        scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
        num_cpus=0,
    ).remote(out_dir)

    os.makedirs(out_dir, exist_ok=True)
    final_path = os.path.join(out_dir, f"map_{map_id}.shf")
    # Write to a temp file first; only ``rename`` once we've verified the full file.
    tmp_path = final_path + ".tmp"
    output_schema: Optional[pa.Schema] = None

    final_size_on_close = -1
    try:
        with open(tmp_path, "wb") as out_file:
            writer = _PartitionWriter(out_file, map_id, compression)
            for block in blocks:
                # Accept any Ray Data Block (Arrow / pandas / ...) at the
                # boundary and normalize to ``pa.Table`` here. Downstream
                # (partition_fn, IPC serialize) is Arrow-only. No-op when
                # already Arrow.
                if not isinstance(block, pa.Table):
                    block = BlockAccessor.for_block(block).to_arrow()
                if output_schema is None:
                    # First-seen schema; reducer uses it to type empty
                    # partitions (ShuffleHandle["schema"]).
                    output_schema = getattr(block, "schema", None)
                if block.num_rows == 0:
                    continue
                for partition_id, shard in partition_fn(block).items():
                    writer.add_shard(partition_id, shard)
            writer.flush_all()

            # Flush userspace to page cache, then sanity-check the file size
            # matches the index. Mismatch = logic bug or silent short write;
            # refuse to publish (the except below unlinks tmp).
            out_file.flush()
            final_size_on_close = out_file.tell()
            if writer.index:
                expected_size = max(
                    off + length
                    for ranges in writer.index.values()
                    for off, length in ranges
                )
            else:
                expected_size = 0
            assert final_size_on_close == expected_size, (
                f"_external_shuffle_map_task: file size mismatch, wrote "
                f"{final_size_on_close} bytes, index implies {expected_size}"
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

    _idx_ranges = _build_range_index(writer.index, num_partitions)
    return {
        "path": os.path.realpath(final_path),
        # Dense per-partition range index (see _build_range_index): row ``p`` is
        # ``(offset, length)`` of partition ``p``'s frame.
        "index_ranges": _idx_ranges,
        # ShuffleFileServer identity: reducers rebuild the actor name from
        # (shuffle_id, node_id) and call ``ray.get_actor`` when they need
        # the handle.
        "shuffle_id": shuffle_id,
        "node_id": node_id,
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


@ray.remote  # pyrefly: ignore[no-matching-overload]
def _external_shuffle_reduce_task(
    *handles_by_input: List[ShuffleHandle],
    partition_id: int,
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
        sources, schema = _handles_to_sources(handles, partition_id)
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
                tables = [schema.empty_table()]
            out.append(tables)
        return out

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

    def _emit_blocks(blocks):
        """Apply fused map (if any) over the full reduce stream, then yield."""
        if map_transformer is None:
            for block in blocks:
                yield from _yield_with_stats(block)
            return
        assert map_task_context is not None and data_context is not None
        with DataContext.current(data_context), TaskContext.current(map_task_context):
            from ray.data._internal.execution.operators.map_transformer import (
                UDFTimeScope,
            )

            map_transformer.override_target_max_block_size(
                map_task_context.target_max_block_size_override
            )
            for out_block in map_transformer.apply_transform(
                blocks, map_task_context, udf_time_scope=UDFTimeScope()
            ):
                yield from _yield_with_stats(out_block)

    if not any(sources_by_input):
        if num_inputs == 1:

            def _no_input_blocks():
                if not emit_empty_partition:
                    return
                assert output_schema is not None
                yield output_schema.empty_table()

        else:

            def _no_input_blocks():
                yield from reduce_fn(
                    partition_id,
                    _with_typed_empty_inputs([[] for _ in range(num_inputs)]),
                )

        yield from _emit_blocks(_no_input_blocks())
    else:
        shuffle_id = next(s for srcs in sources_by_input for s in srcs).shuffle_id
        staging_dir = os.path.join(
            tempfile.gettempdir(), f"ray_shuffle_external_{shuffle_id}_reduce"
        )
        os.makedirs(staging_dir, exist_ok=True)
        prefetch_file = os.path.join(staging_dir, f"reduce_p{partition_id}.bin")

        groups_by_input = [_group_by_server(srcs) for srcs in sources_by_input]
        flat_groups = [group for groups in groups_by_input for group in groups]
        group_input_idx = [
            input_idx
            for input_idx, groups in enumerate(groups_by_input)
            for _ in groups
        ]

        try:
            # Fetch each source region in parallel, pwrite at pre-assigned offsets
            # into this partition's staging file (disjoint → lock-free). Buffered
            # pwrite lands in page cache so the decode-side ``pread``s hit cache.
            total_size, base_offsets, node_sizes = _compute_prefetch_layout(flat_groups)

            accum_tables_by_input: List[List[pa.Table]] = [
                [] for _ in range(num_inputs)
            ]
            output_buffer: Optional[BlockOutputBuffer] = None
            # Codec from data_context.hash_shuffle_compression (same field the map used).
            _compression = (
                data_context if data_context is not None else DataContext.get_current()
            ).hash_shuffle_compression

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
                        zip(base_offsets, node_sizes, flat_groups, group_input_idx)
                    )
                    # Randomize submission order per reducer (seeded by partition_id →
                    # deterministic/retry-stable) so concurrent fan-in spreads evenly
                    # across file servers. Disjoint offsets make reordering safe.
                    if work:
                        random.Random(partition_id).shuffle(work)

                    def _decode_region(base: int, size: int, accum: List[pa.Table]):
                        """Decode one staging-file region's IPC frames into ``accum_tables``."""
                        pos = base
                        end = base + size
                        while pos < end:
                            length = struct.unpack(">Q", os.pread(fd, 8, pos))[0]
                            ipc_buf = os.pread(fd, length, pos + 8)
                            pos += 8 + length
                            accum.append(_read_ipc(ipc_buf, _compression))

                    with ThreadPoolExecutor(max_workers=n_threads) as ex:
                        futs = {
                            ex.submit(
                                _fetch_from_file_server,
                                _PwriteSink(fd, base),
                                group.shuffle_id,
                                group.node_id,
                                group.members,
                                max_bytes_per_fetch,
                            ): (base, size, input_idx)
                            for base, size, group, input_idx in work
                        }
                        for fut in as_completed(futs):
                            fut.result()
                            base, size, input_idx = futs[fut]
                            if size > 0:
                                _decode_region(
                                    base, size, accum_tables_by_input[input_idx]
                                )

                    # Drain the accumulator tail.
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
