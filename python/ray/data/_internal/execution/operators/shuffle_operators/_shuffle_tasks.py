"""Shared remote tasks + helpers for ShuffleMapOp / ShuffleReduceOp."""

import pickle
import time
from typing import Callable, Dict, Generator, Iterable, List, Optional, Tuple, Union

import pyarrow as pa

import ray
from ray import ObjectRef
from ray._raylet import StreamingGeneratorStats
from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    BlockType,
    TaskExecWorkerStats,
)

# Maps rows of a pa.Table block to output partition IDs.  num_partitions
# and any other config are bound via closure.  The operator filters out empty
# entries, so implementations don't need to.
PartitionFn = Callable[[pa.Table], Dict[int, pa.Table]]

# Takes (partition_id, accumulated_shard_tables) and yields output blocks.
# See ShuffleReduceOp for streaming vs. blocking call semantics.
ReduceFn = Callable[[int, List[pa.Table]], Iterable[Block]]


# Number of ObjectRefs fetched per ray.get() call in reducers.
_REDUCE_BATCH_SIZE = 16


# ---------------------------------------------------------------------------
# Map-side helpers
# ---------------------------------------------------------------------------


def _partition_blocks_to_shards(
    blocks: Tuple[Block, ...], partition_fn: PartitionFn
) -> Dict[int, List[pa.Table]]:
    """Run partition_fn on each block; collect non-empty shards by pid.

    Each block is partitioned independently so we never materialize a single
    concatenated table across all inputs.  Chunked columns are defragmented
    here (combine_chunks) before calling partition_fn — this is a real
    performance constraint, not optional: hash_partition's per-column take
    is sensitive to chunk count, and leaving chunked input alone halves map
    throughput.
    """
    partition_accumulators: Dict[int, List[pa.Table]] = {}
    for block in blocks:
        block = TableBlockAccessor.try_convert_block_type(
            block, block_type=BlockType.ARROW
        )
        if block.num_rows == 0:
            continue
        assert isinstance(block, pa.Table), f"Expected pa.Table, got {type(block)}"
        if any(col.num_chunks > 1 for col in block.columns):
            block = block.combine_chunks()
        block_partitions = partition_fn(block)
        for pid, shard in block_partitions.items():
            if shard.num_rows > 0:
                partition_accumulators.setdefault(pid, []).append(shard)
        del block, block_partitions
    return partition_accumulators


def _encode_partition_ipc(
    table: pa.Table,
    ipc_write_options: pa.ipc.IpcWriteOptions,
) -> pa.Buffer:
    """ZSTD-encode one partition's shard as a single Arrow IPC stream.

    ``combine_chunks`` usually collapses a Table to one batch, but the
    Arrow API doesn't strictly guarantee that — e.g. tables hitting the
    2 GB-per-batch offset limit may come out with multiple batches.  We
    write every batch so the reader can re-merge correctly.
    """
    if table.num_columns > 0:
        table = table.combine_chunks()
    batches = table.to_batches()
    if not batches:
        return pa.py_buffer(b"")

    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, batches[0].schema, options=ipc_write_options)
    for batch in batches:
        writer.write_batch(batch)
    writer.close()
    return sink.getvalue()


@ray.remote
def _shuffle_map_task(
    *blocks: Block,
    partition_fn: PartitionFn,
    num_partitions: int,
):
    """Map stage: partition input blocks and return one shard per partition.

    Multiple input blocks may be passed when pre-map merge is enabled; they
    are partitioned individually (to avoid a large combine_chunks on the
    merged table) and their shards are accumulated per pid before encoding.

    Must be called with ``num_returns = num_partitions + 1``.

    Args:
        *blocks: One or more input blocks.
        partition_fn: Callable (pa.Table) -> Dict[int, pa.Table] that
            assigns rows to output partitions.
        num_partitions: Total number of output partitions.

    Returns:
        Tuple of ``num_partitions + 1`` values:

        - Index 0: (BlockMetadata, Dict) — aggregate input metadata and a
          per-partition (rows, bytes) size dict (keys are the partitions
          that received any rows from this mapper).
        - Index 1 … N: a ZSTD-compressed Arrow IPC stream for that
          partition, or ``None`` if the partition received no rows.
    """
    from dataclasses import replace as _dc_replace

    stats = BlockExecStats.builder()

    if not blocks:
        empty_meta = BlockAccessor.for_block(pa.table({})).get_metadata(
            block_exec_stats=stats.build(block_ser_time_s=0)
        )
        return (empty_meta, {}), *([None] * num_partitions)

    # Use BlockAccessor so we work for non-Arrow block types (pandas, numpy)
    # whose num_rows / nbytes attributes aren't named the same.
    accessors = [BlockAccessor.for_block(b) for b in blocks]
    total_rows = sum(a.num_rows() for a in accessors)
    total_bytes = sum((a.size_bytes() or 0) for a in accessors)

    # Empty-input fast path.
    if total_rows == 0:
        input_meta = BlockAccessor.for_block(blocks[0]).get_metadata(
            block_exec_stats=stats.build(block_ser_time_s=0),
        )
        return (input_meta, {}), *([None] * num_partitions)

    # Step 1: partition each input block into (pid -> [shard_table, ...]).
    partition_accumulators = _partition_blocks_to_shards(blocks, partition_fn)

    # Step 2: merge per-pid shards and ZSTD-encode each partition.
    ipc_write_options = pa.ipc.IpcWriteOptions(compression=pa.Codec("zstd"))
    shard_sizes: Dict[int, Tuple[int, int]] = {}
    partition_bufs: List[Optional[pa.Buffer]] = [None] * num_partitions
    for pid in sorted(partition_accumulators.keys()):
        tables = partition_accumulators.pop(pid)
        merged = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        shard_sizes[pid] = (merged.num_rows, merged.nbytes)
        partition_bufs[pid] = _encode_partition_ipc(merged, ipc_write_options)
        del merged

    # Step 3: build aggregate input metadata and return.
    input_meta = BlockAccessor.for_block(blocks[0]).get_metadata(
        block_exec_stats=stats.build(block_ser_time_s=0),
    )
    input_meta = _dc_replace(input_meta, num_rows=total_rows, size_bytes=total_bytes)
    return (input_meta, shard_sizes), *partition_bufs


# ---------------------------------------------------------------------------
# Reduce-side helpers
# ---------------------------------------------------------------------------


def _read_partition_ipc(buf: pa.Buffer) -> Optional[pa.Table]:
    """Decompress one partition shard.  Returns None for empty buffers."""
    if len(buf) == 0:
        return None
    reader = pa.ipc.open_stream(buf)
    schema = reader.schema
    batches: List[pa.RecordBatch] = []
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        if batch.num_rows > 0:
            batches.append(batch)
    if not batches:
        return None
    return pa.Table.from_batches(batches, schema=schema)


@ray.remote(max_calls=1)
def _shuffle_reduce_task(
    shard_refs: List[ObjectRef],
    partition_id: int,
    reduce_fn: ReduceFn,
    target_max_block_size: Optional[int],
    streaming: bool = True,
) -> Generator[Union[Block, bytes], None, None]:
    """Reduce stage: fetch one partition's shards and call reduce_fn.

    Both modes use batched ray.get (_REDUCE_BATCH_SIZE refs at a time) to
    avoid memory spikes from a single large dereference.

    Streaming mode (streaming=True):
        Calls reduce_fn(partition_id, accumulated_tables) whenever the
        accumulator exceeds target_max_block_size, then pushes the produced
        blocks through a BlockOutputBuffer so each emitted block respects
        target_max_block_size.  Peak input accumulator bounded to
        ~target_max_block_size.  reduce_fn must produce valid output from
        partial data.

    Blocking mode (streaming=False):
        Accumulates all shards before calling reduce_fn(partition_id,
        all_tables) once.  Use this when reduce_fn requires the full
        partition (sort, aggregate).

    Args:
        shard_refs: ObjectRefs to this partition's compressed IPC shards
            from all mappers.  May include ``None`` entries for mappers
            that produced no rows for this partition.
        partition_id: Partition this reducer is responsible for.
        reduce_fn: User-supplied reduce callable.
        target_max_block_size: Target output block size (also the streaming
            flush threshold).  ``None`` means emit blocks as-is (no
            BlockOutputBuffer reshaping, no streaming flush) — used under
            the "partition = block" contract.
        streaming: Whether to flush incrementally (True) or accumulate all
            shards before reducing (False).

    Returns:
        Generator yielding output blocks, each followed by a serialized
        BlockMetadataWithSchema.  Emits one block per non-empty partition;
        partitions that received no rows produce no output.
    """
    start_time_s = time.perf_counter()

    accum_tables: List[pa.Table] = []
    accum_bytes: int = 0
    output_buffer: Optional[BlockOutputBuffer] = None  # lazily created

    def _yield_with_stats(block: Block):
        """Yield (block, pickled metadata) following the streaming-gen protocol."""
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

    def _flush(tables: List[pa.Table]):
        nonlocal output_buffer
        if output_buffer is None:
            output_buffer = BlockOutputBuffer(
                OutputBlockSizeOption.of(
                    target_max_block_size=target_max_block_size,
                )
            )
        for block in reduce_fn(partition_id, tables):
            output_buffer.add_block(block)
            while output_buffer.has_next():
                yield from _yield_with_stats(output_buffer.next())

    # Step 1: fetch shard refs in batches, decompress, accumulate.  In
    # streaming mode, when the accumulator reaches target_max_block_size,
    # flush through reduce_fn and yield any ready output blocks.
    for batch_start in range(0, len(shard_refs), _REDUCE_BATCH_SIZE):
        batch = shard_refs[batch_start : batch_start + _REDUCE_BATCH_SIZE]
        for buf in ray.get(batch):
            if buf is None:
                continue
            table = _read_partition_ipc(buf)
            if table is None:
                continue
            accum_tables.append(table)
            accum_bytes += table.nbytes

            if (
                streaming
                and target_max_block_size is not None
                and accum_bytes >= target_max_block_size
            ):
                tables, accum_tables = accum_tables, []
                accum_bytes = 0
                yield from _flush(tables)

    # Step 2: drain remaining shards through reduce_fn.  This is the only
    # reduce_fn call in blocking mode, and the tail-flush in streaming mode.
    if accum_tables:
        yield from _flush(accum_tables)

    # Step 3: if reduce_fn ran at least once, finalize the buffer to flush
    # any partial block.
    if output_buffer is not None:
        output_buffer.finalize()
        while output_buffer.has_next():
            yield from _yield_with_stats(output_buffer.next())
