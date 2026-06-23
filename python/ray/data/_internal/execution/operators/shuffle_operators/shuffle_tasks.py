"""Shared remote tasks + helpers for ShuffleMapOp / ShuffleReduceOp."""

import time
from dataclasses import replace
from typing import Callable, Dict, Generator, Iterable, List, Optional, Tuple, Union

import pyarrow as pa

import ray
from ray import ObjectRef
from ray.data._internal.execution.util import yield_block_with_stats
from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
    BlockMetadataWithSchema,
    BlockType,
    TaskExecWorkerStats,
)

PartitionFn = Callable[[pa.Table], Dict[int, pa.Table]]
ReduceFn = Callable[[int, List[pa.Table]], Iterable[Block]]

# Peak working-set of a shuffle map/reduce task is ~2x the input bytes
SHUFFLE_PEAK_MEMORY_MULTIPLIER = 2


def _ipc_write_options(compression: Optional[str]) -> pa.ipc.IpcWriteOptions:
    """Arrow IPC write options for the given shard compression codec.

    Args:
        compression: A pyarrow codec name such as "lz4" or "zstd", or "none"
            (or None) to write shards uncompressed. See pyarrow.Codec for the
            full list of supported codecs:
            https://arrow.apache.org/docs/python/generated/pyarrow.Codec.html

    Returns:
        IpcWriteOptions for encoding shards; no compression for "none"/None.
    """
    if not compression or compression == "none":
        return pa.ipc.IpcWriteOptions()
    return pa.ipc.IpcWriteOptions(compression=pa.Codec(compression))


def _partition_blocks_to_shards(
    blocks: Tuple[Block, ...], partition_fn: PartitionFn
) -> Dict[int, List[pa.Table]]:
    """Partition each block independently, grouping shards by partition id."""
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
        for partition_id, shard in block_partitions.items():
            if shard.num_rows > 0:
                partition_accumulators.setdefault(partition_id, []).append(shard)
        del block, block_partitions
    return partition_accumulators


def _encode_partition_ipc(
    table: pa.Table,
    ipc_write_options: pa.ipc.IpcWriteOptions,
) -> pa.Buffer:
    """Encode one partition's shard as a single Arrow IPC stream."""
    if table.num_columns > 0:
        table = table.combine_chunks()

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema, options=ipc_write_options) as writer:
        for batch in table.to_batches():
            writer.write_batch(batch)
    return sink.getvalue()


@ray.remote  # pyrefly: ignore[no-matching-overload]
def _shuffle_map_task(
    *blocks: Block,
    partition_fn: PartitionFn,
    num_partitions: int,
    compression: Optional[str],
) -> Tuple[
    Union[Tuple[BlockMetadata, Dict[int, Tuple[int, int]], "pa.Schema"], pa.Buffer],
    ...,
]:
    """Map stage: partition the input blocks and return one shard per partition."""
    stats = BlockExecStats.builder()

    # Use BlockAccessor so we also work for non-Arrow blocks (pandas, numpy)
    accessors = [BlockAccessor.for_block(b) for b in blocks]
    total_rows = sum(a.num_rows() for a in accessors)
    total_bytes = sum((a.size_bytes() or 0) for a in accessors)

    ipc_write_options = _ipc_write_options(compression)
    output_schema = TableBlockAccessor.try_convert_block_type(
        blocks[0], block_type=BlockType.ARROW
    ).schema
    empty_shard = _encode_partition_ipc(output_schema.empty_table(), ipc_write_options)

    partition_accumulators = (
        {} if total_rows == 0 else _partition_blocks_to_shards(blocks, partition_fn)
    )

    shard_sizes: Dict[int, Tuple[int, int]] = {}
    partition_bufs: List[pa.Buffer] = []
    for partition_id in range(num_partitions):
        tables = partition_accumulators.pop(partition_id, None)
        if not tables:
            partition_bufs.append(empty_shard)
            continue
        merged = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        shard_sizes[partition_id] = (merged.num_rows, merged.nbytes)
        partition_bufs.append(_encode_partition_ipc(merged, ipc_write_options))
        del merged

    input_meta = BlockAccessor.for_block(blocks[0]).get_metadata(
        block_exec_stats=stats.build(block_ser_time_s=0),
    )
    input_meta = replace(input_meta, num_rows=total_rows, size_bytes=total_bytes)
    return (input_meta, shard_sizes, output_schema), *partition_bufs


def _read_partition_ipc(buf: pa.Buffer) -> Optional[pa.Table]:
    """Decompress one partition shard."""
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
    return pa.Table.from_batches(batches, schema=schema)


@ray.remote(max_calls=1)
def _shuffle_reduce_task(
    shard_refs: List[ObjectRef],
    partition_id: int,
    reduce_fn: ReduceFn,
    target_max_block_size: Optional[int],
    streaming: bool,
    batch_size: int,
) -> Generator[Union[Block, bytes], None, None]:
    """Reduce stage: fetch one partition's shards and run reduce_fn over them.

    With streaming=True, reduce_fn is called each time the accumulated input
    passes target_max_block_size and its output is reshaped to that size via a
    BlockOutputBuffer; this bounds peak input memory but requires reduce_fn to
    produce valid output from partial input.  With streaming=False, all shards
    are accumulated and reduce_fn is called once, use this when it needs the
    whole partition (sort, aggregate).

    Args:
        shard_refs: ObjectRefs to this partition's IPC shards from every mapper.
            May contain None for mappers that produced no rows here.
        partition_id: Partition this reducer owns.
        reduce_fn: User-supplied reduce callable.
        target_max_block_size: Output block size, and the streaming flush
            threshold.  None emits blocks as-is (no reshaping, no streaming
            flush) -- the "partition = block" contract.
        streaming: Flush incrementally (True) or accumulate then reduce (False).
        batch_size: Number of shard refs to ray.get() at a time.
    """
    start_time_s = time.perf_counter()

    accum_tables: List[pa.Table] = []
    accum_bytes: int = 0
    output_buffer: Optional[BlockOutputBuffer] = None

    def _yield_with_stats(block: Block):
        """Yield a block then its pickled metadata (streaming-gen protocol)."""

        def build_metadata(block_ser_time_s):
            exec_stats = BlockExecStats.builder()
            exec_stats.finish()
            return BlockMetadataWithSchema.from_block(
                block,
                block_exec_stats=exec_stats.build(block_ser_time_s=block_ser_time_s),
                task_exec_stats=TaskExecWorkerStats(
                    task_wall_time_s=time.perf_counter() - start_time_s,
                ),
            )

        yield from yield_block_with_stats(block, build_metadata)

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
    for batch_start in range(0, len(shard_refs), batch_size):
        batch = shard_refs[batch_start : batch_start + batch_size]
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
