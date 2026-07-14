"""Shared remote tasks + helpers for ShuffleMapOp / ShuffleReduceOp."""

import logging
import math
import time
import typing
from dataclasses import replace
from typing import Callable, Dict, Generator, Iterable, List, Optional, Tuple, Union

import pyarrow as pa

import ray
from ray import ObjectRef
from ray.data._internal.execution.interfaces.task_context import TaskContext
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
from ray.data.context import DataContext
from ray.exceptions import GetTimeoutError

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.operators.map_transformer import MapTransformer

logger = logging.getLogger(__name__)

PartitionFn = Callable[[pa.Table], Dict[int, pa.Table]]
ReduceFn = Callable[[int, List[List[pa.Table]]], Iterable[Block]]

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


# Warn once a shard fetch has stalled for this fraction of the fail timeout
_REDUCE_GET_WARN_AT_FRACTION = 1 / 3


def _get_shard_batch(
    batch: List[ObjectRef],
    partition_id: int,
    batch_index: int,
    num_batches: int,
    timeout_s: float,
) -> List[Optional[pa.Buffer]]:
    """``ray.get`` a batch of shard refs, warning then failing if the fetch stalls.

    Args:
        batch: Shard ObjectRefs to fetch (a slice of one partition's shards).
        partition_id: Partition this reducer owns (for logging).
        batch_index: 0-based index of this batch within the partition.
        num_batches: Total number of batches for the partition (for logging).
        timeout_s: ``ray.get`` timeout in seconds.  A non-positive value disables
            the timeout (single blocking fetch).

    Returns:
        The dereferenced shard buffers (some entries may be ``None``).

    Raises:
        GetTimeoutError: If the shards are not available within ``timeout_s``.
    """
    if timeout_s <= 0:
        return ray.get(batch)

    wait_start_s = time.perf_counter()
    warn_timeout_s = timeout_s * _REDUCE_GET_WARN_AT_FRACTION
    try:
        return ray.get(batch, timeout=warn_timeout_s)
    except GetTimeoutError:
        logger.warning(
            f"Shuffle reduce task for partition {partition_id} has waited "
            f"{time.perf_counter() - wait_start_s:.0f}s for {len(batch)} "
            f"shard(s) in batch {batch_index + 1}/{num_batches}."
        )

    try:
        return ray.get(batch, timeout=timeout_s - warn_timeout_s)
    except GetTimeoutError:
        logger.error(
            f"Shuffle reduce task for partition {partition_id} timed out after "
            f"{time.perf_counter() - wait_start_s:.0f}s waiting for {len(batch)} "
            f"shard(s) in batch {batch_index + 1}/{num_batches}."
        )
        raise


def _gather_input_shards(
    shard_refs: List[ObjectRef],
    partition_id: int,
    batch_size: int,
    get_timeout_s: float,
) -> List[pa.Table]:
    """Fetch + decompress every shard of one input for one partition."""
    tables: List[pa.Table] = []
    num_batches = math.ceil(len(shard_refs) / batch_size) if batch_size else 0
    for batch_index, batch_start in enumerate(range(0, len(shard_refs), batch_size)):
        batch = shard_refs[batch_start : batch_start + batch_size]
        for buf in _get_shard_batch(
            batch, partition_id, batch_index, num_batches, get_timeout_s
        ):
            if buf is None:
                continue
            table = _read_partition_ipc(buf)
            if table is None:
                continue
            tables.append(table)
    return tables


@ray.remote
def _shuffle_reduce_task(
    shard_refs_by_input: List[List[ObjectRef]],
    partition_id: int,
    reduce_fn: ReduceFn,
    target_max_block_size: Optional[int],
    batch_size: int,
    get_timeout_s: float,
    map_transformer: Optional["MapTransformer"],
    map_task_context: Optional["TaskContext"],
    data_context: Optional["DataContext"],
) -> Generator[Union[Block, bytes], None, None]:
    """Reduce stage: fetch this partition's shards and run reduce_fn over them.

    ``shard_refs_by_input`` carries one shard-ref list per upstream input -- one
    for single-input shuffles (repartition/sort), several for multi-input ones
    (e.g. join).  Every input's full shard list is accumulated, then reduce_fn is
    called once with ``tables_by_input`` aligned with ``shard_refs_by_input``.

    Args:
        shard_refs_by_input: Per-input lists of ObjectRefs to this partition's
            IPC shards from every mapper.  May contain None for empty shards.
        partition_id: Partition this reducer owns.
        reduce_fn: User-supplied reduce callable.
        target_max_block_size: Output block size.  None emits blocks as-is.
        batch_size: Number of shard refs to ray.get() at a time.
        get_timeout_s: Timeout for batch ray.get().
        map_transformer: Fused downstream map applied to reduce output (or None).
        map_task_context: TaskContext for the fused map, built by the reduce op
            -- carries task_idx, op_name, the block-size override, and per-task
            kwargs (e.g. a Write's ``write_uuid``); None when nothing is fused.
        data_context: DataContext to install for the fused map (or None).
    """
    start_time_s = time.perf_counter()

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

    def _flush(tables_by_input: List[List[pa.Table]]):
        nonlocal output_buffer
        if output_buffer is None:
            output_buffer = BlockOutputBuffer(
                OutputBlockSizeOption.of(
                    target_max_block_size=target_max_block_size,
                )
            )
        for block in reduce_fn(partition_id, tables_by_input):
            output_buffer.add_block(block)
            # Yield raw blocks: a fused map (and `_yield_with_stats`) is applied
            # downstream of ``_reduce_output_blocks``.
            yield from output_buffer.iter_ready_blocks()

    def _reduce_output_blocks():
        # Gather every input's full shard list, then call reduce_fn exactly once
        # with all inputs together (no streaming: a multi-input reducer needs
        # every input's shards, and single-input reducers run blocking too).
        tables_by_input = [
            _gather_input_shards(shard_refs, partition_id, batch_size, get_timeout_s)
            for shard_refs in shard_refs_by_input
        ]
        if any(tables_by_input):
            yield from _flush(tables_by_input)

        # Finalize the buffer to flush any partial block.
        if output_buffer is not None:
            output_buffer.finalize()
            yield from output_buffer.iter_ready_blocks()

    if map_transformer is None:
        for block in _reduce_output_blocks():
            yield from _yield_with_stats(block)
    else:
        assert map_task_context is not None and data_context is not None
        with DataContext.current(data_context), TaskContext.current(map_task_context):
            map_transformer.override_target_max_block_size(
                map_task_context.target_max_block_size_override
            )
            for block in map_transformer.apply_transform(
                _reduce_output_blocks(), map_task_context
            ):
                yield from _yield_with_stats(block)
