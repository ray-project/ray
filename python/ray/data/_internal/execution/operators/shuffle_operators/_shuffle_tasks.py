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


# Cap return objects from each mapper to bound object-store pressure.
_MAX_RETURN_GROUPS = 200

# Number of ObjectRefs fetched per ray.get() call in reducers.
_REDUCE_BATCH_SIZE = 16


def compute_shard_group_size(num_partitions: int) -> int:
    """Choose shard_group_size so each mapper returns ≤ _MAX_RETURN_GROUPS
    objects.  Caps the M×N intermediate object count regardless of partition
    count."""
    return max(1, (num_partitions + _MAX_RETURN_GROUPS - 1) // _MAX_RETURN_GROUPS)


def compute_num_groups(num_partitions: int, shard_group_size: int) -> int:
    return (num_partitions + shard_group_size - 1) // shard_group_size


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


def _encode_group_ipc(
    sorted_pids: List[int],
    pid_tables: Dict[int, pa.Table],
    ipc_write_options: pa.ipc.IpcWriteOptions,
) -> pa.Buffer:
    """ZSTD-encode one group as a single Arrow IPC stream.

    The schema metadata's __pids__ field is a list of [pid, batch_count]
    pairs, in the order batches appear in the stream.  combine_chunks()
    usually collapses a Table to one batch, but the Arrow API doesn't strictly
    guarantee that (e.g. tables hitting the 2 GB-per-batch offset limit can
    come out with multiple chunks).  Encoding the per-pid batch count lets the
    reducer re-merge correctly in those cases instead of silently dropping
    data or desyncing.
    """
    import json as _json

    pid_batches: List[Tuple[int, List[pa.RecordBatch]]] = []
    for pid in sorted_pids:
        table = pid_tables[pid]
        if table.num_columns > 0:
            table = table.combine_chunks()
        pid_batches.append((pid, table.to_batches()))

    # All-empty group: caller shouldn't pass us this, but be defensive.
    first_with_batches = next((bs for _, bs in pid_batches if bs), None)
    if first_with_batches is None:
        return pa.py_buffer(b"")

    pids_with_counts = [(pid, len(batches)) for pid, batches in pid_batches]
    # Copy: schema.metadata is shared across tables that share a schema, and
    # we don't want to mutate the upstream schema's metadata dict.
    schema_meta = dict(first_with_batches[0].schema.metadata or {})
    schema_meta[b"__pids__"] = _json.dumps(pids_with_counts).encode()
    tagged_schema = first_with_batches[0].schema.with_metadata(schema_meta)

    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, tagged_schema, options=ipc_write_options)
    for _, batches in pid_batches:
        for batch in batches:
            writer.write_batch(batch)
    writer.close()
    return sink.getvalue()


@ray.remote
def _shuffle_map_task(
    *blocks: Block,
    partition_fn: PartitionFn,
    num_partitions: int,
    shard_group_size: int = 1,
):
    """Map stage: partition one or more blocks and return grouped shards.

    Multiple input blocks may be passed when pre-map merge is enabled; they
    are partitioned individually (to avoid a large combine_chunks on the
    merged table) and their shards are accumulated before grouping.

    Must be called with num_returns = num_groups + 1 where
    num_groups = ceil(num_partitions / shard_group_size).

    Args:
        *blocks: One or more input blocks.
        partition_fn: Callable (pa.Table) -> Dict[int, pa.Table] that
            assigns rows to output partitions.
        num_partitions: Total number of output partitions.
        shard_group_size: Number of partitions packed into each return object.

    Returns:
        Tuple of num_groups + 1 values:

        - Index 0: (BlockMetadata, Dict) — aggregate input metadata and a
          per-partition (rows, bytes) size dict (keys are the partitions
          that received any rows from this mapper).
        - Index 1 … G: a ZSTD-compressed Arrow IPC stream for the partitions
          in that shard group, or None if the group is entirely empty.
    """
    from dataclasses import replace as _dc_replace

    stats = BlockExecStats.builder()
    num_groups = compute_num_groups(num_partitions, shard_group_size)

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
        return (input_meta, {}), *([None] * num_groups)

    # Step 1: partition each input block into (pid -> [shard_table, ...]).
    partition_accumulators = _partition_blocks_to_shards(blocks, partition_fn)

    # Step 2: bucket pids by their target group_idx.
    group_pid_keys: Dict[int, List[int]] = {}
    for pid in partition_accumulators:
        group_idx = pid // shard_group_size
        group_pid_keys.setdefault(group_idx, []).append(pid)

    # Step 3: merge per-pid shards and ZSTD-encode each group.
    ipc_write_options = pa.ipc.IpcWriteOptions(compression=pa.Codec("zstd"))
    shard_sizes: Dict[int, Tuple[int, int]] = {}
    groups: List[Optional[pa.Buffer]] = [None] * num_groups
    for group_idx, pid_keys in group_pid_keys.items():
        sorted_pids = sorted(pid_keys)
        pid_tables: Dict[int, pa.Table] = {}
        for pid in sorted_pids:
            tables = partition_accumulators.pop(pid)
            merged = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
            pid_tables[pid] = merged
            shard_sizes[pid] = (merged.num_rows, merged.nbytes)
        groups[group_idx] = _encode_group_ipc(
            sorted_pids, pid_tables, ipc_write_options
        )
        del pid_tables
    del group_pid_keys

    # Step 4: build aggregate input metadata and return.
    input_meta = BlockAccessor.for_block(blocks[0]).get_metadata(
        block_exec_stats=stats.build(block_ser_time_s=0),
    )
    input_meta = _dc_replace(input_meta, num_rows=total_rows, size_bytes=total_bytes)
    return (input_meta, shard_sizes), *groups


# ---------------------------------------------------------------------------
# Reduce-side helpers
# ---------------------------------------------------------------------------


def _read_ipc_group(group: pa.Buffer) -> List[Tuple[int, pa.Table]]:
    """Decompress one IPC group and return (partition_id, table) pairs.

    Reads __pids__ (a list of [pid, batch_count] pairs encoded by
    :func:`_encode_group_ipc`) to know how many batches to consume per pid,
    then re-merges them into one table per pid.  Strips __pids__ from
    the table schemas so downstream tables don't carry our routing tag.
    """
    import json as _json

    reader = pa.ipc.open_stream(group)
    pids_with_counts = _json.loads(reader.schema.metadata[b"__pids__"])
    meta = dict(reader.schema.metadata)
    meta.pop(b"__pids__", None)
    clean_schema = (
        reader.schema.with_metadata(meta) if meta else reader.schema.remove_metadata()
    )
    result = []
    for pid, batch_count in pids_with_counts:
        batches = []
        for _ in range(batch_count):
            batch = reader.read_next_batch()
            if batch.num_rows > 0:
                batches.append(batch)
        if batches:
            result.append((pid, pa.Table.from_batches(batches, schema=clean_schema)))
    return result


@ray.remote
def _shuffle_reduce_task(
    group_refs: List[ObjectRef],
    partition_ids: List[int],
    reduce_fn: ReduceFn,
    target_max_block_size: Optional[int],
    streaming: bool = True,
) -> Generator[Union[Block, bytes], None, None]:
    """Reduce stage: fetch shards and call reduce_fn for each partition.

    Both modes use batched ray.get (_REDUCE_BATCH_SIZE refs at a time) to
    avoid memory spikes from a single large dereference.

    Streaming mode (streaming=True):
        Calls reduce_fn(partition_id, accumulated_tables) whenever a
        partition's accumulator exceeds target_max_block_size, then pushes
        the produced blocks through a per-partition BlockOutputBuffer so each
        emitted block respects target_max_block_size.  Peak input accumulator
        per partition is bounded to ~target_max_block_size.  reduce_fn must
        produce valid output from partial data.

    Blocking mode (streaming=False):
        Accumulates all shards for every partition before calling
        reduce_fn(partition_id, all_tables) once per partition.  Use this
        when reduce_fn requires the full partition (sort, aggregate).

    Args:
        group_refs: ObjectRefs to compressed IPC groups from all mappers.
        partition_ids: Partition IDs this reducer is responsible for.
        reduce_fn: User-supplied reduce callable.
        target_max_block_size: Target output block size (also the streaming
            flush threshold).  None means emit blocks as-is (no
            BlockOutputBuffer reshaping, no streaming flush) — used under the
            "partition = block" contract.
        streaming: Whether to flush partitions incrementally (True) or
            accumulate all shards before reducing (False).

    Returns:
        Generator yielding output blocks, each followed by a serialized
        BlockMetadataWithSchema.
    """
    start_time_s = time.perf_counter()

    # Empty-input fast path.
    if not group_refs or not partition_ids:
        return

    partition_ids_set = set(partition_ids)
    accum_tables: Dict[int, List[pa.Table]] = {pid: [] for pid in partition_ids}
    accum_bytes: Dict[int, int] = {pid: 0 for pid in partition_ids}
    output_buffers: Dict[int, BlockOutputBuffer] = {}

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

    def _reduce_into_buffer(pid: int, tables: List[pa.Table]):
        """Call reduce_fn, push outputs through buffer, yield ready blocks.

        When target_max_block_size is None, OutputBlockSizeOption.of
        returns None, and the BlockOutputBuffer emits exactly one block per
        partition (no slicing / coalescing).
        """
        if pid not in output_buffers:
            output_buffers[pid] = BlockOutputBuffer(
                OutputBlockSizeOption.of(
                    target_max_block_size=target_max_block_size,
                )
            )
        buf = output_buffers[pid]
        for block in reduce_fn(pid, tables):
            buf.add_block(block)
            while buf.has_next():
                yield from _yield_with_stats(buf.next())

    # Step 1: fetch group refs in batches, decompress, route shards to their
    # partition accumulators.  In streaming mode, when a partition's
    # accumulator reaches target_max_block_size, flush it through reduce_fn
    # and yield any blocks the output buffer is ready to emit.
    for batch_start in range(0, len(group_refs), _REDUCE_BATCH_SIZE):
        batch = group_refs[batch_start : batch_start + _REDUCE_BATCH_SIZE]
        batch_groups = ray.get(batch)

        for group in batch_groups:
            if group is None:
                continue
            for pid, table in _read_ipc_group(group):
                if pid not in partition_ids_set:
                    continue
                accum_tables[pid].append(table)
                accum_bytes[pid] += table.nbytes

                if (
                    streaming
                    and target_max_block_size is not None
                    and accum_bytes[pid] >= target_max_block_size
                ):
                    tables = accum_tables[pid]
                    accum_tables[pid] = []
                    accum_bytes[pid] = 0
                    yield from _reduce_into_buffer(pid, tables)

        del batch_groups

    # Step 2: all input groups consumed.  Drain any remaining accumulated
    # shards through reduce_fn — this is the only reduce_fn call per
    # partition in blocking mode, and the tail-flush in streaming mode.
    for pid in sorted(partition_ids):
        if accum_tables[pid]:
            yield from _reduce_into_buffer(pid, accum_tables[pid])

    # Step 3: finalize each touched output buffer to flush any partial block
    # that hasn't reached target_max_block_size yet, then yield it.
    for pid in sorted(output_buffers):
        buf = output_buffers[pid]
        buf.finalize()
        while buf.has_next():
            yield from _yield_with_stats(buf.next())
