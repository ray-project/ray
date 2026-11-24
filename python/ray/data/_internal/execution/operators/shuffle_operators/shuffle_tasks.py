"""Shared remote tasks + helpers for ShuffleMapOp / ShuffleReduceOp."""

import abc
import logging
import math
import time
import typing
from dataclasses import replace
from typing import (
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import pyarrow as pa

import ray
from ray import ObjectRef
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.util import yield_block_with_stats
from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data._internal.table_block import TableBlockAccessor
from ray.data._internal.utils.transform_pyarrow import _create_empty_table
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

_SHUFFLE_SLICE_PREFIX = "__shuffle_slice__"
_SHUFFLE_BUNDLE_BYTES_PREFIX = "__shuffle_bundle_bytes__"

PartitionFn = Callable[[pa.Table], Dict[int, pa.Table]]
ReduceFn = Callable[[int, List[List[pa.Table]]], Iterable[Block]]
BlockTransformer = Callable[["pa.Table"], "pa.Table"]


class TaskScopedPartitioner(abc.ABC):
    """A partitioner whose partition function depends on the map task index.

    Map tasks call ``bind(task_index)`` once and partition every block with the
    returned function. Used where mappers must not share state, e.g. random
    shuffle seeds.
    """

    @abc.abstractmethod
    def bind(self, task_index: int) -> PartitionFn:
        ...


def format_shuffle_slice(offset: int, length: int) -> str:
    return f"{_SHUFFLE_SLICE_PREFIX}{int(offset)}:{int(length)}"


def parse_shuffle_slice(input_files: Optional[Sequence[str]]) -> Tuple[int, int]:
    """Byte range beside the partition sentinel. Missing means the whole object."""
    if not input_files:
        return 0, -1
    for item in input_files:
        if item.startswith(_SHUFFLE_SLICE_PREFIX):
            offset_s, length_s = item[len(_SHUFFLE_SLICE_PREFIX) :].split(":")
            return int(offset_s), int(length_s)
    return 0, -1


def format_bundle_nbytes(nbytes: int) -> str:
    return f"{_SHUFFLE_BUNDLE_BYTES_PREFIX}{int(nbytes)}"


def parse_bundle_nbytes(input_files: Optional[Sequence[str]]) -> Optional[int]:
    """Full size of the shared bundle, or None when this block is not packed."""
    if not input_files:
        return None
    for item in input_files:
        if item.startswith(_SHUFFLE_BUNDLE_BYTES_PREFIX):
            return int(item[len(_SHUFFLE_BUNDLE_BYTES_PREFIX) :])
    return None


def resolve_partition_fn(
    partition_fn: Union[PartitionFn, TaskScopedPartitioner], task_index: int
) -> PartitionFn:
    if isinstance(partition_fn, TaskScopedPartitioner):
        return partition_fn.bind(task_index)
    return partition_fn


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


# ``(bundle_index, offset, length)``. None means the partition is empty and is
# not stored in any packed bundle yet.
ShardPlacement = Tuple[int, int, int]


class ShuffleMapTaskResult(NamedTuple):
    """Single return value of a shuffle map task.

    The task ``ray.put``s every bundle before returning, so ``num_returns`` is 1.
    A streaming yield would let a retry re-run from the start and orphan objects
    the driver had already registered, and it would drop inputs too early.
    """

    input_meta: BlockMetadata
    output_schema: pa.Schema
    bundle_refs: Tuple["ObjectRef", ...]
    # Byte size of each bundle. A reducer downloads the whole bundle, not its slice.
    bundle_nbytes: Tuple[int, ...]
    # One entry per partition: bundle index, byte offset, byte length, rows, bytes.
    placements: Tuple[Tuple[int, int, int, int, int], ...]


def pack_partition_ipc(
    shards: Sequence[Optional[Union[bytes, pa.Buffer]]],
    *,
    min_bytes: int,
    max_partitions: int,
) -> Tuple[List[Union[bytes, pa.Buffer]], List[Optional[ShardPlacement]]]:
    """Pack IPC shards into bundles.

    A shard of at least ``min_bytes`` is its own bundle. At a 1 GiB map batch
    and 1024 partitions each shard is exactly 1 MiB, so with the default floor
    nothing is packed and the ref count stays mapper times partitions.

    Smaller shards share a bundle, at most ``max_partitions`` of them. The last
    bundle may be under ``min_bytes``. It is still its own object store value;
    only the small metadata return can be inlined onto the driver.
    Empty shards (``None``) are omitted; the caller points those partitions at
    one shared empty IPC buffer so the reducer still sees a schema.
    """
    floor = max(1, int(min_bytes))
    cap = max(1, int(max_partitions))
    bundles: List[Union[bytes, pa.Buffer]] = []
    placements: List[Optional[ShardPlacement]] = [None] * len(shards)
    pending = bytearray()
    pending_parts: List[Tuple[int, int, int]] = []

    def seal() -> None:
        if not pending_parts:
            return
        bundle_index = len(bundles)
        bundles.append(bytes(pending))
        for partition_id, offset, length in pending_parts:
            placements[partition_id] = (bundle_index, offset, length)
        pending.clear()
        pending_parts.clear()

    for partition_id, shard in enumerate(shards):
        if shard is None or len(shard) == 0:
            continue
        if len(shard) >= floor:
            seal()
            bundle_index = len(bundles)
            bundles.append(shard)
            placements[partition_id] = (bundle_index, 0, len(shard))
            continue
        if pending_parts and (len(pending) >= floor or len(pending_parts) >= cap):
            seal()
        offset = len(pending)
        pending.extend(shard)
        pending_parts.append((partition_id, offset, len(shard)))
        if len(pending) >= floor or len(pending_parts) >= cap:
            seal()
    seal()
    return bundles, placements


def assign_empty_placements(
    bundles: List[Union[bytes, pa.Buffer]],
    placements: List[Optional[ShardPlacement]],
    empty_ipc: Union[bytes, pa.Buffer],
) -> Tuple[List[Union[bytes, pa.Buffer]], List[ShardPlacement]]:
    """Point every empty partition at one shared schema-bearing IPC buffer."""
    if not any(placement is None for placement in placements):
        return bundles, [placement for placement in placements if placement is not None]
    empty_index = len(bundles)
    bundles = list(bundles)
    bundles.append(empty_ipc)
    empty_len = len(empty_ipc)
    filled = [
        (empty_index, 0, empty_len) if placement is None else placement
        for placement in placements
    ]
    return bundles, filled


@ray.remote  # pyrefly: ignore[no-matching-overload]
def _shuffle_map_task(
    *blocks: Block,
    partition_fn: PartitionFn,
    num_partitions: int,
    compression: Optional[str],
    block_transformer: Optional[BlockTransformer] = None,
    min_shard_bytes: int = 1024 * 1024,
    max_partitions_per_bundle: int = 8,
) -> ShuffleMapTaskResult:
    """Map stage: partition the input and return one object of bundle refs."""
    stats = BlockExecStats.builder()

    # Use BlockAccessor so we also work for non-Arrow blocks (pandas, numpy)
    accessors = [BlockAccessor.for_block(b) for b in blocks]
    total_rows = sum(a.num_rows() for a in accessors)
    total_bytes = sum((a.size_bytes() or 0) for a in accessors)

    if block_transformer is not None:
        arrow_inputs = [
            TableBlockAccessor.try_convert_block_type(b, block_type=BlockType.ARROW)
            for a, b in zip(accessors, blocks)
            if a.num_rows() > 0
        ] or [
            TableBlockAccessor.try_convert_block_type(
                blocks[0], block_type=BlockType.ARROW
            )
        ]
        combined_input = transform_pyarrow.concat(arrow_inputs, promote_types=True)
        shard_source_blocks: Tuple[Block, ...] = (block_transformer(combined_input),)
        partition_accumulators = _partition_blocks_to_shards(
            shard_source_blocks, partition_fn
        )
    else:
        shard_source_blocks = blocks
        partition_accumulators = (
            {}
            if total_rows == 0
            else _partition_blocks_to_shards(shard_source_blocks, partition_fn)
        )

    ipc_write_options = _ipc_write_options(compression)
    output_schema = TableBlockAccessor.try_convert_block_type(
        shard_source_blocks[0], block_type=BlockType.ARROW
    ).schema
    empty_shard = _encode_partition_ipc(
        _create_empty_table(output_schema), ipc_write_options
    )

    encoded: List[Optional[pa.Buffer]] = []
    rows_and_bytes: List[Tuple[int, int]] = []
    for partition_id in range(num_partitions):
        tables = partition_accumulators.pop(partition_id, None)
        if not tables:
            encoded.append(None)
            rows_and_bytes.append((0, 0))
            continue
        merged = (
            transform_pyarrow.concat(
                tables,
                promote_types=True,
                preserve_order=True,
            )
            if len(tables) > 1
            else tables[0]
        )
        rows_and_bytes.append((merged.num_rows, merged.nbytes))
        encoded.append(_encode_partition_ipc(merged, ipc_write_options))
        del merged

    bundles, placements = pack_partition_ipc(
        encoded,
        min_bytes=min_shard_bytes,
        max_partitions=max_partitions_per_bundle,
    )
    # Every partition, including an empty one, must reach the reducer with a
    # schema. One shared empty IPC per map task is enough; its ref is not
    # owned by each partition (see ShuffleMapOp).
    bundles, placements = assign_empty_placements(bundles, placements, empty_shard)

    # Put every bundle before returning. A failure before this return leaves
    # the driver with nothing registered and the inputs still alive.
    #
    # These objects are owned by the map worker. After the task returns they
    # fate-share with that worker: if it exits, reducers see OwnerDiedError
    # and Ray will not rebuild the bundles. Returning the bytes instead would
    # make the driver the owner, and would also pull every bundle through the
    # driver. A streaming generator would let a retry orphan objects the
    # driver had already seen. Node loss of a finished map worker fails this
    # shuffle, the same way losing a disk shuffle file server does.
    bundle_refs = tuple(ray.put(bundle) for bundle in bundles)
    bundle_nbytes = tuple(len(bundle) for bundle in bundles)
    placed = tuple(
        (
            placement[0],
            placement[1],
            placement[2],
            rows_and_bytes[partition_id][0],
            rows_and_bytes[partition_id][1],
        )
        for partition_id, placement in enumerate(placements)
    )

    input_meta = BlockAccessor.for_block(blocks[0]).get_metadata(
        block_exec_stats=stats.build(block_ser_time_s=0),
    )
    input_meta = replace(input_meta, num_rows=total_rows, size_bytes=total_bytes)
    return ShuffleMapTaskResult(
        input_meta=input_meta,
        output_schema=output_schema,
        bundle_refs=bundle_refs,
        bundle_nbytes=bundle_nbytes,
        placements=placed,
    )


def _as_ipc_buffer(buf: Union[bytes, bytearray, pa.Buffer]) -> pa.Buffer:
    if isinstance(buf, pa.Buffer):
        return buf
    return pa.py_buffer(buf)


def _slice_ipc(
    buf: Union[bytes, bytearray, pa.Buffer], offset: int, length: int
) -> pa.Buffer:
    """Return one partition's IPC bytes. ``length < 0`` means the whole buffer."""
    raw = _as_ipc_buffer(buf)
    if length < 0:
        return raw
    return raw.slice(offset, length)


def _read_partition_ipc(buf: Union[bytes, bytearray, pa.Buffer]) -> Optional[pa.Table]:
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


# One shard inside a packed bundle: the object, then the IPC byte range.
# ``length < 0`` means the object is entirely this partition.
ShardSlice = Tuple[ObjectRef, int, int]


def _gather_input_shards(
    shards: List[ShardSlice],
    partition_id: int,
    batch_size: int,
    get_timeout_s: float,
) -> List[pa.Table]:
    """Fetch each distinct bundle once, then decode this partition's slices."""
    if not shards:
        return []

    unique_refs: List[ObjectRef] = []
    ref_index: Dict[ObjectRef, int] = {}
    order: List[Tuple[int, int, int]] = []
    for ref, offset, length in shards:
        index = ref_index.get(ref)
        if index is None:
            index = len(unique_refs)
            ref_index[ref] = index
            unique_refs.append(ref)
        order.append((index, offset, length))

    slices_at: Dict[int, List[Tuple[int, int, int]]] = {}
    for position, (index, offset, length) in enumerate(order):
        slices_at.setdefault(index, []).append((position, offset, length))

    decoded: List[Optional[pa.Table]] = [None] * len(order)
    step = batch_size if batch_size and batch_size > 0 else len(unique_refs)
    num_batches = math.ceil(len(unique_refs) / step) if step else 0
    for batch_index, batch_start in enumerate(range(0, len(unique_refs), step)):
        batch = unique_refs[batch_start : batch_start + step]
        fetched = _get_shard_batch(
            batch, partition_id, batch_index, num_batches, get_timeout_s
        )
        for local_index, buf in enumerate(fetched):
            if buf is None:
                continue
            for position, offset, length in slices_at[batch_start + local_index]:
                decoded[position] = _read_partition_ipc(_slice_ipc(buf, offset, length))
        # Drop the compressed bundles before fetching the next batch.
        del fetched

    return [table for table in decoded if table is not None]


@ray.remote
def _shuffle_reduce_task(
    shard_refs_by_input: List[List[ShardSlice]],
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
        shard_refs_by_input: Per-input lists of ``(ref, offset, length)`` slices.
            Several partitions may share one ref; each reducer reads only its range.
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

    exec_stats_builder = BlockExecStats.builder()

    def _yield_with_stats(block: Block):
        """Yield a block then its pickled metadata (streaming-gen protocol)."""
        exec_stats_builder.finish()

        def build_metadata(block_ser_time_s):
            nonlocal exec_stats_builder
            meta = BlockMetadataWithSchema.from_block(
                block,
                block_exec_stats=exec_stats_builder.build(
                    block_ser_time_s=block_ser_time_s,
                    task_idx=partition_id,
                ),
                task_exec_stats=TaskExecWorkerStats(
                    task_wall_time_s=time.perf_counter() - start_time_s,
                ),
            )
            exec_stats_builder = BlockExecStats.builder()
            return meta

        yield from yield_block_with_stats(block, build_metadata)

    def _flush(tables_by_input: List[List[pa.Table]]):
        nonlocal output_buffer
        if output_buffer is None and target_max_block_size is not None:
            output_buffer = BlockOutputBuffer(
                OutputBlockSizeOption.of(
                    target_max_block_size=target_max_block_size,
                )
            )
        for block in reduce_fn(partition_id, tables_by_input):
            # Yield raw blocks: a fused map (and `_yield_with_stats`) is applied
            # downstream of ``_reduce_output_blocks``.
            if output_buffer is None:
                yield block
            else:
                output_buffer.add_block(block)
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
            from ray.data._internal.execution.operators.map_transformer import (
                TransformClock,
            )

            map_transformer.override_target_max_block_size(
                map_task_context.target_max_block_size_override
            )
            for block in map_transformer.apply_transform(
                _reduce_output_blocks(),
                map_task_context,
                clock=TransformClock(),
            ):
                if BlockAccessor.for_block(block).num_rows() == 0:
                    # An unfused MapOperator bundles empty inputs with non-empty
                    # ones, so it never emits an empty block for them; a fused
                    # map sees one partition per task and would. Drop them.
                    continue
                yield from _yield_with_stats(block)
