import logging
import warnings
from typing import Iterable, List, Tuple

import numpy as np

import ray
from ray import ObjectRef
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import (
    BaseRefBundler,
    MapOperator,
    _merge_ref_bundles,
)
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.util import memory_string
from ray.data._internal.logical.operators import Read
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.util import _warn_on_high_parallelism
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource.datasource import ReadTask
from ray.experimental.locations import get_local_object_locations
from ray.util.debug import log_once

TASK_SIZE_WARN_THRESHOLD_BYTES = 1024 * 1024  # 1 MiB

logger = logging.getLogger(__name__)


class ShuffleRefBundler(BaseRefBundler):
    """Groups small blocks from the read operator for WindowShuffle.

    Accumulates single-block RefBundles produced by the read operator. Once
    ``sample_ratio * merge_window`` bundles are buffered, randomly samples
    ``merge_window`` of them, merges them into one multi-block RefBundle, and
    emits it as a single shuffle task input. On finalization, remaining
    bundles are flushed in groups of ``merge_window``.
    """

    def __init__(self, merge_window: int, sample_ratio: int):
        self._merge_window = merge_window
        self._sample_window = merge_window * sample_ratio
        self._buffer: List[RefBundle] = []
        self._buffer_size_bytes: int = 0
        self._finalized = False
        self._rng = np.random.default_rng()

    def num_blocks(self) -> int:
        return sum(len(b.block_refs) for b in self._buffer)

    def add_bundle(self, bundle: RefBundle):
        self._buffer.append(bundle)
        self._buffer_size_bytes += bundle.size_bytes()

    def has_bundle(self) -> bool:
        if self._finalized:
            return len(self._buffer) > 0
        return len(self._buffer) >= self._sample_window

    def size_bytes(self) -> int:
        return self._buffer_size_bytes

    def get_next_bundle(self) -> Tuple[List[RefBundle], RefBundle]:
        assert self.has_bundle()

        n = len(self._buffer)
        take = min(self._merge_window, n)

        if take >= n:
            # Take everything (flush or exact fit).
            selected = self._buffer
            self._buffer = []
        else:
            # Randomly sample merge_window bundles from the buffer.
            indices = self._rng.choice(n, size=take, replace=False)
            indices_set = set(indices.tolist())
            selected = [self._buffer[i] for i in indices]
            self._buffer = [
                b for i, b in enumerate(self._buffer) if i not in indices_set
            ]

        self._buffer_size_bytes = sum(b.size_bytes() for b in self._buffer)
        return list(selected), _merge_ref_bundles(*selected)

    def done_adding_bundles(self):
        self._finalized = True


def _derive_metadata(read_task: ReadTask, read_task_ref: ObjectRef) -> BlockMetadata:
    # NOTE: Use the `get_local_object_locations` API to get the size of the
    # serialized ReadTask, instead of pickling.
    # Because the ReadTask may capture ObjectRef objects, which cannot
    # be serialized out-of-band.
    locations = get_local_object_locations([read_task_ref])
    task_size = locations[read_task_ref]["object_size"]
    if task_size > TASK_SIZE_WARN_THRESHOLD_BYTES and log_once(
        f"large_read_task_{read_task.read_fn.__name__}"
    ):
        warnings.warn(
            "The serialized size of your read function named "
            f"'{read_task.read_fn.__name__}' is {memory_string(task_size)}. This size "
            "is relatively large. As a result, Ray might excessively "
            "spill objects during execution. To fix this issue, avoid accessing "
            f"`self` or other large objects in '{read_task.read_fn.__name__}'."
        )

    return BlockMetadata(
        num_rows=1,
        size_bytes=task_size,
        exec_stats=None,
        input_files=None,
    )


def plan_read_op(
    op: Read,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for Read.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """
    assert len(physical_children) == 0

    def get_input_data(_target_max_block_size) -> List[RefBundle]:
        parallelism = op.get_detected_parallelism()
        assert (
            parallelism is not None
        ), "Read parallelism must be set by the optimizer before execution"

        # Get the original read tasks
        read_tasks = op.datasource_or_legacy_reader.get_read_tasks(
            parallelism,
            per_task_row_limit=op.per_block_limit,
            data_context=data_context,
        )

        _warn_on_high_parallelism(parallelism, len(read_tasks))

        ret = []
        for read_task in read_tasks:
            read_task_ref = ray.put(read_task)
            ref_bundle = RefBundle(
                (
                    (
                        # TODO: figure out a better way to pass read
                        # tasks other than ray.put().
                        read_task_ref,
                        _derive_metadata(read_task, read_task_ref),
                    ),
                ),
                # `owns_blocks` is False, because these refs are the root of the
                # DAG. We shouldn't eagerly free them. Otherwise, the DAG cannot
                # be reconstructed.
                owns_blocks=False,
                schema=None,
            )
            ret.append(ref_bundle)
        return ret

    inputs = InputDataBuffer(data_context, input_data_factory=get_input_data)

    def do_read(blocks: Iterable[ReadTask], _: TaskContext) -> Iterable[Block]:
        for read_task in blocks:
            yield from read_task()

    # Compute effective block size: smaller blocks when WindowShuffle is active
    ws = op.window_shuffle_config
    if ws is not None:
        effective_block_size = data_context.target_max_block_size // ws.merge_window
    else:
        effective_block_size = data_context.target_max_block_size

    # Create a MapTransformer for a read operator
    map_transformer = MapTransformer(
        [
            BlockMapTransformFn(
                do_read,
                is_udf=False,
                output_block_size_option=OutputBlockSizeOption.of(
                    target_max_block_size=effective_block_size,
                ),
            ),
        ]
    )

    read_op = MapOperator.create(
        map_transformer,
        inputs,
        data_context,
        name=op.name,
        compute_strategy=op.compute,
        ray_remote_args=op.ray_remote_args,
        target_max_block_size_override=(
            effective_block_size if ws is not None else None
        ),
    )

    if ws is None:
        return read_op

    # Chain a shuffle operator that concatenates all blocks in a RefBundle
    # and shuffles rows.
    def concat_and_shuffle(blocks: Iterable[Block], _: TaskContext) -> Iterable[Block]:
        import numpy as np
        import pyarrow as pa

        tables = list(blocks)
        if not tables:
            return
        combined = pa.concat_tables(tables)
        n = combined.num_rows
        if n <= 1:
            yield combined
            return
        rng = np.random.default_rng()
        block_size = 128
        starts = np.arange(0, n, block_size)
        rng.shuffle(starts)
        slices = [combined.slice(int(s), min(block_size, n - int(s))) for s in starts]
        yield pa.concat_tables(slices)

    shuffle_transformer = MapTransformer(
        [
            BlockMapTransformFn(
                concat_and_shuffle,
                is_udf=False,
                output_block_size_option=OutputBlockSizeOption.of(
                    target_max_block_size=data_context.target_max_block_size,
                ),
            ),
        ]
    )

    return MapOperator.create(
        shuffle_transformer,
        read_op,
        data_context,
        name="WindowShuffle",
        ref_bundler=ShuffleRefBundler(ws.merge_window, ws.sample_ratio),
    )
