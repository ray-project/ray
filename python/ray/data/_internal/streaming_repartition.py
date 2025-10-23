import math
from collections import deque
from dataclasses import dataclass
from typing import Deque, Iterable, List, Optional

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import RefBundle, TaskContext
from ray.data._internal.execution.operators.map_operator import _TaskInput
from ray.data.block import Block, BlockAccessor, BlockMetadata, Schema
from ray.types import ObjectRef

STREAMING_REPARTITION_SPEC_KEY = "streaming_repartition_spec"


@dataclass
class StreamingRepartitionContributorSpec:
    # Index of the contributing block within the task input bundle.
    block_index: int
    # Slice [start_offset, end_offset) in the contributing block.
    start_offset: int
    end_offset: int

    @property
    def slice_rows(self) -> int:
        return self.end_offset - self.start_offset


@dataclass
class StreamingRepartitionOutputSpec:
    # Number of rows in this output block.
    num_rows: int
    # Contributors used to form this output.
    contributors: List[StreamingRepartitionContributorSpec]


@dataclass
class StreamingRepartitionTaskSpec:
    # A task can emit one or more output blocks in order.
    outputs: List[StreamingRepartitionOutputSpec]


@dataclass
class _PendingBlock:
    block_ref: ObjectRef
    metadata: BlockMetadata
    schema: Optional["Schema"]
    start_offset: int = 0

    @property
    def remaining_rows(self) -> int:
        assert self.metadata.num_rows is not None
        return self.metadata.num_rows - self.start_offset


class StreamingRepartitionTaskBuilder:
    def __init__(self, target_num_rows_per_block: int):
        if target_num_rows_per_block <= 0:
            raise ValueError(
                "target_num_rows_per_block must be positive for streaming repartition."
            )
        self._target_num_rows = target_num_rows_per_block
        self._pending_blocks: Deque[_PendingBlock] = deque()
        self._total_pending_rows = 0
        self._next_output_index = 0

    def add_input(self, ref_bundle: RefBundle) -> List[_TaskInput]:
        schema = ref_bundle.schema
        for block_ref, metadata in ref_bundle.blocks:
            if metadata.num_rows is None:
                raise ValueError(
                    "Streaming repartition requires upstream block metadata to include "
                    "num_rows."
                )
            self._pending_blocks.append(
                _PendingBlock(
                    block_ref=block_ref,
                    metadata=metadata,
                    schema=schema,
                    start_offset=0,
                )
            )
            self._total_pending_rows += metadata.num_rows
        return self._drain_ready_tasks()

    def finish(self) -> List[_TaskInput]:
        return self._drain_ready_tasks(flush_remaining=True)

    def _drain_ready_tasks(self, flush_remaining: bool = False) -> List[_TaskInput]:
        task_inputs: List[_TaskInput] = []
        while self._total_pending_rows >= self._target_num_rows or (
            flush_remaining and self._total_pending_rows > 0
        ):
            # If the first pending block alone has at least one full chunk,
            # issue a single task for as many full target-sized outputs as it contains.
            if (
                self._pending_blocks
                and self._pending_blocks[0].remaining_rows >= self._target_num_rows
            ):
                first = self._pending_blocks[0]
                full_chunks = first.remaining_rows // self._target_num_rows
                if full_chunks > 0:
                    task_inputs.append(
                        self._build_task_from_single_block_full_chunks(full_chunks)
                    )
                    continue

            # Otherwise, build a single-output task that may draw from multiple blocks.
            rows_needed = (
                self._target_num_rows
                if self._total_pending_rows >= self._target_num_rows
                else self._total_pending_rows
            )
            task_inputs.append(self._build_single_output_task(rows_needed))
        return task_inputs

    def _build_single_output_task(self, rows_needed: int) -> _TaskInput:
        contributors: List[StreamingRepartitionContributorSpec] = []
        bundle_blocks: List = []
        bundle_schema = None
        remaining = rows_needed

        # Map from id(_PendingBlock) to its index within current bundle.
        block_to_index = {}

        while remaining > 0 and self._pending_blocks:
            block = self._pending_blocks[0]
            available = block.remaining_rows
            take = min(available, remaining)

            # Lazily add this block to the bundle and assign an index.
            if id(block) not in block_to_index:
                block_index = len(bundle_blocks)
                block_to_index[id(block)] = block_index
                bundle_schema = bundle_schema or block.schema
                bundle_blocks.append(
                    (
                        block.block_ref,
                        _slice_block_metadata(block.metadata, take),
                    )
                )
            else:
                # Update the metadata slice for this block in this task to reflect
                # additional consumption, since we may take from the same block again
                # within the same task only if remaining > 0 (unlikely here, but safe).
                block_index = block_to_index[id(block)]
                prev_ref, prev_meta = bundle_blocks[block_index]
                new_slice_rows = (
                    prev_meta.num_rows + take
                    if prev_meta.num_rows is not None
                    else None
                )
                bundle_blocks[block_index] = (
                    prev_ref,
                    _slice_block_metadata(block.metadata, new_slice_rows)
                    if new_slice_rows is not None
                    else prev_meta,
                )

            start_offset = block.start_offset
            end_offset = start_offset + take

            contributors.append(
                StreamingRepartitionContributorSpec(
                    block_index=block_to_index[id(block)],
                    start_offset=start_offset,
                    end_offset=end_offset,
                )
            )
            block.start_offset += take
            remaining -= take
            self._total_pending_rows -= take

            if block.start_offset >= block.metadata.num_rows:
                self._pending_blocks.popleft()

        ref_bundle = RefBundle(
            blocks=tuple(bundle_blocks),
            schema=bundle_schema,
            owns_blocks=False,
        )

        spec = StreamingRepartitionTaskSpec(
            outputs=[
                StreamingRepartitionOutputSpec(
                    num_rows=rows_needed, contributors=contributors
                )
            ]
        )

        return _TaskInput(
            bundle=ref_bundle,
            task_kwargs={STREAMING_REPARTITION_SPEC_KEY: spec},
        )

    def _build_task_from_single_block_full_chunks(self, full_chunks: int) -> _TaskInput:
        """Build a task input that consumes as many full target-sized chunks
        as possible from the first pending block and emits multiple outputs
        in a single Ray task.
        """
        assert self._pending_blocks
        block = self._pending_blocks[0]
        target = self._target_num_rows
        use_rows = full_chunks * target

        # Build outputs specs, each contributed solely by this one block.
        outputs: List[StreamingRepartitionOutputSpec] = []
        start = block.start_offset
        for _ in range(full_chunks):
            end = start + target
            outputs.append(
                StreamingRepartitionOutputSpec(
                    num_rows=target,
                    contributors=[
                        StreamingRepartitionContributorSpec(
                            block_index=0, start_offset=start, end_offset=end
                        )
                    ],
                )
            )
            start = end

        # Bundle consists of just this block with metadata reflecting rows consumed.
        ref_bundle = RefBundle(
            blocks=(
                (block.block_ref, _slice_block_metadata(block.metadata, use_rows)),
            ),
            schema=block.schema,
            owns_blocks=False,
        )

        # Update builder state to reflect consumption.
        block.start_offset += use_rows
        self._total_pending_rows -= use_rows
        if block.start_offset >= block.metadata.num_rows:
            self._pending_blocks.popleft()

        spec = StreamingRepartitionTaskSpec(outputs=outputs)
        return _TaskInput(
            bundle=ref_bundle,
            task_kwargs={STREAMING_REPARTITION_SPEC_KEY: spec},
        )


def streaming_repartition_block_fn(
    blocks: Iterable[Block], ctx: TaskContext
) -> Iterable[Block]:
    spec: StreamingRepartitionTaskSpec = ctx.kwargs.pop(
        STREAMING_REPARTITION_SPEC_KEY, None
    )
    if spec is None:
        raise ValueError("Missing streaming repartition task spec in TaskContext.")

    # Materialize input blocks to allow arbitrary contributor ordering.
    blocks_list = list(blocks)

    for output in spec.outputs:
        builder = DelegatingBlockBuilder()
        for contributor in output.contributors:
            try:
                block = blocks_list[contributor.block_index]
            except IndexError:
                raise ValueError(
                    "Repartition spec refers to a block index outside the task input."
                )

            accessor = BlockAccessor.for_block(block)
            start = contributor.start_offset
            end = contributor.end_offset

            if start == 0 and end >= accessor.num_rows():
                builder.add_block(block)
            else:
                builder.add_block(accessor.slice(start, end, copy=False))

        yield builder.build()


def _slice_block_metadata(metadata: BlockMetadata, slice_rows: int) -> BlockMetadata:
    if slice_rows <= 0:
        raise ValueError(
            f"slice_rows must be positive for streaming repartition: {slice_rows}"
        )
    size_bytes = metadata.size_bytes

    if metadata.size_bytes is not None and metadata.num_rows:
        per_row = metadata.size_bytes / metadata.num_rows
        size_bytes = max(1, int(math.ceil(per_row * slice_rows)))

    return BlockMetadata(
        num_rows=slice_rows if metadata.num_rows is not None else None,
        size_bytes=size_bytes,
        exec_stats=None,
        input_files=list(metadata.input_files),
    )
