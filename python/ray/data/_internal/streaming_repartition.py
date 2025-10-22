import math
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Deque, Iterable, List, Optional

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import RefBundle, TaskContext
from ray.data._internal.execution.operators.map_operator import _TaskInput
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.types import ObjectRef

if TYPE_CHECKING:  # pragma: no cover
    from ray.data.block import Schema


STREAMING_REPARTITION_SPEC_KEY = "streaming_repartition_spec"


@dataclass
class StreamingRepartitionContributorSpec:
    start_offset: int
    end_offset: int

    @property
    def slice_rows(self) -> int:
        return self.end_offset - self.start_offset


@dataclass
class StreamingRepartitionTaskSpec:
    output_index: int
    output_num_rows: int
    contributors: List[StreamingRepartitionContributorSpec]


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
            rows_needed = (
                self._target_num_rows
                if self._total_pending_rows >= self._target_num_rows
                else self._total_pending_rows
            )
            task_inputs.append(self._build_task_input(rows_needed))
        return task_inputs

    def _build_task_input(self, rows_needed: int) -> _TaskInput:
        contributors: List[StreamingRepartitionContributorSpec] = []
        bundle_blocks = []
        bundle_schema = None
        remaining = rows_needed

        while remaining > 0 and self._pending_blocks:
            block = self._pending_blocks[0]
            available = block.remaining_rows
            take = min(available, remaining)

            start_offset = block.start_offset
            end_offset = start_offset + take

            contributors.append(
                StreamingRepartitionContributorSpec(
                    start_offset=start_offset,
                    end_offset=end_offset,
                )
            )

            bundle_schema = bundle_schema or block.schema
            bundle_blocks.append(
                (
                    block.block_ref,
                    _slice_block_metadata(block.metadata, take),
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
            output_index=self._next_output_index,
            output_num_rows=rows_needed,
            contributors=contributors,
        )
        self._next_output_index += 1

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

    builder = DelegatingBlockBuilder()
    blocks_iter = iter(blocks)

    for contributor in spec.contributors:
        block = next(blocks_iter, None)
        if block is None:
            raise ValueError(
                "Task inputs are missing blocks required by the repartition spec."
            )

        accessor = BlockAccessor.for_block(block)
        start = contributor.start_offset
        end = contributor.end_offset

        if start == 0 and end >= accessor.num_rows():
            builder.add_block(block)
        else:
            builder.add_block(accessor.slice(start, end, copy=False))

    output_block = builder.build()
    if next(blocks_iter, None) is not None:
        raise ValueError("Received more blocks than expected for repartition spec.")
    yield output_block


def _slice_block_metadata(metadata: BlockMetadata, slice_rows: int) -> BlockMetadata:
    if slice_rows <= 0:
        raise ValueError(
            f"slice_rows must be positive for streaming repartition{slice_rows}"
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
