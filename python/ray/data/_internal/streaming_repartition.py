import math
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import RefBundle, TaskContext
from ray.data._internal.execution.operators.map_operator import BaseRefBundler
from ray.data.block import Block, BlockAccessor, BlockMetadata, Schema
from ray.types import ObjectRef

"""Streaming repartition builds fixed-size outputs from a stream of inputs.

    We construct batches here to produce exactly sized outputs from arbitrary [start, end) slices across input blocks.
    The task builder submits a map task only after the total number of rows accumulated across pending blocks reaches
    target num rows (except during the final flush, which may emit a smaller tail block). This allows us to create
    target-sized batches without materializing entire large blocks on the driver.
"""


# Key used to stash the repartition task spec in the TaskContext kwargs
# for the downstream execution function to read and act upon.
STREAMING_REPARTITION_SPEC_KEY = "streaming_repartition_spec"


@dataclass
class StreamingRepartitionContributorSpec:
    """A single slice contribution to an output block.

    Fields:
    - block_index: Index of the source block within the current task input bundle.
    - start_offset/end_offset: Half-open row range [start, end) to take from that
      source block.
    """

    # Index of the contributing block within the task input bundle.
    block_index: int
    # Slice [start_offset, end_offset) in the contributing block.
    start_offset: int
    end_offset: int

    @property
    def num_rows_in_slice(self) -> int:
        """Number of rows contributed by this slice."""
        return self.end_offset - self.start_offset


@dataclass
class StreamingRepartitionOutputSpec:
    """Specification for a single output block produced by a task.

    - num_rows: Expected number of rows in the output block.
    - contributors: List of input block and its range that form the output.
    """

    # Number of rows in this output block.
    num_rows: int
    # Contributors used to form this output.
    contributors: List[StreamingRepartitionContributorSpec]


@dataclass
class StreamingRepartitionTaskSpec:
    """Spec describing the outputs a single Ray task should emit."""

    outputs: List[StreamingRepartitionOutputSpec]


@dataclass
class _PendingBlock:
    """Input block plus consumption state while queued for repartitioning.

    - block_ref/metadata/schema: Upstream block handle and metadata.
    - start_offset: Number of rows already consumed from this block.
    """

    block_ref: ObjectRef
    metadata: BlockMetadata
    schema: Optional["Schema"]
    start_offset: int = 0

    @property
    def remaining_rows(self) -> int:
        """Number of rows left to consume from this block."""
        assert self.metadata.num_rows is not None
        return self.metadata.num_rows - self.start_offset


class StreamingRepartitionRefBundler(BaseRefBundler):
    """Incrementally builds task inputs to produce target-sized outputs.

    Usage:
    - Call `add_input(ref_bundle)` as upstream blocks arrive. This returns zero
      or more `(RefBundle, task_kwargs)` tuples ready to schedule immediately.
    """

    def __init__(self, target_num_rows_per_block: int):
        assert (
            target_num_rows_per_block > 0
        ), "target_num_rows_per_block must be positive for streaming repartition."
        self._target_num_rows = target_num_rows_per_block
        self._pending_blocks: Deque[_PendingBlock] = deque()
        self._ready_bundles: Deque[
            Tuple[List[RefBundle], RefBundle, Dict[str, Any]]
        ] = deque()
        self._total_pending_rows = 0
        self._next_output_index = 0

    def add_bundle(self, ref_bundle: RefBundle):
        schema = ref_bundle.schema
        for block_ref, metadata in ref_bundle.blocks:
            if metadata.num_rows <= 0:  # skip empty blocks
                continue
            self._pending_blocks.append(
                _PendingBlock(
                    block_ref=block_ref,
                    metadata=metadata,
                    schema=schema,
                    start_offset=0,
                )
            )
            self._total_pending_rows += metadata.num_rows

    def has_bundle(self) -> bool:
        self._drain_ready_tasks()
        return len(self._ready_bundles) > 0

    def get_next_bundle(
        self,
    ) -> Tuple[List[RefBundle], RefBundle, Optional[Dict[str, Any]]]:
        return self._ready_bundles.popleft()

    def done_adding_bundles(self):
        self._drain_ready_tasks(flush_remaining=True)

    def num_blocks(self):
        return 0  # TODO: implement

    def size_bytes(self) -> int:
        return 0  # TODO: implement

    def _drain_ready_tasks(self, flush_remaining: bool = False):
        task_inputs: List[Tuple[List[RefBundle], RefBundle, Dict[str, Any]]] = []
        while self._total_pending_rows >= self._target_num_rows or (
            flush_remaining and self._total_pending_rows > 0
        ):
            # If the first pending block alone has at least one full block,
            # issue a single task for as many full target-sized outputs as it contains.
            if (
                self._pending_blocks
                and self._pending_blocks[0].remaining_rows >= self._target_num_rows
            ):
                first = self._pending_blocks[0]
                full_blocks = first.remaining_rows // self._target_num_rows
                if full_blocks > 0:
                    task_inputs.append(
                        self._build_task([self._target_num_rows] * full_blocks)
                    )
                    continue

            # Otherwise, build a single-output task that may draw from multiple blocks.
            rows_needed = (
                self._target_num_rows
                if self._total_pending_rows >= self._target_num_rows
                else self._total_pending_rows
            )
            task_inputs.append(self._build_task([rows_needed]))
        self._ready_bundles.extend(task_inputs)

    def _build_task(
        self, output_rows: List[int]
    ) -> Tuple[List[RefBundle], RefBundle, Dict[str, Any]]:
        total_rows_needed = sum(output_rows)
        assert (
            total_rows_needed <= self._total_pending_rows
        ), "Requested more rows than are pending in the repartition builder."
        used_blocks: List[_PendingBlock] = []
        rows_by_block: List[int] = []
        bundle_schema = None
        outputs: List[StreamingRepartitionOutputSpec] = []

        for num_rows in output_rows:
            contributors: List[StreamingRepartitionContributorSpec] = []
            remaining = num_rows

            while remaining > 0:
                assert (
                    self._pending_blocks
                ), "No pending blocks available to build task."

                block = self._pending_blocks[0]
                if not used_blocks or used_blocks[-1] is not block:
                    used_blocks.append(block)
                    rows_by_block.append(0)
                    bundle_schema = bundle_schema or block.schema

                block_index = len(used_blocks) - 1
                available = block.remaining_rows
                take = min(available, remaining)

                start_offset = block.start_offset
                end_offset = start_offset + take

                contributors.append(
                    StreamingRepartitionContributorSpec(
                        block_index=block_index,
                        start_offset=start_offset,
                        end_offset=end_offset,
                    )
                )

                block.start_offset += take
                rows_by_block[block_index] += take
                self._total_pending_rows -= take
                remaining -= take

                assert block.start_offset <= block.metadata.num_rows

                if block.start_offset == block.metadata.num_rows:
                    self._pending_blocks.popleft()

            outputs.append(
                StreamingRepartitionOutputSpec(
                    num_rows=num_rows, contributors=contributors
                )
            )

        bundle_blocks = [
            (
                block.block_ref,
                _slice_block_metadata(block.metadata, consumed_rows),
            )
            for block, consumed_rows in zip(used_blocks, rows_by_block)
        ]

        ref_bundle = RefBundle(
            blocks=tuple(bundle_blocks),
            schema=bundle_schema,
            owns_blocks=False,
        )

        spec = StreamingRepartitionTaskSpec(outputs=outputs)
        return [], ref_bundle, {STREAMING_REPARTITION_SPEC_KEY: spec}


def streaming_repartition_block_fn(
    blocks: Iterable[Block], ctx: TaskContext
) -> Iterable[Block]:
    spec: StreamingRepartitionTaskSpec = ctx.kwargs.pop(
        STREAMING_REPARTITION_SPEC_KEY, None
    )
    assert spec, "Missing streaming repartition task spec in TaskContext."

    # Materialize input blocks to allow arbitrary contributor ordering.
    blocks_list = list(blocks)

    for output in spec.outputs:
        builder = DelegatingBlockBuilder()
        for contributor in output.contributors:
            assert (
                0 <= contributor.block_index < len(blocks_list)
            ), "Repartition spec refers to a block index outside the task input."
            block = blocks_list[contributor.block_index]

            accessor = BlockAccessor.for_block(block)
            start = contributor.start_offset
            end = contributor.end_offset

            if start == 0 and end >= accessor.num_rows():
                builder.add_block(block)
            else:
                builder.add_block(accessor.slice(start, end, copy=False))
        # Build the output block and verify it matches the expected size.
        built_block = builder.build()
        built_rows = BlockAccessor.for_block(built_block).num_rows()
        assert built_rows == output.num_rows
        yield built_block


def _slice_block_metadata(
    metadata: BlockMetadata, num_rows_in_slice: int
) -> BlockMetadata:
    assert (
        num_rows_in_slice > 0
    ), "num_rows_in_slice must be positive for streaming repartition."
    size_bytes = metadata.size_bytes
    if metadata.size_bytes is not None and metadata.num_rows:
        per_row = metadata.size_bytes / metadata.num_rows
        size_bytes = max(1, int(math.ceil(per_row * num_rows_in_slice)))
    return BlockMetadata(
        num_rows=num_rows_in_slice if metadata.num_rows is not None else None,
        size_bytes=size_bytes,
        exec_stats=None,
        input_files=list(metadata.input_files),
    )
