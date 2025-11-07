from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Optional, Tuple

from ray.data._internal.execution.interfaces import BlockSlice, RefBundle
from ray.data._internal.execution.interfaces.ref_bundle import _slice_block_metadata
from ray.data._internal.execution.operators.map_operator import BaseRefBundler
from ray.data.block import BlockMetadata, Schema
from ray.types import ObjectRef

"""Streaming repartition builds fixed-size outputs from a stream of inputs.

    We construct batches here to produce exactly sized outputs from arbitrary [start, end) slices across input blocks.
    The task builder submits a map task only after the total number of rows accumulated across pending blocks reaches
    target num rows (except during the final flush, which may emit a smaller tail block). This allows us to create
    target-sized batches without materializing entire large blocks on the driver.
"""


@dataclass
class _PendingBlock:
    """Input block plus consumption state while queued for repartitioning.

    - block_ref/metadata/schema: Upstream block handle and metadata.
    - start_offset: Number of rows already consumed from this block.
    """

    block_ref: ObjectRef
    metadata: BlockMetadata
    schema: Optional["Schema"]
    parent_bundle: RefBundle
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
        self._bundle_remaining_blocks: Dict[RefBundle, int] = {}
        self._ready_bundles: Deque[Tuple[List[RefBundle], RefBundle]] = deque()
        self._total_pending_rows = 0
        self._next_output_index = 0

    def add_bundle(self, ref_bundle: RefBundle):
        schema = ref_bundle.schema
        block_count = 0
        for block_ref, metadata in ref_bundle.blocks:
            if metadata.num_rows <= 0:  # skip empty blocks
                continue
            self._pending_blocks.append(
                _PendingBlock(
                    block_ref=block_ref,
                    metadata=metadata,
                    schema=schema,
                    parent_bundle=ref_bundle,
                    start_offset=0,
                )
            )
            self._total_pending_rows += metadata.num_rows
            block_count += 1

        if block_count > 0:
            self._bundle_remaining_blocks[ref_bundle] = block_count

    def has_bundle(self) -> bool:
        self._drain_ready_tasks()
        return len(self._ready_bundles) > 0

    def get_next_bundle(
        self,
    ) -> Tuple[List[RefBundle], RefBundle]:
        return self._ready_bundles.popleft()

    def done_adding_bundles(self):
        self._drain_ready_tasks(flush_remaining=True)

    def num_blocks(self):
        return 0  # TODO: implement

    def size_bytes(self) -> int:
        return 0  # TODO: implement

    def _drain_ready_tasks(self, flush_remaining: bool = False):
        task_inputs: List[Tuple[List[RefBundle], RefBundle]] = []
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

    def _build_task(self, output_rows: List[int]) -> Tuple[List[RefBundle], RefBundle]:
        total_rows_needed = sum(output_rows)
        assert (
            total_rows_needed <= self._total_pending_rows
        ), "Requested more rows than are pending in the repartition builder."
        used_blocks: List[_PendingBlock] = []
        rows_by_block: List[int] = []
        bundle_schema = None
        block_slices: List[BlockSlice] = []
        fully_consumed_refs: List[RefBundle] = []

        for output_index, num_rows in enumerate(output_rows):
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

                block_slices.append(
                    BlockSlice(
                        block_index=block_index,
                        start_offset=start_offset,
                        end_offset=end_offset,
                        output_index=output_index,
                    )
                )

                block.start_offset += take
                rows_by_block[block_index] += take
                self._total_pending_rows -= take
                remaining -= take

                assert block.start_offset <= block.metadata.num_rows

                if block.start_offset == block.metadata.num_rows:
                    self._pending_blocks.popleft()
                    parent_bundle = block.parent_bundle
                    remaining_blocks = self._bundle_remaining_blocks.get(parent_bundle)
                    if remaining_blocks is not None:
                        if remaining_blocks == 1:
                            self._bundle_remaining_blocks.pop(parent_bundle, None)
                            fully_consumed_refs.append(parent_bundle)
                        else:
                            self._bundle_remaining_blocks[parent_bundle] = (
                                remaining_blocks - 1
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
            slices=block_slices,
        )

        return fully_consumed_refs, ref_bundle
