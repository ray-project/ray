from collections import deque
from dataclasses import dataclass
from typing import Deque, List, Optional, Tuple

from ray.data._internal.execution.interfaces import BlockSlice, RefBundle
from ray.data._internal.execution.interfaces.ref_bundle import (
    merge_ref_bundles,
    slice_ref_bundle,
)
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
        self._pending_bundles: Deque[RefBundle] = deque()
        self._consumed_input_bundles: List[RefBundle] = []
        self._ready_bundles: Deque[RefBundle] = deque()
        self._total_pending_rows = 0
        self._next_output_index = 0

    def _try_build_ready_bundle(self, flush_remaining: bool = False):
        if self._total_pending_rows >= self._target_num_rows or flush_remaining:
            rows_needed_from_last_bundle = (
                self._total_pending_rows % self._target_num_rows
            )
            if rows_needed_from_last_bundle > 0:
                last_bundle = self._pending_bundles.pop()
                to_consume, remaining = slice_ref_bundle(
                    last_bundle, rows_needed_from_last_bundle
                )
                merged_bundle = merge_ref_bundles(
                    list(self._pending_bundles) + [to_consume]
                )
                self._ready_bundles.append(merged_bundle)
                self._pending_bundles.clear()
                self._total_pending_rows = 0
                if remaining.num_rows() > 0:
                    self._pending_bundles.append(remaining)
                    self._total_pending_rows += remaining.num_rows()
            else:
                self._ready_bundles.append(
                    merge_ref_bundles(list(self._pending_bundles))
                )
                self._pending_bundles.clear()
                self._total_pending_rows = 0

    def add_bundle(self, ref_bundle: RefBundle):
        schema = ref_bundle.schema
        block_count = 0

        self._total_pending_rows += ref_bundle.num_rows()
        self._pending_bundles.append(
            RefBundle(
                blocks=tuple(ref_bundle.blocks),
                slices=[
                    BlockSlice(start_offset=0, end_offset=metadata.num_rows)
                    for metadata in ref_bundle.metadata
                ],
                schema=schema,
                owns_blocks=False,
            )
        )
        self._try_build_ready_bundle()
        self._consumed_input_bundles.append(ref_bundle)

        if block_count > 0:
            self._bundle_remaining_blocks[ref_bundle] = block_count

    def has_bundle(self) -> bool:
        return len(self._ready_bundles) > 0

    def get_next_bundle(
        self,
    ) -> Tuple[List[RefBundle], RefBundle]:
        consumed_input_bundles = self._consumed_input_bundles
        self._consumed_input_bundles = []
        return consumed_input_bundles, self._ready_bundles.popleft()

    def done_adding_bundles(self):
        if len(self._pending_bundles) > 0:
            self._try_build_ready_bundle(flush_remaining=True)

    def num_blocks(self):
        return sum(len(bundle.blocks) for bundle in self._pending_bundles) + sum(
            len(bundle.blocks) for bundle in self._ready_bundles
        )

    def size_bytes(self) -> int:
        return sum(bundle.size_bytes() for bundle in self._pending_bundles) + sum(
            bundle.size_bytes() for bundle in self._ready_bundles
        )
