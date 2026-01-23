import collections
from typing import TYPE_CHECKING, List, Optional, Tuple

import ray
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
    NAryOperator,
)
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import StatsDict
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
    Schema,
    _merge_schemas,
    to_stats,
)
from ray.data.context import DataContext
from ray.types import ObjectRef

if TYPE_CHECKING:
    from ray.data.block import BlockMetadataWithSchema


class ZipAlignmentBundler:
    """Bundler that accumulates rows until target is reached, then produces a merged RefBundle."""

    def __init__(self, target_num_rows: int):
        """Create a ZipAlignmentBundler.
        Args:
            target_num_rows: The target number of rows to accumulate before producing a merged RefBundle.
        """
        self._target_num_rows = target_num_rows
        self._pending_bundles: collections.deque[RefBundle] = collections.deque()
        self._pending_rows = 0
        self._finalized = False
        # Track mapping from remaining_bundle -> original_bundle for metrics
        # When a bundle is sliced, the remaining portion maps back to the original
        self._remaining_to_original: dict[RefBundle, RefBundle] = {}

    def add_bundle(self, bundle: RefBundle) -> None:
        """Add a bundle to the bundler."""
        self._pending_bundles.append(bundle)
        self._pending_rows += bundle.num_rows()

    def has_bundle(self) -> bool:
        """Check if bundler has enough rows for a full bundle."""
        if self._pending_rows >= self._target_num_rows:
            return True
        if self._finalized and self._pending_rows > 0:
            return True
        return False

    def done_adding_bundles(self) -> None:
        """Signal that no more bundles will be added."""
        self._finalized = True

    def num_blocks(self) -> int:
        """Return total number of blocks in pending bundles."""
        return sum(len(b.block_refs) for b in self._pending_bundles)

    def size_bytes(self) -> int:
        """Return total size in bytes of pending bundles."""
        return sum(b.size_bytes() for b in self._pending_bundles)

    def pending_rows(self) -> int:
        """Return number of pending rows."""
        return self._pending_rows

    def get_next_bundle(self) -> Tuple[RefBundle, List[RefBundle]]:
        """Get the next bundle as (merged_bundle, consumed_bundles).

        Returns a merged RefBundle for exactly target_num_rows (or remaining if finalized).
        Uses RefBundle.slice() for slicing logic. The merged bundle contains block_refs
        and slices attributes that can be used directly.

        consumed_bundles contains the ORIGINAL bundles that were registered with
        on_input_queued, for proper metrics tracking.
        """
        assert self.has_bundle()

        rows_to_take = min(self._target_num_rows, self._pending_rows)
        bundles_to_merge: List[RefBundle] = []
        consumed_bundles: List[RefBundle] = []
        rows_collected = 0

        while rows_collected < rows_to_take and self._pending_bundles:
            bundle = self._pending_bundles.popleft()
            bundle_rows = bundle.num_rows()
            rows_needed = rows_to_take - rows_collected

            # Get the original bundle for metrics tracking
            # If this bundle is a remainder from a previous slice, look up its original
            original_bundle = self._remaining_to_original.pop(bundle, bundle)

            if bundle_rows <= rows_needed:
                # Take entire bundle - original is fully consumed
                bundles_to_merge.append(bundle)
                consumed_bundles.append(original_bundle)
                rows_collected += bundle_rows
                self._pending_rows -= bundle_rows
            else:
                sliced_bundle, remaining_bundle = bundle.slice(rows_needed)
                bundles_to_merge.append(sliced_bundle)
                # Don't add to consumed_bundles yet - original still has remaining rows
                # Track that remaining_bundle maps to the original
                self._remaining_to_original[remaining_bundle] = original_bundle
                self._pending_bundles.appendleft(remaining_bundle)
                rows_collected += rows_needed
                self._pending_rows -= rows_needed

        merged = RefBundle.merge_ref_bundles(bundles_to_merge)
        return merged, consumed_bundles


class ZipOperator(InternalQueueOperatorMixin, NAryOperator):
    """An operator that zips its inputs together in a streaming fashion.

    Uses ZipAlignmentBundler per input to accumulate rows. When all bundlers
    have a ready bundle, extracts slice specs and submits a zip task.
    """

    def __init__(
        self,
        data_context: DataContext,
        target_num_rows_per_block: int,
        *input_ops: PhysicalOperator,
    ):
        assert len(input_ops) >= 2
        self._target_num_rows = target_num_rows_per_block

        # One bundler per input
        self._bundlers: List[ZipAlignmentBundler] = [
            ZipAlignmentBundler(target_num_rows_per_block)
            for _ in range(len(input_ops))
        ]

        self._output_buffer: collections.deque[RefBundle] = collections.deque()
        self._output_metadata: List["BlockMetadataWithSchema"] = []
        self._pending_metadata_refs: List[ObjectRef["BlockMetadataWithSchema"]] = []
        self._merged_schema: Optional[Schema] = None
        super().__init__(data_context, *input_ops)

    def num_outputs_total(self) -> Optional[int]:
        num_outputs = None
        for input_op in self.input_dependencies:
            input_num_outputs = input_op.num_outputs_total()
            if input_num_outputs is None:
                continue
            if num_outputs is None:
                num_outputs = input_num_outputs
            else:
                num_outputs = max(num_outputs, input_num_outputs)
        return num_outputs

    def num_output_rows_total(self) -> Optional[int]:
        num_rows = None
        for input_op in self.input_dependencies:
            input_num_rows = input_op.num_output_rows_total()
            if input_num_rows is None:
                continue
            if num_rows is None:
                num_rows = input_num_rows
            else:
                num_rows = max(num_rows, input_num_rows)
        return num_rows

    def internal_input_queue_num_blocks(self) -> int:
        return sum(b.num_blocks() for b in self._bundlers)

    def internal_input_queue_num_bytes(self) -> int:
        return sum(b.size_bytes() for b in self._bundlers)

    def internal_output_queue_num_blocks(self) -> int:
        return sum(len(bundle.block_refs) for bundle in self._output_buffer)

    def internal_output_queue_num_bytes(self) -> int:
        return sum(bundle.size_bytes() for bundle in self._output_buffer)

    def clear_internal_input_queue(self) -> None:
        for bundler in self._bundlers:
            bundler.done_adding_bundles()
            while bundler.has_bundle():
                _, consumed = bundler.get_next_bundle()
                for bundle in consumed:
                    self._metrics.on_input_dequeued(bundle)

    def clear_internal_output_queue(self) -> None:
        """Clear internal output queue."""
        while self._output_buffer:
            bundle = self._output_buffer.popleft()
            self._metrics.on_output_dequeued(bundle)

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.has_completed()
        assert 0 <= input_index < len(self._bundlers)

        self._bundlers[input_index].add_bundle(refs)
        self._metrics.on_input_queued(refs)
        self._try_submit_zip_tasks()

    def all_inputs_done(self) -> None:
        # Finalize all bundlers
        for bundler in self._bundlers:
            bundler.done_adding_bundles()

        # Flush remaining
        self._try_submit_zip_tasks()

        # Verify all consumed
        for i, bundler in enumerate(self._bundlers):
            if bundler.pending_rows() > 0:
                raise ValueError(
                    f"Input {i} has {bundler.pending_rows()} remaining rows. "
                    "Datasets must have same row count for zip."
                )
        super().all_inputs_done()

    def has_next(self) -> bool:
        return len(self._output_buffer) > 0

    def _get_next_inner(self) -> RefBundle:
        refs = self._output_buffer.popleft()
        self._metrics.on_output_dequeued(refs)
        return refs

    def get_stats(self) -> StatsDict:
        if self._pending_metadata_refs:
            resolved = ray.get(self._pending_metadata_refs)
            self._output_metadata.extend(resolved)
            self._pending_metadata_refs = []
        if not self._output_metadata:
            return {}
        return {self._name: to_stats(self._output_metadata)}

    def implements_accurate_memory_accounting(self):
        return True

    def _try_submit_zip_tasks(self) -> None:
        """Submit zip tasks when all bundlers have a ready bundle."""
        while all(bundler.has_bundle() for bundler in self._bundlers):
            merged_bundles: List[RefBundle] = []
            total_size_bytes = 0
            owns_blocks = True

            for bundler in self._bundlers:
                merged, consumed = bundler.get_next_bundle()
                merged_bundles.append(merged)

                for bundle in consumed:
                    self._metrics.on_input_dequeued(bundle)
                    total_size_bytes += bundle.size_bytes()
                    owns_blocks = owns_blocks and bundle.owns_blocks

            num_rows = merged_bundles[0].num_rows()

            if self._merged_schema is None:
                schemas = [b.schema for b in merged_bundles]
                self._merged_schema = _merge_schemas(schemas)

            output_bundle = self._submit_zip_task(
                merged_bundles, num_rows, total_size_bytes, owns_blocks
            )
            self._output_buffer.append(output_bundle)
            self._metrics.on_output_queued(output_bundle)

    def _submit_zip_task(
        self,
        merged_bundles: List[RefBundle],
        num_rows: int,
        total_size_bytes: int,
        owns_blocks: bool,
    ) -> RefBundle:
        """Submit a zip task with the merged bundles containing slice info."""
        zip_remote = cached_remote_fn(_zip_blocks_with_slices, num_returns=2)

        all_block_refs = [list(b.block_refs) for b in merged_bundles]
        slice_tuples = []
        for bundle in merged_bundles:
            input_slices = []
            for (_, meta), block_slice in zip(bundle.blocks, bundle.slices):
                if block_slice is None:
                    input_slices.append((0, meta.num_rows))
                else:
                    input_slices.append(
                        (block_slice.start_offset, block_slice.end_offset)
                    )
            slice_tuples.append(input_slices)

        result_block, meta_ref = zip_remote.remote(all_block_refs, slice_tuples)
        self._pending_metadata_refs.append(meta_ref)

        estimated_metadata = BlockMetadata(
            num_rows=num_rows,
            size_bytes=total_size_bytes,
            input_files=None,
            exec_stats=None,
        )

        return RefBundle(
            [(result_block, estimated_metadata)],
            owns_blocks=owns_blocks,
            schema=self._merged_schema,
        )


def _zip_blocks_with_slices(
    block_groups: List[List[ObjectRef[Block]]],
    slice_groups: List[List[Tuple[int, int]]],
) -> Tuple[Block, "BlockMetadataWithSchema"]:
    """Remote task: fetch blocks, apply slices, and zip together."""
    stats = BlockExecStats.builder()

    all_block_refs = [ref for group in block_groups for ref in group]
    all_blocks = ray.get(all_block_refs)

    fetched_groups: List[
        List[Block]
    ] = []  # Reconstruct block groups from flattened list
    idx = 0
    for group in block_groups:
        fetched_groups.append(all_blocks[idx : idx + len(group)])
        idx += len(group)

    merged_blocks = []
    for blocks, slices in zip(fetched_groups, slice_groups):
        builder = DelegatingBlockBuilder()
        for block, (start, end) in zip(blocks, slices):
            accessor = BlockAccessor.for_block(block)
            if start > 0 or end < accessor.num_rows():
                block = accessor.slice(start, end, copy=False)
            builder.add_block(block)
        merged_blocks.append(builder.build())

    result = merged_blocks[0]
    for other in merged_blocks[1:]:
        result = BlockAccessor.for_block(result).zip(other)

    from ray.data.block import BlockMetadataWithSchema

    return result, BlockMetadataWithSchema.from_block(result, stats=stats.build())
