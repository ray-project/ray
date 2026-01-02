import collections
from typing import TYPE_CHECKING, List, Optional, Tuple

import ray
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.interfaces.ref_bundle import BlockSlice
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
    """Bundler that accumulates rows until target is reached, then produces slice specs.

    Similar to BaseRefBundler but designed for zip alignment - it produces
    (block_refs, slices) tuples instead of RefBundle objects.
    """

    def __init__(self, target_num_rows: int):
        self._target_num_rows = target_num_rows
        self._pending_bundles: collections.deque[RefBundle] = collections.deque()
        self._pending_rows = 0
        self._consumed_rows_in_first = 0  # Rows already consumed from first bundle
        self._finalized = False

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

    def get_next_bundle(
        self,
    ) -> Tuple[List[ObjectRef[Block]], List[BlockSlice], List[RefBundle], Optional[Schema]]:
        """Get the next bundle as (block_refs, slices, consumed_bundles, schema).

        Returns slice specs for exactly target_num_rows (or remaining if finalized).
        """
        assert self.has_bundle()

        rows_to_take = min(self._target_num_rows, self._pending_rows)
        block_refs: List[ObjectRef[Block]] = []
        slices: List[BlockSlice] = []
        consumed_bundles: List[RefBundle] = []
        schema = None
        skip_rows = self._consumed_rows_in_first

        rows_remaining = rows_to_take
        while rows_remaining > 0 and self._pending_bundles:
            bundle = self._pending_bundles[0]
            if schema is None:
                schema = bundle.schema

            bundle_total = bundle.num_rows()
            available = bundle_total - skip_rows

            if available <= rows_remaining:
                # Take all remaining rows from this bundle
                self._extract_slices(bundle, skip_rows, bundle_total, block_refs, slices)
                rows_remaining -= available
                self._pending_rows -= available
                self._pending_bundles.popleft()
                consumed_bundles.append(bundle)
                self._consumed_rows_in_first = 0
                skip_rows = 0
            else:
                # Take partial rows
                end_row = skip_rows + rows_remaining
                self._extract_slices(bundle, skip_rows, end_row, block_refs, slices)
                self._pending_rows -= rows_remaining
                self._consumed_rows_in_first = end_row
                rows_remaining = 0

        return block_refs, slices, consumed_bundles, schema

    def _extract_slices(
        self,
        bundle: RefBundle,
        global_start: int,
        global_end: int,
        block_refs: List[ObjectRef[Block]],
        slices: List[BlockSlice],
    ) -> None:
        """Extract BlockSlice specs for rows [global_start, global_end) from a bundle."""
        current_row = 0
        for (block_ref, meta), existing_slice in zip(bundle.blocks, bundle.slices):
            if existing_slice is not None:
                block_start = existing_slice.start_offset
                block_end = existing_slice.end_offset
            else:
                block_start = 0
                block_end = meta.num_rows

            block_rows = block_end - block_start
            block_global_start = current_row
            block_global_end = current_row + block_rows

            if block_global_end <= global_start:
                current_row = block_global_end
                continue
            if block_global_start >= global_end:
                break

            overlap_start = max(global_start, block_global_start)
            overlap_end = min(global_end, block_global_end)
            local_start = block_start + (overlap_start - block_global_start)
            local_end = block_start + (overlap_end - block_global_start)

            block_refs.append(block_ref)
            slices.append(BlockSlice(start_offset=local_start, end_offset=local_end))
            current_row = block_global_end


class ZipOperator(InternalQueueOperatorMixin, NAryOperator):
    """An operator that zips its inputs together in a streaming fashion.

    Uses ZipAlignmentBundler per input to accumulate rows. When ALL bundlers
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
        """Clear internal input queues."""
        for bundler in self._bundlers:
            bundler.done_adding_bundles()
            while bundler.has_bundle():
                _, _, consumed, _ = bundler.get_next_bundle()
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
        """Submit zip tasks when ALL bundlers have a ready bundle."""
        while all(bundler.has_bundle() for bundler in self._bundlers):
            # All bundlers ready - extract from each
            all_block_refs: List[List[ObjectRef[Block]]] = []
            all_slices: List[List[BlockSlice]] = []
            schemas: List[Optional[Schema]] = []
            total_size_bytes = 0
            owns_blocks = True
            num_rows = None

            for bundler in self._bundlers:
                block_refs, slices, consumed, schema = bundler.get_next_bundle()
                all_block_refs.append(block_refs)
                all_slices.append(slices)
                schemas.append(schema)

                # Report consumed bundles and accumulate metadata
                for bundle in consumed:
                    self._metrics.on_input_dequeued(bundle)
                    total_size_bytes += bundle.size_bytes()
                    owns_blocks = owns_blocks and bundle.owns_blocks

                # Calculate num_rows from slices
                bundle_rows = sum(s.end_offset - s.start_offset for s in slices)
                if num_rows is None:
                    num_rows = bundle_rows

            merged_schema = _merge_schemas(schemas)
            if merged_schema is not None:
                self._merged_schema = merged_schema

            output_bundle = self._submit_zip_task(
                all_block_refs, all_slices, num_rows, total_size_bytes, owns_blocks
            )
            self._output_buffer.append(output_bundle)
            self._metrics.on_output_queued(output_bundle)

    def _submit_zip_task(
        self,
        all_block_refs: List[List[ObjectRef[Block]]],
        all_slices: List[List[BlockSlice]],
        num_rows: int,
        total_size_bytes: int,
        owns_blocks: bool,
    ) -> RefBundle:
        """Submit a zip task with the calculated slices."""
        zip_remote = cached_remote_fn(_zip_blocks_with_slices, num_returns=2)

        # Convert BlockSlice to tuples for serialization
        slice_tuples = [
            [(s.start_offset, s.end_offset) for s in input_slices]
            for input_slices in all_slices
        ]

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

    merged_blocks = []
    for blocks, slices in zip(block_groups, slice_groups):
        builder = DelegatingBlockBuilder()
        for block_ref, (start, end) in zip(blocks, slices):
            block = ray.get(block_ref)
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
