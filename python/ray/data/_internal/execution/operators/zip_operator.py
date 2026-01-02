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


class ZipOperator(InternalQueueOperatorMixin, NAryOperator):
    """An operator that zips its inputs together in a streaming fashion.

    This operator handles block alignment internally, accumulating rows from
    each input until the target row count is reached, then zipping them together.
    This avoids the overhead of separate StreamingRepartition operators.
    """

    def __init__(
        self,
        data_context: DataContext,
        target_num_rows_per_block: int,
        *input_ops: PhysicalOperator,
    ):
        """Create a ZipOperator.

        Args:
            data_context: The data context.
            target_num_rows_per_block: Target number of rows per output block.
            input_ops: Operators generating input data for this operator to zip.
        """
        assert len(input_ops) >= 2
        self._target_num_rows = target_num_rows_per_block

        # Pending bundles per input (not yet aligned)
        self._pending_bundles: List[collections.deque[RefBundle]] = [
            collections.deque() for _ in range(len(input_ops))
        ]
        # Track total pending rows per input
        self._pending_rows: List[int] = [0] * len(input_ops)
        # Track if each input is done
        self._input_done: List[bool] = [False] * len(input_ops)

        self._output_buffer: collections.deque[RefBundle] = collections.deque()
        self._output_metadata: List["BlockMetadataWithSchema"] = []
        self._pending_metadata_refs: List[ObjectRef["BlockMetadataWithSchema"]] = []
        self._merged_schema: Optional[Schema] = None
        super().__init__(
            data_context,
            *input_ops,
        )

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
        return sum(
            len(bundle.block_refs) for buf in self._pending_bundles for bundle in buf
        )

    def internal_input_queue_num_bytes(self) -> int:
        return sum(
            bundle.size_bytes() for buf in self._pending_bundles for bundle in buf
        )

    def internal_output_queue_num_blocks(self) -> int:
        return sum(len(bundle.block_refs) for bundle in self._output_buffer)

    def internal_output_queue_num_bytes(self) -> int:
        return sum(bundle.size_bytes() for bundle in self._output_buffer)

    def clear_internal_input_queue(self) -> None:
        """Clear internal input queues."""
        for i, pending_buffer in enumerate(self._pending_bundles):
            while pending_buffer:
                bundle = pending_buffer.popleft()
                self._metrics.on_input_dequeued(bundle)
            self._pending_rows[i] = 0

    def clear_internal_output_queue(self) -> None:
        """Clear internal output queue."""
        while self._output_buffer:
            bundle = self._output_buffer.popleft()
            self._metrics.on_output_dequeued(bundle)

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.has_completed()
        assert 0 <= input_index < len(self._input_dependencies), input_index
        self._pending_bundles[input_index].append(refs)
        self._pending_rows[input_index] += refs.num_rows()
        self._metrics.on_input_queued(refs)
        self._try_build_and_zip()

    def input_done(self, input_index: int) -> None:
        """Called when a specific input is done producing data."""
        self._input_done[input_index] = True
        # Try to flush remaining data
        self._try_build_and_zip(flush_remaining=all(self._input_done))

    def all_inputs_done(self) -> None:
        # Mark all inputs as done and flush
        for i in range(len(self._input_done)):
            self._input_done[i] = True
        self._try_build_and_zip(flush_remaining=True)

        # Verify all pending bundles are consumed
        for i, pending_buffer in enumerate(self._pending_bundles):
            if len(pending_buffer) > 0:
                raise ValueError(
                    f"Input {i} has {self._pending_rows[i]} remaining rows "
                    f"after all inputs done. Datasets must have same row count."
                )
        super().all_inputs_done()

    def has_next(self) -> bool:
        return len(self._output_buffer) > 0

    def _get_next_inner(self) -> RefBundle:
        refs = self._output_buffer.popleft()
        self._metrics.on_output_dequeued(refs)
        return refs

    def get_stats(self) -> StatsDict:
        # Resolve any pending metadata refs in batch (deferred from _zip_bundles)
        if self._pending_metadata_refs:
            resolved = ray.get(self._pending_metadata_refs)
            self._output_metadata.extend(resolved)
            self._pending_metadata_refs = []
        if not self._output_metadata:
            return {}
        return {self._name: to_stats(self._output_metadata)}

    def implements_accurate_memory_accounting(self):
        return True

    def _try_build_and_zip(self, flush_remaining: bool = False) -> None:
        """Try to build aligned bundles and zip them.

        This method accumulates rows from each input until we have at least
        target_num_rows from ALL inputs, then extracts exactly target_num_rows
        from each input and zips them together.
        """
        while True:
            # Check if all inputs have enough rows
            min_pending = min(self._pending_rows)

            if min_pending >= self._target_num_rows:
                # We can build a full target-sized bundle
                rows_to_take = self._target_num_rows
            elif flush_remaining and min_pending > 0:
                # All inputs done, flush remaining rows
                rows_to_take = min_pending
            else:
                # Not enough rows yet
                break

            # Extract exactly rows_to_take from each input
            aligned_bundles = []
            for i in range(len(self._pending_bundles)):
                bundle, fully_consumed = self._extract_rows(i, rows_to_take)
                aligned_bundles.append(bundle)
                # Only report bundles that were fully consumed (actually in the queue)
                for consumed_bundle in fully_consumed:
                    self._metrics.on_input_dequeued(consumed_bundle)

            # Merge schemas
            merged_schema = _merge_schemas([b.schema for b in aligned_bundles])
            if merged_schema is not None:
                self._merged_schema = merged_schema

            # Zip the aligned bundles
            output_bundle = self._zip_aligned_bundles(aligned_bundles)
            self._output_buffer.append(output_bundle)
            self._metrics.on_output_queued(output_bundle)

    def _extract_rows(
        self, input_index: int, num_rows: int
    ) -> Tuple[RefBundle, List[RefBundle]]:
        """Extract exactly num_rows from the pending bundles for an input.

        Returns:
            A tuple of (extracted_bundle, fully_consumed_bundles) where:
            - extracted_bundle: A RefBundle containing exactly num_rows
            - fully_consumed_bundles: List of original bundles that were fully consumed
              (these are the bundles that were originally added to the input queue)
        """
        pending = self._pending_bundles[input_index]
        extracted_blocks: List[Tuple[ObjectRef[Block], BlockMetadata]] = []
        extracted_slices: List[Optional[BlockSlice]] = []
        fully_consumed_bundles: List[RefBundle] = []
        rows_remaining = num_rows
        schema = None
        owns_blocks = True

        while rows_remaining > 0 and pending:
            bundle = pending[0]
            bundle_rows = bundle.num_rows()

            if schema is None:
                schema = bundle.schema

            if bundle_rows <= rows_remaining:
                # Take the whole bundle
                pending.popleft()
                # Only track as consumed if this was an original bundle (not a slice remainder)
                # We detect this by checking if it's in our tracked original bundles
                fully_consumed_bundles.append(bundle)
                for (block_ref, meta), block_slice in zip(bundle.blocks, bundle.slices):
                    extracted_blocks.append((block_ref, meta))
                    extracted_slices.append(block_slice)
                rows_remaining -= bundle_rows
                self._pending_rows[input_index] -= bundle_rows
                owns_blocks = owns_blocks and bundle.owns_blocks
            else:
                # Need to slice this bundle
                # Take rows_remaining rows from the front
                sliced_bundle, remaining_bundle = bundle.slice(rows_remaining)
                pending.popleft()
                # The original bundle is now consumed (replaced by remaining_bundle)
                fully_consumed_bundles.append(bundle)
                pending.appendleft(remaining_bundle)
                # Re-add remaining_bundle to metrics since it's still pending
                self._metrics.on_input_queued(remaining_bundle)

                for (block_ref, meta), block_slice in zip(
                    sliced_bundle.blocks, sliced_bundle.slices
                ):
                    extracted_blocks.append((block_ref, meta))
                    extracted_slices.append(block_slice)

                self._pending_rows[input_index] -= rows_remaining
                rows_remaining = 0
                owns_blocks = owns_blocks and bundle.owns_blocks

        extracted_bundle = RefBundle(
            blocks=tuple(extracted_blocks),
            schema=schema,
            owns_blocks=owns_blocks,
            slices=extracted_slices,
        )

        return extracted_bundle, fully_consumed_bundles

    def _zip_aligned_bundles(self, bundles: List[RefBundle]) -> RefBundle:
        """Zip aligned bundles together in a single remote task.

        The bundles may contain slice metadata, which will be applied in the
        remote task before zipping.
        """
        assert len(bundles) >= 2
        zip_bundle_group_remote = cached_remote_fn(
            _zip_bundle_group_remote, num_returns=2
        )

        # Prepare block groups with slice info
        block_groups = []
        slice_groups = []
        for bundle in bundles:
            block_groups.append([block for block, _ in bundle.blocks])
            slice_groups.append(bundle.slices)

        result_block, meta_with_schema_ref = zip_bundle_group_remote.remote(
            block_groups, slice_groups
        )

        self._pending_metadata_refs.append(meta_with_schema_ref)

        for bundle in bundles:
            bundle.destroy_if_owned()

        owns_blocks = all(bundle.owns_blocks for bundle in bundles)

        estimated_metadata = BlockMetadata(
            num_rows=bundles[0].num_rows(),
            size_bytes=sum(bundle.size_bytes() for bundle in bundles),
            input_files=None,
            exec_stats=None,
        )

        return RefBundle(
            [
                (
                    result_block,
                    estimated_metadata,
                )
            ],
            owns_blocks=owns_blocks,
            schema=self._merged_schema,
        )


def _zip_bundle_group_remote(
    block_groups: List[List[Block]],
    slice_groups: List[List[Optional[BlockSlice]]],
) -> Tuple[Block, "BlockMetadataWithSchema"]:
    """Remote task that applies slices and zips blocks together.

    This combines the slicing (previously done in StreamingRepartition) and
    zipping into a single remote task, avoiding intermediate materialization.
    """
    stats = BlockExecStats.builder()

    merged_blocks = []
    for blocks, slices in zip(block_groups, slice_groups):
        builder = DelegatingBlockBuilder()
        for block_ref, block_slice in zip(blocks, slices):
            block = ray.get(block_ref)
            if block_slice is not None:
                # Apply slice
                accessor = BlockAccessor.for_block(block)
                block = accessor.slice(
                    block_slice.start_offset, block_slice.end_offset, copy=False
                )
            builder.add_block(block)
        merged_blocks.append(builder.build())

    result = merged_blocks[0]
    for other in merged_blocks[1:]:
        result = BlockAccessor.for_block(result).zip(other)

    from ray.data.block import BlockMetadataWithSchema

    return result, BlockMetadataWithSchema.from_block(result, stats=stats.build())
