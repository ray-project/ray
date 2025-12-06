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
from ray.data.block import Block, BlockAccessor, BlockExecStats, to_stats
from ray.data.context import DataContext

if TYPE_CHECKING:

    from ray.data.block import BlockMetadataWithSchema


class ZipOperator(InternalQueueOperatorMixin, NAryOperator):
    """An operator that zips its inputs together in a streaming fashion."""

    def __init__(
        self,
        data_context: DataContext,
        *input_ops: PhysicalOperator,
    ):
        """Create a ZipOperator.

        Args:
            input_ops: Operators generating input data for this operator to zip.
        """
        assert len(input_ops) >= 2
        self._input_buffers: List[collections.deque[RefBundle]] = [
            collections.deque() for _ in range(len(input_ops))
        ]
        self._output_buffer: collections.deque[RefBundle] = collections.deque()
        self._output_metadata: List["BlockMetadataWithSchema"] = []
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
            len(bundle.block_refs) for buf in self._input_buffers for bundle in buf
        )

    def internal_input_queue_num_bytes(self) -> int:
        return sum(bundle.size_bytes() for buf in self._input_buffers for bundle in buf)

    def internal_output_queue_num_blocks(self) -> int:
        return sum(len(bundle.block_refs) for bundle in self._output_buffer)

    def internal_output_queue_num_bytes(self) -> int:
        return sum(bundle.size_bytes() for bundle in self._output_buffer)

    def clear_internal_input_queue(self) -> None:
        """Clear internal input queues."""
        for input_buffer in self._input_buffers:
            while input_buffer:
                bundle = input_buffer.popleft()
                self._metrics.on_input_dequeued(bundle)

    def clear_internal_output_queue(self) -> None:
        """Clear internal output queue."""
        while self._output_buffer:
            bundle = self._output_buffer.popleft()
            self._metrics.on_output_dequeued(bundle)

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert not self.completed()
        assert 0 <= input_index <= len(self._input_dependencies), input_index
        self._input_buffers[input_index].append(refs)
        self._metrics.on_input_queued(refs)
        self._try_zip_ready_bundles()

    def all_inputs_done(self) -> None:

        self._try_zip_ready_bundles()
        assert all(len(buffer) == 0 for buffer in self._input_buffers), [
            len(buffer) for buffer in self._input_buffers
        ]
        super().all_inputs_done()

    def has_next(self) -> bool:
        return len(self._output_buffer) > 0

    def _get_next_inner(self) -> RefBundle:
        refs = self._output_buffer.popleft()
        self._metrics.on_output_dequeued(refs)
        return refs

    def get_stats(self) -> StatsDict:
        if not self._output_metadata:
            return {}
        return {self._name: to_stats(self._output_metadata)}

    def implements_accurate_memory_accounting(self):
        return True

    def _try_zip_ready_bundles(self) -> None:
        """Zip bundles whenever each input has data available."""
        while all(buffer and len(buffer) > 0 for buffer in self._input_buffers):
            bundles = [buffer.popleft() for buffer in self._input_buffers]
            assert all(
                bundle.num_rows() == bundles[0].num_rows() for bundle in bundles
            ), [bundle.num_rows() for bundle in bundles]

            for bundle in bundles:
                self._metrics.on_input_dequeued(bundle)

            output_bundle = self._zip_bundles(bundles)
            self._output_buffer.append(output_bundle)
            self._metrics.on_output_queued(output_bundle)

    def _zip_bundles(self, bundles: List[RefBundle]) -> RefBundle:
        assert len(bundles) >= 2
        zip_bundle_group_remote = cached_remote_fn(
            _zip_bundle_group_remote, num_returns=2
        )
        block_groups = [[block for block, _ in bundle.blocks] for bundle in bundles]

        result_block, meta_with_schema = zip_bundle_group_remote.remote(block_groups)
        meta_with_schema = ray.get(meta_with_schema)

        for bundle in bundles:
            bundle.destroy_if_owned()

        self._output_metadata.append(meta_with_schema)

        owns_blocks = all(bundle.owns_blocks for bundle in bundles)

        return RefBundle(
            [
                (
                    result_block,
                    meta_with_schema.metadata,
                )
            ],
            owns_blocks=owns_blocks,
            schema=meta_with_schema.schema,
        )


def _zip_bundle_group_remote(
    block_groups: List[List[Block]],
) -> Tuple[Block, "BlockMetadataWithSchema"]:
    stats = BlockExecStats.builder()

    merged_blocks = []
    for blocks in block_groups:
        builder = DelegatingBlockBuilder()
        for block in blocks:
            builder.add_block(ray.get(block))
        merged_blocks.append(builder.build())

    result = merged_blocks[0]
    for other in merged_blocks[1:]:
        result = BlockAccessor.for_block(result).zip(other)

    from ray.data.block import BlockMetadataWithSchema

    return result, BlockMetadataWithSchema.from_block(result, stats=stats.build())
