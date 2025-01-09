import logging
from collections import deque
from typing import Deque, Dict, Generator, List, Union

import ray
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    MetadataOpTask,
    OpTask,
    PhysicalOperator,
)
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.execution.operators.base_physical_operator import (
    OneToOneOperator,
)
from ray.data._internal.stats import StatsDict
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.context import DataContext
from ray.types import ObjectRef

logger = logging.getLogger(__name__)


@ray.remote
def _partition_block(
    max_num_rows_per_block: int,
    *input_blocks: Block,
) -> Generator[Union[List[BlockMetadata], Block], None, None]:
    """
    Partition input blocks by maximum number of rows granularity using a generator
    pattern.

    Args:
        max_num_rows_per_block: The maximum number of rows granularity for partition.
        input_blocks: The input blocks to partition.

    Yields:
        - A list of all BlockMetadata as the first yield.
        - Subsequent yields are individual blocks.
    """

    # Initialize lists to store metadata and partitioned blocks
    metadata_list: List[BlockMetadata] = []
    block_list: List[Block] = []

    # Initialize a builder to accumulate rows into new blocks
    cur_block_builder = DelegatingBlockBuilder()

    # Iterate over each input block and partition based on max_num_rows_per_block
    for block in input_blocks:

        # Create an accessor to interact with the current block data
        accessor = BlockAccessor.for_block(block)

        # Get the number of rows in the current block
        num_rows_in_block = accessor.num_rows()

        # Initialize the starting index for partitioning the current block
        start_idx_in_block = 0

        # Continue partitioning the current block until all rows are processed
        while start_idx_in_block < num_rows_in_block:

            # Calculate the remaining rows to process in the current block
            remaining_rows_in_block = num_rows_in_block - start_idx_in_block

            # Get the current number of rows in the block being built
            num_rows_in_cur_block = cur_block_builder.num_rows()

            # If the remaining rows plus the current block's rows do not exceed the
            # max limit
            if remaining_rows_in_block + num_rows_in_cur_block < max_num_rows_per_block:

                # Add remaining rows to the current block builder
                end_idx_in_block = start_idx_in_block + remaining_rows_in_block
                cur_block_builder.add_block(
                    accessor.slice(start_idx_in_block, end_idx_in_block, copy=True)
                )
                break  # Exit the while loop as no partitioning is needed yet

            # If the current block can exactly match the max row limit
            elif (
                remaining_rows_in_block + num_rows_in_cur_block
                == max_num_rows_per_block
            ):

                # Partition the current block and create a new one
                end_idx_in_block = start_idx_in_block + remaining_rows_in_block
                cur_block_builder.add_block(
                    accessor.slice(start_idx_in_block, end_idx_in_block, copy=True)
                )

                # Finalize the partitioned block and store its metadata and content
                partitioned_block = cur_block_builder.build()
                metadata = BlockAccessor.for_block(partitioned_block).get_metadata()
                metadata_list.append(metadata)
                block_list.append(partitioned_block)

                # Update the starting index for the next partition
                start_idx_in_block += remaining_rows_in_block

                # Reset the builder for the next partition
                cur_block_builder = DelegatingBlockBuilder()

            # If the current block exceeds the max row limit, partition it
            else:

                # Partition the block into smaller pieces based on the max row limit
                end_idx_in_block = start_idx_in_block + max_num_rows_per_block
                cur_block_builder.add_block(
                    accessor.slice(start_idx_in_block, end_idx_in_block, copy=True)
                )

                # Finalize the partitioned block and store its metadata and content
                partitioned_block = cur_block_builder.build()
                metadata = BlockAccessor.for_block(partitioned_block).get_metadata()
                metadata_list.append(metadata)
                block_list.append(partitioned_block)

                # Update the starting index for the next partition
                start_idx_in_block += max_num_rows_per_block

                # Reset the builder for the next partition
                cur_block_builder = DelegatingBlockBuilder()

    # After processing all blocks, check if there are any remaining rows to finalize
    if cur_block_builder.num_rows() > 0:

        # Finalize and store any remaining block
        partitioned_block = cur_block_builder.build()
        metadata = BlockAccessor.for_block(partitioned_block).get_metadata()
        metadata_list.append(metadata)
        block_list.append(partitioned_block)

    # Yield the list of all metadata first
    yield metadata_list

    # Yield the partitioned blocks after the metadata
    for block in block_list:
        yield block


class StreamingRepartitionOperator(OneToOneOperator):
    """Physical operator that implements streaming repartition."""

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        max_num_rows_per_block: int,
    ):
        name = f"StreamingRepartition(max_num_rows_per_block={max_num_rows_per_block})"
        super().__init__(
            name=name,
            input_op=input_op,
            data_context=data_context,
            target_max_block_size=None,
        )
        self._max_num_rows_per_block = max_num_rows_per_block

        # TODO: (srinathk) Need to expose these configurations?
        # Args for repartition_block.
        self._repartition_task_ray_remote_args = {
            "num_cpus": 1.0,
            "num_returns": "streaming",
            "max_retries": -1,
        }

        # Output queue after stream repartition
        self._output_queue: Deque[RefBundle] = deque()

        # In-flight stream repartition tasks, keyed by their task indexes.
        self._repartition_tasks: Dict[int, MetadataOpTask] = {}
        self._repartition_task_next_idx = 0

    def _add_input_inner(self, input_refs: RefBundle, input_index: int) -> None:
        assert input_index == 0, input_index
        self._metrics.on_input_queued(input_refs)

        if input_refs.num_rows() < self._max_num_rows_per_block:
            # Skip partitioning given num_rows is under max_num_rows_per_block
            # threshold.
            self._output_queue.append(input_refs)
            self._metrics.on_output_queued(input_refs)
            if self._all_repartition_tasks_done():
                logger.debug(
                    f"Streaming repartition for max_num_rows_per_block: "
                    f"{self._max_num_rows_per_block} completed."
                )
            return

        obj_generator_refs = _partition_block.options(
            **self._repartition_task_ray_remote_args,
        ).remote(
            self._max_num_rows_per_block,
            *input_refs.block_refs,
        )

        metadata_list_ref: ObjectRef[List[BlockMetadata]] = None
        block_refs: List[ObjectRef[Block]] = []

        for idx, result in enumerate(obj_generator_refs):
            if idx == 0:
                # First yield: metadata list
                metadata_list_ref = result
            else:
                # Subsequent yields: blocks
                block_refs.append(result)

        task_idx = self._repartition_task_next_idx
        self._repartition_task_next_idx += 1

        def _on_task_complete():
            nonlocal input_refs, block_refs, metadata_list_ref
            self._repartition_tasks.pop(task_idx)

            # Construct RefBundles.
            metadata_list = ray.get(metadata_list_ref)
            ref_bundles = [
                RefBundle([(block_ref, metadata)], owns_blocks=True)
                for block_ref, metadata in zip(block_refs, metadata_list)
            ]

            # Move blocks to output_queue.
            for _, ref_bundle in enumerate(ref_bundles):
                if ref_bundle.num_rows() == 0:
                    continue
                self._metrics.on_input_queued(ref_bundle)
                self._output_queue.append(ref_bundle)
                self._metrics.on_output_queued(ref_bundle)

            if self._all_repartition_tasks_done():
                logger.debug(
                    f"Streaming repartition for max_num_rows_per_block: "
                    f"{self._max_num_rows_per_block} completed."
                )

            # Update metrics.
            input_refs.destroy_if_owned()
            self._metrics.on_input_dequeued(input_refs)

        # TODO(srinathk): we use `MetadataOpTask` here because `DataOpTask` requires
        # the task to return a streaming generator. We should rename them
        # to avoid confusion.
        self._repartition_tasks[task_idx] = MetadataOpTask(
            task_idx,
            metadata_list_ref,
            _on_task_complete,
        )

    def start(self, options: ExecutionOptions) -> None:
        return super().start(options)

    def shutdown(self) -> None:
        return super().shutdown()

    def _all_repartition_tasks_done(self):
        return self._inputs_complete and len(self._repartition_tasks) == 0

    def all_inputs_done(self):
        super().all_inputs_done()
        logger.debug(
            f"Streaming repartition for max_num_rows_per_block: "
            f"{self._max_num_rows_per_block} kicked off for all inputs."
        )

    def get_stats(self) -> StatsDict:
        return {}

    def has_next(self) -> bool:
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        out = self._output_queue.popleft()
        self._metrics.on_output_dequeued(out)
        return out

    def get_active_tasks(self) -> List[OpTask]:
        res: List[OpTask] = []
        res.extend(self._repartition_tasks.values())
        return res

    def progress_str(self) -> str:
        return f"{len(self._repartition_tasks)} repartition tasks"

    def implements_accurate_memory_accounting(self) -> bool:
        return True

    def current_processor_usage(self) -> ExecutionResources:
        # Count stream repartition tasks.
        num_cpus = (
            len(self._repartition_tasks)
            * self._repartition_task_ray_remote_args["num_cpus"]
        )
        return ExecutionResources(
            cpu=num_cpus,
            gpu=0,
        )

    def base_resource_usage(self) -> ExecutionResources:
        return ExecutionResources()

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(
            cpu=self._repartition_task_ray_remote_args.get("num_cpus", 0),
            gpu=0,
            object_store_memory=0,
        )
