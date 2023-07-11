from typing import Any, Dict, List, Optional, Tuple

import ray
from ray.data._internal.execution.interfaces import RefBundle, TaskContext
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskScheduler
from ray.data._internal.planner.exchange.shuffle_task_spec import ShuffleTaskSpec
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.split import _split_at_indices
from ray.data._internal.stats import StatsDict
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.types import ObjectRef


class SplitRepartitionTaskScheduler(ExchangeTaskScheduler):
    """
    The split (non-shuffle) repartition scheduler.

    First, we calculate global splits needed to produce `output_num_blocks` blocks.
    After the split blocks are generated accordingly, reduce tasks are scheduled
    to combine split blocks together.
    """

    def execute(
        self,
        refs: List[RefBundle],
        output_num_blocks: int,
        ctx: TaskContext,
        map_ray_remote_args: Optional[Dict[str, Any]] = None,
        reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[RefBundle], StatsDict]:
        input_num_rows = 0
        input_owned_by_consumer = True
        for ref_bundle in refs:
            block_num_rows = ref_bundle.num_rows()
            if block_num_rows is None:
                raise ValueError(
                    "Cannot split partition on blocks with unknown number of rows."
                )
            input_num_rows += block_num_rows
            if not ref_bundle.owns_blocks:
                input_owned_by_consumer = False

        # Compute the (output_num_blocks) indices needed for an equal split of the
        # input blocks. When output_num_blocks=1, the total number of
        # input rows is used as the end index during the split calculation,
        # so that we can combine all input blocks into a single output block.
        indices = []
        if output_num_blocks == 1:
            indices = [input_num_rows]
        else:
            cur_idx = 0
            for _ in range(output_num_blocks - 1):
                cur_idx += input_num_rows / output_num_blocks
                indices.append(int(cur_idx))
        assert len(indices) <= output_num_blocks, (indices, output_num_blocks)

        if map_ray_remote_args is None:
            map_ray_remote_args = {}
        if reduce_ray_remote_args is None:
            reduce_ray_remote_args = {}
        if "scheduling_strategy" not in reduce_ray_remote_args:
            reduce_ray_remote_args = reduce_ray_remote_args.copy()
            reduce_ray_remote_args["scheduling_strategy"] = "SPREAD"

        blocks_with_metadata: List[Tuple[ObjectRef[Block], BlockMetadata]] = []
        for ref_bundle in refs:
            blocks_with_metadata.extend(ref_bundle.blocks)
        split_return = _split_at_indices(
            blocks_with_metadata, indices, input_owned_by_consumer
        )
        split_block_refs, split_metadata = [], []
        for b, m in zip(*split_return):
            split_block_refs.append(b)
            split_metadata.extend(m)

        sub_progress_bar_dict = ctx.sub_progress_bar_dict
        bar_name = ShuffleTaskSpec.SPLIT_REPARTITION_SUB_PROGRESS_BAR_NAME
        assert bar_name in sub_progress_bar_dict, sub_progress_bar_dict
        reduce_bar = sub_progress_bar_dict[bar_name]

        reduce_task = cached_remote_fn(self._exchange_spec.reduce)
        reduce_return = [
            reduce_task.options(**reduce_ray_remote_args, num_returns=2).remote(
                *self._exchange_spec._reduce_args,
                *split_block_refs[j],
            )
            for j in range(output_num_blocks)
            # Only process splits which contain blocks.
            if len(split_block_refs[j]) > 0
        ]

        reduce_block_refs, reduce_metadata = zip(*reduce_return)
        reduce_metadata = reduce_bar.fetch_until_complete(list(reduce_metadata))
        reduce_block_refs, reduce_metadata = list(reduce_block_refs), list(
            reduce_metadata
        )

        # Handle empty blocks.
        if len(reduce_block_refs) < output_num_blocks:
            import pyarrow as pa

            from ray.data._internal.arrow_block import ArrowBlockBuilder
            from ray.data._internal.pandas_block import (
                PandasBlockBuilder,
                PandasBlockSchema,
            )

            num_empty_blocks = output_num_blocks - len(reduce_block_refs)
            first_block_schema = reduce_metadata[0].schema
            if first_block_schema is None:
                raise ValueError(
                    "Cannot split partition on blocks with unknown block format."
                )
            elif isinstance(first_block_schema, pa.Schema):
                builder = ArrowBlockBuilder()
            elif isinstance(first_block_schema, PandasBlockSchema):
                builder = PandasBlockBuilder()
            empty_block = builder.build()
            empty_meta = BlockAccessor.for_block(empty_block).get_metadata(
                input_files=None, exec_stats=None
            )  # No stats for empty block.
            empty_block_refs, empty_metadata = zip(
                *[(ray.put(empty_block), empty_meta) for _ in range(num_empty_blocks)]
            )
            reduce_block_refs.extend(empty_block_refs)
            reduce_metadata.extend(empty_metadata)

        output = []
        for block, meta in zip(reduce_block_refs, reduce_metadata):
            output.append(
                RefBundle([(block, meta)], owns_blocks=input_owned_by_consumer)
            )
        stats = {
            "split": split_metadata,
            "reduce": reduce_metadata,
        }

        return (output, stats)
