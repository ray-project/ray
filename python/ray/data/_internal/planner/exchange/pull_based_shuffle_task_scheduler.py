import logging
from typing import Any, Dict, List, Optional, Tuple

from ray._private.ray_constants import CALLER_MEMORY_USAGE_PER_OBJECT_REF
from ray.data._internal.execution.interfaces import RefBundle, TaskContext
from ray.data._internal.planner.exchange.interfaces import (
    ExchangeTaskScheduler,
    ExchangeTaskSpec,
)
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import StatsDict
from ray.data._internal.util import convert_bytes_to_human_readable_str

logger = logging.getLogger(__name__)


class PullBasedShuffleTaskScheduler(ExchangeTaskScheduler):
    """
    The pull-based map-reduce shuffle scheduler.

    Map tasks are first scheduled to generate map output blocks. After all map output
    are generated, then reduce tasks are scheduled to combine map output blocks
    together.

    The concept here is similar to
    "MapReduce: Simplified Data Processing on Large Clusters"
    (https://dl.acm.org/doi/10.1145/1327452.1327492).
    """

    def execute(
        self,
        refs: List[RefBundle],
        output_num_blocks: int,
        task_ctx: TaskContext,
        map_ray_remote_args: Optional[Dict[str, Any]] = None,
        reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
        _debug_limit_execution_to_num_blocks: Optional[int] = None,
    ) -> Tuple[List[RefBundle], StatsDict]:

        # TODO: eagerly delete the input and map output block references in order to
        # eagerly release the blocks' memory.
        input_blocks_list = []
        for ref_bundle in refs:
            input_blocks_list.extend(ref_bundle.block_refs)
        input_num_blocks = len(input_blocks_list)
        input_owned = all(b.owns_blocks for b in refs)

        caller_memory_usage = (
            input_num_blocks * output_num_blocks * CALLER_MEMORY_USAGE_PER_OBJECT_REF
        )
        self.warn_on_driver_memory_usage(
            caller_memory_usage,
            "Execution is estimated to use at least "
            f"{convert_bytes_to_human_readable_str(caller_memory_usage)} "
            "of driver memory. Ensure that the driver machine has at least "
            "this much memory to ensure job completion.\n\n"
            "To reduce the "
            "amount of driver memory needed, enable push-based shuffle using "
            "RAY_DATA_PUSH_BASED_SHUFFLE=1 "
            "(https://docs.ray.io/en/latest/data/performance-tips.html"
            ").",
        )

        if map_ray_remote_args is None:
            map_ray_remote_args = {}
        if reduce_ray_remote_args is None:
            reduce_ray_remote_args = {}
        if "scheduling_strategy" not in reduce_ray_remote_args:
            reduce_ray_remote_args = reduce_ray_remote_args.copy()
            reduce_ray_remote_args["scheduling_strategy"] = "SPREAD"

        shuffle_map = cached_remote_fn(self._exchange_spec.map)
        shuffle_reduce = cached_remote_fn(self._exchange_spec.reduce)

        sub_progress_bar_dict = task_ctx.sub_progress_bar_dict
        bar_name = ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME
        assert bar_name in sub_progress_bar_dict, sub_progress_bar_dict
        map_bar = sub_progress_bar_dict[bar_name]

        if _debug_limit_execution_to_num_blocks is not None:
            input_blocks_list = input_blocks_list[:_debug_limit_execution_to_num_blocks]
            logger.debug(f"Limiting execution to {len(input_blocks_list)} map tasks")
        shuffle_map_out = [
            shuffle_map.options(
                **map_ray_remote_args,
                num_returns=1 + output_num_blocks,
            ).remote(i, block, output_num_blocks, *self._exchange_spec._map_args)
            for i, block in enumerate(input_blocks_list)
        ]

        # The first item returned is the BlockMetadata.
        shuffle_map_metadata = []
        for i, refs in enumerate(shuffle_map_out):
            shuffle_map_metadata.append(refs[-1])
            shuffle_map_out[i] = refs[:-1]

        if _debug_limit_execution_to_num_blocks is not None:
            while len(shuffle_map_out) < output_num_blocks:
                # Repeat the first map task's results.
                shuffle_map_out.append(shuffle_map_out[0][:])

        shuffle_map_metadata = map_bar.fetch_until_complete(shuffle_map_metadata)

        self.warn_on_high_local_memory_store_usage()

        bar_name = ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME
        assert bar_name in sub_progress_bar_dict, sub_progress_bar_dict
        reduce_bar = sub_progress_bar_dict[bar_name]

        if _debug_limit_execution_to_num_blocks is not None:
            output_num_blocks = _debug_limit_execution_to_num_blocks
            logger.debug(f"Limiting execution to {output_num_blocks} reduce tasks")
        shuffle_reduce_out = [
            shuffle_reduce.options(**reduce_ray_remote_args, num_returns=2).remote(
                *self._exchange_spec._reduce_args,
                *[shuffle_map_out[i][j] for i in range(input_num_blocks)],
            )
            for j in range(output_num_blocks)
        ]

        # Release map task outputs from the Ray object store.
        del shuffle_map_out

        new_blocks, new_metadata = [], []
        if shuffle_reduce_out:
            new_blocks, new_metadata = zip(*shuffle_reduce_out)
        new_metadata = reduce_bar.fetch_until_complete(list(new_metadata))

        self.warn_on_high_local_memory_store_usage()

        output = []
        for block, meta in zip(new_blocks, new_metadata):
            output.append(
                RefBundle(
                    [
                        (
                            block,
                            meta,
                        )
                    ],
                    owns_blocks=input_owned,
                )
            )
        stats = {
            "map": shuffle_map_metadata,
            "reduce": new_metadata,
        }

        return (output, stats)
