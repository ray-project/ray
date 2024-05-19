import logging
import time
from typing import Any, Dict, List, Optional, Tuple, TypeVar

import ray
from ray.data._internal.execution.interfaces import RefBundle, TaskContext
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskScheduler
from ray.data._internal.planner.exchange.repartition_task_spec import (
    RepartitionByColumnTaskSpec,
)
from ray.data._internal.repartition_by_column import repartition_runner
from ray.data._internal.stats import StatsDict
from ray.data.block import BlockMetadata

logger = logging.getLogger(__name__)


KeyType = TypeVar("KeyType")


class RepartitionByColumnTaskScheduler(ExchangeTaskScheduler):
    """Split-by-column experiment"""

    def execute(
        self,
        refs: List[RefBundle],
        output_num_blocks: int,
        ctx: TaskContext,
        map_ray_remote_args: Optional[Dict[str, Any]] = None,
        reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[RefBundle], StatsDict]:
        """
        Args:
            output_num_blocks: not used as it's determined by the actual split
        """
        time_start = time.perf_counter()

        input_owned_by_consumer = all(rb.owns_blocks for rb in refs)

        logger.info(f"number of RefBundles = {len(refs)}")

        if map_ray_remote_args is None:
            map_ray_remote_args = {}
        if reduce_ray_remote_args is None:
            reduce_ray_remote_args = {}
        if "scheduling_strategy" not in reduce_ray_remote_args:
            reduce_ray_remote_args = reduce_ray_remote_args.copy()
            reduce_ray_remote_args["scheduling_strategy"] = "SPREAD"

        # TODO: currently, 'concurrency' only means number of actors.
        num_actors = self._exchange_spec._map_args[1]

        ref_id = 0
        # drop the metadata
        all_blocks = [b for ref_bundle in refs for b, _ in ref_bundle.blocks]

        total_number_of_rows_input = sum(
            [m.num_rows for ref_bundle in refs for _, m in ref_bundle.blocks]
        )
        logger.info(f"total number of rows input = {total_number_of_rows_input}")

        tstart = time.perf_counter()
        result_refs = list(
            repartition_runner(
                ref_id,
                all_blocks,
                self._exchange_spec._map_args,
            )
        )
        tend = time.perf_counter()
        logger.info(
            f"Finished repartitioning {len(result_refs)=}, ({(tend-tstart):.2f}s)"
        )

        # all_keys = []
        # for _ in range(num_actors):
        #     all_keys.append(result_refs.pop())
        all_keys, result_refs = result_refs[-num_actors:], result_refs[:-num_actors]
        all_metadata, result_refs = result_refs[-num_actors:], result_refs[:-num_actors]
        # all_metadata = []
        # for _ in range(num_actors):
        #     all_metadata.append(result_refs.pop())

        sub_progress_bar_dict = ctx.sub_progress_bar_dict
        bar_name = RepartitionByColumnTaskSpec.SPLIT_SUB_PROGRESS_BAR_NAME
        assert bar_name in sub_progress_bar_dict, sub_progress_bar_dict
        # map_bar = sub_progress_bar_dict[bar_name]

        # all_metadata = map_bar.fetch_until_complete(all_metadata)
        all_metadata = ray.get(all_metadata)
        all_metadata: List[BlockMetadata] = [
            m for metadata in all_metadata for m in metadata
        ]
        time_mid = time.perf_counter()
        logger.info(
            f"repartition time (up to all_metadata)= {(time_mid - time_start):.4}s"
        )

        # all_keys = ray.get(all_keys)
        # all_keys = [m for keys in all_keys for m in keys]

        all_blocks = [
            RefBundle([(block, meta)], input_owned_by_consumer)
            for block, meta in zip(result_refs, all_metadata)
        ]

        assert (
            len(all_blocks)
            == len(all_metadata)
            #     len(all_blocks) == len(all_metadata) == len(all_keys)
        ), f"{len(all_blocks)=}, {len(all_metadata)=}, {len(all_keys)=}"

        logger.info(f"number of output blocks = {len(all_blocks)}")
        # # logger.info(f"number of keys = {len(all_keys)}")
        # total_number_of_rows = sum(
        #     [stat.num_rows for stat in all_metadata if stat.num_rows is not None]
        # )
        # logger.info(f"total number of rows = {total_number_of_rows}")

        # TODO: add progress bar
        # TODO: use reduce_bar.fetch_until_complete etc
        # TODO: handle block metadata better

        stats = {"repartition": all_metadata}

        time_end = time.perf_counter()
        logger.info(f"repartition time = {(time_end - time_start):.4}s")

        return (all_blocks, stats)
