import logging
import math
from typing import Callable, List, Optional, Dict, Any, Tuple, TypeVar, Union

import ray
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.data.block import Block, BlockAccessor, BlockMetadata, BlockExecStats
from ray.data.impl.shuffle import ShuffleOp
from ray.data.impl.progress_bar import ProgressBar
from ray.data.impl.block_list import BlockList
from ray.data.impl.remote_fn import cached_remote_fn

logger = logging.getLogger(__name__)


T = TypeVar("T")
U = TypeVar("U")


class _PushBasedShuffleTaskSchedule:
    def __init__(
        self,
        output_num_blocks: int,
        num_rounds: int,
        num_map_tasks_per_round: int,
        num_merge_tasks_per_round: int,
        merge_task_placement: List[str],
    ):
        self.num_rounds = num_rounds
        self.num_map_tasks_per_round = num_map_tasks_per_round
        self.num_merge_tasks_per_round = num_merge_tasks_per_round
        self.merge_task_placement = merge_task_placement
        self._merge_task_options = [
            {"scheduling_strategy": NodeAffinitySchedulingStrategy(node_id, soft=True)}
            for node_id in self.merge_task_placement
        ]

        self._compute_reducers_per_merge_task(output_num_blocks)

    def get_num_reducers_per_merge_idx(self, merge_idx: int) -> int:
        """
        Each intermediate merge task will produce outputs for a partition of P
        final reduce tasks. This helper function returns P based on the merge
        task index.
        """
        return self.num_reducers_per_merge_idx[merge_idx]

    def get_merge_idx_for_reducer_idx(self, reducer_idx: int) -> int:
        return self.reducer_idx_to_merge_idx[reducer_idx]

    def get_merge_task_options(self, merge_idx):
        return self._merge_task_options[merge_idx]

    def _compute_reducers_per_merge_task(self, output_num_blocks):
        """
        Each intermediate merge task will produce outputs for a partition of P
        final reduce tasks. This helper function computes P based on the merge
        task index.
        """
        self.num_reducers_per_merge_idx = []
        self.reducer_idx_to_merge_idx = []
        for merge_idx in range(self.num_merge_tasks_per_round):
            partition_size = output_num_blocks // self.num_merge_tasks_per_round
            extra_blocks = output_num_blocks % self.num_merge_tasks_per_round
            if merge_idx < extra_blocks:
                partition_size += 1

            self.num_reducers_per_merge_idx.append(partition_size)
            for _ in range(partition_size):
                if len(self.reducer_idx_to_merge_idx) < output_num_blocks:
                    self.reducer_idx_to_merge_idx.append(merge_idx)


class PushBasedShufflePlan(ShuffleOp):
    """
    Push-based shuffle merges intermediate map outputs on the reducer nodes
    while other map tasks are executing. The merged outputs are merged again
    during a final reduce stage. This works as follows:

    1. Submit rounds of concurrent map and merge tasks until all map inputs
    have been processed. In each round, we execute:

       M map tasks
         Each produces N outputs. Each output contains P blocks.
       N merge tasks
         Takes 1 output from each of M map tasks.
         Each produces P outputs.
       Where M and N are chosen to maximize parallelism across CPUs. Note that
       this assumes that all CPUs in the cluster will be dedicated to the
       shuffle job.

       Map and merge tasks are pipelined so that we always merge the previous
       round of map outputs while executing the next round of map tasks.

    2. In the final reduce stage:
       R reduce tasks
         Takes 1 output from one of the merge tasks from every round.

    Notes:
        N * P = R = total number of output blocks
        M / N = merge factor - the ratio of map : merge tasks is to improve
          pipelined parallelism. For example, if map takes twice as long to
          execute as merge, then we should set this to 2.
        See paper at https://arxiv.org/abs/2203.05072 for more details.
    """

    def execute(
        self,
        input_blocks: BlockList,
        output_num_blocks: int,
        clear_input_blocks: bool,
        *,
        map_ray_remote_args: Optional[Dict[str, Any]] = None,
        reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
        merge_factor: int = 2,
    ) -> Tuple[BlockList, Dict[str, List[BlockMetadata]]]:
        logger.info("Using experimental push-based shuffle.")
        # TODO(swang): For jobs whose reduce work is heavier than the map work,
        # we should support fractional merge factors.
        # TODO(swang): For large jobs, we should try to choose the merge factor
        # automatically, e.g., by running one test round of map and merge tasks
        # and comparing their run times.
        # TODO(swang): Add option to automatically reduce write amplification
        # during map-merge stage, by limiting how many partitions can be
        # processed concurrently.
        input_blocks_list = input_blocks.get_blocks()
        # Preemptively clear the blocks list since we will incrementally delete
        # the last remaining references as we submit the dependent map tasks
        # during the map-merge stage.
        if clear_input_blocks:
            input_blocks.clear()

        if map_ray_remote_args is None:
            map_ray_remote_args = {}
        if reduce_ray_remote_args is None:
            reduce_ray_remote_args = {}
        # The placement strategy for reduce tasks is overwritten to colocate
        # them with their inputs from the merge stage, so remove any
        # pre-specified scheduling strategy here.
        reduce_ray_remote_args = reduce_ray_remote_args.copy()
        reduce_ray_remote_args.pop("scheduling_strategy", None)

        map_fn = self._map_partition
        merge_fn = self._merge

        def map_partition(*args, **kwargs):
            return map_fn(self.map, *args, **kwargs)

        def merge(*args, **kwargs):
            return merge_fn(self.reduce, *args, **kwargs)

        shuffle_map = cached_remote_fn(map_partition)
        shuffle_merge = cached_remote_fn(merge)

        def submit_map_task(arg):
            mapper_idx, block = arg
            # NOTE(swang): Results are shuffled between map and merge tasks, so
            # there is no advantage to colocating specific map and merge tasks.
            # Therefore, we do not specify a node affinity policy for map tasks
            # in case the caller or Ray has a better scheduling strategy, e.g.,
            # based on data locality.
            map_result = shuffle_map.options(
                **map_ray_remote_args,
                num_returns=1 + schedule.num_merge_tasks_per_round,
            ).remote(
                mapper_idx,
                block,
                output_num_blocks,
                schedule,
                *self._map_args,
            )
            metadata_ref = map_result.pop(0)
            return metadata_ref, map_result

        def submit_merge_task(arg):
            merge_idx, map_results = arg
            num_merge_returns = schedule.get_num_reducers_per_merge_idx(merge_idx)
            merge_result = shuffle_merge.options(
                num_returns=1 + num_merge_returns,
                **schedule.get_merge_task_options(merge_idx),
            ).remote(
                *map_results,
                reduce_args=self._reduce_args,
            )
            metadata_ref = merge_result.pop(0)
            return metadata_ref, merge_result

        # Compute all constants used for task scheduling.
        num_cpus_per_node_map = _get_num_cpus_per_node_map()
        schedule = self._compute_shuffle_schedule(
            num_cpus_per_node_map,
            len(input_blocks_list),
            merge_factor,
            output_num_blocks,
        )

        # ObjectRef results from the last round of tasks. Used to add
        # backpressure during pipelining of map and merge tasks.
        last_map_metadata_results = []
        last_merge_metadata_results = []
        # Final outputs from the map-merge stage.
        # This is a map from merge task index to a nested list of merge results
        # (ObjectRefs). Each merge task index corresponds to a partition of P
        # final reduce tasks.
        all_merge_results = [[] for _ in range(schedule.num_merge_tasks_per_round)]
        shuffle_map_metadata = []
        shuffle_merge_metadata = []
        map_bar = ProgressBar("Shuffle Map", position=0, total=len(input_blocks_list))

        # Execute the map-merge stage. This submits tasks in rounds of M map
        # tasks and N merge tasks each. Task execution between map and merge is
        # pipelined, so that while executing merge for one round of inputs, we
        # also execute the map tasks for the following round.
        input_blocks_list = list(enumerate(input_blocks_list))
        while input_blocks_list:
            # Execute one round of the map stage.
            # Pop from the inputs so that we can clear the memory ASAP.
            round_input_blocks = []
            try:
                for _ in range(schedule.num_map_tasks_per_round):
                    round_input_blocks.append(input_blocks_list.pop(0))
            except IndexError:
                pass
            (
                prev_map_metadata,
                last_map_metadata_results,
                map_results,
            ) = _execute_pipelined_stage(
                submit_map_task,
                last_map_metadata_results,
                round_input_blocks,
                progress_bar=map_bar,
            )
            shuffle_map_metadata += prev_map_metadata

            # Shuffle the map results for the merge tasks.
            merge_args = [
                (merge_idx, [map_result.pop(0) for map_result in map_results])
                for merge_idx in range(schedule.num_merge_tasks_per_round)
            ]
            assert all([not map_result for map_result in map_results])
            # Execute one round of the merge stage.
            (
                prev_merge_metadata,
                last_merge_metadata_results,
                merge_results,
            ) = _execute_pipelined_stage(
                submit_merge_task,
                last_merge_metadata_results,
                merge_args,
            )
            shuffle_merge_metadata += prev_merge_metadata
            for merge_idx, merge_result in enumerate(merge_results):
                all_merge_results[merge_idx].append(merge_result)
            del merge_results

        # Wait for last map and merge tasks to finish.
        prev_map_metadata, _, _ = _execute_pipelined_stage(
            None, last_map_metadata_results, [], progress_bar=map_bar
        )
        shuffle_map_metadata += prev_map_metadata
        map_bar.close()
        prev_merge_metadata, _, _ = _execute_pipelined_stage(
            None, last_merge_metadata_results, []
        )
        shuffle_merge_metadata += prev_merge_metadata

        # Execute and wait for the reduce stage.
        new_metadata, new_blocks = self._execute_reduce_stage(
            output_num_blocks, schedule, reduce_ray_remote_args, all_merge_results
        )

        stats = {
            "map": shuffle_map_metadata,
            "merge": shuffle_merge_metadata,
            "reduce": new_metadata,
        }

        return BlockList(list(new_blocks), list(new_metadata)), stats

    def _execute_reduce_stage(
        self,
        output_num_blocks: int,
        schedule: _PushBasedShuffleTaskSchedule,
        reduce_ray_remote_args: Dict[str, Any],
        all_merge_results: List[List[ObjectRef]],
    ):
        shuffle_reduce = cached_remote_fn(self.reduce)
        # Execute the final reduce stage.
        shuffle_reduce_out = []
        for reducer_idx in range(output_num_blocks):
            merge_idx = schedule.get_merge_idx_for_reducer_idx(reducer_idx)
            # Submit one partition of reduce tasks, one for each of the P
            # outputs produced by the corresponding merge task.
            # We also add the merge task arguments so that the reduce task
            # is colocated with its inputs.
            shuffle_reduce_out.append(
                shuffle_reduce.options(
                    **reduce_ray_remote_args,
                    **schedule.get_merge_task_options(merge_idx),
                    num_returns=2,
                ).remote(
                    *self._reduce_args,
                    *[
                        merge_results.pop(0)
                        for merge_results in all_merge_results[merge_idx]
                    ],
                )
            )
        for merge_idx, merge_results in enumerate(all_merge_results):
            assert all(len(merge_result) == 0 for merge_result in merge_results), (
                "Reduce stage did not process outputs from merge tasks at index: "
                f"{merge_idx}"
            )
        assert (
            len(shuffle_reduce_out) == output_num_blocks
        ), f"Expected {output_num_blocks} outputs, produced {len(shuffle_reduce_out)}"

        reduce_bar = ProgressBar("Shuffle Reduce", total=output_num_blocks)
        reduce_blocks, reduce_metadata = zip(*shuffle_reduce_out)
        reduce_metadata = reduce_bar.fetch_until_complete(list(reduce_metadata))
        reduce_bar.close()

        return reduce_metadata, reduce_blocks

    @staticmethod
    def _map_partition(
        map_fn,
        idx: int,
        block: Block,
        output_num_blocks: int,
        schedule: _PushBasedShuffleTaskSchedule,
        *map_args: List[Any],
    ) -> List[Union[BlockMetadata, Block]]:
        mapper_outputs = map_fn(idx, block, output_num_blocks, *map_args)
        meta = mapper_outputs.pop(0)

        parts = []
        merge_idx = 0
        while mapper_outputs:
            partition_size = schedule.get_num_reducers_per_merge_idx(merge_idx)
            parts.append(mapper_outputs[:partition_size])
            mapper_outputs = mapper_outputs[partition_size:]
            merge_idx += 1
        assert len(parts) == schedule.num_merge_tasks_per_round, (
            len(parts),
            schedule.num_merge_tasks_per_round,
        )
        return [meta] + parts

    @staticmethod
    def _merge(
        reduce_fn,
        *all_mapper_outputs: List[List[Block]],
        reduce_args: Optional[List[Any]] = None,
    ) -> List[Union[BlockMetadata, Block]]:
        """
        Returns list of [BlockMetadata, O1, O2, O3, ...output_num_blocks].
        """
        assert (
            len({len(mapper_outputs) for mapper_outputs in all_mapper_outputs}) == 1
        ), "Received different number of map inputs"
        stats = BlockExecStats.builder()
        merged_outputs = []
        if not reduce_args:
            reduce_args = []
        for mapper_outputs in zip(*all_mapper_outputs):
            block, meta = reduce_fn(*reduce_args, *mapper_outputs)
            merged_outputs.append(block)
        meta = BlockAccessor.for_block(block).get_metadata(
            input_files=None, exec_stats=stats.build()
        )
        return [meta] + merged_outputs

    @staticmethod
    def _compute_shuffle_schedule(
        num_cpus_per_node_map: Dict[str, int],
        num_input_blocks: int,
        merge_factor: int,
        num_output_blocks: int,
    ) -> _PushBasedShuffleTaskSchedule:
        num_cpus_total = sum(v for v in num_cpus_per_node_map.values())
        task_parallelism = min(num_cpus_total, num_input_blocks)

        num_tasks_per_map_merge_group = merge_factor + 1
        num_merge_tasks_per_round = max(
            task_parallelism // num_tasks_per_map_merge_group, 1
        )
        num_map_tasks_per_round = max(task_parallelism - num_merge_tasks_per_round, 1)

        num_rounds = math.ceil(num_input_blocks / num_map_tasks_per_round)
        # Scheduling args for assigning merge tasks to nodes. We use node-affinity
        # scheduling here to colocate merge tasks that output to the same
        # reducer.
        merge_task_placement = PushBasedShufflePlan._compute_merge_task_placement(
            num_merge_tasks_per_round, merge_factor, num_cpus_per_node_map
        )
        return _PushBasedShuffleTaskSchedule(
            num_output_blocks,
            num_rounds,
            num_map_tasks_per_round,
            num_merge_tasks_per_round,
            merge_task_placement,
        )

    @staticmethod
    def _compute_merge_task_placement(
        num_merge_tasks_per_round: int,
        merge_factor: int,
        num_cpus_per_node_map: Dict[str, int],
    ) -> List[str]:
        """
        Compute the task options needed to schedule merge tasks.

        Each merge task should be grouped with `merge_factor` map tasks for
        pipelining. These groups should then be spread across nodes according
        to CPU availability for load-balancing. Across the rounds of merge
        tasks, we also want to make sure that the i-th task from each round is
        colocated on the same node to reduce data movement during the final
        reduce stage.

        Args:
            num_merge_tasks_per_round: The total number of merge tasks in each
                round.
            merge_factor: The ratio of map : merge tasks.
            num_cpus_per_node_map: A map from node ID to the number of CPUs
                available on that node.

        Returns: A list with num_merge_tasks_per_round, where the i-th element
            is the node ID where the i-th merge task should be placed.
        """
        merge_task_placement = []
        merge_tasks_assigned = 0
        node_ids = list(num_cpus_per_node_map)
        leftover_cpu_map = {}
        while (
            merge_tasks_assigned < num_merge_tasks_per_round and num_cpus_per_node_map
        ):
            node_id = node_ids[merge_tasks_assigned % len(node_ids)]
            # We group `merge_factor` map tasks with 1 merge task.
            if num_cpus_per_node_map[node_id] >= merge_factor + 1:
                num_cpus_per_node_map[node_id] -= merge_factor + 1
                merge_task_placement.append(node_id)
                merge_tasks_assigned += 1
            else:
                leftover_cpu_map[node_id] = num_cpus_per_node_map.pop(node_id)
                node_ids.remove(node_id)
        while merge_tasks_assigned < num_merge_tasks_per_round:
            node_id = max(leftover_cpu_map, key=leftover_cpu_map.get)
            # We should have enough CPUs to schedule a round of merge tasks in
            # parallel.
            assert leftover_cpu_map[node_id] != 0
            assert leftover_cpu_map[node_id] <= merge_factor
            merge_task_placement.append(node_id)
            merge_tasks_assigned += 1
            leftover_cpu_map[node_id] -= 1
        return merge_task_placement


def _execute_pipelined_stage(
    stage_fn: Callable[[T], Tuple[ObjectRef, U]],
    prev_metadata_refs: List[ObjectRef],
    stage_args: List[T],
    progress_bar: Optional[ProgressBar] = None,
) -> Tuple[List[BlockMetadata], List[ObjectRef], List[U]]:
    """
    Helper function to execute a stage of tasks. This will wait for the
    previous round of tasks to complete before submitting the next.
    """
    # TODO(swang): Straggler tasks can cause pipeline bubbles. Instead of
    # waiting for all previous tasks, we should wait for some tasks on each
    # node to finish.
    if progress_bar is not None:
        prev_metadata = progress_bar.fetch_until_complete(prev_metadata_refs)
    else:
        prev_metadata = ray.get(prev_metadata_refs)
    prev_metadata_refs.clear()

    metadata_refs = []
    data_outputs = []
    while stage_args:
        arg = stage_args.pop(0)
        metadata_ref, data_output = stage_fn(arg)
        metadata_refs.append(metadata_ref)
        data_outputs.append(data_output)
    return prev_metadata, metadata_refs, data_outputs


def _get_num_cpus_per_node_map() -> Dict[str, int]:
    nodes = ray.nodes()
    # Map from per-node resource name to number of CPUs available on that
    # node.
    num_cpus_per_node_map = {}
    for node in nodes:
        resources = node["Resources"]
        num_cpus = int(resources.get("CPU", 0))
        if num_cpus == 0:
            continue
        num_cpus_per_node_map[node["NodeID"]] = num_cpus
    return num_cpus_per_node_map
