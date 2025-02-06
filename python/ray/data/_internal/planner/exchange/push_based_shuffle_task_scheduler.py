import logging
import math
from typing import Any, Dict, List, Optional, Tuple, TypeVar, Union

import ray
from ray._private.ray_constants import CALLER_MEMORY_USAGE_PER_OBJECT_REF
from ray.data._internal.execution.interfaces import RefBundle, TaskContext
from ray.data._internal.planner.exchange.interfaces import (
    ExchangeTaskScheduler,
    ExchangeTaskSpec,
)
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.stats import StatsDict
from ray.data._internal.util import convert_bytes_to_human_readable_str
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadata, to_stats
from ray.data.context import DataContext
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)


T = TypeVar("T")
U = TypeVar("U")


class _MergeTaskSchedule:
    def __init__(self, output_num_blocks: int, num_merge_tasks_per_round: int):
        self.output_num_blocks = output_num_blocks
        self.num_merge_tasks_per_round = num_merge_tasks_per_round
        self.num_reducers_per_merger = output_num_blocks // num_merge_tasks_per_round
        self._num_mergers_with_extra_reducer = (
            output_num_blocks % num_merge_tasks_per_round
        )

        if self.num_reducers_per_merger == 0:
            self.num_merge_tasks_per_round = self._num_mergers_with_extra_reducer
            self.num_reducers_per_merger = 1
            self._num_mergers_with_extra_reducer = 0

    def __repr__(self):
        return (
            f"    num merge tasks per round: {self.num_merge_tasks_per_round}\n"
            f"    num reduce tasks per merge task: {self.num_reducers_per_merger}\n"
            "    num merge tasks with extra reduce task: "
            f"{self._num_mergers_with_extra_reducer}"
        )

    def get_num_reducers_per_merge_idx(self, merge_idx: int) -> int:
        """
        Each intermediate merge task will produce outputs for a partition of P
        final reduce tasks. This helper function returns P based on the merge
        task index.
        """
        assert merge_idx < self.num_merge_tasks_per_round
        num_reducers_for_cur_merger = self.num_reducers_per_merger
        if merge_idx < self._num_mergers_with_extra_reducer:
            num_reducers_for_cur_merger += 1
        return num_reducers_for_cur_merger

    def get_merge_idx_for_reducer_idx(self, reducer_idx: int) -> int:
        if (
            reducer_idx
            < (self.num_reducers_per_merger + 1) * self._num_mergers_with_extra_reducer
        ):
            merge_idx = reducer_idx // (self.num_reducers_per_merger + 1)
        else:
            reducer_idx -= (
                self.num_reducers_per_merger + 1
            ) * self._num_mergers_with_extra_reducer
            merge_idx = (
                self._num_mergers_with_extra_reducer
                + reducer_idx // self.num_reducers_per_merger
            )
        assert merge_idx < self.num_merge_tasks_per_round
        return merge_idx

    def round_robin_reduce_idx_iterator(self):
        """
        When there are multiple nodes, merge tasks are spread throughout the
        cluster to improve load-balancing. Each merge task produces outputs for
        a contiguous partition of reduce tasks. This method creates an iterator
        that returns reduce task indices round-robin across the merge tasks.
        This can be used to submit reduce tasks in a way that spreads the load
        evenly across the cluster.
        """
        idx = 0
        round_idx = 0
        while idx < self.output_num_blocks:
            for merge_idx in range(self.num_merge_tasks_per_round):
                if merge_idx < self._num_mergers_with_extra_reducer:
                    reduce_idx = merge_idx * (self.num_reducers_per_merger + 1)
                    num_reducers_for_cur_merger = self.num_reducers_per_merger + 1
                else:
                    reduce_idx = self._num_mergers_with_extra_reducer * (
                        self.num_reducers_per_merger + 1
                    )
                    merge_idx -= self._num_mergers_with_extra_reducer
                    reduce_idx += merge_idx * self.num_reducers_per_merger
                    num_reducers_for_cur_merger = self.num_reducers_per_merger

                if round_idx >= num_reducers_for_cur_merger:
                    continue

                reduce_idx += round_idx
                yield reduce_idx
                idx += 1
            round_idx += 1


class _PushBasedShuffleStage:
    def __init__(
        self,
        output_num_blocks: int,
        num_rounds: int,
        num_map_tasks_per_round: int,
        merge_task_placement: List[str],
    ):
        # The number of rounds of map-merge tasks. Reducer tasks are given the
        # outputs of the merge tasks as inputs. Reducer tasks receive one input
        # per round.
        self.num_rounds = num_rounds
        # The number of map tasks per round of map-merge tasks. The map task
        # produces one output per merge task in the same round.
        self.num_map_tasks_per_round = num_map_tasks_per_round

        node_strategies = {
            node_id: {
                "scheduling_strategy": NodeAffinitySchedulingStrategy(
                    node_id, soft=True
                )
            }
            for node_id in set(merge_task_placement)
        }
        self._merge_task_options = [
            node_strategies[node_id] for node_id in merge_task_placement
        ]

        self.merge_schedule = _MergeTaskSchedule(
            output_num_blocks, len(merge_task_placement)
        )

    def get_estimated_num_refs(self) -> int:
        # Number of intermediate blocks = Number of rounds x (map tasks per
        # round * merge tasks per round).
        num_intermediate_refs = self.num_rounds * (
            self.num_map_tasks_per_round * self.merge_schedule.num_merge_tasks_per_round
        )
        # Number of input blocks + intermediate blocks + output blocks.
        num_refs_total = (
            (self.num_rounds * self.num_map_tasks_per_round)
            + num_intermediate_refs
            + self.merge_schedule.output_num_blocks
        )
        return num_refs_total

    def get_merge_task_options(self, merge_idx):
        return self._merge_task_options[merge_idx]

    def __repr__(self):
        return (
            "num map tasks per round (num args per merge task): "
            f"{self.num_map_tasks_per_round}\n"
            f"num rounds (num args per reduce task): {self.num_rounds}\n"
            "merge task placement: \n"
            f"{self.merge_schedule}"
        )


class _PipelinedStageExecutor:
    def __init__(
        self,
        stage_iter,
        num_tasks_per_round: int,
        max_concurrent_rounds: int = 1,
        progress_bar: Optional[ProgressBar] = None,
    ):
        self._stage_iter = stage_iter
        self._num_tasks_per_round = num_tasks_per_round
        self._max_concurrent_rounds = max_concurrent_rounds
        self._progress_bar = progress_bar

        self._rounds: List[List[ObjectRef]] = []
        self._task_idx = 0

        self._submit_round()

        self._num_block_bytes_stored_at_driver = 0

    def __iter__(self):
        return self

    def __next__(self) -> List[BlockMetadata]:
        """
        Submit one round of tasks. If we already have the max concurrent rounds
        in flight, first wait for the oldest round of tasks to finish.
        """
        prev_metadata = []
        if all(len(r) == 0 for r in self._rounds):
            raise StopIteration

        if len(self._rounds) >= self._max_concurrent_rounds:
            prev_metadata_refs = self._rounds.pop(0)
            if prev_metadata_refs:
                if self._progress_bar is not None:
                    prev_metadata = self._progress_bar.fetch_until_complete(
                        prev_metadata_refs
                    )
                    # TODO(swang): Eagerly free the previous round's args.
                    # See https://github.com/ray-project/ray/issues/42145.
                else:
                    prev_metadata = ray.get(prev_metadata_refs)

        self._submit_round()

        return prev_metadata

    def _submit_round(self):
        assert len(self._rounds) < self._max_concurrent_rounds
        task_round = []
        for _ in range(self._num_tasks_per_round):
            try:
                task_round.append(next(self._stage_iter))
            except StopIteration:
                break
        self._rounds.append(task_round)


class _MapStageIterator:
    def __init__(self, input_blocks_list, shuffle_map, map_args):
        self._input_blocks_list = input_blocks_list
        self._shuffle_map = shuffle_map
        self._map_args = map_args

        self._mapper_idx = 0
        self._map_results = []

    def __iter__(self):
        return self

    def __next__(self):
        if not self._input_blocks_list:
            raise StopIteration

        block = self._input_blocks_list.pop(0)
        # NOTE(swang): Results are shuffled between map and merge tasks, so
        # there is no advantage to colocating specific map and merge tasks.
        # Therefore, we do not specify a node affinity policy for map tasks
        # in case the caller or Ray has a better scheduling strategy, e.g.,
        # based on data locality.
        map_result = self._shuffle_map.remote(
            self._mapper_idx,
            block,
            *self._map_args,
        )
        metadata_ref = map_result.pop(-1)
        self._map_results.append(map_result)
        self._mapper_idx += 1
        return metadata_ref

    def pop_map_results(self) -> List[List[ObjectRef]]:
        map_results = self._map_results
        self._map_results = []
        return map_results


class _MergeStageIterator:
    def __init__(
        self,
        map_stage_iter: _MapStageIterator,
        shuffle_merge,
        stage: _PushBasedShuffleStage,
        reduce_args,
    ):
        self._map_stage_iter = map_stage_iter
        self._shuffle_merge = shuffle_merge
        self._stage = stage
        self._reduce_args = reduce_args

        self._merge_idx = 0
        self._map_result_buffer = None
        # Final outputs from the map-merge stage.
        # This is a map from merge task index to a nested list of merge results
        # (ObjectRefs). Each merge task index corresponds to a partition of P
        # final reduce tasks.
        self._all_merge_results = [
            [] for _ in range(self._stage.merge_schedule.num_merge_tasks_per_round)
        ]

    def __next__(self):
        if not self._map_result_buffer or not self._map_result_buffer[0]:
            assert self._merge_idx == 0
            self._map_result_buffer = self._map_stage_iter.pop_map_results()

        if not self._map_result_buffer:
            raise StopIteration

        # Shuffle the map results for the merge tasks.
        merge_args = [map_result.pop(0) for map_result in self._map_result_buffer]
        num_merge_returns = self._stage.merge_schedule.get_num_reducers_per_merge_idx(
            self._merge_idx
        )
        merge_result = self._shuffle_merge.options(
            num_returns=1 + num_merge_returns,
            **self._stage.get_merge_task_options(self._merge_idx),
        ).remote(
            *merge_args,
            reduce_args=self._reduce_args,
        )
        metadata_ref = merge_result.pop(-1)
        self._all_merge_results[self._merge_idx].append(merge_result)
        del merge_result

        self._merge_idx += 1
        self._merge_idx %= self._stage.merge_schedule.num_merge_tasks_per_round
        return metadata_ref

    def pop_merge_results(self) -> List[List[ObjectRef]]:
        """Return a nested list of merge task results. The list at index i
        stores the outputs of the i-th merge task submitted during each
        map-merge round. Each merge task returns a list of outputs because it
        may produce outputs for multiple downstream reduce tasks.
        """
        all_merge_results = self._all_merge_results
        self._all_merge_results = []
        return all_merge_results


class _ReduceStageIterator:
    def __init__(
        self,
        stage: _PushBasedShuffleStage,
        shuffle_reduce,
        all_merge_results: List[List[List[ObjectRef]]],
        ray_remote_args,
        reduce_args: List[Any],
        _debug_limit_execution_to_num_blocks: Optional[int],
    ):
        self._shuffle_reduce = shuffle_reduce
        self._stage = stage
        self._reduce_arg_blocks: List[Tuple[int, List[ObjectRef]]] = []
        self._ray_remote_args = ray_remote_args
        self._reduce_args = reduce_args

        for reduce_idx in self._stage.merge_schedule.round_robin_reduce_idx_iterator():
            merge_idx = self._stage.merge_schedule.get_merge_idx_for_reducer_idx(
                reduce_idx
            )
            reduce_arg_blocks = [
                merge_results.pop(0) for merge_results in all_merge_results[merge_idx]
            ]
            self._reduce_arg_blocks.append((reduce_idx, reduce_arg_blocks))

        assert len(self._reduce_arg_blocks) == stage.merge_schedule.output_num_blocks

        if _debug_limit_execution_to_num_blocks is not None:
            self._reduce_arg_blocks = self._reduce_arg_blocks[
                :_debug_limit_execution_to_num_blocks
            ]
            logger.debug(
                f"Limiting execution to {len(self._reduce_arg_blocks)} reduce tasks"
            )

        for merge_idx, merge_results in enumerate(all_merge_results):
            assert all(len(merge_result) == 0 for merge_result in merge_results), (
                "Reduce stage did not process outputs from merge tasks at index: "
                f"{merge_idx}"
            )

        self._reduce_results: List[Tuple[int, ObjectRef]] = []

    def __iter__(self):
        return self

    def __next__(self):
        if not self._reduce_arg_blocks:
            raise StopIteration

        reduce_idx, reduce_arg_blocks = self._reduce_arg_blocks.pop(0)
        merge_idx = self._stage.merge_schedule.get_merge_idx_for_reducer_idx(reduce_idx)
        # Submit one partition of reduce tasks, one for each of the P
        # outputs produced by the corresponding merge task.
        # We also add the merge task arguments so that the reduce task
        # is colocated with its inputs.
        block, meta = self._shuffle_reduce.options(
            **self._ray_remote_args,
            **self._stage.get_merge_task_options(merge_idx),
            num_returns=2,
        ).remote(*self._reduce_args, *reduce_arg_blocks, partial_reduce=False)
        self._reduce_results.append((reduce_idx, block))
        return meta

    def pop_reduce_results(self):
        reduce_results = self._reduce_results
        self._reduce_results = []
        return reduce_results


class PushBasedShuffleTaskScheduler(ExchangeTaskScheduler):
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
          execute as merge, then we should set this to 2. If pipeline bubbles
          appear and the merge tasks are much longer than the map tasks, then
          the merge factor should be decreased, and vice versa.
        See paper at https://arxiv.org/abs/2203.05072 for more details.
    """

    def execute(
        self,
        refs: List[RefBundle],
        output_num_blocks: int,
        task_ctx: TaskContext,
        map_ray_remote_args: Optional[Dict[str, Any]] = None,
        reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
        merge_factor: float = 2,
        _debug_limit_execution_to_num_blocks: int = None,
    ) -> Tuple[List[RefBundle], StatsDict]:
        logger.debug("Using experimental push-based shuffle.")
        # TODO: Preemptively clear the blocks list since we will incrementally delete
        # the last remaining references as we submit the dependent map tasks during the
        # map-merge stage.

        # TODO(swang): For jobs whose reduce work is heavier than the map work,
        # we should support fractional merge factors.
        # TODO(swang): For large jobs, we should try to choose the merge factor
        # automatically, e.g., by running one test round of map and merge tasks
        # and comparing their run times.
        # TODO(swang): Add option to automatically reduce write amplification
        # during map-merge stage, by limiting how many partitions can be
        # processed concurrently.
        input_blocks_list = []
        for ref_bundle in refs:
            input_blocks_list.extend(ref_bundle.block_refs)
        input_owned = all(b.owns_blocks for b in refs)

        if map_ray_remote_args is None:
            map_ray_remote_args = {}
        if reduce_ray_remote_args is None:
            reduce_ray_remote_args = {}
        # The placement strategy for reduce tasks is overwritten to colocate
        # them with their inputs from the merge stage, so remove any
        # pre-specified scheduling strategy here.
        reduce_ray_remote_args = reduce_ray_remote_args.copy()
        reduce_ray_remote_args.pop("scheduling_strategy", None)

        # Compute all constants used for task scheduling.
        num_cpus_per_node_map = _get_num_cpus_per_node_map()
        stage = self._compute_shuffle_schedule(
            num_cpus_per_node_map,
            len(input_blocks_list),
            merge_factor,
            output_num_blocks,
        )

        caller_memory_usage = (
            stage.get_estimated_num_refs() * CALLER_MEMORY_USAGE_PER_OBJECT_REF
        )
        self.warn_on_driver_memory_usage(
            caller_memory_usage,
            "Execution is estimated to use at least "
            f"{convert_bytes_to_human_readable_str(caller_memory_usage)}"
            " of driver memory. Ensure that the driver machine has at least "
            "this much memory to ensure job completion.",
        )

        # TODO(swang): Use INFO level. Currently there is no easy way to set
        # the logging level to DEBUG from a driver script, so just print
        # verbosely for now.
        # See https://github.com/ray-project/ray/issues/42002.
        logger.debug(f"Push-based shuffle schedule:\n{stage}")

        map_fn = self._map_partition
        merge_fn = self._merge

        def map_partition(*args, **kwargs):
            return map_fn(self._exchange_spec.map, *args, **kwargs)

        def merge(*args, **kwargs):
            return merge_fn(self._exchange_spec.reduce, *args, **kwargs)

        shuffle_map = cached_remote_fn(map_partition)
        shuffle_map = shuffle_map.options(
            **map_ray_remote_args,
            num_returns=1 + stage.merge_schedule.num_merge_tasks_per_round,
        )

        if _debug_limit_execution_to_num_blocks is not None:
            input_blocks_list = input_blocks_list[:_debug_limit_execution_to_num_blocks]
            logger.debug(f"Limiting execution to {len(input_blocks_list)} map tasks")
        map_stage_iter = _MapStageIterator(
            input_blocks_list,
            shuffle_map,
            [output_num_blocks, stage.merge_schedule, *self._exchange_spec._map_args],
        )

        sub_progress_bar_dict = task_ctx.sub_progress_bar_dict
        bar_name = ExchangeTaskSpec.MAP_SUB_PROGRESS_BAR_NAME
        assert bar_name in sub_progress_bar_dict, sub_progress_bar_dict
        map_bar = sub_progress_bar_dict[bar_name]
        map_stage_executor = _PipelinedStageExecutor(
            map_stage_iter, stage.num_map_tasks_per_round, progress_bar=map_bar
        )

        shuffle_merge = cached_remote_fn(merge)
        merge_stage_iter = _MergeStageIterator(
            map_stage_iter, shuffle_merge, stage, self._exchange_spec._reduce_args
        )
        merge_stage_executor = _PipelinedStageExecutor(
            merge_stage_iter,
            stage.merge_schedule.num_merge_tasks_per_round,
            max_concurrent_rounds=2,
        )
        # Execute the map-merge stage. This submits tasks in rounds of M map
        # tasks and N merge tasks each. Task execution between map and merge is
        # pipelined, so that while executing merge for one round of inputs, we
        # also execute the map tasks for the following round.
        map_done = False
        merge_done = False
        map_stage_metadata = []
        merge_stage_metadata = []
        while not (map_done and merge_done):
            try:
                map_stage_metadata += next(map_stage_executor)
            except StopIteration:
                map_done = True
                break

            try:
                merge_stage_metadata += next(merge_stage_executor)
            except StopIteration:
                merge_done = True
                break

            self.warn_on_high_local_memory_store_usage()

        all_merge_results = merge_stage_iter.pop_merge_results()

        if _debug_limit_execution_to_num_blocks is not None:
            for merge_idx in range(len(all_merge_results)):
                while len(all_merge_results[merge_idx]) < stage.num_rounds:
                    # Repeat the first merge task's results.
                    all_merge_results[merge_idx].append(
                        all_merge_results[merge_idx][0][:]
                    )

        # Execute and wait for the reduce stage.
        bar_name = ExchangeTaskSpec.REDUCE_SUB_PROGRESS_BAR_NAME
        assert bar_name in sub_progress_bar_dict, sub_progress_bar_dict
        reduce_bar = sub_progress_bar_dict[bar_name]

        shuffle_reduce = cached_remote_fn(self._exchange_spec.reduce)
        reduce_stage_iter = _ReduceStageIterator(
            stage,
            shuffle_reduce,
            all_merge_results,
            reduce_ray_remote_args,
            self._exchange_spec._reduce_args,
            _debug_limit_execution_to_num_blocks,
        )

        max_reduce_tasks_in_flight = output_num_blocks
        ctx = DataContext.get_current()
        if ctx.pipeline_push_based_shuffle_reduce_tasks:
            # If pipelining is enabled, we should still try to utilize all
            # cores.
            max_reduce_tasks_in_flight = min(
                max_reduce_tasks_in_flight, sum(num_cpus_per_node_map.values())
            )

        reduce_stage_executor = _PipelinedStageExecutor(
            reduce_stage_iter,
            max_reduce_tasks_in_flight,
            max_concurrent_rounds=2,
            progress_bar=reduce_bar,
        )
        reduce_stage_metadata = []
        while True:
            try:
                reduce_stage_metadata += next(reduce_stage_executor)
            except StopIteration:
                break

            self.warn_on_high_local_memory_store_usage()

        new_blocks = reduce_stage_iter.pop_reduce_results()
        sorted_blocks = [
            (block[0], block[1], reduce_stage_metadata[i])
            for i, block in enumerate(new_blocks)
        ]
        sorted_blocks.sort(key=lambda x: x[0])

        new_blocks, reduce_stage_metadata = [], []
        if sorted_blocks:
            _, new_blocks, reduce_stage_metadata = zip(*sorted_blocks)
        del sorted_blocks

        if _debug_limit_execution_to_num_blocks is not None:
            output_num_blocks = min(
                _debug_limit_execution_to_num_blocks, output_num_blocks
            )

        assert (
            len(new_blocks) == output_num_blocks
        ), f"Expected {output_num_blocks} outputs, produced {len(new_blocks)}"

        output = []
        for block, meta in zip(new_blocks, reduce_stage_metadata):
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
            "map": to_stats(map_stage_metadata),
            "merge": to_stats(merge_stage_metadata),
            "reduce": to_stats(reduce_stage_metadata),
        }

        return (output, stats)

    @staticmethod
    def _map_partition(
        map_fn,
        idx: int,
        block: Block,
        output_num_blocks: int,
        schedule: _MergeTaskSchedule,
        *map_args: List[Any],
    ) -> List[Union[BlockMetadata, Block]]:
        mapper_outputs = map_fn(idx, block, output_num_blocks, *map_args)

        # A merge task may produce results for multiple downstream reducer
        # tasks. Therefore, each map task should give each merge task a
        # partition of its outputs, where the length of the partition is equal
        # to the number of reducers downstream to the merge task.
        partition = []
        merge_idx = 0
        while merge_idx < schedule.num_merge_tasks_per_round and mapper_outputs:
            output = mapper_outputs.pop(0)
            partition.append(output)

            if len(partition) == schedule.get_num_reducers_per_merge_idx(merge_idx):
                yield partition

                partition = []
                merge_idx += 1

        assert not partition
        assert len(mapper_outputs) == 1, (
            mapper_outputs,
            "The last output should be a BlockMetadata",
        )
        assert isinstance(mapper_outputs[0], BlockMetadata)
        yield mapper_outputs[0]

        assert merge_idx == schedule.num_merge_tasks_per_round, (
            merge_idx,
            schedule.num_merge_tasks_per_round,
        )

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
        if not reduce_args:
            reduce_args = []

        num_rows = 0
        size_bytes = 0
        schema = None
        for i, mapper_outputs in enumerate(zip(*all_mapper_outputs)):
            block, meta = reduce_fn(*reduce_args, *mapper_outputs, partial_reduce=True)
            yield block

            block = BlockAccessor.for_block(block)
            num_rows += block.num_rows()
            size_bytes += block.size_bytes()
            schema = block.schema()
            del block

        yield BlockMetadata(
            num_rows=num_rows,
            size_bytes=size_bytes,
            schema=schema,
            input_files=None,
            exec_stats=stats.build(),
        )

    @staticmethod
    def _compute_shuffle_schedule(
        num_cpus_per_node_map: Dict[str, int],
        num_input_blocks: int,
        merge_factor: float,
        num_output_blocks: int,
    ) -> _PushBasedShuffleStage:
        num_cpus_total = sum(v for v in num_cpus_per_node_map.values())
        logger.debug(
            f"Found {num_cpus_total} CPUs available CPUs for push-based shuffle."
        )
        num_tasks_per_map_merge_group = merge_factor + 1
        num_total_merge_tasks = math.ceil(num_input_blocks / merge_factor)

        num_merge_tasks_per_round = 0
        merge_task_placement = []
        leftover_cpus = 0
        # Compute the total number of merge tasks and their node placement.
        # Each merge task should be grouped with `merge_factor` map tasks for
        # pipelining. These groups should then be spread across nodes according
        # to CPU availability for load-balancing.
        num_input_blocks_remaining = num_input_blocks
        for node, num_cpus in num_cpus_per_node_map.items():
            # First find how many merge tasks we should run on this node.
            # We take the min of the number of CPUs on this node and the number
            # of input blocks that we haven't scheduled yet, in case there are
            # fewer input blocks than CPU slots on this node.
            num_cpu_slots = min(num_cpus, num_input_blocks_remaining)
            num_merge_tasks_on_cur_node = round(
                num_cpu_slots / num_tasks_per_map_merge_group
            )
            # For small datasets, the number of tasks to run may be less than
            # the total CPU slots available.
            num_merge_tasks_on_cur_node = min(
                num_merge_tasks_on_cur_node, num_total_merge_tasks
            )
            for i in range(num_merge_tasks_on_cur_node):
                merge_task_placement.append(node)
                # We schedule `merge_factor` many map tasks for every merge
                # task. Subtract from the number of input blocks remaining to
                # account for cases where the number of map tasks is smaller
                # than the available CPU slots.
                num_input_blocks_remaining -= merge_factor
                num_cpus -= num_tasks_per_map_merge_group
            num_merge_tasks_per_round += num_merge_tasks_on_cur_node

            # Handle the case where a single node cannot fit a group of map and
            # merge tasks, but we can spread the group across multiple distinct
            # nodes.
            leftover_cpus += num_cpus
            if (
                leftover_cpus >= num_tasks_per_map_merge_group
                and num_merge_tasks_per_round < num_total_merge_tasks
            ):
                merge_task_placement.append(node)
                num_merge_tasks_per_round += 1
                leftover_cpus -= num_tasks_per_map_merge_group
                num_input_blocks_remaining -= merge_factor

            num_input_blocks_remaining = max(0, num_input_blocks_remaining)

        if num_merge_tasks_per_round == 0:
            # For small datasets, make sure we have at least one merge task.
            for node, num_cpus in num_cpus_per_node_map.items():
                if num_cpus >= 1:
                    merge_task_placement.append(node)
                    num_merge_tasks_per_round = 1
                    break

        assert num_merge_tasks_per_round == len(merge_task_placement)
        assert num_merge_tasks_per_round > 0, num_merge_tasks_per_round
        # Use the remaining CPUs to execute map tasks.
        num_map_tasks_per_round = num_cpus_total - num_merge_tasks_per_round
        num_map_tasks_per_round = min(num_map_tasks_per_round, num_input_blocks)
        # Make sure there is at least one map task in each round.
        num_map_tasks_per_round = max(num_map_tasks_per_round, 1)

        num_rounds = math.ceil(num_input_blocks / num_map_tasks_per_round)
        return _PushBasedShuffleStage(
            num_output_blocks,
            num_rounds,
            num_map_tasks_per_round,
            merge_task_placement,
        )


def _get_num_cpus_per_node_map() -> Dict[str, int]:
    total_resources_by_node = ray.state.total_resources_per_node()
    # Map from per-node resource name to number of CPUs available on that
    # node.
    num_cpus_per_node_map = {}
    for node_id, resources in total_resources_by_node.items():
        num_cpus = int(resources.get("CPU", 0))
        if num_cpus == 0:
            continue
        num_cpus_per_node_map[node_id] = num_cpus
    return num_cpus_per_node_map
