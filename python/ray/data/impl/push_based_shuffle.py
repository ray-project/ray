import math
from typing import List, Optional, Dict, Any, Tuple, Union
from collections import defaultdict

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.data.block import Block, BlockAccessor, BlockMetadata, BlockExecStats
from ray.data.impl.shuffle import ShuffleOp
from ray.data.impl.progress_bar import ProgressBar
from ray.data.impl.block_list import BlockList
from ray.data.impl.remote_fn import cached_remote_fn


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
    """

    @staticmethod
    def _get_merge_partition_size(
        merge_idx, output_num_blocks: int, num_merge_tasks_per_round: int
    ) -> int:
        """
        Each intermediate merge task will produce outputs for a partition of P
        final reduce tasks. This helper function computes P based on the merge
        task index.
        """
        partition_size = output_num_blocks // num_merge_tasks_per_round
        extra_blocks = output_num_blocks % num_merge_tasks_per_round
        if merge_idx < extra_blocks:
            partition_size += 1
        return partition_size

    @staticmethod
    def _map_partition(
        map_fn,
        idx: int,
        block: Block,
        output_num_blocks: int,
        num_merge_tasks_per_round: int,
        *map_args: List[Any],
    ) -> List[Union[BlockMetadata, Block]]:
        mapper_outputs = map_fn(idx, block, output_num_blocks, *map_args)
        meta = mapper_outputs.pop(0)

        parts = []
        merge_idx = 0
        while mapper_outputs:
            partition_size = PushBasedShufflePlan._get_merge_partition_size(
                merge_idx, output_num_blocks, num_merge_tasks_per_round
            )
            parts.append(mapper_outputs[:partition_size])
            mapper_outputs = mapper_outputs[partition_size:]
            merge_idx += 1
        assert len(parts) == num_merge_tasks_per_round, (
            len(parts),
            num_merge_tasks_per_round,
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
            len(set(len(mapper_outputs) for mapper_outputs in all_mapper_outputs)) == 1
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
    def _get_cluster_cpu_map():
        nodes = ray.nodes()
        # Map from per-node resource name to number of CPUs available on that
        # node.
        cpu_map = {}
        for node in nodes:
            resources = node["Resources"]
            num_cpus = int(resources.get("CPU", 0))
            if num_cpus == 0:
                continue
            cpu_map[node["NodeID"]] = num_cpus
        return cpu_map

    @staticmethod
    def _compute_merge_task_args(
        num_merge_tasks_per_round: int, merge_factor: int, cpu_map
    ):
        merge_to_node_map = []
        merge_tasks_assigned = 0
        node_ids = list(cpu_map)
        leftover_cpu_map = {}
        while merge_tasks_assigned < num_merge_tasks_per_round and cpu_map:
            node_id = node_ids[merge_tasks_assigned % len(node_ids)]
            if cpu_map[node_id] >= merge_factor + 1:
                cpu_map[node_id] -= merge_factor + 1
                merge_to_node_map.append(node_id)
                merge_tasks_assigned += 1
            else:
                leftover_cpu_map[node_id] = cpu_map.pop(node_id)
                node_ids.remove(node_id)
        while merge_tasks_assigned < num_merge_tasks_per_round:
            node_id = max(leftover_cpu_map, key=leftover_cpu_map.get)
            # We should have enough CPUs to schedule a round of merge tasks in
            # parallel.
            assert leftover_cpu_map[node_id] != 0
            assert leftover_cpu_map[node_id] <= merge_factor
            merge_to_node_map.append(node_id)
            merge_tasks_assigned += 1
            leftover_cpu_map[node_id] -= 1
        merge_task_args = [
            {"scheduling_strategy": NodeAffinitySchedulingStrategy(node_id, soft=True)}
            for node_id in merge_to_node_map
        ]
        return merge_task_args

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
        # TODO(swang): For large jobs, we should try to choose the merge factor
        # automatically, e.g., by running one test round of map and merge tasks
        # and comparing their run times.
        input_blocks_list = input_blocks.get_blocks()
        input_num_blocks = len(input_blocks_list)

        if map_ray_remote_args is None:
            map_ray_remote_args = {}
        if reduce_ray_remote_args is None:
            reduce_ray_remote_args = {}
        # The placement strategy for reduce tasks is overwritten to colocate
        # them with their inputs from the merge stage, so remove any
        # pre-specified scheduling strategy here.
        reduce_ray_remote_args.pop("scheduling_strategy", None)

        map_fn = self._map_partition
        merge_fn = self._merge

        def map_partition(*args, **kwargs):
            return map_fn(self.map, *args, **kwargs)

        def merge(*args, **kwargs):
            return merge_fn(self.reduce, *args, **kwargs)

        shuffle_map = cached_remote_fn(map_partition)
        shuffle_merge = cached_remote_fn(merge)
        shuffle_reduce = cached_remote_fn(self.reduce)

        # Compute all constants used for task scheduling.
        cpu_map = PushBasedShufflePlan._get_cluster_cpu_map()
        num_cpus_total = sum(v for v in cpu_map.values())
        task_parallelism = min(num_cpus_total, input_num_blocks)
        # Total number of merge tasks.
        num_merge_tasks = math.ceil(input_num_blocks / merge_factor)
        # Number of map-merge rounds.
        num_rounds = math.ceil((input_num_blocks + num_merge_tasks) / task_parallelism)
        # M: Number of map tasks in one map-merge round.
        num_map_tasks_per_round = math.ceil(input_num_blocks / num_rounds)
        # N: Number of merge tasks in one map-merge round. Each map_partition
        # task will send one output to each of these merge tasks.
        num_merge_tasks_per_round = math.ceil(num_merge_tasks / num_rounds)
        # Scheduling args for assigning merge tasks to nodes. We use node-affinity
        # scheduling here to colocate merge tasks that output to the same
        # reducer.
        merge_task_args = self._compute_merge_task_args(
            num_merge_tasks_per_round, merge_factor, cpu_map
        )

        # Intermediate results for the map-merge stage.
        map_results = []
        # ObjectRef results from the last round of tasks. Used to add
        # backpressure during pipelining of map and merge tasks.
        last_map_metadata_results = []
        last_merge_metadata_results = []
        mapper_idx = 0
        # Preemptively clear the blocks list since we will incrementally delete
        # the last remaining references as we submit the dependent map tasks
        # during the map-merge stage.
        if clear_input_blocks:
            input_blocks.clear()

        # Final outputs from the map-merge stage.
        # This is a map from merge task index to a nested list of merge results
        # (ObjectRefs). Each merge task index corresponds to a partition of P
        # final reduce tasks.
        all_merge_results = defaultdict(list)
        shuffle_map_metadata = []
        shuffle_merge_metadata = []
        map_bar = ProgressBar("Shuffle Map", position=0, total=input_num_blocks)
        # Execute the map-merge stage. This submits tasks in rounds of M map
        # tasks and N merge tasks each. Task execution between map and merge is
        # pipelined, so that while executing merge for one round of inputs, we
        # also execute the map tasks for the following round.
        while input_blocks_list:
            round_input_blocks = input_blocks_list[:num_map_tasks_per_round]
            input_blocks_list = input_blocks_list[num_map_tasks_per_round:]
            # Wait for previous round of map tasks to finish before submitting
            # more map tasks.
            if last_map_metadata_results:
                shuffle_map_metadata += map_bar.fetch_until_complete(
                    last_map_metadata_results
                )
            map_results = []
            for block in round_input_blocks:
                map_results.append(
                    shuffle_map.options(
                        **map_ray_remote_args,
                        num_returns=1 + num_merge_tasks_per_round,
                    ).remote(
                        mapper_idx,
                        block,
                        output_num_blocks,
                        num_merge_tasks_per_round,
                        *self._map_args,
                    )
                )
                mapper_idx += 1

            # The first item returned by each map task is the BlockMetadata.
            last_map_metadata_results = [
                map_result.pop(0) for map_result in map_results
            ]

            # Wait for previous round of merge tasks to finish before
            # submitting more merge tasks.
            if last_merge_metadata_results:
                shuffle_merge_metadata = ray.get(last_merge_metadata_results)
            merge_results = []
            for merge_idx in range(num_merge_tasks_per_round):
                num_merge_returns = self._get_merge_partition_size(
                    merge_idx, output_num_blocks, num_merge_tasks_per_round
                )
                assert merge_idx < len(merge_task_args)
                merge_results.append(
                    shuffle_merge.options(
                        num_returns=1 + num_merge_returns, **merge_task_args[merge_idx]
                    ).remote(
                        *[map_result[merge_idx] for map_result in map_results],
                        reduce_args=self._reduce_args,
                    )
                )
            # The first item returned by each merge task is the BlockMetadata.
            last_merge_metadata_results = [
                merge_result.pop(0) for merge_result in merge_results
            ]
            for merge_idx, merge_result in enumerate(merge_results):
                all_merge_results[merge_idx].append(merge_result)
            del merge_results

        # Wait for last map and merge tasks to finish.
        if last_map_metadata_results:
            shuffle_map_metadata += map_bar.fetch_until_complete(
                last_map_metadata_results
            )
            del last_map_metadata_results
            map_bar.close()
        del map_results
        if last_merge_metadata_results:
            shuffle_merge_metadata += ray.get(last_merge_metadata_results)
            del last_merge_metadata_results

        # Execute the final reduce stage.
        shuffle_reduce_out = []
        for merge_idx in range(num_merge_tasks_per_round):
            num_merge_returns = self._get_merge_partition_size(
                merge_idx, output_num_blocks, num_merge_tasks_per_round
            )
            for reduce_idx in range(num_merge_returns):
                # Submit one partition of reduce tasks, one for each of the P
                # outputs produced by the corresponding merge task.
                # We also add the merge task arguments so that the reduce task
                # is colocated with its inputs.
                shuffle_reduce_out.append(
                    shuffle_reduce.options(
                        **reduce_ray_remote_args,
                        **merge_task_args[merge_idx],
                        num_returns=2,
                    ).remote(
                        *self._reduce_args,
                        *[
                            merge_results[reduce_idx]
                            for merge_results in all_merge_results[merge_idx]
                        ],
                    )
                )
            # Eagerly delete the merge outputs in order to release the blocks'
            # memory.
            del all_merge_results[merge_idx]

        assert len(all_merge_results) == 0, (
            "Reduce stage did not process outputs from all merge tasks: "
            f"{list(all_merge_results.keys())}"
        )
        assert (
            len(shuffle_reduce_out) == output_num_blocks
        ), f"Expected {output_num_blocks} outputs, produced {len(shuffle_reduce_out)}"

        reduce_bar = ProgressBar("Shuffle Reduce", total=output_num_blocks)
        new_blocks, new_metadata = zip(*shuffle_reduce_out)
        new_metadata = reduce_bar.fetch_until_complete(list(new_metadata))
        reduce_bar.close()

        stats = {
            "map": shuffle_map_metadata,
            "merge": shuffle_merge_metadata,
            "reduce": new_metadata,
        }

        return BlockList(list(new_blocks), list(new_metadata)), stats
