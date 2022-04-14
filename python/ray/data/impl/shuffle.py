import math
from typing import TypeVar, List, Optional, Dict, Any, Tuple, Union, Callable, Iterable
from collections import defaultdict

import numpy as np

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.data.block import Block, BlockAccessor, BlockMetadata, BlockExecStats
from ray.data.impl.progress_bar import ProgressBar
from ray.data.impl.block_list import BlockList
from ray.data.impl.delegating_block_builder import DelegatingBlockBuilder
from ray.data.impl.remote_fn import cached_remote_fn

T = TypeVar("T")


class ShuffleOp:
    """
    A generic shuffle operator. Callers should implement the `map` and `reduce`
    static methods. Any custom arguments for map and reduce tasks should be
    specified by setting `ShuffleOp._map_args` and `ShuffleOp._reduce_args`.
    """

    def __init__(self, map_args: List[Any] = None, reduce_args: List[Any] = None):
        self._map_args = map_args or []
        self._reduce_args = reduce_args or []
        assert isinstance(self._map_args, list)
        assert isinstance(self._reduce_args, list)

    @staticmethod
    def map(
        idx: int, block: Block, output_num_blocks: int, *map_args: List[Any]
    ) -> List[Union[BlockMetadata, Block]]:
        """
        Map function to be run on each input block.

        Returns list of [BlockMetadata, O1, O2, O3, ...output_num_blocks].
        """
        raise NotImplementedError

    @staticmethod
    def reduce(*mapper_outputs: List[Block]) -> (Block, BlockMetadata):
        """
        Reduce function to be run for each output block.
        """
        raise NotImplementedError

    def execute(
        self,
        input_blocks: BlockList,
        output_num_blocks: int,
        clear_input_blocks: bool,
        *,
        map_ray_remote_args: Optional[Dict[str, Any]] = None,
        reduce_ray_remote_args: Optional[Dict[str, Any]] = None,
    ) -> Tuple[BlockList, Dict[str, List[BlockMetadata]]]:
        input_blocks_list = input_blocks.get_blocks()
        input_num_blocks = len(input_blocks_list)

        if map_ray_remote_args is None:
            map_ray_remote_args = {}
        if reduce_ray_remote_args is None:
            reduce_ray_remote_args = {}
        if "scheduling_strategy" not in reduce_ray_remote_args:
            reduce_ray_remote_args = reduce_ray_remote_args.copy()
            reduce_ray_remote_args["scheduling_strategy"] = "SPREAD"

        shuffle_map = cached_remote_fn(self.map)
        shuffle_reduce = cached_remote_fn(self.reduce)

        map_bar = ProgressBar("Shuffle Map", total=input_num_blocks)

        shuffle_map_out = [
            shuffle_map.options(
                **map_ray_remote_args,
                num_returns=1 + output_num_blocks,
            ).remote(i, block, output_num_blocks, *self._map_args)
            for i, block in enumerate(input_blocks_list)
        ]

        # The first item returned is the BlockMetadata.
        shuffle_map_metadata = []
        for i, refs in enumerate(shuffle_map_out):
            shuffle_map_metadata.append(refs[0])
            shuffle_map_out[i] = refs[1:]

        # Eagerly delete the input block references in order to eagerly release
        # the blocks' memory.
        del input_blocks_list
        if clear_input_blocks:
            input_blocks.clear()
        shuffle_map_metadata = map_bar.fetch_until_complete(shuffle_map_metadata)
        map_bar.close()

        reduce_bar = ProgressBar("Shuffle Reduce", total=output_num_blocks)
        shuffle_reduce_out = [
            shuffle_reduce.options(**reduce_ray_remote_args, num_returns=2,).remote(
                *self._reduce_args,
                *[shuffle_map_out[i][j] for i in range(input_num_blocks)],
            )
            for j in range(output_num_blocks)
        ]
        # Eagerly delete the map block references in order to eagerly release
        # the blocks' memory.
        del shuffle_map_out
        new_blocks, new_metadata = zip(*shuffle_reduce_out)
        new_metadata = reduce_bar.fetch_until_complete(list(new_metadata))
        reduce_bar.close()

        stats = {
            "map": shuffle_map_metadata,
            "reduce": new_metadata,
        }

        return BlockList(list(new_blocks), list(new_metadata)), stats


class PushBasedShuffleOp(ShuffleOp):
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
         Takes 1 output from the i-th merge task from every round.

    Notes:
        N * P = R = total number of output blocks
        M / N = merge factor - the ratio of map : merge tasks is to improve
          pipelined parallelism. For example, if map takes twice as long to
          execute as merge, then we should set this to 2.
    """

    @staticmethod
    def map(
        idx: int, block: Block, output_num_blocks: int, *map_args: List[Any]
    ) -> List[Union[BlockMetadata, Block]]:
        raise NotImplementedError

    @staticmethod
    def reduce(*mapper_outputs: List[Block]) -> (Block, BlockMetadata):
        raise NotImplementedError

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
            partition_size = PushBasedShuffleOp._get_merge_partition_size(
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
        reduce_args=Optional[List[Any]],
    ) -> List[Union[BlockMetadata, Block]]:
        """
        Returns list of [BlockMetadata, O1, O2, O3, ...output_num_blocks].
        """
        assert (
            len(set(len(mapper_outputs) for mapper_outputs in all_mapper_outputs)) == 1
        ), "Received different number of map inputs"
        stats = BlockExecStats.builder()
        merged_outputs = []
        reduce_args = reduce_args or []
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
        try:
            reduce_ray_remote_args.pop("scheduling_strategy")
        except KeyError:
            pass

        def map_partition(*args, **kwargs):
            return self._map_partition(self.map, *args, **kwargs)

        def merge(*args, **kwargs):
            return self._merge(self.reduce, *args, **kwargs)

        shuffle_map = cached_remote_fn(map_partition)
        shuffle_merge = cached_remote_fn(merge)
        shuffle_reduce = cached_remote_fn(self.reduce)

        # Compute all constants used for task scheduling.
        cpu_map = PushBasedShuffleOp._get_cluster_cpu_map()
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
        # Scheduling args for assign merge tasks to nodes. We use node-affinity
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
        reduce_bar.block_until_complete(list(new_blocks))
        new_metadata = ray.get(list(new_metadata))
        reduce_bar.close()

        stats = {
            "map": shuffle_map_metadata,
            "merge": shuffle_merge_metadata,
            "reduce": new_metadata,
        }

        return BlockList(list(new_blocks), list(new_metadata)), stats


class ShufflePartitionOp(PushBasedShuffleOp):
    """
    Operator used for `random_shuffle` and `repartition` transforms.
    """

    def __init__(
        self,
        block_udf=None,
        random_shuffle: bool = False,
        random_seed: Optional[int] = None,
    ):
        super().__init__(
            map_args=[block_udf, random_shuffle, random_seed],
            reduce_args=[random_shuffle, random_seed],
        )

    @staticmethod
    def map(
        idx: int,
        block: Block,
        output_num_blocks: int,
        block_udf: Optional[Callable[[Block], Iterable[Block]]],
        random_shuffle: bool,
        random_seed: Optional[int],
    ) -> List[Union[BlockMetadata, Block]]:
        stats = BlockExecStats.builder()
        if block_udf:
            # TODO(ekl) note that this effectively disables block splitting.
            blocks = list(block_udf(block))
            if len(blocks) > 1:
                builder = BlockAccessor.for_block(blocks[0]).builder()
                for b in blocks:
                    builder.add_block(b)
                block = builder.build()
            else:
                block = blocks[0]
        block = BlockAccessor.for_block(block)

        # Randomize the distribution of records to blocks.
        if random_shuffle:
            seed_i = random_seed + idx if random_seed is not None else None
            block = block.random_shuffle(seed_i)
            block = BlockAccessor.for_block(block)

        slice_sz = max(1, math.ceil(block.num_rows() / output_num_blocks))
        slices = []
        for i in range(output_num_blocks):
            slices.append(block.slice(i * slice_sz, (i + 1) * slice_sz, copy=True))

        # Randomize the distribution order of the blocks (this prevents empty
        # outputs when input blocks are very small).
        if random_shuffle:
            random = np.random.RandomState(seed_i)
            random.shuffle(slices)

        num_rows = sum(BlockAccessor.for_block(s).num_rows() for s in slices)
        assert num_rows == block.num_rows(), (num_rows, block.num_rows())
        metadata = block.get_metadata(input_files=None, exec_stats=stats.build())
        return [metadata] + slices

    @staticmethod
    def reduce(
        random_shuffle: bool, random_seed: Optional[int], *mapper_outputs: List[Block]
    ) -> (Block, BlockMetadata):
        stats = BlockExecStats.builder()
        builder = DelegatingBlockBuilder()
        for block in mapper_outputs:
            builder.add_block(block)
        new_block = builder.build()
        accessor = BlockAccessor.for_block(new_block)
        if random_shuffle:
            new_block = accessor.random_shuffle(
                random_seed if random_seed is not None else None
            )
            accessor = BlockAccessor.for_block(new_block)
        new_metadata = BlockMetadata(
            num_rows=accessor.num_rows(),
            size_bytes=accessor.size_bytes(),
            schema=accessor.schema(),
            input_files=None,
            exec_stats=stats.build(),
        )
        return new_block, new_metadata
