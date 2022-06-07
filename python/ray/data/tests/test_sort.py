import random
from collections import defaultdict

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray

from ray.tests.conftest import *  # noqa
from ray.data.block import BlockAccessor
from ray.data.tests.conftest import *  # noqa
from ray.data._internal.push_based_shuffle import PushBasedShufflePlan


@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
def test_sort_simple(ray_start_regular, use_push_based_shuffle):
    ctx = ray.data.context.DatasetContext.get_current()
    try:
        original = ctx.use_push_based_shuffle
        ctx.use_push_based_shuffle = use_push_based_shuffle

        num_items = 100
        parallelism = 4
        xs = list(range(num_items))
        random.shuffle(xs)
        ds = ray.data.from_items(xs, parallelism=parallelism)
        assert ds.sort().take(num_items) == list(range(num_items))
        # Make sure we have rows in each block.
        assert len([n for n in ds.sort()._block_num_rows() if n > 0]) == parallelism
        assert ds.sort(descending=True).take(num_items) == list(
            reversed(range(num_items))
        )
        assert ds.sort(key=lambda x: -x).take(num_items) == list(
            reversed(range(num_items))
        )

        # Test empty dataset.
        ds = ray.data.from_items([])
        s1 = ds.sort()
        assert s1.count() == 0
        assert s1.take() == ds.take()
        ds = ray.data.range(10).filter(lambda r: r > 10).sort()
        assert ds.count() == 0
    finally:
        ctx.use_push_based_shuffle = original


@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
def test_sort_partition_same_key_to_same_block(
    ray_start_regular, use_push_based_shuffle
):
    ctx = ray.data.context.DatasetContext.get_current()

    try:
        original = ctx.use_push_based_shuffle
        ctx.use_push_based_shuffle = use_push_based_shuffle

        num_items = 100
        xs = [1] * num_items
        ds = ray.data.from_items(xs)
        sorted_ds = ds.repartition(num_items).sort()

        # We still have 100 blocks
        assert len(sorted_ds._block_num_rows()) == num_items
        # Only one of them is non-empty
        count = sum(1 for x in sorted_ds._block_num_rows() if x > 0)
        assert count == 1
        # That non-empty block contains all rows
        total = sum(x for x in sorted_ds._block_num_rows() if x > 0)
        assert total == num_items
    finally:
        ctx.use_push_based_shuffle = original


@pytest.mark.parametrize("num_items,parallelism", [(100, 1), (1000, 4)])
@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
def test_sort_arrow(ray_start_regular, num_items, parallelism, use_push_based_shuffle):
    ctx = ray.data.context.DatasetContext.get_current()

    try:
        original = ctx.use_push_based_shuffle
        ctx.use_push_based_shuffle = use_push_based_shuffle

        a = list(reversed(range(num_items)))
        b = [f"{x:03}" for x in range(num_items)]
        shard = int(np.ceil(num_items / parallelism))
        offset = 0
        dfs = []
        while offset < num_items:
            dfs.append(
                pd.DataFrame(
                    {"a": a[offset : offset + shard], "b": b[offset : offset + shard]}
                )
            )
            offset += shard
        if offset < num_items:
            dfs.append(pd.DataFrame({"a": a[offset:], "b": b[offset:]}))
        ds = ray.data.from_pandas(dfs)

        def assert_sorted(sorted_ds, expected_rows):
            assert [tuple(row.values()) for row in sorted_ds.iter_rows()] == list(
                expected_rows
            )

        assert_sorted(ds.sort(key="a"), zip(reversed(a), reversed(b)))
        # Make sure we have rows in each block.
        assert (
            len([n for n in ds.sort(key="a")._block_num_rows() if n > 0]) == parallelism
        )
        assert_sorted(ds.sort(key="b"), zip(a, b))
        assert_sorted(ds.sort(key="a", descending=True), zip(a, b))
    finally:
        ctx.use_push_based_shuffle = original


@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
def test_sort_arrow_with_empty_blocks(ray_start_regular, use_push_based_shuffle):
    ctx = ray.data.context.DatasetContext.get_current()

    try:
        original = ctx.use_push_based_shuffle
        ctx.use_push_based_shuffle = use_push_based_shuffle

        assert (
            BlockAccessor.for_block(pa.Table.from_pydict({})).sample(10, "A").num_rows
            == 0
        )

        partitions = BlockAccessor.for_block(
            pa.Table.from_pydict({})
        ).sort_and_partition([1, 5, 10], "A", descending=False)
        assert len(partitions) == 4
        for partition in partitions:
            assert partition.num_rows == 0

        assert (
            BlockAccessor.for_block(pa.Table.from_pydict({}))
            .merge_sorted_blocks([pa.Table.from_pydict({})], "A", False)[0]
            .num_rows
            == 0
        )

        ds = ray.data.from_items(
            [{"A": (x % 3), "B": x} for x in range(3)], parallelism=3
        )
        ds = ds.filter(lambda r: r["A"] == 0)
        assert [row.as_pydict() for row in ds.sort("A").iter_rows()] == [
            {"A": 0, "B": 0}
        ]

        # Test empty dataset.
        ds = ray.data.range_table(10).filter(lambda r: r["value"] > 10)
        assert (
            len(
                ray.data._internal.sort.sample_boundaries(
                    ds._plan.execute().get_blocks(), "value", 3
                )
            )
            == 2
        )
        assert ds.sort("value").count() == 0
    finally:
        ctx.use_push_based_shuffle = original


def test_push_based_shuffle_schedule():
    def _test(num_input_blocks, merge_factor, num_cpus_per_node_map):
        num_cpus = sum(v for v in num_cpus_per_node_map.values())
        schedule = PushBasedShufflePlan._compute_shuffle_schedule(
            num_cpus_per_node_map, num_input_blocks, merge_factor, num_input_blocks
        )
        # All input blocks will be processed.
        assert (
            schedule.num_rounds * schedule.num_map_tasks_per_round >= num_input_blocks
        )
        # Each round of tasks does not over-subscribe CPUs.
        assert (
            schedule.num_map_tasks_per_round + schedule.num_merge_tasks_per_round
            <= max(num_cpus, 2)
        )
        # Merge factor between map : merge tasks is approximately correct.
        if schedule.num_map_tasks_per_round > merge_factor:
            actual_merge_factor = (
                schedule.num_map_tasks_per_round // schedule.num_merge_tasks_per_round
            )
            next_highest_merge_factor = schedule.num_map_tasks_per_round // (
                schedule.num_merge_tasks_per_round + 1
            )
            assert next_highest_merge_factor <= merge_factor <= actual_merge_factor
        else:
            assert schedule.num_merge_tasks_per_round == 1

        # Tasks are evenly distributed.
        tasks_per_node = defaultdict(int)
        for node_id in schedule.merge_task_placement:
            tasks_per_node[node_id] += 1
        low = min(tasks_per_node.values())
        high = low + 1
        assert low <= max(tasks_per_node.values()) <= high

        # Reducers are evenly distributed across mergers.
        low = min(schedule.num_reducers_per_merge_idx)
        high = low + 1
        assert low <= max(schedule.num_reducers_per_merge_idx) <= high

    for num_cpus in range(1, 20):
        _test(20, 3, {"node1": num_cpus})
    _test(20, 3, {"node1": 100})
    _test(100, 3, {"node1": 10, "node2": 10, "node3": 10})
    _test(100, 10, {"node1": 10, "node2": 10, "node3": 10})


def test_push_based_shuffle_stats(ray_start_cluster):
    ctx = ray.data.context.DatasetContext.get_current()
    try:
        original = ctx.use_push_based_shuffle
        ctx.use_push_based_shuffle = True

        cluster = ray_start_cluster
        cluster.add_node(
            resources={"bar:1": 100},
            num_cpus=10,
            _system_config={"max_direct_call_object_size": 0},
        )
        cluster.add_node(resources={"bar:2": 100}, num_cpus=10)
        cluster.add_node(resources={"bar:3": 100}, num_cpus=0)

        ray.init(cluster.address)

        parallelism = 100
        ds = ray.data.range(1000, parallelism=parallelism).random_shuffle()
        assert "random_shuffle_merge" in ds.stats()
        # Check all nodes used.
        assert "2 nodes used" in ds.stats()
        assert "1 nodes used" not in ds.stats()

        # Check all merge tasks are included in stats.
        internal_stats = ds._plan.stats()
        num_merge_tasks = len(internal_stats.stages["random_shuffle_merge"])
        # Merge factor is 2 for random_shuffle ops.
        merge_factor = 2
        assert (
            parallelism // (merge_factor + 1)
            <= num_merge_tasks
            <= parallelism // merge_factor
        )

    finally:
        ctx.use_push_based_shuffle = original


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
