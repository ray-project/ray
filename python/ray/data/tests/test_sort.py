import logging
import random
from collections import defaultdict

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data import Dataset
from ray.data._internal.planner.exchange.push_based_shuffle_task_scheduler import (
    PushBasedShuffleTaskScheduler,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKey, SortTaskSpec
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize(
    "descending,boundaries",
    [
        (True, list(range(100, 1000, 200))),
        (False, list(range(100, 1000, 200))),
        (True, [1, 998]),
        (False, [1, 998]),
        # Test float.
        (True, [501.5]),
        (False, [501.5]),
    ],
)
def test_sort_with_specified_boundaries(ray_start_regular, descending, boundaries):
    num_items = 1000
    ds = ray.data.range(num_items)
    ds = ds.sort("id", descending, boundaries).materialize()

    items = range(num_items)
    boundaries = [0] + sorted([round(b) for b in boundaries]) + [num_items]
    expected_blocks = [
        items[boundaries[i] : boundaries[i + 1]] for i in range(len(boundaries) - 1)
    ]
    if descending:
        expected_blocks = [list(reversed(block)) for block in reversed(expected_blocks)]

    blocks = list(ds.iter_batches(batch_size=None))
    assert len(blocks) == len(expected_blocks)
    for block, expected_block in zip(blocks, expected_blocks):
        assert np.all(block["id"] == expected_block)


def test_sort_simple(ray_start_regular, use_push_based_shuffle):
    num_items = 100
    parallelism = 4
    xs = list(range(num_items))
    random.shuffle(xs)
    ds = ray.data.from_items(xs, parallelism=parallelism)
    assert extract_values("item", ds.sort("item").take(num_items)) == list(
        range(num_items)
    )
    # Make sure we have rows in each block.
    assert len([n for n in ds.sort("item")._block_num_rows() if n > 0]) == parallelism

    assert extract_values(
        "item", ds.sort("item", descending=True).take(num_items)
    ) == list(reversed(range(num_items)))

    # Test empty dataset.
    ds = ray.data.from_items([])
    s1 = ds.sort("item")
    assert s1.count() == 0
    assert s1.take() == ds.take()
    ds = ray.data.range(10).filter(lambda r: r["id"] > 10).sort("id")
    assert ds.count() == 0


def test_sort_partition_same_key_to_same_block(
    ray_start_regular, use_push_based_shuffle
):
    num_items = 100
    xs = [1] * num_items
    ds = ray.data.from_items(xs)
    sorted_ds = ds.repartition(num_items).sort("item")

    # We still have 100 blocks
    assert len(sorted_ds._block_num_rows()) == num_items
    # Only one of them is non-empty
    count = sum(1 for x in sorted_ds._block_num_rows() if x > 0)
    assert count == 1
    # That non-empty block contains all rows
    total = sum(x for x in sorted_ds._block_num_rows() if x > 0)
    assert total == num_items


@pytest.mark.parametrize("num_items,parallelism", [(100, 1), (1000, 4)])
@pytest.mark.parametrize("use_polars", [False, True])
def test_sort_arrow(
    ray_start_regular, num_items, parallelism, use_push_based_shuffle, use_polars
):
    ctx = ray.data.context.DataContext.get_current()

    try:
        original_use_polars = ctx.use_polars
        ctx.use_polars = use_polars

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
        ds = ray.data.from_pandas(dfs).map_batches(
            lambda t: t, batch_format="pyarrow", batch_size=None
        )

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
        ctx.use_polars = original_use_polars


@pytest.mark.parametrize("use_polars", [False, True])
def test_sort_arrow_with_empty_blocks(
    ray_start_regular, use_push_based_shuffle, use_polars
):
    ctx = ray.data.context.DataContext.get_current()

    try:
        original_use_polars = ctx.use_polars
        ctx.use_polars = use_polars

        assert (
            BlockAccessor.for_block(pa.Table.from_pydict({}))
            .sample(10, SortKey("A"))
            .num_rows
            == 0
        )

        partitions = BlockAccessor.for_block(
            pa.Table.from_pydict({})
        ).sort_and_partition([1, 5, 10], SortKey("A"))
        assert len(partitions) == 4
        for partition in partitions:
            assert partition.num_rows == 0

        assert (
            BlockAccessor.for_block(pa.Table.from_pydict({}))
            .merge_sorted_blocks([pa.Table.from_pydict({})], SortKey("A"))[0]
            .num_rows
            == 0
        )

        ds = ray.data.from_items(
            [{"A": (x % 3), "B": x} for x in range(3)], parallelism=3
        )
        ds = ds.filter(lambda r: r["A"] == 0)
        assert list(ds.sort("A").iter_rows()) == [{"A": 0, "B": 0}]

        # Test empty dataset.
        ds = ray.data.range(10).filter(lambda r: r["id"] > 10)
        assert (
            len(
                SortTaskSpec.sample_boundaries(
                    ds._plan.execute().get_blocks(), SortKey("id"), 3
                )
            )
            == 2
        )
        assert ds.sort("id").count() == 0
    finally:
        ctx.use_polars = original_use_polars


@pytest.mark.parametrize("descending", [False, True])
@pytest.mark.parametrize("batch_format", ["pyarrow", "pandas"])
def test_sort_with_multiple_keys(ray_start_regular, descending, batch_format):
    num_items = 1000
    num_blocks = 100
    df = pd.DataFrame(
        {
            "a": [random.choice("ABCD") for _ in range(num_items)],
            "b": [x % 3 for x in range(num_items)],
            "c": [bool(random.getrandbits(1)) for _ in range(num_items)],
        }
    )
    ds = ray.data.from_pandas(df).map_batches(
        lambda t: t,
        batch_format=batch_format,
        batch_size=None,
    )
    df.sort_values(["a", "b", "c"], inplace=True, ascending=not descending)
    sorted_ds = ds.repartition(num_blocks).sort(["a", "b", "c"], descending=descending)

    # Number of blocks is preserved
    assert len(sorted_ds._block_num_rows()) == num_blocks
    # Rows are sorted over the dimensions
    assert [tuple(row.values()) for row in sorted_ds.iter_rows()] == list(
        zip(df["a"], df["b"], df["c"])
    )


@pytest.mark.parametrize("num_items,parallelism", [(100, 1), (1000, 4)])
def test_sort_pandas(ray_start_regular, num_items, parallelism, use_push_based_shuffle):
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
    assert len([n for n in ds.sort(key="a")._block_num_rows() if n > 0]) == parallelism
    assert_sorted(ds.sort(key="b"), zip(a, b))
    assert_sorted(ds.sort(key="a", descending=True), zip(a, b))


def test_sort_pandas_with_empty_blocks(ray_start_regular, use_push_based_shuffle):
    assert (
        BlockAccessor.for_block(pa.Table.from_pydict({}))
        .sample(10, SortKey("A"))
        .num_rows
        == 0
    )

    partitions = BlockAccessor.for_block(pa.Table.from_pydict({})).sort_and_partition(
        [1, 5, 10], SortKey("A")
    )
    assert len(partitions) == 4
    for partition in partitions:
        assert partition.num_rows == 0

    assert (
        BlockAccessor.for_block(pa.Table.from_pydict({}))
        .merge_sorted_blocks([pa.Table.from_pydict({})], SortKey("A"))[0]
        .num_rows
        == 0
    )

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in range(3)], parallelism=3)
    ds = ds.filter(lambda r: r["A"] == 0)
    assert list(ds.sort("A").iter_rows()) == [{"A": 0, "B": 0}]

    # Test empty dataset.
    ds = ray.data.range(10).filter(lambda r: r["id"] > 10)
    assert (
        len(
            SortTaskSpec.sample_boundaries(
                ds._plan.execute().get_blocks(), SortKey("id"), 3
            )
        )
        == 2
    )
    assert ds.sort("id").count() == 0


def test_sort_with_one_block(shutdown_only, use_push_based_shuffle):
    ray.init(num_cpus=8)
    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.verbose_progress = True
    ctx.use_push_based_shuffle = True

    # Use a dataset that will produce only one block to sort.
    ray.data.range(1024).map_batches(
        lambda _: pa.table([pa.array([1])], ["token_counts"])
    ).sum("token_counts")


def test_push_based_shuffle_schedule():
    def _test(num_input_blocks, merge_factor, num_cpus_per_node_map):
        num_cpus = sum(v for v in num_cpus_per_node_map.values())
        op_cls = PushBasedShuffleTaskScheduler
        schedule = op_cls._compute_shuffle_schedule(
            num_cpus_per_node_map, num_input_blocks, merge_factor, num_input_blocks
        )
        # All input blocks will be processed.
        assert (
            schedule.num_rounds * schedule.num_map_tasks_per_round >= num_input_blocks
        )
        # Each round of tasks does not over-subscribe CPUs.
        assert (
            schedule.num_map_tasks_per_round
            + schedule.merge_schedule.num_merge_tasks_per_round
            <= max(num_cpus, 2)
        )
        print(
            "map",
            schedule.num_map_tasks_per_round,
            "merge",
            schedule.merge_schedule.num_merge_tasks_per_round,
            "num_cpus",
            num_cpus,
            "merge_factor",
            merge_factor,
        )
        # Merge factor between map : merge tasks is approximately correct.
        if schedule.num_map_tasks_per_round > merge_factor:
            actual_merge_factor = (
                schedule.num_map_tasks_per_round
                / schedule.merge_schedule.num_merge_tasks_per_round
            )
            next_highest_merge_factor = schedule.num_map_tasks_per_round / (
                schedule.merge_schedule.num_merge_tasks_per_round + 1
            )
            assert actual_merge_factor - 1 <= merge_factor <= actual_merge_factor + 1, (
                next_highest_merge_factor,
                merge_factor,
                actual_merge_factor,
            )
        else:
            assert schedule.merge_schedule.num_merge_tasks_per_round == 1, (
                schedule.num_map_tasks_per_round,
                merge_factor,
            )

        # Tasks are evenly distributed.
        tasks_per_node = defaultdict(int)
        for i in range(schedule.merge_schedule.num_merge_tasks_per_round):
            task_options = schedule.get_merge_task_options(i)
            node_id = task_options["scheduling_strategy"].node_id
            tasks_per_node[node_id] += 1
        low = min(tasks_per_node.values())
        high = low + 1
        assert low <= max(tasks_per_node.values()) <= high

        # Reducers are evenly distributed across mergers.
        num_reducers_per_merge_idx = [
            schedule.merge_schedule.get_num_reducers_per_merge_idx(i)
            for i in range(schedule.merge_schedule.num_merge_tasks_per_round)
        ]
        high = max(num_reducers_per_merge_idx)
        for num_reducers in num_reducers_per_merge_idx:
            assert num_reducers == high or num_reducers == high - 1

        for merge_idx in range(schedule.merge_schedule.num_merge_tasks_per_round):
            assert isinstance(
                schedule.merge_schedule.get_num_reducers_per_merge_idx(merge_idx), int
            )
            assert schedule.merge_schedule.get_num_reducers_per_merge_idx(merge_idx) > 0

        reduce_idxs = list(range(schedule.merge_schedule.output_num_blocks))
        actual_num_reducers_per_merge_idx = [
            0 for _ in range(schedule.merge_schedule.num_merge_tasks_per_round)
        ]
        for reduce_idx in schedule.merge_schedule.round_robin_reduce_idx_iterator():
            reduce_idxs.pop(reduce_idxs.index(reduce_idx))
            actual_num_reducers_per_merge_idx[
                schedule.merge_schedule.get_merge_idx_for_reducer_idx(reduce_idx)
            ] += 1
        # Check that each reduce task is submitted exactly once.
        assert len(reduce_idxs) == 0
        # Check that each merge and reduce task are correctly paired.
        for i, num_reducers in enumerate(actual_num_reducers_per_merge_idx):
            assert (
                num_reducers == num_reducers_per_merge_idx[i]
            ), f"""Merge task [{i}] has {num_reducers} downstream reduce tasks,
            expected {num_reducers_per_merge_idx[i]}."""
            assert num_reducers > 0

    for num_cpus in range(1, 20):
        _test(20, 3, {"node1": num_cpus})
    _test(20, 3, {"node1": 100})
    _test(100, 3, {"node1": 10, "node2": 10, "node3": 10})
    _test(100, 10, {"node1": 10, "node2": 10, "node3": 10})
    # Regression test for https://github.com/ray-project/ray/issues/25863.
    _test(1000, 2, {f"node{i}": 16 for i in range(20)})
    # Regression test for https://github.com/ray-project/ray/issues/37754.
    _test(260, 2, {"node1": 128})
    _test(1, 2, {"node1": 128})

    # Test float merge_factor.
    for cluster_config in [
        {"node1": 10},
        {"node1": 10, "node2": 10},
    ]:
        _test(100, 1, cluster_config)
        _test(100, 1.3, cluster_config)
        _test(100, 1.6, cluster_config)
        _test(100, 1.75, cluster_config)
        _test(100, 2, cluster_config)

        _test(1, 1.2, cluster_config)
        _test(2, 1.2, cluster_config)


def test_push_based_shuffle_stats(ray_start_cluster):
    ctx = ray.data.context.DataContext.get_current()
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
        ds = ds.materialize()
        assert "RandomShuffleMerge" in ds.stats()
        # Check all nodes used.
        assert "2 nodes used" in ds.stats()
        assert "1 nodes used" not in ds.stats()

        # Check all merge tasks are included in stats.
        internal_stats = ds._plan.stats()
        num_merge_tasks = len(internal_stats.metadata["RandomShuffleMerge"])
        # Merge factor is 2 for random_shuffle ops.
        merge_factor = 2
        assert (
            parallelism // (merge_factor + 1)
            <= num_merge_tasks
            <= parallelism // merge_factor
        )

    finally:
        ctx.use_push_based_shuffle = original


def test_sort_multinode(ray_start_cluster, use_push_based_shuffle):
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
    ds = ray.data.range(1000, parallelism=parallelism).random_shuffle().sort("id")
    for i, row in enumerate(ds.iter_rows()):
        assert row["id"] == i


def patch_ray_remote(condition, callback):
    original_ray_remote = ray.remote

    def ray_remote_override(*args, **kwargs):
        def wrapper(fn):
            remote_fn = original_ray_remote(*args, **kwargs)(fn)
            if condition(fn):
                original_remote_options = remote_fn.options

                def options(**task_options):
                    callback(task_options)
                    original_options = original_remote_options(**task_options)
                    return original_options

                remote_fn.options = options
            return remote_fn

        return wrapper

    ray.remote = ray_remote_override
    return original_ray_remote


def patch_ray_get(callback):
    original_ray_get = ray.get

    def ray_get_override(object_refs):
        callback(object_refs)
        return original_ray_get(object_refs)

    ray.get = ray_get_override
    return original_ray_get


@pytest.mark.parametrize("pipeline", [False, True])
def test_push_based_shuffle_reduce_stage_scheduling(ray_start_cluster, pipeline):
    ctx = ray.data.context.DataContext.get_current()
    try:
        original = ctx.use_push_based_shuffle
        ctx.use_push_based_shuffle = True
        ctx.pipeline_push_based_shuffle_reduce_tasks = pipeline

        num_cpus_per_node = 8
        num_nodes = 3
        num_output_blocks = 100

        task_context = {
            "reduce_options_submitted": [],
            # The total number of CPUs available.
            "pipelined_parallelism": num_cpus_per_node * num_nodes,
            # The total number of reduce tasks.
            "total_parallelism": num_output_blocks,
            "num_instances_below_parallelism": 0,
        }

        def reduce_options_patch(task_options):
            task_context["reduce_options_submitted"].append(task_options)

        def check_pipelined(refs):
            if task_context["reduce_options_submitted"]:
                # Check that we have the correct number of tasks in flight.
                if pipeline:
                    # When pipelining, we should limit the number of reduce
                    # tasks in flight based on how many CPUs are in the
                    # cluster.
                    if not (
                        task_context["pipelined_parallelism"]
                        <= len(task_context["reduce_options_submitted"])
                        <= 2 * task_context["pipelined_parallelism"]
                    ):
                        task_context["num_instances_below_parallelism"] += 1
                else:
                    # When not pipelining, we should submit all reduce tasks at
                    # once.
                    assert (
                        len(task_context["reduce_options_submitted"])
                        == task_context["total_parallelism"]
                    )

                # Check that tasks are close to evenly spread across the nodes.
                nodes = defaultdict(int)
                for options in task_context["reduce_options_submitted"]:
                    nodes[options["scheduling_strategy"].node_id] += 1
                assert len(nodes) > 1
                assert min(nodes.values()) >= max(nodes.values()) // 2

            task_context["reduce_options_submitted"].clear()

        ray_remote = patch_ray_remote(
            lambda fn: "reduce" in fn.__name__, reduce_options_patch
        )
        ray_get = patch_ray_get(check_pipelined)

        cluster = ray_start_cluster
        for _ in range(num_nodes):
            cluster.add_node(
                num_cpus=num_cpus_per_node,
            )

        ray.init(cluster.address)

        ds = ray.data.range(1000, parallelism=num_output_blocks).random_shuffle()
        # Only the last round should have fewer tasks in flight.
        assert task_context["num_instances_below_parallelism"] <= 1
        task_context["num_instances_below_parallelism"] = 0

        ds = ds.sort("id")
        # Only the last round should have fewer tasks in flight.
        assert task_context["num_instances_below_parallelism"] <= 1
        task_context["num_instances_below_parallelism"] = 0
        for i, row in enumerate(ds.iter_rows()):
            assert row["id"] == i

    finally:
        ctx.use_push_based_shuffle = original
        ray.remote = ray_remote
        ray.get = ray_get


SHUFFLE_ALL_TO_ALL_OPS = [
    Dataset.random_shuffle,
    lambda ds: ds.sort(key="id"),
    lambda ds: ds.groupby("id").map_groups(lambda group: group),
]


@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
@pytest.mark.parametrize(
    "shuffle_op",
    SHUFFLE_ALL_TO_ALL_OPS,
)
def test_debug_limit_shuffle_execution_to_num_blocks(
    ray_start_regular, restore_data_context, use_push_based_shuffle, shuffle_op
):
    DataContext.get_current().use_push_based_shuffle = use_push_based_shuffle
    shuffle_fn = shuffle_op

    parallelism = 100
    ds = ray.data.range(1000, parallelism=parallelism)
    shuffled_ds = shuffle_fn(ds).materialize()
    shuffled_ds = shuffled_ds.materialize()
    assert shuffled_ds.num_blocks() == parallelism

    DataContext.get_current().set_config(
        "debug_limit_shuffle_execution_to_num_blocks", 1
    )
    shuffled_ds = shuffle_fn(ds).materialize()
    shuffled_ds = shuffled_ds.materialize()
    assert shuffled_ds.num_blocks() == 1


@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
def test_memory_usage(ray_start_regular, restore_data_context, use_push_based_shuffle):
    DataContext.get_current().use_push_based_shuffle = use_push_based_shuffle

    parallelism = 2
    ds = ray.data.range(int(1e8), parallelism=parallelism)
    ds = ds.random_shuffle().materialize()

    stats = ds._get_stats_summary()
    # TODO(swang): Sort on this dataset seems to produce significant skew, so
    # one task uses much more memory than the other.
    for op_stats in stats.operators_stats:
        assert op_stats.memory["max"] < 2000


@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
@pytest.mark.parametrize("under_threshold", [False, True])
def test_sort_object_ref_warnings(
    ray_start_regular,
    restore_data_context,
    use_push_based_shuffle,
    under_threshold,
    propagate_logs,
    caplog,
):
    # Test that we warn iff expected driver memory usage from
    # storing ObjectRefs is higher than the configured
    # threshold.
    warning_str = "Execution is estimated to use"
    warning_str_with_bytes = (
        "Execution is estimated to use at least "
        f"{90 if use_push_based_shuffle else 300}KB"
    )

    DataContext.get_current().use_push_based_shuffle = use_push_based_shuffle
    if not under_threshold:
        DataContext.get_current().warn_on_driver_memory_usage_bytes = 10_000

    ds = ray.data.range(int(1e8), parallelism=10)
    with caplog.at_level(logging.WARNING, logger="ray.data.dataset"):
        ds = ds.random_shuffle().materialize()

    if under_threshold:
        assert warning_str not in caplog.text
        assert warning_str_with_bytes not in caplog.text
    else:
        assert warning_str in caplog.text
        assert warning_str_with_bytes in caplog.text


@pytest.mark.parametrize("use_push_based_shuffle", [False, True])
@pytest.mark.parametrize("under_threshold", [False, True])
def test_sort_inlined_objects_warnings(
    ray_start_regular,
    restore_data_context,
    use_push_based_shuffle,
    under_threshold,
    propagate_logs,
    caplog,
):
    # Test that we warn iff expected driver memory usage from
    # storing tiny Ray objects on driver heap is higher than
    # the configured threshold.
    if use_push_based_shuffle:
        warning_strs = [
            "More than 3MB of driver memory used",
            "More than 7MB of driver memory used",
        ]
    else:
        warning_strs = [
            "More than 8MB of driver memory used",
        ]

    DataContext.get_current().use_push_based_shuffle = use_push_based_shuffle
    if not under_threshold:
        DataContext.get_current().warn_on_driver_memory_usage_bytes = 3_000_000

    ds = ray.data.range(int(1e6), parallelism=10)
    with caplog.at_level(logging.WARNING, logger="ray.data.dataset"):
        ds = ds.random_shuffle().materialize()

    if under_threshold:
        assert all(warning_str not in caplog.text for warning_str in warning_strs)
    else:
        assert all(warning_str in caplog.text for warning_str in warning_strs)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
