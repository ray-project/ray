from collections import defaultdict

import pytest

import ray
from ray._raylet import NodeID
from ray.data._internal.planner.exchange.push_based_shuffle_task_scheduler import (
    PushBasedShuffleTaskScheduler,
)


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

    node_id_1 = NodeID.from_random().hex()
    node_id_2 = NodeID.from_random().hex()
    node_id_3 = NodeID.from_random().hex()
    for num_cpus in range(1, 20):
        _test(20, 3, {node_id_1: num_cpus})
    _test(20, 3, {node_id_1: 100})
    _test(100, 3, {node_id_1: 10, node_id_2: 10, node_id_3: 10})
    _test(100, 10, {node_id_1: 10, node_id_2: 10, node_id_3: 10})
    # Regression test for https://github.com/ray-project/ray/issues/25863.
    _test(1000, 2, {NodeID.from_random().hex(): 16 for i in range(20)})
    # Regression test for https://github.com/ray-project/ray/issues/37754.
    _test(260, 2, {node_id_1: 128})
    _test(1, 2, {node_id_1: 128})

    # Test float merge_factor.
    for cluster_config in [
        {node_id_1: 10},
        {node_id_1: 10, node_id_2: 10},
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
        ds = ray.data.range(1000, override_num_blocks=parallelism).random_shuffle()
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


def test_sort_multinode(ray_start_cluster, configure_shuffle_method):
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
    ds = (
        ray.data.range(1000, override_num_blocks=parallelism)
        .random_shuffle()
        .sort("id")
    )
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

    def ray_get_override(object_refs, *args, **kwargs):
        callback(object_refs)
        return original_ray_get(object_refs, *args, **kwargs)

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

        ds = ray.data.range(
            1000, override_num_blocks=num_output_blocks
        ).random_shuffle()
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
