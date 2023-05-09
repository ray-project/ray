import pytest

import ray
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    ExecutionOptions,
)
from ray.data._internal.compute import TaskPoolStrategy, ActorPoolStrategy
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.tests.test_operators import _mul2_transform
from ray.data.tests.conftest import *  # noqa


SMALL_STR = "hello" * 120


def test_resource_utils(ray_start_10_cpus_shared):
    r1 = ExecutionResources()
    r2 = ExecutionResources(cpu=1)
    r3 = ExecutionResources(gpu=1)
    r4 = ExecutionResources(cpu=1, gpu=1, object_store_memory=100 * 1024 * 1024)
    r5 = ExecutionResources(cpu=1, gpu=1, object_store_memory=1024 * 1024 * 1024)

    # Test str.
    assert r3.object_store_memory_str() == "None"
    assert r4.object_store_memory_str() == "100.0 MiB"
    assert r5.object_store_memory_str() == "1.0 GiB"

    # Test add.
    assert r1.add(r1) == r1
    assert r1.add(r2) == r2
    assert r2.add(r2) == ExecutionResources(cpu=2)
    assert r2.add(r3) == ExecutionResources(cpu=1, gpu=1)
    assert r4.add(r4) == ExecutionResources(
        cpu=2, gpu=2, object_store_memory=200 * 1024 * 1024
    )

    # Test limit.
    for r in [r1, r2, r3, r4, r5]:
        assert r.satisfies_limit(r)
        assert r.satisfies_limit(ExecutionResources())
    assert r2.satisfies_limit(r3)
    assert r3.satisfies_limit(r2)
    assert r4.satisfies_limit(r5)
    assert not r5.satisfies_limit(r4)


def test_resource_canonicalization(ray_start_10_cpus_shared):
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    op = MapOperator.create(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=TaskPoolStrategy(),
    )
    assert op.base_resource_usage() == ExecutionResources()
    assert op.incremental_resource_usage(input_queue_size=1) == ExecutionResources(
        cpu=1, gpu=0
    )
    assert op._ray_remote_args == {"num_cpus": 1}

    op = MapOperator.create(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=TaskPoolStrategy(),
        ray_remote_args={"num_gpus": 2},
    )
    assert op.base_resource_usage() == ExecutionResources()
    assert op.incremental_resource_usage(input_queue_size=1) == ExecutionResources(
        cpu=0, gpu=2
    )
    assert op._ray_remote_args == {"num_gpus": 2}

    with pytest.raises(ValueError):
        MapOperator.create(
            _mul2_transform,
            input_op=input_op,
            name="TestMapper",
            compute_strategy=TaskPoolStrategy(),
            ray_remote_args={"num_gpus": 2, "num_cpus": 1},
        )


def test_task_pool_resource_reporting(ray_start_10_cpus_shared):
    input_op = InputDataBuffer(make_ref_bundles([[SMALL_STR] for i in range(100)]))
    op = MapOperator.create(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=TaskPoolStrategy(),
    )
    assert op.current_resource_usage() == ExecutionResources(
        cpu=0, gpu=0, object_store_memory=0
    )
    op.start(ExecutionOptions())
    op.add_input(input_op.get_next(), 0)
    op.add_input(input_op.get_next(), 0)
    usage = op.current_resource_usage()
    assert usage.cpu == 2, usage
    assert usage.gpu == 0, usage
    assert usage.object_store_memory == pytest.approx(1280, rel=0.5), usage


def test_task_pool_resource_reporting_with_bundling(ray_start_10_cpus_shared):
    input_op = InputDataBuffer(make_ref_bundles([[SMALL_STR] for i in range(100)]))
    op = MapOperator.create(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=TaskPoolStrategy(),
        min_rows_per_bundle=3,
    )
    assert op.current_resource_usage() == ExecutionResources(
        cpu=0, gpu=0, object_store_memory=0
    )
    op.start(ExecutionOptions())
    op.add_input(input_op.get_next(), 0)
    usage = op.current_resource_usage()
    # No tasks submitted yet due to bundling.
    assert usage.cpu == 0, usage
    assert usage.gpu == 0, usage
    # Queued bundles (in bundler) still count against object storage usage.
    assert usage.object_store_memory == pytest.approx(800, rel=0.5), usage
    op.add_input(input_op.get_next(), 0)
    usage = op.current_resource_usage()
    # No tasks submitted yet due to bundling.
    assert usage.cpu == 0, usage
    assert usage.gpu == 0, usage
    # Queued bundles (in bundler) still count against object storage usage.
    assert usage.object_store_memory == pytest.approx(1600, rel=0.5), usage
    op.add_input(input_op.get_next(), 0)
    usage = op.current_resource_usage()
    # Task has now been submitted since we've met the minimum bundle size.
    assert usage.cpu == 1, usage
    assert usage.gpu == 0, usage
    assert usage.object_store_memory == pytest.approx(2400, rel=0.5), usage


def test_actor_pool_resource_reporting(ray_start_10_cpus_shared):
    input_op = InputDataBuffer(make_ref_bundles([[SMALL_STR] for i in range(100)]))
    op = MapOperator.create(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=ActorPoolStrategy(
            min_size=2, max_size=10, max_tasks_in_flight_per_actor=2
        ),
    )
    op.start(ExecutionOptions())

    # Minimum actors should be started.
    assert op._actor_pool.num_running_actors() == 2

    assert op.base_resource_usage() == ExecutionResources(cpu=2, gpu=0)
    # All actors are inactive, therefore shouldn't need to scale up when
    # submitting a new task, so incremental resource usage should be 0.
    assert op.incremental_resource_usage(input_queue_size=1) == ExecutionResources(
        cpu=0, gpu=0
    )
    # Actors should count against CPU utilization.
    assert op.current_resource_usage() == ExecutionResources(
        cpu=2, gpu=0, object_store_memory=0
    )

    # Add inputs.
    for i in range(4):
        # Actor pool size of 2 with 2 tasks in flight per actor should be able to
        # handle 4 new inputs without scaling up.
        # So incremental resource usage should still be 0.
        assert op.incremental_resource_usage(
            input_queue_size=4 - i
        ) == ExecutionResources(cpu=0, gpu=0)
        op.add_input(input_op.get_next(), 0)
        usage = op.current_resource_usage()
        assert usage.cpu == 2, usage
        assert usage.gpu == 0, usage
        # Queued bundles still count against object store usage.
        assert usage.object_store_memory == pytest.approx((i + 1) * 800, rel=0.5), usage

    # Actors have started and the pool is actively running tasks.
    usage = op.current_resource_usage()
    assert usage.cpu == 2, usage
    assert usage.gpu == 0, usage
    # Now that tasks have been submitted, object store memory is accounted for.
    assert usage.object_store_memory == pytest.approx(2560, rel=0.5), usage

    # Now that both actors have maxed out, a new task would trigger scale-up, so
    # incremental resource usage should be 1 CPU.
    inc_usage = op.incremental_resource_usage(input_queue_size=1)
    assert inc_usage.cpu == 1, inc_usage
    assert inc_usage.gpu == 0, inc_usage

    # Indicate that no more inputs will arrive.
    op.inputs_done()

    # Wait until tasks are done.
    work_refs = op.get_work_refs()
    while work_refs:
        for work_ref in work_refs:
            ray.get(work_ref)
            op.notify_work_completed(work_ref)
        work_refs = op.get_work_refs()

    # Work is done and the pool has been scaled down.
    usage = op.current_resource_usage()
    assert usage.cpu == 0, usage
    assert usage.gpu == 0, usage
    # The output dataframe of _mult2_transform is ~1375 bytes.
    # So 4 outputs results in ~5500 bytes in the object store.
    assert usage.object_store_memory == pytest.approx(5500, rel=0.5), usage

    # Consume task outputs.
    while op.has_next():
        op.get_next()

    # Work is done, pool has been scaled down, and outputs have been consumed.
    usage = op.current_resource_usage()
    assert usage.cpu == 0, usage
    assert usage.gpu == 0, usage
    assert usage.object_store_memory == 0, usage


def test_actor_pool_resource_reporting_with_bundling(ray_start_10_cpus_shared):
    input_op = InputDataBuffer(make_ref_bundles([[SMALL_STR] for i in range(100)]))
    op = MapOperator.create(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=ActorPoolStrategy(
            min_size=2, max_size=10, max_tasks_in_flight_per_actor=2
        ),
        min_rows_per_bundle=2,
    )
    op.start(ExecutionOptions())
    assert op.base_resource_usage() == ExecutionResources(cpu=2, gpu=0)

    # Minimum actors should be started.
    assert op._actor_pool.num_running_actors() == 2

    assert op.base_resource_usage() == ExecutionResources(cpu=2, gpu=0)
    # All actors are inactive, therefore shouldn't need to scale up when
    # submitting a new task, so incremental resource usage should be 0.
    assert op.incremental_resource_usage(input_queue_size=1) == ExecutionResources(
        cpu=0, gpu=0
    )
    # Actors should count against CPU utilization.
    assert op.current_resource_usage() == ExecutionResources(
        cpu=2, gpu=0, object_store_memory=0
    )

    # Add inputs.
    for i in range(8):
        # Actor pool size of 2 with 2 tasks in flight per actor and 2 rows per bundle
        # should be able to handle 8 new inputs without scaling up.
        # So incremental resource usage should still be 0.
        assert op.incremental_resource_usage(
            input_queue_size=4 - i
        ) == ExecutionResources(cpu=0, gpu=0)
        op.add_input(input_op.get_next(), 0)
        usage = op.current_resource_usage()
        assert usage.cpu == 2, usage
        assert usage.gpu == 0, usage
        # Queued bundles still count against object store usage.
        assert usage.object_store_memory == pytest.approx((i + 1) * 800, rel=0.5), usage

    # Actors have started and the pool is actively running tasks.
    usage = op.current_resource_usage()
    assert usage.cpu == 2, usage
    assert usage.gpu == 0, usage
    # Now that tasks have been submitted, object store memory is accounted for.
    assert usage.object_store_memory == pytest.approx(6400, rel=0.5), usage

    # Now that both actors have maxed out, a new task would trigger scale-up, so
    # incremental resource usage should be 1 CPU.
    inc_usage = op.incremental_resource_usage(input_queue_size=1)
    assert inc_usage.cpu == 1, inc_usage
    assert inc_usage.gpu == 0, inc_usage

    # Indicate that no more inputs will arrive.
    op.inputs_done()

    # Wait until tasks are done.
    work_refs = op.get_work_refs()
    while work_refs:
        for work_ref in work_refs:
            ray.get(work_ref)
            op.notify_work_completed(work_ref)
        work_refs = op.get_work_refs()

    # Work is done and the pool has been scaled down.
    usage = op.current_resource_usage()
    assert usage.cpu == 0, usage
    assert usage.gpu == 0, usage
    # The output dataframe of _mult2_transform is ~1375 bytes.
    # So 8 outputs results in ~11000 bytes in the object store.
    assert usage.object_store_memory == pytest.approx(11000, rel=0.5), usage

    # Consume task outputs.
    while op.has_next():
        op.get_next()

    # Work is done, pool has been scaled down, and outputs have been consumed.
    usage = op.current_resource_usage()
    assert usage.cpu == 0, usage
    assert usage.gpu == 0, usage
    assert usage.object_store_memory == 0, usage


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
