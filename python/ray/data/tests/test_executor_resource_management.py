import pytest

import ray
from ray.data._internal.compute import ActorPoolStrategy, TaskPoolStrategy
from ray.data._internal.execution.interfaces import ExecutionOptions, ExecutionResources
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_operators import _mul2_map_data_prcessor
from ray.data.tests.util import run_op_tasks_sync

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
        _mul2_map_data_prcessor,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=TaskPoolStrategy(),
    )
    assert op.base_resource_usage() == ExecutionResources()
    data_context = ray.data.DataContext.get_current()
    inc_obj_store_mem = (
        data_context._max_num_blocks_in_streaming_gen_buffer
        * data_context.target_max_block_size
    )
    assert op.incremental_resource_usage() == ExecutionResources(
        cpu=1,
        gpu=0,
        object_store_memory=inc_obj_store_mem,
    )
    assert op._ray_remote_args == {"num_cpus": 1}

    op = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=TaskPoolStrategy(),
        ray_remote_args={"num_gpus": 2},
    )
    assert op.base_resource_usage() == ExecutionResources()
    assert op.incremental_resource_usage() == ExecutionResources(
        cpu=0, gpu=2, object_store_memory=inc_obj_store_mem
    )
    assert op._ray_remote_args == {"num_gpus": 2}

    with pytest.raises(ValueError):
        MapOperator.create(
            _mul2_map_data_prcessor,
            input_op=input_op,
            name="TestMapper",
            compute_strategy=TaskPoolStrategy(),
            ray_remote_args={"num_gpus": 2, "num_cpus": 1},
        )


def test_scheduling_strategy_overrides(ray_start_10_cpus_shared, restore_data_context):
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    op = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=TaskPoolStrategy(),
        ray_remote_args={"num_gpus": 2, "scheduling_strategy": "DEFAULT"},
    )
    assert op._ray_remote_args == {"num_gpus": 2, "scheduling_strategy": "DEFAULT"}

    ray.data.DataContext.get_current().scheduling_strategy = "DEFAULT"
    op = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=TaskPoolStrategy(),
        ray_remote_args={"num_gpus": 2},
    )
    assert op._ray_remote_args == {"num_gpus": 2}


def test_task_pool_resource_reporting(ray_start_10_cpus_shared):
    ctx = ray.data.DataContext.get_current()
    ctx._max_num_blocks_in_streaming_gen_buffer = 1
    input_op = InputDataBuffer(make_ref_bundles([[SMALL_STR] for i in range(100)]))
    op = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=TaskPoolStrategy(),
    )
    op.start(ExecutionOptions())

    assert op.current_processor_usage() == ExecutionResources(cpu=0, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == 0
    assert op.metrics.obj_store_mem_pending_task_inputs == 0
    assert op.metrics.obj_store_mem_pending_task_outputs == 0

    op.add_input(input_op.get_next(), 0)
    op.add_input(input_op.get_next(), 0)
    assert op.current_processor_usage() == ExecutionResources(cpu=2, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == 0
    assert op.metrics.obj_store_mem_pending_task_inputs == pytest.approx(1600, rel=0.5)
    assert op.metrics.obj_store_mem_pending_task_outputs == pytest.approx(
        2  # Number of active tasks
        * ctx._max_num_blocks_in_streaming_gen_buffer
        * ctx.target_max_block_size,
        rel=0.5,
    )

    run_op_tasks_sync(op)

    assert op.current_processor_usage() == ExecutionResources(cpu=0, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == pytest.approx(3200, rel=0.5)
    assert op.metrics.obj_store_mem_pending_task_inputs == 0
    assert op.metrics.obj_store_mem_pending_task_outputs == 0


def test_task_pool_resource_reporting_with_bundling(ray_start_10_cpus_shared):
    ctx = ray.data.DataContext.get_current()
    ctx._max_num_blocks_in_streaming_gen_buffer = 1
    input_op = InputDataBuffer(make_ref_bundles([[SMALL_STR] for i in range(100)]))
    op = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=TaskPoolStrategy(),
        min_rows_per_bundle=3,
    )
    op.start(ExecutionOptions())

    assert op.current_processor_usage() == ExecutionResources(cpu=0, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == 0
    assert op.metrics.obj_store_mem_pending_task_inputs == 0
    assert op.metrics.obj_store_mem_pending_task_outputs == 0

    op.add_input(input_op.get_next(), 0)
    # No tasks submitted yet due to bundling.
    assert op.current_processor_usage() == ExecutionResources(cpu=0, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == pytest.approx(800, rel=0.5)
    assert op.metrics.obj_store_mem_internal_outqueue == 0
    assert op.metrics.obj_store_mem_pending_task_inputs == 0
    assert op.metrics.obj_store_mem_pending_task_outputs == 0

    op.add_input(input_op.get_next(), 0)
    # No tasks submitted yet due to bundling.
    assert op.current_processor_usage() == ExecutionResources(cpu=0, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == pytest.approx(1600, rel=0.5)
    assert op.metrics.obj_store_mem_internal_outqueue == 0
    assert op.metrics.obj_store_mem_pending_task_inputs == 0
    assert op.metrics.obj_store_mem_pending_task_outputs == 0

    op.add_input(input_op.get_next(), 0)
    # Task has now been submitted since we've met the minimum bundle size.
    assert op.current_processor_usage() == ExecutionResources(cpu=1, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == 0
    assert op.metrics.obj_store_mem_pending_task_inputs == pytest.approx(2400, rel=0.5)
    assert op.metrics.obj_store_mem_pending_task_outputs == pytest.approx(
        1  # Number of active tasks
        * ctx._max_num_blocks_in_streaming_gen_buffer
        * ctx.target_max_block_size,
        rel=0.5,
    )


def test_actor_pool_resource_reporting(ray_start_10_cpus_shared, restore_data_context):
    ctx = ray.data.DataContext.get_current()
    ctx._max_num_blocks_in_streaming_gen_buffer = 1
    input_op = InputDataBuffer(make_ref_bundles([[SMALL_STR] for i in range(100)]))
    op = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=ActorPoolStrategy(
            min_size=2, max_size=10, max_tasks_in_flight_per_actor=2
        ),
    )
    op.start(ExecutionOptions())

    data_context = ray.data.DataContext.get_current()
    inc_obj_store_mem = (
        data_context._max_num_blocks_in_streaming_gen_buffer
        * data_context.target_max_block_size
    )
    assert op.base_resource_usage() == ExecutionResources(cpu=2, gpu=0)
    # All actors are idle (pending creation), therefore shouldn't need to scale up when
    # submitting a new task, so incremental resource usage should be 0.
    assert op.incremental_resource_usage() == ExecutionResources(
        cpu=0, gpu=0, object_store_memory=inc_obj_store_mem
    )
    assert op.current_processor_usage() == ExecutionResources(cpu=2, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == 0
    assert op.metrics.obj_store_mem_pending_task_inputs == 0
    assert op.metrics.obj_store_mem_pending_task_outputs == 0

    # Add inputs.
    for i in range(4):
        # Pool is still idle while waiting for actors to start, so additional tasks
        # shouldn't trigger scale-up, so incremental resource usage should still be 0.
        assert op.incremental_resource_usage() == ExecutionResources(
            cpu=0, gpu=0, object_store_memory=inc_obj_store_mem
        )
        op.add_input(input_op.get_next(), 0)
        assert op.current_processor_usage() == ExecutionResources(cpu=2, gpu=0)
        assert op.metrics.obj_store_mem_internal_inqueue == pytest.approx(
            (i + 1) * 800, rel=0.5
        )
        assert op.metrics.obj_store_mem_internal_outqueue == 0
        assert op.metrics.obj_store_mem_pending_task_inputs == 0
        assert op.metrics.obj_store_mem_pending_task_outputs == 0

    # Wait for actors to start.
    assert op.num_active_tasks() == 2
    run_op_tasks_sync(op, only_existing=True)

    # Now that both actors have started, a new task would trigger scale-up, so
    inc_usage = op.incremental_resource_usage()
    assert inc_usage.cpu == 1, inc_usage
    assert inc_usage.gpu == 0, inc_usage

    # Actors have now started and the pool is actively running tasks.
    assert op.current_processor_usage() == ExecutionResources(cpu=2, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == 0
    assert op.metrics.obj_store_mem_pending_task_inputs == pytest.approx(3200, rel=0.5)
    assert op.metrics.obj_store_mem_pending_task_outputs == pytest.approx(
        2  # We launched 4 tasks across 2 actor, but only 2 tasks run at a time
        * ctx._max_num_blocks_in_streaming_gen_buffer
        * ctx.target_max_block_size,
        rel=0.5,
    )

    # Indicate that no more inputs will arrive.
    op.all_inputs_done()

    # Wait until tasks are done.
    run_op_tasks_sync(op)

    # Work is done and the pool has been scaled down.
    assert op.current_processor_usage() == ExecutionResources(cpu=0, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == pytest.approx(
        6400,
        rel=0.5,
    )
    assert op.metrics.obj_store_mem_pending_task_inputs == 0
    assert op.metrics.obj_store_mem_pending_task_outputs == 0

    # Consume task outputs.
    while op.has_next():
        op.get_next()

    # Work is done, pool has been scaled down, and outputs have been consumed.
    assert op.current_processor_usage() == ExecutionResources(cpu=0, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == 0
    assert op.metrics.obj_store_mem_pending_task_inputs == 0
    assert op.metrics.obj_store_mem_pending_task_outputs == 0


def test_actor_pool_resource_reporting_with_bundling(ray_start_10_cpus_shared):
    ctx = ray.data.DataContext.get_current()
    ctx._max_num_blocks_in_streaming_gen_buffer = 1
    input_op = InputDataBuffer(make_ref_bundles([[SMALL_STR] for i in range(100)]))
    op = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=ActorPoolStrategy(min_size=2, max_size=10),
        min_rows_per_bundle=2,
    )
    op.start(ExecutionOptions())

    data_context = ray.data.DataContext.get_current()
    inc_obj_store_mem = (
        data_context._max_num_blocks_in_streaming_gen_buffer
        * data_context.target_max_block_size
    )
    assert op.base_resource_usage() == ExecutionResources(cpu=2, gpu=0)
    # All actors are idle (pending creation), therefore shouldn't need to scale up when
    # submitting a new task, so incremental resource usage should be 0.
    assert op.incremental_resource_usage() == ExecutionResources(
        cpu=0, gpu=0, object_store_memory=inc_obj_store_mem
    )
    assert op.current_processor_usage() == ExecutionResources(cpu=2, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == 0
    assert op.metrics.obj_store_mem_pending_task_inputs == 0
    assert op.metrics.obj_store_mem_pending_task_outputs == 0

    # Add inputs.
    for i in range(4):
        # Pool is still idle while waiting for actors to start, so additional tasks
        # shouldn't trigger scale-up, so incremental resource usage should still be 0.
        assert op.incremental_resource_usage() == ExecutionResources(
            cpu=0, gpu=0, object_store_memory=inc_obj_store_mem
        )
        op.add_input(input_op.get_next(), 0)
        assert op.current_processor_usage() == ExecutionResources(cpu=2, gpu=0)
        assert op.metrics.obj_store_mem_internal_inqueue == pytest.approx(
            (i + 1) * 800, rel=0.5
        )
        assert op.metrics.obj_store_mem_internal_outqueue == 0
        assert op.metrics.obj_store_mem_pending_task_inputs == 0
        assert op.metrics.obj_store_mem_pending_task_outputs == 0

    # Pool is still idle while waiting for actors to start.
    assert op.current_processor_usage() == ExecutionResources(cpu=2, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == pytest.approx(3200, rel=0.5)
    assert op.metrics.obj_store_mem_internal_outqueue == 0
    assert op.metrics.obj_store_mem_pending_task_inputs == 0
    assert op.metrics.obj_store_mem_pending_task_outputs == 0

    # Wait for actors to start.
    assert op.num_active_tasks() == 2
    run_op_tasks_sync(op, only_existing=True)

    # Now that both actors have started, a new task would trigger scale-up, so
    inc_usage = op.incremental_resource_usage()
    assert inc_usage.cpu == 1, inc_usage
    assert inc_usage.gpu == 0, inc_usage

    # Actors have now started and the pool is actively running tasks.
    assert op.current_processor_usage() == ExecutionResources(cpu=2, gpu=0)

    # Indicate that no more inputs will arrive.
    op.all_inputs_done()

    # Wait until tasks are done.
    run_op_tasks_sync(op)

    # Work is done and the pool has been scaled down.
    assert op.current_processor_usage() == ExecutionResources(cpu=0, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == pytest.approx(6400, rel=0.5)
    assert op.metrics.obj_store_mem_pending_task_inputs == 0
    assert op.metrics.obj_store_mem_pending_task_outputs == 0

    # Consume task outputs.
    while op.has_next():
        op.get_next()

    # Work is done, pool has been scaled down, and outputs have been consumed.
    assert op.current_processor_usage() == ExecutionResources(cpu=0, gpu=0)
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == 0
    assert op.metrics.obj_store_mem_pending_task_inputs == 0
    assert op.metrics.obj_store_mem_pending_task_outputs == 0


def test_limit_resource_reporting(ray_start_10_cpus_shared):
    input_op = InputDataBuffer(
        make_ref_bundles([[SMALL_STR, SMALL_STR] for i in range(2)])
    )  # Two two-row bundles
    op = LimitOperator(3, input_op)
    op.start(ExecutionOptions())

    assert op.current_processor_usage() == ExecutionResources(
        cpu=0, gpu=0, object_store_memory=0
    )
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == 0

    op.add_input(input_op.get_next(), 0)
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == pytest.approx(1600, rel=0.5)

    op.add_input(input_op.get_next(), 0)
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == pytest.approx(2400, rel=0.5)

    while op.has_next():
        op.get_next()
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == 0


def test_output_splitter_resource_reporting(ray_start_10_cpus_shared):
    input_op = InputDataBuffer(make_ref_bundles([[SMALL_STR] for i in range(4)]))
    op = OutputSplitter(input_op, 2, equal=False, locality_hints=["0", "1"])
    op.start(ExecutionOptions())

    assert op.current_processor_usage() == ExecutionResources(
        cpu=0, gpu=0, object_store_memory=0
    )
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == 0

    # 2 * n inputs are bufferred to optimize locality.
    for i in range(3):
        op.add_input(input_op.get_next(), 0)
        assert op.metrics.obj_store_mem_internal_inqueue == pytest.approx(
            800 * (i + 1), rel=0.5
        )
        assert op.metrics.obj_store_mem_internal_outqueue == 0

    op.add_input(input_op.get_next(), 0)
    assert op.metrics.obj_store_mem_internal_inqueue == pytest.approx(2400, rel=0.5)
    assert op.metrics.obj_store_mem_internal_outqueue == pytest.approx(800, rel=0.5)

    op.all_inputs_done()
    while op.has_next():
        op.get_next()
    assert op.metrics.obj_store_mem_internal_inqueue == 0
    assert op.metrics.obj_store_mem_internal_outqueue == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
