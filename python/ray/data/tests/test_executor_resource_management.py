import pytest

from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    ExecutionOptions,
)
from ray.data._internal.compute import TaskPoolStrategy, ActorPoolStrategy
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.tests.test_operators import _mul2_transform


def test_resource_utils():
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
        assert r.satisfies_limits(r)
        assert r.satisfies_limits(ExecutionResources())
    assert r2.satisfies_limits(r3)
    assert r3.satisfies_limits(r2)
    assert r4.satisfies_limits(r5)
    assert not r5.satisfies_limits(r4)


def test_resource_canonicalization():
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    op = MapOperator(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=TaskPoolStrategy(),
    )
    assert op.base_resource_usage() == ExecutionResources()
    assert op.incremental_resource_usage() == ExecutionResources(cpu=1, gpu=0)
    assert op._ray_remote_args == {"num_cpus": 1}

    op = MapOperator(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=TaskPoolStrategy(),
        ray_remote_args={"num_gpus": 2},
    )
    assert op.base_resource_usage() == ExecutionResources()
    assert op.incremental_resource_usage() == ExecutionResources(cpu=0, gpu=2)
    assert op._ray_remote_args == {"num_gpus": 2}

    with pytest.raises(ValueError):
        MapOperator(
            _mul2_transform,
            input_op=input_op,
            name="TestMapper",
            compute_strategy=TaskPoolStrategy(),
            ray_remote_args={"num_gpus": 2, "num_cpus": 1},
        )


def test_task_pool_resource_reporting():
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    op = MapOperator(
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
    assert 80 < usage.object_store_memory < 160, usage


def test_actor_pool_resource_reporting():
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    op = MapOperator(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=ActorPoolStrategy(2, 2),
    )
    op.start(ExecutionOptions())
    assert op.base_resource_usage() == ExecutionResources(cpu=2, gpu=0)
    assert op.incremental_resource_usage() == ExecutionResources(cpu=0, gpu=0)
    assert op.current_resource_usage() == ExecutionResources(
        cpu=2, gpu=0, object_store_memory=0
    )

    # Pool is idle.
    assert op.current_resource_usage() == ExecutionResources(
        cpu=2, gpu=0, object_store_memory=0
    )

    # Pool is active running tasks.
    for _ in range(4):
        assert op.incremental_resource_usage() == ExecutionResources(cpu=0, gpu=0)
        op.add_input(input_op.get_next(), 0)
    usage = op.current_resource_usage()
    assert usage.cpu == 2, usage
    assert usage.gpu == 0, usage
    assert 160 < usage.object_store_memory < 320, usage

    # Any further inputs would require adding new actors.
    # TODO: test autoscaling resource reporting.
    # assert op.incremental_resource_usage() == ExecutionResources(cpu=1, gpu=0)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
