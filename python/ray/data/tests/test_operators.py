import pytest
from typing import List, Iterable, Any
import time

import ray
from ray.data.block import Block
from ray.data._internal.execution.interfaces import RefBundle, PhysicalOperator
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_operator_state import ActorPool
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.util import make_ref_bundles
from ray.tests.conftest import *  # noqa
from ray._private.test_utils import wait_for_condition


def _get_blocks(bundle: RefBundle, output_list: List[Block]):
    for block, _ in bundle.blocks:
        output_list.append(ray.get(block))


def _mul2_transform(block_iter: Iterable[Block]) -> Iterable[Block]:
    for block in block_iter:
        yield [b * 2 for b in block]


def _take_outputs(op: PhysicalOperator) -> List[Any]:
    output = []
    while op.has_next():
        ref = op.get_next()
        assert ref.owns_blocks, ref
        _get_blocks(ref, output)
    return output


def test_input_data_buffer(ray_start_regular_shared):
    # Create with bundles.
    inputs = make_ref_bundles([[1, 2], [3], [4, 5]])
    op = InputDataBuffer(inputs)

    # Check we return all bundles in order.
    assert _take_outputs(op) == [[1, 2], [3], [4, 5]]


def test_all_to_all_operator():
    def dummy_all_transform(bundles: List[RefBundle]):
        return make_ref_bundles([[1, 2], [3, 4]]), {"FooStats": []}

    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    op = AllToAllOperator(
        dummy_all_transform, input_op=input_op, num_outputs=2, name="TestAll"
    )

    # Feed data.
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.inputs_done(0)

    # Check we return transformed bundles.
    assert _take_outputs(op) == [[1, 2], [3, 4]]
    stats = op.get_stats()
    assert "FooStats" in stats


def test_map_operator_bulk(ray_start_regular_shared):
    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    op = MapOperator(_mul2_transform, input_op=input_op, name="TestMapper")

    # Feed data and block on exec.
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.inputs_done(0)
    for work in op.get_work_refs():
        ray.get(work)
        op.notify_work_completed(work)

    # Check we return transformed bundles in order.
    assert _take_outputs(op) == [[i * 2] for i in range(100)]

    # Check dataset stats.
    stats = op.get_stats()
    assert "TestMapper" in stats, stats
    assert len(stats["TestMapper"]) == 100, stats

    # Check memory stats.
    metrics = op.get_metrics()
    assert metrics["obj_store_mem_alloc"] == pytest.approx(8800, 0.5), metrics
    assert metrics["obj_store_mem_peak"] == pytest.approx(8800, 0.5), metrics
    assert metrics["obj_store_mem_freed"] == pytest.approx(6400, 0.5), metrics


def test_map_operator_streamed(ray_start_regular_shared):
    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    op = MapOperator(_mul2_transform, input_op=input_op, name="TestMapper")

    # Feed data and implement streaming exec.
    output = []
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
        for work in op.get_work_refs():
            ray.get(work)
            op.notify_work_completed(work)
        assert op.has_next()
        while op.has_next():
            ref = op.get_next()
            assert ref.owns_blocks, ref
            _get_blocks(ref, output)

    # Check equivalent to bulk execution in order.
    assert output == [[i * 2] for i in range(100)]
    metrics = op.get_metrics()
    assert metrics["obj_store_mem_alloc"] == pytest.approx(8800, 0.5), metrics
    assert metrics["obj_store_mem_peak"] == pytest.approx(88, 0.5), metrics
    assert metrics["obj_store_mem_freed"] == pytest.approx(6400, 0.5), metrics


def test_map_operator_min_rows_per_bundle(ray_start_regular_shared):
    # Simple sanity check of batching behavior.
    def _check_batch(block_iter: Iterable[Block]) -> Iterable[Block]:
        block_iter = list(block_iter)
        assert len(block_iter) == 5, block_iter
        for block in block_iter:
            yield block

    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(10)]))
    op = MapOperator(
        _check_batch,
        input_op=input_op,
        name="TestMapper",
        min_rows_per_bundle=5,
    )

    # Feed data and block on exec.
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.inputs_done(0)
    for work in op.get_work_refs():
        ray.get(work)
        op.notify_work_completed(work)

    # Check we return transformed bundles in order.
    assert _take_outputs(op) == [[i] for i in range(10)]


def test_map_operator_ray_args(shutdown_only):
    ray.shutdown()
    ray.init(num_cpus=0, num_gpus=1)
    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(10)]))
    op = MapOperator(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        ray_remote_args={"num_cpus": 0, "num_gpus": 1},
    )

    # Feed data and block on exec.
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.inputs_done(0)
    for work in op.get_work_refs():
        ray.get(work)
        op.notify_work_completed(work)

    # Check we don't hang and complete with num_gpus=1.
    assert _take_outputs(op) == [[i * 2] for i in range(10)]


def test_map_operator_shutdown():
    ray.shutdown()
    ray.init(num_cpus=0, num_gpus=1)

    def _sleep(block_iter: Iterable[Block]) -> Iterable[Block]:
        time.sleep(999)

    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(10)]))
    op = MapOperator(
        _sleep,
        input_op=input_op,
        name="TestMapper",
        ray_remote_args={"num_cpus": 0, "num_gpus": 1},
    )

    # Start one task and then cancel.
    op.add_input(input_op.get_next(), 0)
    assert len(op.get_work_refs()) == 1
    op.shutdown()

    # Task should be cancelled.
    wait_for_condition(lambda: (ray.available_resources().get("GPU", 0) == 1.0))


# TODO(Clark): Put this into a test_map_operator_state.py module.
# TODO(Clark): Split into separate tests.
def test_map_operator_actor_pool():
    ray.shutdown()
    ray.init(num_cpus=4)
    # Create actor pool.
    pool = ActorPool()
    assert pool.num_actors == 0

    @ray.remote
    class PoolWorker:
        def inc(self, x):
            return x + 1

    actors = [PoolWorker.remote() for _ in range(4)]
    for actor in actors:
        pool.add_actor(actor)
    # Check that pool count is as expected.
    assert pool.num_actors == 4
    # Check that picked actors are the same actors we gave it.
    picked_actors = []
    for _ in range(4):
        picked_actors.append(pool.pick_actor())
    assert set(picked_actors) == set(actors)
    # Check that picked actors each have 1 task in flight.
    for actor in picked_actors:
        assert pool.get_tasks_in_flight(actor) == 1
    # Check that no actors are killed since none are idle (all are currently picked
    # once).
    pool.kill_idle_actors()
    assert pool.num_actors == 4
    # Check that lone idle actor is the one picked.
    returned_actor = picked_actors.pop()
    pool.return_actor(returned_actor)
    assert pool.get_tasks_in_flight(returned_actor) == 0
    picked_actor = pool.pick_actor()
    assert picked_actor == returned_actor
    # Check that lone idle actor is killed.
    pool.return_actor(returned_actor)
    pool.kill_idle_actors()
    assert not pool.has_actor(returned_actor)
    assert pool.num_actors == 3
    # Check that repeated picking works, and that we always pick the least loaded actor.
    for _ in range(3):
        pool.pick_actor()
    for actor in picked_actors:
        # Each actor should have been picked one more time.
        assert pool.get_tasks_in_flight(actor) == 2
    # Check that asking the pool to kill future actors as they become idle works.
    pool.kill_future_idle_actors()
    for actor in picked_actors:
        # Actor was picked twice, so return it twice.
        for _ in range(2):
            pool.return_actor(actor)
        # Check that actor is no longer in pool.
        assert not pool.has_actor(actor)
        # Check that actor has been killed.
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(actor.inc.remote(1))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
