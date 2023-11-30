import itertools
import random
import threading
import time
from typing import Any, List

import pandas as pd
import pytest

import ray
from ray import cloudpickle
from ray._private.test_utils import wait_for_condition
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    RefBundle,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    create_map_transformer_from_block_fn,
)
from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import extract_values


def make_map_transformer(block_fn):
    def map_fn(block_iter, _):
        for block in block_iter:
            yield pd.DataFrame({"id": block_fn(block["id"])})

    return create_map_transformer_from_block_fn(map_fn)


def ref_bundles_to_list(bundles: List[RefBundle]) -> List[List[Any]]:
    output = []
    for bundle in bundles:
        for block, _ in bundle.blocks:
            output.append(list(ray.get(block)["id"]))
    return output


def test_autoshutdown_dangling_executors(ray_start_10_cpus_shared):
    from ray.data._internal.execution import streaming_executor

    num_runs = 5

    # Test that when an interator is fully consumed,
    # the executor should be shut down.
    initial = streaming_executor._num_shutdown
    for _ in range(num_runs):
        ds = ray.data.range(100).repartition(10)
        it = iter(ds.iter_batches(batch_size=10, prefetch_batches=0))
        while True:
            try:
                next(it)
            except StopIteration:
                break
    assert streaming_executor._num_shutdown - initial == num_runs

    # Test that when an partially-consumed iterator is deleted,
    # the executor should be shut down.
    initial = streaming_executor._num_shutdown
    for _ in range(num_runs):
        ds = ray.data.range(100).repartition(10)
        it = iter(ds.iter_batches(batch_size=10, prefetch_batches=0))
        next(it)
        del it
        del ds
    assert streaming_executor._num_shutdown - initial == num_runs

    # Test that the executor is shut down when it's deleted,
    # even if not using iterators.
    initial = streaming_executor._num_shutdown
    for _ in range(num_runs):
        executor = StreamingExecutor(ExecutionOptions())
        del executor
    assert streaming_executor._num_shutdown - initial == num_runs


def test_pipelined_execution(ray_start_10_cpus_shared):
    executor = StreamingExecutor(ExecutionOptions(preserve_order=True))
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator.create(
        make_map_transformer(lambda block: [b * -1 for b in block]), o1
    )
    o3 = MapOperator.create(
        make_map_transformer(lambda block: [b * 2 for b in block]), o2
    )

    def reverse_sort(inputs: List[RefBundle], ctx):
        reversed_list = inputs[::-1]
        return reversed_list, {}

    ctx = DataContext.get_current()
    o4 = AllToAllOperator(reverse_sort, o3, ctx.target_max_block_size)
    it = executor.execute(o4)
    output = ref_bundles_to_list(it)
    expected = [[x * -2] for x in range(20)][::-1]
    assert output == expected, (output, expected)


def test_output_split_e2e(ray_start_10_cpus_shared):
    executor = StreamingExecutor(ExecutionOptions())
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = OutputSplitter(o1, 2, equal=True)
    it = executor.execute(o2)

    class Consume(threading.Thread):
        def __init__(self, idx):
            self.idx = idx
            self.out = []
            super().__init__()

        def run(self):
            while True:
                try:
                    self.out.append(it.get_next(output_split_idx=self.idx))
                except Exception as e:
                    print(e)
                    raise

    c0 = Consume(0)
    c1 = Consume(1)
    c0.start()
    c1.start()
    c0.join()
    c1.join()

    def get_outputs(out: List[RefBundle]):
        outputs = []
        for bundle in out:
            for block, _ in bundle.blocks:
                ids: pd.Series = ray.get(block)["id"]
                outputs.extend(ids.values)
        return outputs

    assert get_outputs(c0.out) == list(range(0, 20, 2))
    assert get_outputs(c1.out) == list(range(1, 20, 2))
    assert len(c0.out) == 10, c0.out
    assert len(c1.out) == 10, c0.out


def test_streaming_split_e2e(ray_start_10_cpus_shared):
    def get_lengths(*iterators, use_iter_batches=True):
        lengths = []

        class Runner(threading.Thread):
            def __init__(self, it):
                self.it = it
                super().__init__()

            def run(self):
                it = self.it
                x = 0
                if use_iter_batches:
                    for batch in it.iter_batches():
                        for arr in batch.values():
                            x += arr.size
                else:
                    for _ in it.iter_rows():
                        x += 1
                lengths.append(x)

        runners = [Runner(it) for it in iterators]
        for r in runners:
            r.start()
        for r in runners:
            r.join()

        lengths.sort()
        return lengths

    ds = ray.data.range(1000)
    (
        i1,
        i2,
    ) = ds.streaming_split(2, equal=True)
    for _ in range(2):
        lengths = get_lengths(i1, i2)
        assert lengths == [500, 500], lengths

    ds = ray.data.range(1)
    (
        i1,
        i2,
    ) = ds.streaming_split(2, equal=True)
    for _ in range(2):
        lengths = get_lengths(i1, i2)
        assert lengths == [0, 0], lengths

    ds = ray.data.range(1)
    (
        i1,
        i2,
    ) = ds.streaming_split(2, equal=False)
    for _ in range(2):
        lengths = get_lengths(i1, i2)
        assert lengths == [0, 1], lengths

    ds = ray.data.range(1000, parallelism=10)
    for equal_split, use_iter_batches in itertools.product(
        [True, False], [True, False]
    ):
        i1, i2, i3 = ds.streaming_split(3, equal=equal_split)
        for _ in range(2):
            lengths = get_lengths(i1, i2, i3, use_iter_batches=use_iter_batches)
            if equal_split:
                assert lengths == [333, 333, 333], lengths
            else:
                assert lengths == [300, 300, 400], lengths


def test_streaming_split_barrier(ray_start_10_cpus_shared):
    ds = ray.data.range(20, parallelism=20)
    (
        i1,
        i2,
    ) = ds.streaming_split(2, equal=True)

    @ray.remote
    def consume(x, times):
        i = 0
        for _ in range(times):
            for _ in x.iter_rows():
                i += 1
        return i

    # Succeeds.
    ray.get([consume.remote(i1, 2), consume.remote(i2, 2)])
    ray.get([consume.remote(i1, 2), consume.remote(i2, 2)])
    ray.get([consume.remote(i1, 2), consume.remote(i2, 2)])

    # Blocks forever since one reader is stalled.
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get([consume.remote(i1, 2), consume.remote(i2, 1)], timeout=3)


def test_streaming_split_invalid_iterator(ray_start_10_cpus_shared):
    ds = ray.data.range(20, parallelism=20)
    (
        i1,
        i2,
    ) = ds.streaming_split(2, equal=True)

    @ray.remote
    def consume(x, times):
        i = 0
        for _ in range(times):
            for _ in x.iter_rows():
                i += 1
        return i

    # InvalidIterator error from too many concurrent readers.
    with pytest.raises(ValueError):
        ray.get(
            [
                consume.remote(i1, 4),
                consume.remote(i2, 4),
                consume.remote(i1, 4),
                consume.remote(i2, 4),
            ]
        )


@pytest.mark.skip(
    reason="Incomplete implementation of _validate_dag causes other errors, so we "
    "remove DAG validation for now; see https://github.com/ray-project/ray/pull/37829"
)
def test_e2e_option_propagation(ray_start_10_cpus_shared, restore_data_context):
    DataContext.get_current().new_execution_backend = True
    DataContext.get_current().use_streaming_executor = True

    def run():
        ray.data.range(5, parallelism=5).map(
            lambda x: x, compute=ray.data.ActorPoolStrategy(size=2)
        ).take_all()

    DataContext.get_current().execution_options.resource_limits = ExecutionResources()
    run()

    DataContext.get_current().execution_options.resource_limits.cpu = 1
    with pytest.raises(ValueError):
        run()


def test_configure_spread_e2e(ray_start_10_cpus_shared, restore_data_context):
    from ray import remote_function

    tasks = []

    def _test_hook(fn, args, strategy):
        if "map_task" in str(fn):
            tasks.append(strategy)

    remote_function._task_launch_hook = _test_hook
    DataContext.get_current().use_streaming_executor = True
    DataContext.get_current().execution_options.preserve_order = True
    DataContext.get_current().large_args_threshold = 0

    # Simple 2-stage pipeline.
    ray.data.range(2, parallelism=2).map(lambda x: x, num_cpus=2).take_all()

    # Read tasks get SPREAD by default, subsequent ones use default policy.
    tasks = sorted(tasks)
    assert tasks == ["DEFAULT", "DEFAULT", "SPREAD", "SPREAD"]


def test_scheduling_progress_when_output_blocked(
    ray_start_10_cpus_shared, restore_data_context
):
    # Processing stages should fully finish even if output is completely stalled.

    @ray.remote
    class Counter:
        def __init__(self):
            self.i = 0

        def inc(self):
            self.i += 1

        def get(self):
            return self.i

    counter = Counter.remote()

    def func(x):
        ray.get(counter.inc.remote())
        return x

    DataContext.get_current().use_streaming_executor = True
    DataContext.get_current().execution_options.preserve_order = True

    # Only take the first item from the iterator.
    it = iter(
        ray.data.range(100, parallelism=100)
        .map_batches(func, batch_size=None)
        .iter_batches(batch_size=None)
    )
    next(it)
    # The pipeline should fully execute even when the output iterator is blocked.
    wait_for_condition(lambda: ray.get(counter.get.remote()) == 100)
    # Check we can take the rest.
    assert [b["id"] for b in it] == [[x] for x in range(1, 100)]


def test_backpressure_from_output(ray_start_10_cpus_shared, restore_data_context):
    # Here we set the memory limit low enough so the output getting blocked will
    # actually stall execution.

    @ray.remote
    class Counter:
        def __init__(self):
            self.i = 0

        def inc(self):
            self.i += 1

        def get(self):
            return self.i

    counter = Counter.remote()

    def func(x):
        ray.get(counter.inc.remote())
        return x

    ctx = DataContext.get_current()
    ctx.use_streaming_executor = True
    ctx.execution_options.resource_limits.object_store_memory = 10000

    # Only take the first item from the iterator.
    ds = ray.data.range(100000, parallelism=100).map_batches(func, batch_size=None)
    it = iter(ds.iter_batches(batch_size=None))
    next(it)
    time.sleep(3)  # Pause a little so anything that would be executed runs.
    num_finished = ray.get(counter.get.remote())
    assert num_finished < 20, num_finished
    # Check intermediate stats reporting.
    stats = ds.stats()
    assert "100/100 blocks executed" not in stats, stats

    # Check we can get the rest.
    for rest in it:
        pass
    assert ray.get(counter.get.remote()) == 100
    # Check final stats reporting.
    stats = ds.stats()
    assert "100/100 blocks executed" in stats, stats


def test_e2e_liveness_with_output_backpressure_edge_case(
    ray_start_10_cpus_shared, restore_data_context
):
    # At least one operator is ensured to be running, if the output becomes idle.
    ctx = DataContext.get_current()
    ctx.use_streaming_executor = True
    ctx.execution_options.preserve_order = True
    ctx.execution_options.resource_limits.object_store_memory = 1
    ds = ray.data.range(10000, parallelism=100).map(lambda x: x, num_cpus=2)
    # This will hang forever if the liveness logic is wrong, since the output
    # backpressure will prevent any operators from running at all.
    assert extract_values("id", ds.take_all()) == list(range(10000))


def test_e2e_autoscaling_up(ray_start_10_cpus_shared, restore_data_context):
    DataContext.get_current().new_execution_backend = True
    DataContext.get_current().use_streaming_executor = True

    @ray.remote(max_concurrency=10)
    class Barrier:
        def __init__(self, n, delay=0):
            self.n = n
            self.delay = delay
            self.max_waiters = 0
            self.cur_waiters = 0

        def wait(self):
            self.cur_waiters += 1
            if self.cur_waiters > self.max_waiters:
                self.max_waiters = self.cur_waiters
            self.n -= 1
            print("wait", self.n)
            while self.n > 0:
                time.sleep(0.1)
            time.sleep(self.delay)
            print("wait done")
            self.cur_waiters -= 1

        def get_max_waiters(self):
            return self.max_waiters

    b1 = Barrier.remote(6)

    def barrier1(x):
        ray.get(b1.wait.remote(), timeout=10)
        return x

    # Tests that we autoscale up to necessary size.
    # 6 tasks + 1 tasks in flight per actor => need at least 6 actors to run.
    ray.data.range(6, parallelism=6).map_batches(
        barrier1,
        compute=ray.data.ActorPoolStrategy(
            min_size=1, max_size=6, max_tasks_in_flight_per_actor=1
        ),
        batch_size=None,
    ).take_all()
    assert ray.get(b1.get_max_waiters.remote()) == 6

    b2 = Barrier.remote(3, delay=2)

    def barrier2(x):
        ray.get(b2.wait.remote(), timeout=10)
        return x

    # Tests that we don't over-scale up.
    # 6 tasks + 2 tasks in flight per actor => only scale up to 3 actors
    ray.data.range(6, parallelism=6).map_batches(
        barrier2,
        compute=ray.data.ActorPoolStrategy(
            min_size=1, max_size=3, max_tasks_in_flight_per_actor=2
        ),
        batch_size=None,
    ).take_all()
    assert ray.get(b2.get_max_waiters.remote()) == 3

    # Tests that the max pool size is respected.
    b3 = Barrier.remote(6)

    def barrier3(x):
        ray.get(b3.wait.remote(), timeout=2)
        return x

    # This will hang, since the actor pool is too small.
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.data.range(6, parallelism=6).map(
            barrier3, compute=ray.data.ActorPoolStrategy(min_size=1, max_size=2)
        ).take_all()


def test_e2e_autoscaling_down(ray_start_10_cpus_shared, restore_data_context):
    DataContext.get_current().new_execution_backend = True
    DataContext.get_current().use_streaming_executor = True

    def f(x):
        time.sleep(1)
        return x

    # Tests that autoscaling works even when resource constrained via actor killing.
    # To pass this, we need to autoscale down to free up slots for task execution.
    DataContext.get_current().execution_options.resource_limits.cpu = 2
    ray.data.range(5, parallelism=5).map_batches(
        f,
        compute=ray.data.ActorPoolStrategy(min_size=1, max_size=2),
        batch_size=None,
    ).map_batches(lambda x: x, batch_size=None, num_cpus=2).take_all()


def test_can_pickle(ray_start_10_cpus_shared, restore_data_context):
    DataContext.get_current().new_execution_backend = True
    DataContext.get_current().use_streaming_executor = True

    ds = ray.data.range(1000000)
    it = iter(ds.iter_batches())
    next(it)

    # Should work even if a streaming exec is in progress.
    ds2 = cloudpickle.loads(cloudpickle.dumps(ds))
    assert ds2.count() == 1000000


def test_streaming_fault_tolerance(ray_start_10_cpus_shared, restore_data_context):
    DataContext.get_current().new_execution_backend = True
    DataContext.get_current().use_streaming_executor = True

    def f(x):
        import os

        if random.random() > 0.9:
            print("force exit")
            os._exit(1)
        return x

    # Test recover.
    base = ray.data.range(1000, parallelism=100)
    ds1 = base.map_batches(
        f, compute=ray.data.ActorPoolStrategy(size=4), max_task_retries=999
    )
    ds1.take_all()

    # Test disabling fault tolerance.
    ds2 = base.map_batches(
        f, compute=ray.data.ActorPoolStrategy(size=4), max_restarts=0
    )
    with pytest.raises(ray.exceptions.RayActorError):
        ds2.take_all()


def test_streaming_split_with_custom_data_context(
    ray_start_10_cpus_shared, restore_data_context
):
    # Tests that custom DataContext can be properly propagated
    # when using `streaming_split()`.
    block_size = 123 * 1024 * 1024
    data_context = DataContext.get_current()
    data_context.target_max_block_size = block_size
    data_context.set_config("foo", "bar")

    def f(x):
        assert DataContext.get_current().target_max_block_size == block_size
        assert DataContext.get_current().get_config("foo") == "bar"
        return x

    num_splits = 2
    splits = ray.data.range(10, parallelism=10).map(f).streaming_split(num_splits)

    @ray.remote
    def consume(split):
        for _ in split.iter_rows():
            pass

    assert ray.get([consume.remote(split) for split in splits]) == [None] * num_splits


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
