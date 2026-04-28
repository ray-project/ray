import itertools
import random
import threading
import time
from typing import Any, List

import numpy as np
import pandas as pd
import pytest

import ray
from ray import cloudpickle
from ray._common.test_utils import wait_for_condition
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
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

    return MapTransformer([BlockMapTransformFn(map_fn)])


def ref_bundles_to_list(bundles: List[RefBundle]) -> List[List[Any]]:
    output = []
    for bundle in bundles:
        for block_ref in bundle.block_refs:
            output.append(list(ray.get(block_ref)["id"]))
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
        executor = StreamingExecutor(DataContext.get_current())
        o = InputDataBuffer(DataContext.get_current(), [])
        # Start the executor. Because non-started executors don't
        # need to be shut down.
        executor.execute(o)
        del executor
    assert streaming_executor._num_shutdown - initial == num_runs


def test_pipelined_execution(ray_start_10_cpus_shared, restore_data_context):
    ctx = DataContext.get_current()
    ctx.execution_options.preserve_order = True
    executor = StreamingExecutor(ctx)
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o2 = MapOperator.create(
        make_map_transformer(lambda block: [b * -1 for b in block]),
        o1,
        ctx,
    )
    o3 = MapOperator.create(
        make_map_transformer(lambda block: [b * 2 for b in block]),
        o2,
        ctx,
    )

    def reverse_sort(inputs: List[RefBundle], ctx):
        reversed_list = inputs[::-1]
        return reversed_list, {}

    ctx = DataContext.get_current()
    o4 = AllToAllOperator(reverse_sort, o3, ctx, ctx.target_max_block_size)
    it = executor.execute(o4)
    output = ref_bundles_to_list(it)
    expected = [[x * -2] for x in range(20)][::-1]
    assert output == expected, (output, expected)


def test_output_split_e2e(ray_start_10_cpus_shared):
    executor = StreamingExecutor(DataContext.get_current())
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o2 = OutputSplitter(o1, 2, equal=True, data_context=DataContext.get_current())
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
            for block_ref in bundle.block_refs:
                ids: pd.Series = ray.get(block_ref)["id"]
                outputs.extend(ids.values)
        return outputs

    assert get_outputs(c0.out) == list(range(0, 20, 2))
    assert get_outputs(c1.out) == list(range(1, 20, 2))
    assert len(c0.out) == 10, c0.out
    assert len(c1.out) == 10, c0.out


def test_output_split_shutdown_preserves_sibling_split_queues(ray_start_10_cpus_shared):
    """If one split consumer finishes first, executor shutdown must not clear the
    other split's output queue; slower consumers still need those RefBundles.
    """
    executor = StreamingExecutor(DataContext.get_current())
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o2 = OutputSplitter(o1, 2, equal=True, data_context=DataContext.get_current())
    it = executor.execute(o2)

    slow_ready = threading.Event()
    slow_go = threading.Event()
    c0_out: List[RefBundle] = []
    c1_out: List[RefBundle] = []
    thread_errors: List[BaseException] = []

    def consume_split(idx: int, out: List[RefBundle], hold_before_reads: bool):
        try:
            if hold_before_reads:
                slow_ready.set()
                slow_go.wait()
            while True:
                try:
                    out.append(it.get_next(output_split_idx=idx))
                except StopIteration:
                    break
        except BaseException as e:
            thread_errors.append(e)

    t_slow = threading.Thread(target=consume_split, args=(1, c1_out, True))
    t_fast = threading.Thread(target=consume_split, args=(0, c0_out, False))
    t_slow.start()
    assert slow_ready.wait(timeout=30)
    t_fast.start()
    t_fast.join(timeout=60)
    assert not t_fast.is_alive()
    slow_go.set()
    t_slow.join(timeout=60)
    assert not t_slow.is_alive()
    assert not thread_errors, thread_errors

    def get_outputs(out: List[RefBundle]):
        outputs = []
        for bundle in out:
            for block_ref in bundle.block_refs:
                ids: pd.Series = ray.get(block_ref)["id"]
                outputs.extend(ids.values)
        return outputs

    assert get_outputs(c0_out) == list(range(0, 20, 2))
    assert get_outputs(c1_out) == list(range(1, 20, 2))


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

    ds = ray.data.range(1000, override_num_blocks=10)
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
    ds = ray.data.range(20, override_num_blocks=20)
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
    ds = ray.data.range(20, override_num_blocks=20)
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


def test_streaming_split_independent_finish(ray_start_10_cpus_shared):
    """Test that stream_split iterators can finish independently without
    waiting for other iterators to finish. Otherwise, this would cause
    deadlocks.
    """
    num_blocks_per_split = 10
    num_splits = 2
    ds = ray.data.range(
        num_splits * num_blocks_per_split,
        override_num_blocks=num_splits * num_blocks_per_split,
    )
    (
        i1,
        i2,
    ) = ds.streaming_split(num_splits, equal=True)

    @ray.remote(max_concurrency=2)
    class SignalActor:
        def __init__(self):
            self._event = threading.Event()

        def wait(self):
            self._event.wait()

        def set(self):
            self._event.set()

    @ray.remote
    class Consumer:
        def consume(self, it, signal_actor, split_index):
            for i, _ in enumerate(it.iter_batches(batch_size=None, prefetch_batches=0)):
                if i == num_blocks_per_split // 2 and split_index == 0:
                    # The first consumer waits for the second consumer to
                    # finish first in the middle of the iteration.
                    print("before wait")
                    ray.get(signal_actor.wait.remote())
                    print("after wait")
            if split_index == 1:
                # The second consumer sends a signal to unblock the
                # first consumer. It should finish the iteration independently.
                # Otherwise, there will be a deadlock.
                print("before set")
                # Sleep some time to make sure the other
                # consume calls wait first.
                time.sleep(2)
                ray.get(signal_actor.set.remote())
                print("after set")
            pass

    signal_actor = SignalActor.remote()
    consumer1 = Consumer.remote()
    consumer2 = Consumer.remote()

    ready, _ = ray.wait(
        [
            consumer1.consume.remote(i1, signal_actor, 0),
            consumer2.consume.remote(i2, signal_actor, 1),
        ],
        num_returns=2,
        timeout=20,
    )

    assert len(ready) == 2


def test_streaming_split_error_propagation(
    ray_start_10_cpus_shared, restore_data_context
):
    # Test propagating errors from Dataset execution start-up
    # (e.g. actor pool start-up timeout) to streaming_split iterators.
    ctx = DataContext.get_current()
    ctx.wait_for_min_actors_s = 1

    num_splits = 5
    ds = ray.data.range(100)

    class SlowUDF:
        def __init__(self):
            # This UDF slows down actor creation, and thus
            # will trigger the actor pool start-up timeout error.
            time.sleep(10)

        def __call__(self, batch):
            return batch

    ds = ds.map_batches(
        SlowUDF,
        concurrency=2,
    )
    splits = ds.streaming_split(num_splits, equal=True)

    @ray.remote
    class Consumer:
        def consume(self, split):
            with pytest.raises(
                ray.exceptions.GetTimeoutError,
                match="Timed out while starting actors.",
            ):
                for _ in split.iter_batches():
                    pass
            return "ok"

    consumers = [Consumer.remote() for _ in range(num_splits)]
    res = ray.get([c.consume.remote(split) for c, split in zip(consumers, splits)])
    assert res == ["ok"] * num_splits


def test_streaming_split_schema_before_execution(ray_start_10_cpus_shared):
    """Test schema retrieval from splits before execution starts."""
    ds = ray.data.range(20, override_num_blocks=20)
    i1, i2 = ds.streaming_split(2, equal=True)

    schema1 = i1.schema()
    schema2 = i2.schema()

    assert schema1 is not None
    assert "id" in schema1.names
    assert schema1 == schema2


def test_streaming_split_schema_during_execution(ray_start_10_cpus_shared):
    """Test schema retrieval from splits during execution."""
    from ray._common.test_utils import SignalActor

    # Use two signals to coordinate: `started` confirms the executor is running,
    # `blocker` keeps map tasks alive so the executor stays active.
    started = SignalActor.remote()
    blocker = SignalActor.remote()

    def blocking_fn(row):
        ray.get(started.send.remote())
        ray.get(blocker.wait.remote())
        return row

    ds = ray.data.range(20, override_num_blocks=20).map(blocking_fn)
    i1, i2 = ds.streaming_split(2, equal=True)

    @ray.remote
    def consume(x):
        for _ in x.iter_rows():
            pass

    # Start consumers — this triggers the executor on the coordinator.
    refs = [consume.remote(i1), consume.remote(i2)]

    # Wait until a map task has started, guaranteeing the executor is alive.
    ray.get(started.wait.remote())

    # schema() should raise because execution is active.
    with pytest.raises(ray.exceptions.RayTaskError, match="Cannot call schema()"):
        i1.schema()

    # Unblock map tasks so consumers can finish.
    ray.get(blocker.send.remote())
    ray.get(refs)


def test_streaming_split_schema_after_execution(ray_start_10_cpus_shared):
    """Test schema retrieval after execution completes."""
    ds = ray.data.range(20, override_num_blocks=20)
    i1, i2 = ds.streaming_split(2, equal=True)

    @ray.remote
    def consume(x):
        for _ in x.iter_rows():
            pass

    # Run a full epoch to completion.
    ray.get([consume.remote(i1), consume.remote(i2)])

    # schema() should work after execution finishes.
    schema = i1.schema()
    assert schema is not None
    assert "id" in schema.names


def test_streaming_split_context(ray_start_10_cpus_shared):
    """Test that get_context() returns a valid DataContext from the coordinator."""
    ds = ray.data.range(10)
    i1, i2 = ds.streaming_split(2, equal=True)

    ctx = i1.get_context()
    assert isinstance(ctx, ray.data.DataContext)


def test_streaming_split_dataset_tag(ray_start_10_cpus_shared):
    """Test that _get_dataset_tag() returns correct tags from the coordinator."""
    ds = ray.data.range(10)
    i1, i2 = ds.streaming_split(2, equal=True)

    tag1 = i1._get_dataset_tag()
    tag2 = i2._get_dataset_tag()
    assert "_split_0" in tag1
    assert "_split_1" in tag2


def test_configure_spread_e2e(ray_start_10_cpus_shared, restore_data_context):
    from ray import remote_function

    tasks = []

    def _test_hook(fn, args, strategy):
        if "map_task" in str(fn):
            tasks.append(strategy)

    remote_function._task_launch_hook = _test_hook
    DataContext.get_current().execution_options.preserve_order = True
    DataContext.get_current().large_args_threshold = 0

    # Simple 2-operator pipeline.
    ray.data.range(2, override_num_blocks=2).map(lambda x: x, num_cpus=2).take_all()

    # Read tasks get SPREAD by default, subsequent ones use default policy.
    tasks = sorted(tasks)
    assert tasks == ["DEFAULT", "DEFAULT", "SPREAD", "SPREAD"]


def test_scheduling_progress_when_output_blocked(
    ray_start_10_cpus_shared, restore_data_context
):
    # Processing operators should fully finish even if output is completely stalled.

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

    DataContext.get_current().execution_options.preserve_order = True

    # Only take the first item from the iterator.
    it = iter(
        ray.data.range(100, override_num_blocks=100)
        .map_batches(func, batch_size=None)
        .iter_batches(batch_size=None)
    )
    next(it)
    # The pipeline should fully execute even when the output iterator is blocked.
    wait_for_condition(lambda: ray.get(counter.get.remote()) == 100)
    # Check we can take the rest.
    assert [b["id"] for b in it] == [[x] for x in range(1, 100)]


def test_task_submission_backpressure_from_paused_consumer(
    ray_start_10_cpus_shared, restore_data_context
):
    # Here we set the memory limit low enough so the output getting blocked will
    # actually stall execution.
    block_size = 10 * 1024 * 1024

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
        return {
            "data": [np.ones(block_size, dtype=np.uint8)],
        }

    ctx = DataContext.get_current()
    ctx.target_max_block_size = block_size
    ctx.execution_options.resource_limits = ctx.execution_options.resource_limits.copy(
        object_store_memory=block_size
    )

    # Only take the first item from the iterator.
    ds = ray.data.range(100, override_num_blocks=100).map_batches(func, batch_size=None)
    it = iter(ds.iter_batches(batch_size=None, prefetch_batches=0))
    next(it)
    time.sleep(3)  # Pause a little so anything that would be executed runs.
    num_finished = ray.get(counter.get.remote())
    assert num_finished < 20, num_finished
    # Check intermediate stats reporting.
    stats = ds.stats()
    assert "100 tasks executed" not in stats, stats

    # Check we can get the rest.
    for rest in it:
        pass
    assert ray.get(counter.get.remote()) == 100
    # Check final stats reporting.
    stats = ds.stats()
    assert "100 tasks executed" in stats, stats


@pytest.mark.parametrize("streaming_split", [False, True])
def test_output_backpressure_from_paused_consumer(
    ray_start_10_cpus_shared, restore_data_context, streaming_split
):
    """The terminal operator's output queue should not grow beyond the
    budget from pulling blocks from in-flight tasks when a consumer is paused."""
    ctx = DataContext.get_current()
    block_size = 1024
    ctx.target_max_block_size = block_size
    ctx.execution_options.resource_limits = ctx.execution_options.resource_limits.copy(
        object_store_memory=block_size
    )
    # Disable downstream capacity backpressure to isolate the test to
    # the resource budget escape hatch.
    ctx.downstream_capacity_backpressure_ratio = None

    @ray.remote
    class Counter:
        def __init__(self):
            self.n = 0

        def inc(self):
            self.n += 1

        def get(self):
            return self.n

    counter = Counter.remote()

    def generate_many_blocks(batch):
        while True:
            ray.get(counter.inc.remote())
            yield {"data": np.zeros((1, block_size), dtype=np.uint8)}

    ds = ray.data.range(1, override_num_blocks=1).map_batches(
        generate_many_blocks, batch_size=None
    )
    if streaming_split:
        ds = ds.streaming_split(1)[0]
    it = iter(ds.iter_batches(batch_size=None, prefetch_batches=0))

    # Consume first batch to start the pipeline and get the executor.
    next(it)

    # Let the pipeline run and fill up the budget.
    time.sleep(3)
    count_before = ray.get(counter.get.remote())
    # Make sure the consumer is not still pulling -- it should have been throttled by the budget.
    time.sleep(1)
    count_after = ray.get(counter.get.remote())
    growth = count_after - count_before
    assert growth == 0


def test_e2e_autoscaling_down(ray_start_10_cpus_shared, restore_data_context):
    class UDFClass:
        def __call__(self, x):
            time.sleep(1)
            return x

    # Tests that autoscaling works even when resource constrained via actor killing.
    # To pass this, we need to autoscale down to free up slots for task execution.
    DataContext.get_current().execution_options.resource_limits = (
        DataContext.get_current().execution_options.resource_limits.copy(cpu=2)
    )
    ray.data.range(5, override_num_blocks=5).map_batches(
        UDFClass,
        compute=ray.data.ActorPoolStrategy(min_size=1, max_size=2),
        batch_size=None,
    ).map_batches(lambda x: x, batch_size=None, num_cpus=2).take_all()


def test_can_pickle(ray_start_10_cpus_shared, restore_data_context):
    ds = ray.data.range(1000000)
    it = iter(ds.iter_batches())
    next(it)

    # Should work even if a streaming exec is in progress.
    ds2 = cloudpickle.loads(cloudpickle.dumps(ds))
    assert ds2.count() == 1000000


def test_streaming_fault_tolerance(ray_start_10_cpus_shared, restore_data_context):
    class RandomExit:
        def __call__(self, x):
            import os

            if random.random() > 0.9:
                print("force exit")
                os._exit(1)
            return x

    # Test recover.
    base = ray.data.range(1000, override_num_blocks=100)
    ds1 = base.map_batches(
        RandomExit, compute=ray.data.ActorPoolStrategy(size=4), max_task_retries=999
    )
    ds1.take_all()

    # Test disabling fault tolerance.
    ds2 = base.map_batches(
        RandomExit, compute=ray.data.ActorPoolStrategy(size=4), max_restarts=0
    )
    with pytest.raises(ray.exceptions.RayActorError):
        ds2.take_all()


def test_e2e_liveness_with_output_backpressure_edge_case(
    ray_start_10_cpus_shared, restore_data_context
):
    # At least one operator is ensured to be running, if the output becomes idle.
    ctx = DataContext.get_current()
    ctx.execution_options.preserve_order = True
    ctx.execution_options.resource_limits = ctx.execution_options.resource_limits.copy(
        object_store_memory=1
    )
    ds = ray.data.range(10000, override_num_blocks=100).map(lambda x: x, num_cpus=2)
    # This will hang forever if the liveness logic is wrong, since the output
    # backpressure will prevent any operators from running at all.
    assert extract_values("id", ds.take_all()) == list(range(10000))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
