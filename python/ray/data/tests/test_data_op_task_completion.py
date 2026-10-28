"""Completion must release tasks without bypassing output backpressure."""

import gc
import os
import pickle
import signal
import sys
import time
import weakref
from typing import Any, Optional
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.data._internal.execution.block_ref_counter import BlockRefCounter
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    RefBundle,
    physical_operator,
)
from ray.data._internal.execution.interfaces.physical_operator import DataOpTask
from ray.data._internal.execution.metadata_fetcher import (
    InlineMetadataFetcher,
    MetadataFetcher,
    ThreadedMetadataFetcher,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.streaming_executor_state import (
    OutputBackpressureGuard,
    build_streaming_topology,
    process_completed_tasks,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.stats import Timer
from ray.data.block import BlockAccessor, BlockMetadataWithSchema, TaskExecWorkerStats
from ray.data.context import DataContext
from ray.data.tests.util import create_map_transformer_from_block_fn
from ray.exceptions import GetTimeoutError, ObjectLostError, RayTaskError


@ray.remote
def _generate(count):
    if count < 0:
        raise ValueError("generation failed")
    for i in range(count):
        block = pa.table({"value": [i]})
        accessor = BlockAccessor.for_block(block)
        yield block
        yield pickle.dumps(
            BlockMetadataWithSchema.from_metadata(
                accessor.get_metadata(
                    task_exec_stats=TaskExecWorkerStats(task_wall_time_s=0.001)
                ),
                schema=accessor.schema(),
            )
        )


@ray.remote
def _fail_dependency():
    raise ValueError("dependency failed")


def _make_task(
    gen: ray.ObjectRefGenerator, *, threaded: bool = False, **kwargs: Any
) -> tuple[DataOpTask, MetadataFetcher, list[RefBundle], list[Optional[Exception]]]:
    """Build a task without advancing its stream or deferred metadata state."""
    outputs, done = [], []
    task = DataOpTask(
        0,
        gen,
        BlockRefCounter(),
        "test",
        output_ready_callback=outputs.append,
        task_done_callback=lambda *args: done.append(args[0]),
        **kwargs,
    )
    fetcher = ThreadedMetadataFetcher() if threaded else InlineMetadataFetcher()
    return task, fetcher, outputs, done


def _step(task: DataOpTask, fetcher: MetadataFetcher, budget: Optional[int]) -> int:
    read = task.on_data_ready(budget, fetcher)
    fetcher.submit("test", [task])
    assert not fetcher.emit_ready_and_fire_done_callbacks()
    return read


@pytest.mark.parametrize("threaded", [False, True])
@pytest.mark.parametrize("case", ["empty", "consumed", "unread"])
def test_completion_with_zero_budget(ray_start_2_cpus, threaded, case):
    gen = _generate.remote(0 if case == "empty" else 1)
    # Core completion alone must not let an unread block bypass backpressure.
    ray.get(gen.completed(), timeout=20)
    task, fetcher, outputs, done = _make_task(gen, threaded=threaded)
    fetcher.start()

    def step(budget):
        return _step(task, fetcher, budget)

    try:
        if case == "consumed":
            # Wait for actual emission, not an assumed completion delay.
            def emitted():
                step(1)
                return bool(outputs)

            wait_for_condition(emitted, timeout=20)
        for _ in range(3):
            assert step(0) == 0
        assert task.has_finished == (case != "unread")
        assert done == ([] if case == "unread" else [None])
        assert len(outputs) == (1 if case == "consumed" else 0)
        if case == "unread":

            def finished():
                step(1)
                return task.has_finished

            wait_for_condition(finished, timeout=20)
            assert len(outputs) == 1
            assert done == [None]
    finally:
        fetcher.stop()


def test_zero_budget_preserves_pending_metadata(ray_start_2_cpus, monkeypatch):
    gen = _generate.remote(1)
    ray.get(gen.completed(), timeout=20)
    task, fetcher, outputs, done = _make_task(gen, threaded=False)
    # Both refs have been pulled but the metadata fetch is not yet ready.
    with monkeypatch.context() as m:
        m.setattr(fetcher, "in_data_ready_get_object_size", lambda task: None)
        assert task.on_data_ready(1, fetcher) == 0
    assert gen._stream_exhausted()
    assert not task.pending_block_ref.is_nil()
    assert task.on_data_ready(0, fetcher) == 0
    assert not task.has_finished and not outputs and not done
    assert task.on_data_ready(1, fetcher) > 0
    task.on_data_ready(0, fetcher)
    assert task.has_finished and len(outputs) == 1 and done == [None]


@pytest.mark.parametrize("threaded", [False, True])
@pytest.mark.parametrize("budget", [1, None])
def test_last_pair_emits_before_completion(ray_start_2_cpus, threaded, budget):
    gen = _generate.remote(1)
    ray.get(gen.completed(), timeout=20)
    events = []
    task = DataOpTask(
        0,
        gen,
        BlockRefCounter(),
        "test",
        output_ready_callback=lambda bundle: events.append("output"),
        task_done_callback=lambda *args: events.append("done"),
    )
    fetcher = ThreadedMetadataFetcher() if threaded else InlineMetadataFetcher()
    assert task.on_data_ready(budget, fetcher) > 0
    if threaded:
        # DRAINED is allowed before submit, but it must not signal completion.
        assert task.is_drained() and not task.has_finished
        assert events == [] and len(fetcher._pending_deferred) == 1
        assert not fetcher.emit_ready_and_fire_done_callbacks()
        assert events == []
        pair = fetcher._pending_deferred[0]
        fetcher.submit("test", [task])
        assert task.has_pending_emits()
        # Zero budget must not complete the task ahead of its deferred pair.
        assert task.on_data_ready(0, fetcher) == 0
        assert not fetcher.emit_ready_and_fire_done_callbacks()
        assert not task.has_finished and events == []
        # Deliver metadata deterministically without starting the fetch thread.
        with fetcher._results_lock:
            fetcher._results[pair.meta_ref] = ray.get(pair.meta_ref)
        assert not fetcher.emit_ready_and_fire_done_callbacks()
    assert task.has_finished and events == ["output", "done"]
    assert _step(task, fetcher, 0) == 0
    assert events == ["output", "done"]


@pytest.mark.parametrize("threaded", [False, True])
def test_failure_is_reported_after_budget_resumes(ray_start_2_cpus, threaded):
    gen = _generate.remote(-1)
    ready, _ = ray.wait([gen.completed()], timeout=20)
    assert ready
    # A generator-body exception is a stream item, unlike a failed dependency.
    assert not gen._stream_exhausted()
    task, fetcher, outputs, done = _make_task(gen, threaded=threaded)
    # An unread exception ref still follows the existing error path; EOF-only
    # completion must not silently discard the task failure.
    assert task.on_data_ready(0, fetcher) == 0
    assert not task.has_finished and not done
    failures = []

    def finished():
        try:
            task.on_data_ready(1, fetcher)
        except ValueError as exc:
            failures.append(exc)
        fetcher.submit("test", [task])
        failures.extend(exc for _, exc in fetcher.emit_ready_and_fire_done_callbacks())
        return task.has_finished

    wait_for_condition(finished, timeout=20)
    assert not outputs and len(done) == 1 and len(failures) == 1
    assert isinstance(done[0], ValueError)
    assert "generation failed" in str(failures[0])
    assert task.on_data_ready(0, fetcher) == 0
    assert len(done) == 1


@pytest.mark.parametrize("threaded", [False, True])
def test_dependency_failure_is_reported_with_zero_budget(ray_start_2_cpus, threaded):
    gen = _generate.remote(_fail_dependency.remote())
    assert ray.wait([gen.completed()], timeout=20)[0]
    wait_for_condition(gen._stream_exhausted)
    blocks, metadata = [], []
    task, fetcher, outputs, done = _make_task(
        gen,
        threaded=threaded,
        block_ready_callback=blocks.append,
        metadata_ready_callback=metadata.append,
    )
    if threaded:
        assert task.on_data_ready(0, fetcher) == 0
    else:
        with pytest.raises(RayTaskError, match="dependency failed"):
            task.on_data_ready(0, fetcher)
    fetcher.submit("test", [task])
    failures = fetcher.emit_ready_and_fire_done_callbacks()
    assert len(failures) == int(threaded)
    assert task.has_finished and len(done) == 1
    assert isinstance(done[0], RayTaskError) and "dependency failed" in str(done[0])
    assert not outputs and not metadata
    assert blocks == [gen.completed()]
    assert _step(task, fetcher, 0) == 0
    assert len(done) == 1 and blocks == [gen.completed()]


@pytest.mark.parametrize("threaded", [False, True])
@pytest.mark.parametrize("unavailable_at", ["wait", "get"])
def test_zero_budget_retries_unavailable_end_of_stream(
    ray_start_2_cpus, monkeypatch, threaded, unavailable_at
):
    gen = _generate.remote(0)
    ray.get(gen.completed(), timeout=20)
    assert gen._stream_exhausted()
    task, fetcher, outputs, done = _make_task(gen, threaded=threaded)
    clock = MagicMock(wraps=time)
    clock.monotonic.return_value = 100.0
    monkeypatch.setattr(physical_operator, "time", clock)
    original_get = ray.get
    original_wait = ray.wait
    timeouts = []
    waits = []

    def not_ready(refs, *args, **kwargs):
        if refs == [gen.completed()]:
            waits.append(kwargs.get("timeout"))
            assert kwargs["fetch_local"]
            if unavailable_at == "wait":
                return [], refs
        return original_wait(refs, *args, **kwargs)

    def unavailable(ref, *args, **kwargs):
        if ref == gen.completed():
            # Inject only the final object's fetch failure. Keep the real Core
            # stream and DataOpTask state transitions in this test.
            timeouts.append(kwargs.get("timeout"))
            raise GetTimeoutError("end-of-stream object not available")
        return original_get(ref, *args, **kwargs)

    with monkeypatch.context() as m:
        m.setattr(ray, "get", unavailable)
        m.setattr(ray, "wait", not_ready)
        assert task.on_data_ready(0, fetcher) == 0
        for _ in range(20):
            assert task.on_data_ready(0, fetcher) == 0
        clock.monotonic.return_value = 100.0 + (
            physical_operator.TERMINAL_CHECK_INTERVAL_S / 2
        )
        assert task.on_data_ready(0, fetcher) == 0
        assert waits == [0]
        assert timeouts == ([0] if unavailable_at == "get" else [])
        clock.monotonic.return_value = (
            100.0 + physical_operator.TERMINAL_CHECK_INTERVAL_S
        )
        assert task.on_data_ready(0, fetcher) == 0
        assert waits == [0, 0]
    assert timeouts == ([0, 0] if unavailable_at == "get" else [])
    assert not task.is_drained() and not task.has_finished and not done
    assert task.pending_block_ref.is_nil() and task.pending_meta_ref.is_nil()

    # Once the object becomes available, the same zero budget can finish the
    # task; the failed poll must not consume a ref or lose the completion.
    clock.monotonic.return_value += physical_operator.TERMINAL_CHECK_INTERVAL_S
    assert task.on_data_ready(0, fetcher) == 0
    fetcher.submit("test", [task])
    assert not fetcher.emit_ready_and_fire_done_callbacks()
    assert task.has_finished and done == [None]
    assert not outputs
    assert task.on_data_ready(0, fetcher) == 0
    assert done == [None]


@pytest.mark.parametrize("threaded", [False, True])
# Only a lineage-tracked loss is re-raised; every other terminal error,
# including an untracked loss, drains the task with the error recorded.
@pytest.mark.parametrize("failure", ["lost_tracked", "lost_untracked", "task"])
def test_zero_budget_propagates_terminal_error(
    ray_start_2_cpus, monkeypatch, threaded, failure
):
    gen = _generate.remote(0)
    ray.get(gen.completed(), timeout=20)
    assert gen._stream_exhausted()
    needs_reconstruction = failure == "lost_tracked"
    blocks, metadata = [], []
    task, fetcher, outputs, done = _make_task(
        gen,
        threaded=threaded,
        lineage_task_id="seed:0" if needs_reconstruction else None,
        block_ready_callback=blocks.append,
        metadata_ready_callback=metadata.append,
    )
    if failure == "task":
        error = RayTaskError(
            "generate", "test traceback", ValueError("failed")
        ).as_instanceof_cause()
    else:
        error = ObjectLostError(gen.completed().hex(), None, "injected EOS loss")
    original_get = ray.get

    def lost(ref, *args, **kwargs):
        if ref == gen.completed():
            raise error
        return original_get(ref, *args, **kwargs)

    with monkeypatch.context() as m:
        m.setattr(ray, "get", lost)
        if needs_reconstruction or not threaded:
            with pytest.raises(type(error)):
                task.on_data_ready(0, fetcher)
        else:
            assert task.on_data_ready(0, fetcher) == 0
    assert not outputs
    assert blocks == [gen.completed()] and not metadata
    assert task.pending_block_ref == gen.completed()
    if needs_reconstruction:
        # The executor needs the task still active to abort/reconstruct it.
        assert not task.is_drained() and not task.has_finished and not done
        task.mark_aborted(error)
        task.mark_aborted(error)
    else:
        fetcher.submit("test", [task])
        failures = fetcher.emit_ready_and_fire_done_callbacks()
        assert [exc for _, exc in failures] == ([error] if threaded else [])
    assert task.has_finished and done == [error]
    assert task.on_data_ready(0, fetcher) == 0
    assert done == [error]
    assert blocks == [gen.completed()]


@pytest.mark.parametrize("threaded", [False, True])
def test_zero_budget_releases_task_resources_with_queued_output(
    ray_start_2_cpus, threaded
):
    """Completion releases task CPU accounting without forgetting queued bytes."""
    release = SignalActor.remote()

    def transform(blocks, ctx):
        yield from blocks
        # Keep the generator alive until its only output has reached the queue.
        ray.get(release.wait.remote())

    context = DataContext.get_current()
    inputs = make_ref_bundles([[42]])
    source = InputDataBuffer(context, [])
    op = MapOperator.create(
        create_map_transformer_from_block_fn(transform, disable_block_shaping=True),
        source,
        context,
        ray_remote_args={"num_cpus": 1},
    )
    counter = BlockRefCounter()
    topology = build_streaming_topology(op, ExecutionOptions(), counter)
    guard = MagicMock(spec=OutputBackpressureGuard)
    guard.should_unblock.return_value = False

    class OutputBudget:
        name = "TestOutputBudget"
        budget = None

        def max_task_output_bytes_to_read(self, candidate):
            return self.budget if candidate is op else None

    policy = OutputBudget()
    fetcher = ThreadedMetadataFetcher() if threaded else InlineMetadataFetcher()
    fetcher.start()

    def step():
        assert (
            process_completed_tasks(
                topology, [policy], 0, guard, metadata_fetcher=fetcher
            )
            == 0
        )

    try:
        op.add_input(inputs[0], 0)
        task = op.get_active_tasks()[0]

        def output_queued():
            step()
            return len(topology[op].output_queue) == 1

        wait_for_condition(output_queued)
        assert op.current_logical_usage().cpu == 1
        queued_bytes = topology[op].output_queue_bytes()
        assert queued_bytes > 0
        live_bytes = counter.get_object_store_memory_usage(op.id)
        assert live_bytes > 0

        policy.budget = 0
        ray.get(release.send.remote())
        ray.get(task.get_waitable().completed(), timeout=20)

        def task_finished():
            step()
            return not op.get_active_tasks()

        wait_for_condition(task_finished)
        assert op.current_logical_usage().cpu == 0
        assert topology[op].output_queue_bytes() == queued_bytes
        assert counter.get_object_store_memory_usage(op.id) == live_bytes
        assert len(topology[op].output_queue) == 1
        bundle = topology[op].output_queue.pop()
        block = ray.get(bundle.block_refs[0])
        assert BlockAccessor.for_block(block).to_arrow().to_pydict() == {"id": [42]}
    finally:
        ray.get(release.send.remote())
        fetcher.stop()
        op.shutdown(timer=Timer())
        source.shutdown(timer=Timer())


@pytest.mark.parametrize("threaded", [False, True])
def test_zero_budget_recovers_lost_terminal_object(
    monkeypatch, ray_start_cluster, threaded
):
    """Lost plasma return objects are reconstructed without output budget, and
    polling them does not hold up a healthy task in the same round."""
    monkeypatch.setenv("RAY_max_direct_call_object_size", "0")
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0, object_store_memory=100 * 1024**2)
    ray.init(address=cluster.address)
    worker = cluster.add_node(num_cpus=1, object_store_memory=100 * 1024**2)
    cluster.wait_for_nodes()
    gens = [_generate.remote(1) for _ in range(3)]
    for gen in gens:
        # Consume refs without fetching the terminal object to the owner.
        assert not gen._next_sync(timeout_s=20).is_nil()
        assert not gen._next_sync(timeout_s=20).is_nil()
    terminals = [gen.completed() for gen in gens]
    ready, _ = ray.wait(terminals, num_returns=len(gens), timeout=20, fetch_local=False)
    assert len(ready) == len(gens)
    wait_for_condition(lambda: all(gen._stream_exhausted() for gen in gens))
    cluster.remove_node(worker)
    tasks = [_make_task(gen, threaded=threaded) for gen in gens]
    healthy = _generate.options(num_cpus=0).remote(0)
    ray.get(healthy.completed(), timeout=20)
    healthy_task, healthy_fetcher, _, healthy_done = _make_task(
        healthy, threaded=threaded
    )

    # Terminal objects must only be read with zero timeouts, so unavailable
    # ones cannot serialize blocking reads ahead of the healthy task.
    terminal_refs = set(terminals) | {healthy.completed()}
    original_get, original_wait = ray.get, ray.wait
    timeouts = []

    def checked_get(ref, *args, **kwargs):
        if isinstance(ref, ray.ObjectRef) and ref in terminal_refs:
            timeouts.append(kwargs.get("timeout"))
        return original_get(ref, *args, **kwargs)

    def checked_wait(refs, *args, **kwargs):
        if any(ref in terminal_refs for ref in refs):
            timeouts.append(kwargs.get("timeout"))
        return original_wait(refs, *args, **kwargs)

    start = time.monotonic()
    with monkeypatch.context() as m:
        m.setattr(ray, "get", checked_get)
        m.setattr(ray, "wait", checked_wait)
        for task, fetcher, _, done in tasks:
            assert _step(task, fetcher, 0) == 0
            assert not task.is_drained() and not task.has_finished and not done
        assert _step(healthy_task, healthy_fetcher, 0) == 0
    # Loose bound: a blocking read would cost up to a second per lost terminal.
    assert time.monotonic() - start < 2.0
    assert timeouts and set(timeouts) == {0}
    assert healthy_task.has_finished and healthy_done == [None]

    cluster.add_node(num_cpus=1, object_store_memory=100 * 1024**2)
    cluster.wait_for_nodes()

    def finished():
        for task, fetcher, _, _ in tasks:
            assert _step(task, fetcher, 0) == 0
        return all(task.has_finished for task, _, _, _ in tasks)

    wait_for_condition(finished, timeout=60)
    assert all(done == [None] for _, _, _, done in tasks)


@pytest.mark.skipif(sys.platform == "win32", reason="SIGSTOP is unavailable on Windows")
def test_aborted_terminal_polls_release_references(ray_start_cluster, monkeypatch):
    """Aborting tasks while their terminal node is stalled must not retain the
    tasks or their terminal refs."""
    from ray._private import ray_constants

    def local_ref_counts():
        core_worker = ray._private.worker.global_worker.core_worker
        return core_worker.get_all_reference_counts()

    monkeypatch.setenv("RAY_max_direct_call_object_size", "0")
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0, object_store_memory=100 * 1024**2)
    ray.init(address=cluster.address)
    worker = cluster.add_node(num_cpus=1, object_store_memory=100 * 1024**2)
    gens = [_generate.remote(0) for _ in range(4)]
    terminals = [gen.completed() for gen in gens]
    ready, _ = ray.wait(terminals, num_returns=len(gens), timeout=20, fetch_local=False)
    assert len(ready) == len(gens)
    wait_for_condition(lambda: all(gen._stream_exhausted() for gen in gens))
    # Core exposes ASCII hex IDs as bytes. Check live references first so this
    # probe cannot pass vacuously by looking up the wrong key type.
    ref_ids = [ref.hex().encode("ascii") for ref in terminals]
    del ready, terminals
    counts = local_ref_counts()
    assert all(counts[ref_id]["local"] > 0 for ref_id in ref_ids)
    live = weakref.WeakSet()
    pid = worker.all_processes[ray_constants.PROCESS_TYPE_RAYLET][0].process.pid
    os.kill(pid, signal.SIGSTOP)
    try:
        while gens:
            task, fetcher, outputs, done = _make_task(gens.pop())
            live.add(task)
            assert task.on_data_ready(0, fetcher) == 0
            assert not task.has_finished
            task.mark_aborted(ValueError("aborted"))
            assert len(done) == 1 and not outputs
            del task, fetcher, outputs, done

        def released():
            gc.collect()
            counts = local_ref_counts()
            return len(live) == 0 and all(
                counts.get(ref_id, {}).get("local", 0) == 0 for ref_id in ref_ids
            )

        wait_for_condition(released)
    finally:
        os.kill(pid, signal.SIGCONT)


@pytest.mark.parametrize("terminal", ["success", "failure", "abort"])
def test_terminal_closes_backpressure_interval(monkeypatch, terminal):
    clock = [100.0]
    monkeypatch.setattr(
        "ray.data._internal.execution.interfaces.physical_operator.time.perf_counter",
        lambda: clock[0],
    )
    reports = []
    task = DataOpTask(
        0,
        MagicMock(),
        MagicMock(spec=BlockRefCounter),
        "test",
        task_done_callback=lambda *args: reports.append(args),
    )
    task._track_task_output_backpressure(0)
    clock[0] += 60
    error = ValueError("failed")
    if terminal == "abort":
        task.mark_aborted(error)
        task.mark_aborted(error)
    else:
        task._task_error = error if terminal == "failure" else None
        task.mark_done()
    assert len(reports) == 1
    assert task._total_output_backpressure_s == 60
    assert task._start_output_backpressure_s is None
    if terminal == "success":
        assert reports[0][2].task_output_backpressure_s == 60
    else:
        assert reports[0] == (error, None, None)
    clock[0] += 30
    task._finish_task_output_backpressure()
    assert task.on_data_ready(0, InlineMetadataFetcher()) == 0
    assert task._start_output_backpressure_s is None
    assert task._total_output_backpressure_s == 60


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
