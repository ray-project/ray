# coding: utf-8

import gc
import logging
import os
import sys
import time

import numpy as np
import pytest

from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.test_utils import client_test_enabled
from ray._private.worker import _wait_generators_bulk
from ray.exceptions import (
    ActorDiedError,
    ObjectRefStreamEndOfStreamError,
    RayTaskError,
    TaskCancelledError,
    WorkerCrashedError,
)

if client_test_enabled():
    from ray.util.client import ray
else:
    import ray
    import ray.util.state

logger = logging.getLogger(__name__)


def test_wait(ray_start_regular):
    @ray.remote
    def f(delay):
        time.sleep(delay)
        return

    object_refs = [f.remote(0), f.remote(0), f.remote(0), f.remote(0)]
    ready_ids, remaining_ids = ray.wait(object_refs)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 3
    ready_ids, remaining_ids = ray.wait(object_refs, num_returns=4)
    assert set(ready_ids) == set(object_refs)
    assert remaining_ids == []

    object_refs = [f.remote(0), f.remote(5)]
    ready_ids, remaining_ids = ray.wait(object_refs, timeout=0.5, num_returns=2)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 1

    # Verify that calling wait with duplicate object refs throws an
    # exception.
    x = ray.put(1)
    with pytest.raises(Exception):
        ray.wait([x, x])

    # Make sure it is possible to call wait with an empty list.
    ready_ids, remaining_ids = ray.wait([])
    assert ready_ids == []
    assert remaining_ids == []

    # Test semantics of num_returns with no timeout.
    obj_refs = [ray.put(i) for i in range(10)]
    (found, rest) = ray.wait(obj_refs, num_returns=2)
    assert len(found) == 2
    assert len(rest) == 8

    # Verify that incorrect usage raises a TypeError.
    x = ray.put(1)
    with pytest.raises(TypeError):
        ray.wait(x)
    with pytest.raises(TypeError):
        ray.wait(1)
    with pytest.raises(TypeError):
        ray.wait([1])


def test_wait_timing(ray_start_2_cpus):
    @ray.remote
    def f():
        time.sleep(1)

    future = f.remote()

    start = time.time()
    ready, not_ready = ray.wait([future], timeout=0.2)
    assert 0.2 < time.time() - start < 0.3
    assert len(ready) == 0
    assert len(not_ready) == 1


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test_wait_always_fetch_local(monkeypatch, ray_start_cluster):
    monkeypatch.setenv("RAY_scheduler_report_pinned_bytes_only", "false")
    cluster = ray_start_cluster
    head_node = cluster.add_node(num_cpus=0, object_store_memory=300e6)
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(num_cpus=1, object_store_memory=300e6)

    @ray.remote(num_cpus=1)
    def return_large_object():
        # 100mb so will spill on worker, but not once on head
        return np.zeros(100 * 1024 * 1024, dtype=np.uint8)

    @ray.remote(num_cpus=0)
    def small_local_task():
        return 1

    put_on_head = {ray._raylet.RAY_NODE_ID_KEY: head_node.node_id}
    put_on_worker = {ray._raylet.RAY_NODE_ID_KEY: worker_node.node_id}
    x = small_local_task.options(label_selector=put_on_head).remote()
    y = return_large_object.options(label_selector=put_on_worker).remote()
    z = return_large_object.options(label_selector=put_on_worker).remote()

    # will return when tasks are done
    ray.wait([x, y, z], num_returns=3, fetch_local=False)
    assert (
        ray._private.state.available_resources_per_node()[head_node.node_id][
            "object_store_memory"
        ]
        > 250e6
    )

    # x should be immediately available locally, start fetching y and z
    ray.wait([x, y, z], num_returns=1, fetch_local=True)
    assert (
        ray._private.state.available_resources_per_node()[head_node.node_id][
            "object_store_memory"
        ]
        > 250e6
    )

    time.sleep(5)
    # y, z should be pulled here
    assert (
        ray._private.state.available_resources_per_node()[head_node.node_id][
            "object_store_memory"
        ]
        < 150e6
    )


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_generators_bulk_fetch_local(monkeypatch, ray_start_cluster):
    monkeypatch.setenv("RAY_scheduler_report_pinned_bytes_only", "false")
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0, object_store_memory=500e6)
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(num_cpus=2, object_store_memory=500e6)

    @ray.remote(num_cpus=1)
    def gen_large_objects():
        # 100mb so the object is stored in plasma.
        yield np.zeros(100 * 1024 * 1024, dtype=np.uint8)
        yield np.ones(100 * 1024 * 1024, dtype=np.uint8)

    put_on_worker = {ray._raylet.RAY_NODE_ID_KEY: worker_node.node_id}
    gen1 = gen_large_objects.options(label_selector=put_on_worker).remote()
    gen2 = gen_large_objects.options(label_selector=put_on_worker).remote()

    ready = _wait_generators_bulk(
        [(gen1, [True, False]), (gen2, [False, True])],
        num_return=2,
        timeout=10,
    )
    assert len(ready) == 2
    assert [gen for gen, _ in ready] == [gen1, gen2]
    assert all(len(refs) == 2 for _, refs in ready)

    assert np.all(ray.get(ready[0][1][0], timeout=0) == 0)
    assert np.all(ray.get(ready[1][1][1], timeout=0) == 1)


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_generators_bulk_wait_for_at_most_num_return(ray_start_regular):
    @ray.remote
    def gen(base, delays):
        for i, delay in enumerate(delays):
            time.sleep(delay)
            yield base + i

    gen1 = gen.remote(10, [0, 0, 0])
    gen2 = gen.remote(20, [0, 5])

    ready = _wait_generators_bulk(
        [(gen1, [True, False]), (gen2, [False, True])],
        num_return=1,
        timeout=2,
    )

    assert len(ready) == 1
    ready_gen, refs = ready[0]
    assert ready_gen is gen1
    assert ray.get(refs) == [10, 11]

    # The returned refs are consumed from the stream.
    assert ray.get(next(gen1)) == 12


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_generators_bulk_timeout(ray_start_regular):
    @ray.remote(num_cpus=0, max_concurrency=2)
    class Signal:
        def __init__(self):
            self.ready = False

        def wait(self):
            while not self.ready:
                time.sleep(0.01)

        def send(self):
            self.ready = True

    @ray.remote
    def slow_gen(signal):
        ray.get(signal.wait.remote())
        yield 1

    signal = Signal.remote()
    gen = slow_gen.remote(signal)
    assert _wait_generators_bulk([(gen, [False])], timeout=0.01) == []
    ray.get(signal.send.remote())
    ready = _wait_generators_bulk([(gen, [False])], timeout=5)
    assert len(ready) == 1
    ready_gen, refs = ready[0]
    assert ready_gen is gen
    assert ray.get(refs) == [1]


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_generators_bulk_validation(ray_start_regular):
    @ray.remote
    def gen():
        yield 1

    gen = gen.remote()

    with pytest.raises(TypeError):
        _wait_generators_bulk({})
    with pytest.raises(TypeError):
        _wait_generators_bulk([(ray.put(1), [False])])
    with pytest.raises(TypeError):
        _wait_generators_bulk([(gen, False)])
    with pytest.raises(ValueError):
        _wait_generators_bulk([(gen, [])])
    with pytest.raises(ValueError):
        _wait_generators_bulk([(gen, [False])], num_return=2)


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__consume_next_ref_n_rejects_unready(ray_start_regular):
    """Consuming before the last requested ref is ready must raise rather than
    silently advancing past (and dropping) the not-yet-produced object."""

    @ray.remote(num_cpus=0, max_concurrency=2)
    class Signal:
        def __init__(self):
            self.ready = False

        def wait(self):
            while not self.ready:
                time.sleep(0.01)

        def send(self):
            self.ready = True

    @ray.remote
    def slow_gen(signal):
        ray.get(signal.wait.remote())
        yield 1

    signal = Signal.remote()
    gen = slow_gen.remote(signal)

    # Peek without waiting: the ref isn't produced yet, so consuming it is rejected.
    gen._get_next_ref_n(1)
    with pytest.raises(ValueError):
        gen._consume_next_ref_n(1)

    # After the value is produced, the same generator consumes normally.
    ray.get(signal.send.remote())
    ready = _wait_generators_bulk([(gen, [False])], timeout=10)
    assert len(ready) == 1
    assert ray.get(ready[0][1]) == [1]


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_generators_bulk_after_eof_raise_EndOfStreamError(ray_start_regular):
    @ray.remote
    def empty_gen():
        if False:
            yield 1

    empty = empty_gen.remote()
    ready = _wait_generators_bulk([(empty, [True, True, True])], timeout=1)
    assert len(ready) == 1
    ready_gen, refs = ready[0]
    assert ready_gen is empty
    assert len(set(refs)) == 3
    for ref in refs:
        with pytest.raises(ObjectRefStreamEndOfStreamError):
            ray.get(ref)


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_generators_bulk_after_partial_eof(ray_start_regular):
    @ray.remote
    def one_item_gen():
        yield 1

    one_item = one_item_gen.remote()
    ready = _wait_generators_bulk([(one_item, [False, False, False])], timeout=1)
    assert len(ready) == 1
    ready_gen, refs = ready[0]
    assert ready_gen is one_item
    assert len(set(refs)) == 3
    assert ray.get(refs[0]) == 1
    for ref in refs[1:]:
        with pytest.raises(ObjectRefStreamEndOfStreamError):
            ray.get(ref)

    ready_again = _wait_generators_bulk([(one_item, [False])], timeout=1)
    assert len(ready_again) == 1
    _, refs_again = ready_again[0]
    assert len(refs_again) == 1
    assert refs_again[0] not in refs
    with pytest.raises(ObjectRefStreamEndOfStreamError):
        ray.get(refs_again[0])


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_generators_bulk_after_partial_error(ray_start_regular):
    @ray.remote
    def one_item_then_error_gen():
        yield 1
        raise ValueError("expected test error")

    one_item_then_error = one_item_then_error_gen.remote()
    ready = _wait_generators_bulk(
        [(one_item_then_error, [False, False, False])], timeout=1
    )
    assert len(ready) == 1
    ready_gen, refs = ready[0]
    assert ready_gen is one_item_then_error
    assert len(set(refs)) == 3
    assert ray.get(refs[0]) == 1
    with pytest.raises(RayTaskError) as exc_info:
        ray.get(refs[1])
    assert isinstance(exc_info.value.as_instanceof_cause(), ValueError)
    with pytest.raises(ObjectRefStreamEndOfStreamError):
        ray.get(refs[2])


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__get_next_ref_n_cancelled_surfaces_task_cancelled(ray_start_regular):
    """A ref peeked in advance that the generator will never produce because the
    task was cancelled must surface TaskCancelledError -- the same error as the
    generator completion ref -- not a generic end-of-stream."""

    @ray.remote(num_returns="streaming")
    def gen():
        # Block forever so the value at stream position 0 is never produced.
        time.sleep(1000)
        yield "value"

    g = gen.remote()

    # Eagerly peek the value ref before the task is cancelled: EOF is not marked
    # yet, so the peeked ref is only materialized once the stream ends.
    [value_ref] = g._get_next_ref_n(1)

    ray.cancel(g)

    with pytest.raises(TaskCancelledError):
        ray.get(value_ref, timeout=30)
    # Oracle: the peeked ref surfaces the same error as the completion ref.
    with pytest.raises(TaskCancelledError):
        ray.get(g.completed(), timeout=30)


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__get_next_ref_n_consumed_value_not_repeated_after_cancel(ray_start_regular):
    """If cancellation collides with an already-produced value ref, consuming the
    peeked value must still advance the bulk cursor past that ref."""

    @ray.remote
    class Signal:
        def __init__(self):
            self.ready = False

        def wait(self):
            while not self.ready:
                time.sleep(0.01)

        def send(self):
            self.ready = True

    @ray.remote(num_returns="streaming")
    def gen(signal):
        yield 1
        ray.get(signal.wait.remote())
        yield 2

    signal = Signal.remote()
    g = gen.remote(signal)

    [first_ref] = g._get_next_ref_n(1)
    assert ray.get(first_ref) == 1

    ray.cancel(g)
    with pytest.raises(TaskCancelledError):
        ray.get(g.completed(), timeout=30)

    g._consume_next_ref_n(1)
    [next_ref] = g._get_next_ref_n(1)
    assert next_ref != first_ref
    with pytest.raises(TaskCancelledError):
        ray.get(next_ref, timeout=30)


@pytest.mark.skipif(
    sys.platform == "win32", reason="sys.exit() actor crash flaky on Windows"
)
@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__get_next_ref_n_actor_crash_surfaces_actor_died(ray_start_regular):
    """A ref peeked in advance that a streaming generator method will never
    produce because its actor died must surface ActorDiedError, matching the
    generator completion ref."""

    @ray.remote
    class Crasher:
        @ray.method(num_returns="streaming")
        def gen(self):
            # Die before producing the value at stream position 0.
            sys.exit(0)
            yield "value"

    actor = Crasher.remote()
    g = actor.gen.remote()

    [value_ref] = g._get_next_ref_n(1)

    with pytest.raises(ActorDiedError):
        ray.get(value_ref, timeout=30)
    # Oracle: the peeked ref surfaces the same error as the completion ref.
    with pytest.raises(ActorDiedError):
        ray.get(g.completed(), timeout=30)


@pytest.mark.skipif(
    sys.platform == "win32", reason="sys.exit() actor crash flaky on Windows"
)
@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__get_next_ref_n_actor_crash_after_yield_surfaces_actor_died(ray_start_regular):
    """After one successful yield, an actor that dies before producing the next
    value must leave the produced ref retrievable while the peeked next ref
    surfaces ActorDiedError (the EOF marker lands on an already-produced index)."""

    @ray.remote
    class Crasher:
        @ray.method(num_returns="streaming")
        def gen(self, signal):
            yield "value"
            # Block until the consumer has taken the first value, then die
            # before producing the value at stream position 1. SignalActor is
            # async so send() runs concurrently with this wait().
            ray.get(signal.wait.remote())
            sys.exit(0)
            yield "never"

    signal = SignalActor.remote()
    actor = Crasher.remote()
    g = actor.gen.remote(signal)

    # Peek the produced value ref and the not-yet-produced next ref together.
    value_ref, next_ref = g._get_next_ref_n(2)

    # The first yield completed and stays retrievable.
    assert ray.get(value_ref, timeout=30) == "value"

    # Let the actor die now that the first value has been consumed.
    signal.send.remote()

    # The second position is never produced because the actor died.
    with pytest.raises(ActorDiedError):
        ray.get(next_ref, timeout=30)
    # Oracle: the peeked ref surfaces the same error as the completion ref.
    with pytest.raises(ActorDiedError):
        ray.get(g.completed(), timeout=30)


@pytest.mark.skipif(
    sys.platform == "win32", reason="os._exit() worker crash flaky on Windows"
)
@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__get_next_ref_n_worker_crash_surfaces_worker_crashed(ray_start_regular):
    """A ref peeked in advance that a streaming generator task will never produce
    because its worker crashed must surface WorkerCrashedError, matching the
    generator completion ref. This exercises a non-actor terminal error type
    (WORKER_DIED) on the propagation path, complementing the actor-death and
    cancellation cases."""

    # max_retries=0 so the worker crash fails the task immediately instead of
    # being retried, giving a deterministic terminal error.
    @ray.remote(num_returns="streaming", max_retries=0)
    def gen():
        # Crash the worker before producing the value at stream position 0.
        os._exit(1)
        yield "value"

    g = gen.remote()

    [value_ref] = g._get_next_ref_n(1)

    with pytest.raises(WorkerCrashedError):
        ray.get(value_ref, timeout=30)
    # Oracle: the peeked ref surfaces the same error as the completion ref.
    with pytest.raises(WorkerCrashedError):
        ray.get(g.completed(), timeout=30)


def _assert_no_owned_refs_leak():
    """Wait until the owner holds no live references and the store is empty."""

    def check():
        gc.collect()
        core_worker = ray._private.worker.global_worker.core_worker
        ref_counts = core_worker.get_all_reference_counts()
        for rc in ref_counts.values():
            if rc["local"] != 0 or rc["submitted"] != 0:
                return False
        return core_worker.get_memory_store_size() == 0

    wait_for_condition(check, timeout=30)


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test__wait_generators_bulk_no_ref_leak(ray_start_regular):
    """Draining a generator entirely via _wait_generators_bulk must not leak
    owner-side references for the consumed objects.

    The bulk peek (peek_object_ref_stream_n) hands back ObjectRefs that add their
    own local reference, while the owner-side stream reference taken at peek/report
    time is only released for *unconsumed* refs at stream teardown. This test
    confirms whether consumed refs leave that owner-side reference dangling.
    """

    @ray.remote
    def gen():
        for i in range(3):
            yield i

    g = gen.remote()

    collected = []
    saw_eof = False
    while not saw_eof:
        ready = _wait_generators_bulk([(g, [True, True, True])], timeout=10)
        assert len(ready) == 1
        # Avoid binding the generator object to a local (e.g. via tuple unpacking),
        # which would keep its stream alive and prevent teardown.
        refs = ready[0][1]
        for ref in refs:
            try:
                collected.append(ray.get(ref))
            except ObjectRefStreamEndOfStreamError:
                saw_eof = True
                break

    assert collected == [0, 1, 2]

    # Drop every handle to the generator and its (consumed) objects.
    del g, ready, refs, ref

    _assert_no_owned_refs_leak()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
