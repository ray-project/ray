import asyncio
import gc
import os
import signal
import sys
import threading
import time
from typing import List, Optional, Tuple, Type

import numpy as np
import pytest

import ray
from ray._common.test_utils import run_string_as_driver, wait_for_condition
from ray.util.state import list_tasks


def _list_tasks(**kwargs):
    """Same as ``list_tasks`` but pinned to this test's cluster. When several local Ray instances exist"""
    return list_tasks(address=ray.get_runtime_context().gcs_address, **kwargs)


@pytest.mark.parametrize("actor", [False, True])
def test_streaming_generator_backpressure_basic(shutdown_only, actor):
    """Verify backpressure works with
    _generator_backpressure_num_objects = 0
    """
    ray.init(num_cpus=1)

    @ray.remote
    class Reporter:
        def __init__(self):
            self.reported = set()

        def report(self, i):
            self.reported.add(i)

        def reported(self):
            return self.reported

    TOTAL_RETURN = 10

    @ray.remote(_generator_backpressure_num_objects=1)
    def f(reporter):
        for i in range(TOTAL_RETURN):
            print("yield ", i)
            ray.get(reporter.report.remote(i))
            yield i

    @ray.remote
    class A:
        @ray.method(_generator_backpressure_num_objects=1)
        def f(self, reporter):
            for i in range(TOTAL_RETURN):
                print("yield ", i)
                ray.get(reporter.report.remote(i))
                yield i

    reporter = Reporter.remote()

    def check_reported(i):
        return i in ray.get(reporter.reported.remote())

    if actor:
        a = A.remote()
        gen = a.f.remote(reporter)
    else:
        gen = f.remote(reporter)

    # Don't iterate everything to test is deleing the generator
    # would stop backpressure (see the end of the test).
    for i in range(TOTAL_RETURN - 4):
        print("iteration ", i)
        r, _ = ray.wait([gen])
        assert len(r) == 1
        wait_for_condition(lambda: check_reported(i))
        wait_for_condition(lambda: not check_reported(i + 1))
        # Wait a little bit to make sure it is backpressured.
        time.sleep(2)
        wait_for_condition(lambda: not check_reported(i + 1))
        # Consume the ref -> task will progress.
        ray.get(next(gen))
        wait_for_condition(lambda: check_reported(i + 1))

    """
    Verify deleting a generator will stop backpressure
    and proceed a task.
    """

    del gen
    del r
    wait_for_condition(lambda: check_reported(TOTAL_RETURN - 1))


@pytest.mark.parametrize("backpressure_size", [2, 3, 5, 7, 10, 15])
def test_streaming_generator_backpressure_multiple_objects(
    shutdown_only, backpressure_size
):
    """Verify backpressure works when it needs more than 1 objects
    to backpressure.
    """
    ray.init()

    @ray.remote
    class Reporter:
        def __init__(self):
            self.reported = set()

        def report(self, i):
            self.reported.add(i)

        def reported(self):
            return self.reported

    TOTAL_RETURN = 10

    @ray.remote(_generator_backpressure_num_objects=backpressure_size)
    def f(reporter):
        for i in range(TOTAL_RETURN):
            print("yield ", i)
            ray.get(reporter.report.remote(i))
            yield np.random.rand(1024)  # 8KB

    reporter = Reporter.remote()

    def check_reported(i):
        return i in ray.get(reporter.reported.remote())

    gen = f.remote(reporter)
    # It is backpressured for every 2 objects.
    for i in range(0, TOTAL_RETURN - backpressure_size, backpressure_size):
        print("iteration ", i)
        r, _ = ray.wait([gen])
        assert len(r) == 1
        for j in range(backpressure_size):
            wait_for_condition(lambda: check_reported(i + j))
        wait_for_condition(lambda: not check_reported(i + backpressure_size))
        # Wait a little bit to make sure it is backpressured.
        time.sleep(2)
        wait_for_condition(lambda: not check_reported(i + backpressure_size))
        # Consume the ref -> task will progress.
        for j in range(backpressure_size):
            ray.get(next(gen))
            # We cannot have more values than TOTAL_RETURN - 1, so
            # we should use min here.
            wait_for_condition(
                lambda: check_reported(min(i + backpressure_size + j, TOTAL_RETURN - 1))
            )


def test_caller_failure_doesnt_hang(shutdown_only):
    """
    Verify if the caller fails (e.g., exception or crash),
    the generator is not backpressured forever.
    """
    ray.init(num_cpus=2)

    @ray.remote(_generator_backpressure_num_objects=1)
    def f():
        for i in range(5):
            print("yield", i)
            yield i

    @ray.remote
    class Caller:
        def f(self, failure_type, hang, task_name):
            gen = f.options(name=task_name).remote()
            assert ray.get(next(gen)) == 0

            if hang:
                time.sleep(300)

            if failure_type == "exc":
                raise ValueError
            elif failure_type == "exit":
                sys.exit(1)
            else:
                pass

    # If caller finishes, f should finish.
    print("Check caller finishes")
    caller = Caller.remote()
    caller.f.remote(None, False, "1")

    def verify():
        task = _list_tasks(filters=[("name", "=", "1")])[0]
        assert task.state == "FINISHED"
        return True

    wait_for_condition(verify)

    # If caller fails with an exception, f should finish.
    print("Check caller raises an exception")
    caller = Caller.remote()
    r = caller.f.remote("exc", False, "2")  # noqa

    def verify():
        task = _list_tasks(filters=[("name", "=", "2")])[0]
        assert task.state == "FINISHED"
        return True

    wait_for_condition(verify)

    # If caller fails with an exit, f (child) should finish.
    print("Check caller exits")
    caller = Caller.remote()
    r = caller.f.remote("exit", False, "3")  # noqa

    def verify():
        task = _list_tasks(filters=[("name", "=", "3")])[0]
        assert task.state == "FINISHED"
        return True

    wait_for_condition(verify)

    # If caller fails by a system exit, f (child) should fail.
    print("Check caller killed")
    caller = Caller.remote()
    r = caller.f.remote(None, True, "4")  # noqa
    wait_for_condition(
        lambda: _list_tasks(filters=[("name", "=", "4")])[0].state == "RUNNING"
    )
    ray.kill(caller)

    def verify():
        task = _list_tasks(filters=[("name", "=", "4")])[0]
        assert task.state == "FAILED"
        return True

    wait_for_condition(verify)


def test_backpressure_async_generator(shutdown_only):
    """
    Verify that both per-method and actor-wide backpressure work for async
    streaming generators, and that the per-method threshold actually limits how
    far ahead an async generator runs.
    """
    ray.init(num_cpus=1)

    TOTAL_RETURN = 10

    @ray.remote
    class Reporter:
        def __init__(self):
            self.reported = set()

        def report(self, i):
            self.reported.add(i)

        def reported(self):
            return self.reported

    @ray.remote
    class A:
        async def f(self, reporter):
            for i in range(TOTAL_RETURN):
                await reporter.report.remote(i)
                yield i

    reporter = Reporter.remote()

    def check_reported(i):
        return i in ray.get(reporter.reported.remote())

    a = A.remote()
    gen = a.f.options(_generator_backpressure_num_objects=1).remote(reporter)

    # With a threshold of 1, the async generator must not produce object i + 1
    # until object i has been consumed.
    for i in range(TOTAL_RETURN - 1):
        wait_for_condition(lambda: check_reported(i))
        # i + 1 must not be produced while i is unconsumed.
        time.sleep(1)
        assert not check_reported(i + 1), "async generator was not backpressured"
        # Consume the ref -> the task is allowed to produce one more.
        ray.get(next(gen))
        wait_for_condition(lambda: check_reported(i + 1))
    ray.get(next(gen))

    # Actor-wide backpressure on an async actor.
    @ray.remote(_actor_generator_backpressure_num_objects=1)
    class AsyncActorWithActorBP:
        async def f(self):
            for i in range(TOTAL_RETURN):
                yield i

    async_actor_bp = AsyncActorWithActorBP.remote()
    gen = async_actor_bp.f.remote()
    assert [ray.get(ref) for ref in gen] == list(range(TOTAL_RETURN))


def test_backpressure_async_generator_concurrent_no_deadlock(shutdown_only):
    """
    Regression test: concurrent backpressured async streaming generators on the
    same async actor must not deadlock, even when the caller drains them out of
    submission order.

    Output reporting is serialized on a single shared executor thread. If a
    generator parked under backpressure held that thread, a second concurrent
    generator could never report its objects, so draining the second generator
    before the first would hang forever.
    """
    ray.init(num_cpus=1)

    N = 20

    @ray.remote
    class A:
        async def f(self, n):
            for i in range(n):
                yield i

    a = A.remote()
    # Start two generators concurrently; each backpressures after 1 object.
    gen_a = a.f.options(_generator_backpressure_num_objects=1).remote(N)
    gen_b = a.f.options(_generator_backpressure_num_objects=1).remote(N)

    # Let both tasks start and become backpressured before we consume anything.
    time.sleep(1)

    result = {}

    def drain():
        # Drain the second generator fully first, then the first. This is the
        # ordering that deadlocks if the parked generator holds the shared
        # report thread.
        result["b"] = [ray.get(ref) for ref in gen_b]
        result["a"] = [ray.get(ref) for ref in gen_a]

    t = threading.Thread(target=drain, daemon=True)
    t.start()
    t.join(timeout=60)
    assert not t.is_alive(), "concurrent backpressured async generators deadlocked"
    assert result["a"] == list(range(N))
    assert result["b"] == list(range(N))


def test_backpressure_invalid(shutdown_only):
    """
    Verify invalid cases.
    1. Verify _generator_backpressure_num_objects == 0 is not allowed.
    2. Verify _num_objects_per_yield == 0 is not allowed.
    """
    ray.init(num_cpus=1)

    with pytest.raises(ValueError, match="backpressure_num_objects=0 is not allowed"):

        @ray.remote(_generator_backpressure_num_objects=0)
        def f():
            pass

    with pytest.raises(ValueError, match="_num_objects_per_yield"):

        @ray.remote(_num_objects_per_yield=0)
        def g():
            pass

    with pytest.raises(ValueError, match="_num_objects_per_yield"):

        class Actor:
            @ray.method(_num_objects_per_yield=0)
            def g(self):
                pass


def test_threaded_actor_generator_backpressure(shutdown_only):
    ray.init()

    @ray.remote(max_concurrency=10)
    class Actor:
        def f(self):
            for i in range(30):
                time.sleep(0.1)
                print("yield", i)
                yield np.ones(1024 * 1024) * i

    async def main():
        a = Actor.remote()

        async def run():
            i = 0
            gen = a.f.options(_generator_backpressure_num_objects=1).remote()
            async for ref in gen:
                val = ray.get(ref)
                print(val)
                print(ref)
                assert np.array_equal(val, np.ones(1024 * 1024) * i)
                i += 1
                del ref

        coroutines = [run() for _ in range(10)]

        await asyncio.gather(*coroutines)

    asyncio.run(main())


def test_backpressure_pause_signal(shutdown_only):
    """Verify the signal can be caught while the main thread is blocked
    by a backpressure.
    """
    ray.init(num_cpus=2)

    @ray.remote(
        _generator_backpressure_num_objects=1,
        max_retries=0,
    )
    def backpressure_task():
        for i in range(5):
            print("yield ", i)
            yield i

    gen = backpressure_task.remote()
    print(ray.get(next(gen)))

    def wait_for_task():
        t = _list_tasks(filters=[("name", "=", "backpressure_task")])[0]
        assert t.state == "RUNNING"
        return True

    wait_for_condition(wait_for_task)

    t = _list_tasks(filters=[("name", "=", "backpressure_task")])[0]
    os.kill(t.worker_pid, signal.SIGTERM)
    with pytest.raises(ray.exceptions.WorkerCrashedError):
        ray.get(gen._generator_ref)


# ----------------------------------------------------------------------------
# Actor-wide streaming-generator backpressure
# ----------------------------------------------------------------------------

_ACTOR_GEN_BP_WAIT_S = 30


@ray.remote
class TagReporter:
    """Records ``(tag, i)`` for each value a generator is about to yield."""

    def __init__(self):
        self.records: List[Tuple[str, int]] = []

    def report(self, tag: str, i: int) -> None:
        self.records.append((tag, i))

    def total_len(self) -> int:
        return len(self.records)

    def count_tag(self, tag: str) -> int:
        return sum(1 for t, _ in self.records if t == tag)


def _dual_stream_actor_class(
    *,
    actor_cap: int,
    method_cap: Optional[int],
) -> Type:
    """Threaded dual-stream actor: shared actor-wide cap, optional tighter per-method cap.

    When ``method_cap`` is ``None``, only ``_actor_generator_backpressure_num_objects``
    is used (owner threshold includes that cap via ``EffectiveStreamingGenerator...``).
    When set, ``method_cap`` is the per-method ``_generator_backpressure_num_objects``
    for the intersection test (tighter than ``actor_cap``).
    """

    if method_cap is None:

        @ray.remote(
            max_concurrency=2,
            _actor_generator_backpressure_num_objects=actor_cap,
        )
        class _A:
            def gen(self, reporter, tag: str):
                for i in range(5):
                    ray.get(reporter.report.remote(tag, i))
                    yield i

        return _A

    @ray.remote(
        max_concurrency=2,
        _actor_generator_backpressure_num_objects=actor_cap,
    )
    class _B:
        @ray.method(_generator_backpressure_num_objects=method_cap)
        def gen(self, reporter, tag: str):
            for i in range(5):
                ray.get(reporter.report.remote(tag, i))
                yield i

    return _B


def _drain_all(gens):
    """Consume every value from each generator in `gens` to completion."""
    for gen in gens:
        try:
            while True:
                ray.get(next(gen))
        except StopIteration:
            pass


def _drain_cancelled(gen):
    """Best-effort drain after ``ray.cancel``; the stream may end with an error."""
    try:
        while True:
            ray.get(next(gen))
    except StopIteration:
        pass
    except (
        ray.exceptions.RayTaskError,
        ray.exceptions.TaskCancelledError,
    ):
        pass


def test_actor_generator_backpressure_single_task(shutdown_only):
    """Actor-wide cap only (no per-method ``_generator_backpressure_num_objects``)."""
    ray.init(num_cpus=2)
    reporter = TagReporter.remote()

    @ray.remote(_actor_generator_backpressure_num_objects=3)
    class A:
        def gen(self, rep, tag):
            for i in range(5):
                ray.get(rep.report.remote(tag, i))
                yield i

    a = A.remote()
    g = a.gen.remote(reporter, "a")

    # First 3 items get reported, then the producer is parked at the cap.
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("a")) == 3,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    # Consume one → exactly one more is produced.
    ray.get(next(g))
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("a")) == 4,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    # Drain the rest, generator ends.
    _drain_all([g])

    # Fire again: the cap is reclaimed, the new task fills up to 3 again.
    g2 = a.gen.remote(reporter, "a")
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("a")) == 5 + 3,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )
    _drain_all([g2])


def test_actor_generator_backpressure_task_completes_before_consumed(shutdown_only):
    ray.init(num_cpus=2)
    reporter = TagReporter.remote()

    @ray.remote(max_concurrency=1, _actor_generator_backpressure_num_objects=4)
    class A:
        def gen(self, rep, tag):
            for i in range(2):
                ray.get(rep.report.remote(tag, i))
                yield i

        def ping(self):
            return "ok"

    a = A.remote()

    g1 = a.gen.remote(reporter, "1")
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("1")) == 2,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    assert ray.get(a.ping.remote(), timeout=2) == "ok"

    g2 = a.gen.remote(reporter, "2")
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("2")) == 2,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    g3 = a.gen.remote(reporter, "3")
    time.sleep(1)
    assert ray.get(reporter.count_tag.remote("3")) == 0

    assert ray.get(next(g1)) == 0
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("3")) == 1,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    _drain_all([g1, g2, g3])


def test_actor_generator_backpressure_num_objects_per_yield(shutdown_only):
    """Actor-wide cap counts objects, not yields, when ``_num_objects_per_yield`` > 1."""
    ray.init(num_cpus=2)
    reporter = TagReporter.remote()

    # Cap of 4 objects with 2 objects per yield admits at most 2 yields (4
    # objects) before parking. With the (buggy) per-yield accounting the cap
    # would instead admit 4 yields (8 objects) before parking.
    @ray.remote(_actor_generator_backpressure_num_objects=4)
    class A:
        @ray.method(_num_objects_per_yield=2)
        def gen(self, rep, tag):
            for i in range(5):
                ray.get(rep.report.remote(tag, i))
                yield i, i

    a = A.remote()
    g = a.gen.remote(reporter, "a")

    # Exactly two yields (= 4 objects) fill the cap, then the producer parks.
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("a")) == 2,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )
    time.sleep(1)
    assert ray.get(reporter.count_tag.remote("a")) == 2

    # Consuming a ref frees budget below the cap and admits one more yield.
    ray.get(next(g))
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("a")) == 3,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    # Draining the rest lets the generator run to completion (5 yields total).
    _drain_all([g])
    assert ray.get(reporter.count_tag.remote("a")) == 5


def test_actor_generator_backpressure_mt_actor(shutdown_only):
    """Two concurrent sync generator tasks; actor-wide cap 6; reclaim on drain."""
    ray.init(num_cpus=4)
    reporter = TagReporter.remote()
    A = _dual_stream_actor_class(actor_cap=6, method_cap=None)
    a = A.remote()

    g1 = a.gen.remote(reporter, "1")
    g2 = a.gen.remote(reporter, "2")

    wait_for_condition(
        lambda: ray.get(reporter.total_len.remote()) == 6,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    # Consume one ref from g1 — exactly one more report.
    ray.get(next(g1))
    wait_for_condition(
        lambda: ray.get(reporter.total_len.remote()) == 7,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    _drain_all([g1])
    # Pull the rest from g2 (consumption unblocks the producer path for the tail).
    _drain_all([g2])
    assert ray.get(reporter.count_tag.remote("1")) == 5
    assert ray.get(reporter.count_tag.remote("2")) == 5
    assert ray.get(reporter.total_len.remote()) == 10


def test_actor_generator_backpressure_mt_with_method_cap(shutdown_only):
    """Intersections: per-method cap 2 (owner defers sooner) under actor cap 6."""
    ray.init(num_cpus=4)
    reporter = TagReporter.remote()
    A = _dual_stream_actor_class(actor_cap=6, method_cap=2)
    a = A.remote()

    g1 = a.gen.remote(reporter, "1")
    g2 = a.gen.remote(reporter, "2")

    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("1")) == 2,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("2")) == 2,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    ray.get(next(g1))
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("1")) == 3,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    _drain_all([g1, g2])


def test_actor_generator_backpressure_reclaim_on_cancel(shutdown_only):
    """``ray.cancel`` on one stream reclaims actor-wide slots for the other."""
    ray.init(num_cpus=4)
    reporter = TagReporter.remote()
    A = _dual_stream_actor_class(actor_cap=6, method_cap=None)
    a = A.remote()

    g1 = a.gen.remote(reporter, "1")
    g2 = a.gen.remote(reporter, "2")

    wait_for_condition(
        lambda: ray.get(reporter.total_len.remote()) == 6,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    ray.cancel(g1)
    _drain_cancelled(g1)

    _drain_all([g2])
    assert ray.get(reporter.count_tag.remote("2")) == 5


def test_actor_generator_backpressure_reclaim_on_del_gen(shutdown_only):
    """Deleting ``g2`` after partial reads frees BP budget so ``g1`` can emit further yields."""
    ray.init(num_cpus=4)
    reporter = TagReporter.remote()

    @ray.remote(max_concurrency=2, _actor_generator_backpressure_num_objects=6)
    class A:
        def __init__(self):
            self._extend = threading.Event()

        def enable_extend(self) -> None:
            self._extend.set()

        def gen(self, rep, tag: str):
            if tag == "2":
                for i in range(5):
                    ray.get(rep.report.remote(tag, i))
                    yield i
                return
            for i in range(16):
                ray.get(rep.report.remote(tag, i))
                yield i
                if i == 5:
                    self._extend.wait()

    a = A.remote()
    g1 = a.gen.remote(reporter, "1")
    g2 = a.gen.remote(reporter, "2")

    wait_for_condition(
        lambda: ray.get(reporter.total_len.remote()) == 6,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    n_take = 2
    for _ in range(n_take):
        ray.get(next(g2))

    n1_before = ray.get(reporter.count_tag.remote("1"))

    del g2
    gc.collect()

    ray.get(a.enable_extend.remote())

    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("1")) >= n1_before + n_take,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    _drain_all([g1])
    assert ray.get(reporter.count_tag.remote("1")) == 16


def test_actor_generator_backpressure_large_actor_small_method(shutdown_only):
    """Large actor cap does not bypass per-method cap on the executor."""
    ray.init(num_cpus=4)
    reporter = TagReporter.remote()
    A = _dual_stream_actor_class(actor_cap=50, method_cap=2)
    a = A.remote()

    g1 = a.gen.remote(reporter, "1")
    g2 = a.gen.remote(reporter, "2")

    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("1")) == 2,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("2")) == 2,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    ray.get(next(g1))
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("1")) == 3,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    _drain_all([g1, g2])


def test_actor_generator_backpressure_reclaim_on_exception(shutdown_only):
    """Yield-then-raise reclaims cap so a follow-up task can use the budget."""
    ray.init()

    @ray.remote(max_concurrency=2, _actor_generator_backpressure_num_objects=1)
    class A:
        def yields_then_raises(self):
            yield 0
            raise RuntimeError("boom")

        def ok(self):
            for i in range(3):
                yield i

    a = A.remote()

    bad = a.yields_then_raises.remote()
    ray.get(next(bad))
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(next(bad))

    good = a.ok.remote()
    seen = [ray.get(next(good)) for _ in range(3)]
    assert seen == [0, 1, 2]


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Uses SIGTERM on worker_pid from task metadata.",
)
def test_actor_generator_backpressure_reconstruction_worker_crash(shutdown_only):
    """Actor-wide BP sets owner effective threshold 1; worker SIGTERM mid-stream retries."""
    ray.init(num_cpus=2)
    reporter = TagReporter.remote()

    @ray.remote(
        max_concurrency=1,
        max_restarts=5,
        max_task_retries=10,
        _actor_generator_backpressure_num_objects=3,
    )
    class Producer:
        def gen(self, rep):
            for i in range(5):
                ray.get(rep.report.remote("recon", i))
                time.sleep(0.25)
                yield i

    p = Producer.remote()
    gen = p.gen.options(name="actor_wide_bp_recon").remote(reporter)

    assert ray.get(next(gen)) == 0

    def running():
        tasks = _list_tasks(filters=[("name", "=", "actor_wide_bp_recon")])
        return bool(tasks) and tasks[0].state == "RUNNING"

    wait_for_condition(running, timeout=30)
    t = _list_tasks(filters=[("name", "=", "actor_wide_bp_recon")])[0]
    os.kill(t.worker_pid, signal.SIGTERM)

    values = [0]

    def drained_five_values():
        if len(values) >= 5:
            return True
        ref = next(gen)
        try:
            values.append(ray.get(ref))
        except (
            ray.exceptions.WorkerCrashedError,
            ray.exceptions.RayActorError,
        ):
            pass
        return len(values) >= 5

    wait_for_condition(drained_five_values, timeout=120, raise_exceptions=True)

    assert values == list(range(5))
    # Retries replay producer-side reporting before owner-visible consumption catches up.
    assert ray.get(reporter.count_tag.remote("recon")) >= 5

    try:
        while True:
            ray.get(next(gen))
    except StopIteration:
        pass


def test_actor_generator_backpressure_zero_value_rejected(shutdown_only):
    """``_actor_generator_backpressure_num_objects=0`` is invalid."""
    with pytest.raises(
        ValueError,
        match=r"_actor_generator_backpressure_num_objects must be > 0",
    ):

        @ray.remote(_actor_generator_backpressure_num_objects=0)
        class A:
            def f(self):
                yield 0


def test_actor_generator_backpressure_reclaim_on_owner_death(shutdown_only):
    namespace = "actor_generator_backpressure_owner_death"
    actor_name = f"actor_generator_backpressure_owner_death_actor_{os.getpid()}"
    reporter_name = f"actor_generator_backpressure_owner_death_reporter_{os.getpid()}"
    address = ray.init(num_cpus=2, namespace=namespace).address_info["address"]

    driver = f"""
import os

import ray
from ray._common.test_utils import wait_for_condition

ray.init(address="{address}", namespace="{namespace}")


@ray.remote(name="{reporter_name}", lifetime="detached")
class Reporter:
    def __init__(self):
        self.counts = {{}}

    def report(self, tag):
        self.counts[tag] = self.counts.get(tag, 0) + 1

    def count(self, tag):
        return self.counts.get(tag, 0)


@ray.remote(
    name="{actor_name}",
    lifetime="detached",
    max_concurrency=1,
    _actor_generator_backpressure_num_objects=2,
)
class A:
    def gen(self, reporter, tag, count):
        for i in range(count):
            ray.get(reporter.report.remote(tag))
            yield i

    def ping(self):
        return "ok"


reporter = Reporter.remote()
a = A.remote()
g = a.gen.remote(reporter, "first", 1)
wait_for_condition(lambda: ray.get(reporter.count.remote("first")) == 1, timeout=10)
assert ray.get(a.ping.remote(), timeout=10) == "ok"

# Exit
os._exit(0)
"""

    run_string_as_driver(driver)
    a = ray.get_actor(actor_name, namespace=namespace)
    reporter = ray.get_actor(reporter_name, namespace=namespace)
    try:
        g = a.gen.remote(reporter, "second", 2)
        wait_for_condition(
            lambda: ray.get(reporter.count.remote("second")) == 2,
            timeout=_ACTOR_GEN_BP_WAIT_S,
        )
        _drain_all([g])
    finally:
        ray.kill(a)
        ray.kill(reporter)


def test_actor_generator_backpressure_owner_death_unblocks_task_waiter(shutdown_only):
    namespace = "actor_generator_backpressure_owner_death_waiter"
    actor_name = f"actor_generator_backpressure_owner_death_waiter_actor_{os.getpid()}"
    reporter_name = (
        f"actor_generator_backpressure_owner_death_waiter_reporter_{os.getpid()}"
    )
    address = ray.init(num_cpus=2, namespace=namespace).address_info["address"]

    driver = f"""
import os

import ray
from ray._common.test_utils import wait_for_condition

ray.init(address="{address}", namespace="{namespace}")


@ray.remote(name="{reporter_name}", lifetime="detached")
class Reporter:
    def __init__(self):
        self.counts = {{}}

    def report(self, tag):
        self.counts[tag] = self.counts.get(tag, 0) + 1

    def count(self, tag):
        return self.counts.get(tag, 0)


@ray.remote(
    name="{actor_name}",
    lifetime="detached",
    max_concurrency=1,
    _actor_generator_backpressure_num_objects=10,
)
class A:
    @ray.method(_generator_backpressure_num_objects=1)
    def gen(self, reporter, tag):
        for i in range(2):
            ray.get(reporter.report.remote(tag))
            yield i

    def ping(self):
        return "ok"


reporter = Reporter.remote()
a = A.remote()
g = a.gen.remote(reporter, "first")
wait_for_condition(lambda: ray.get(reporter.count.remote("first")) == 1, timeout=10)

os._exit(0)
"""

    run_string_as_driver(driver)
    a = ray.get_actor(actor_name, namespace=namespace)
    reporter = ray.get_actor(reporter_name, namespace=namespace)
    try:
        assert ray.get(a.ping.remote(), timeout=_ACTOR_GEN_BP_WAIT_S) == "ok"
    finally:
        ray.kill(a)
        ray.kill(reporter)


def test_actor_generator_backpressure_owner_death_skips_between_yield_work(
    shutdown_only,
):
    """After owner death we should not run another iteration of between-yield
    user code before exiting. HandleOwnerDied marks the task canceled; the
    executor loop bails before the next gen.send."""
    namespace = "actor_generator_backpressure_owner_death_skip"
    actor_name = f"actor_generator_backpressure_owner_death_skip_actor_{os.getpid()}"
    reporter_name = (
        f"actor_generator_backpressure_owner_death_skip_reporter_{os.getpid()}"
    )
    address = ray.init(num_cpus=2, namespace=namespace).address_info["address"]

    driver = f"""
import os

import ray
from ray._common.test_utils import wait_for_condition

ray.init(address="{address}", namespace="{namespace}")


@ray.remote(name="{reporter_name}", lifetime="detached")
class Reporter:
    def __init__(self):
        self.between_yield_count = 0

    def between_yield(self):
        self.between_yield_count += 1

    def count(self):
        return self.between_yield_count


@ray.remote(
    name="{actor_name}",
    lifetime="detached",
    max_concurrency=1,
    _actor_generator_backpressure_num_objects=10,
)
class A:
    @ray.method(_generator_backpressure_num_objects=1)
    def gen(self, reporter):
        for i in range(10):
            ray.get(reporter.between_yield.remote())
            yield i

    def ping(self):
        return "ok"


reporter = Reporter.remote()
a = A.remote()
g = a.gen.remote(reporter)
# After this returns, the executor has run the between-yield call exactly
# once and is parked in WaitUntilObjectConsumed for yield 0.
wait_for_condition(lambda: ray.get(reporter.count.remote()) == 1, timeout=10)

os._exit(0)
"""

    run_string_as_driver(driver)
    a = ray.get_actor(actor_name, namespace=namespace)
    reporter = ray.get_actor(reporter_name, namespace=namespace)
    try:
        # The actor must accept new tasks (gen task drained).
        assert ray.get(a.ping.remote(), timeout=_ACTOR_GEN_BP_WAIT_S) == "ok"
        # And the between-yield code must not have run another iteration after
        # owner death — the count stays at the one call made before the driver
        # exited.
        assert ray.get(reporter.count.remote()) == 1
    finally:
        ray.kill(a)
        ray.kill(reporter)


def _async_dual_stream_actor_class(
    *,
    actor_cap: int,
    method_cap: Optional[int],
) -> Type:
    """Async counterpart of ``_dual_stream_actor_class``."""
    if method_cap is None:

        @ray.remote(_actor_generator_backpressure_num_objects=actor_cap)
        class _A:
            async def gen(self, reporter, tag: str):
                for i in range(5):
                    await reporter.report.remote(tag, i)
                    yield i

        return _A

    @ray.remote(_actor_generator_backpressure_num_objects=actor_cap)
    class _B:
        @ray.method(_generator_backpressure_num_objects=method_cap)
        async def gen(self, reporter, tag: str):
            for i in range(5):
                await reporter.report.remote(tag, i)
                yield i

    return _B


def test_actor_generator_backpressure_async_single_task(shutdown_only):
    """Async generator, actor-wide cap only (no per-method cap)."""
    ray.init(num_cpus=2)
    reporter = TagReporter.remote()

    @ray.remote(_actor_generator_backpressure_num_objects=3)
    class A:
        async def gen(self, rep, tag):
            for i in range(5):
                await rep.report.remote(tag, i)
                yield i

    a = A.remote()
    g = a.gen.remote(reporter, "a")

    # First 3 items get reported, then the producer parks at the cap.
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("a")) == 3,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )
    # Confirm it is actually parked (no further production without consumption).
    time.sleep(1)
    assert ray.get(reporter.count_tag.remote("a")) == 3

    # Consume one → exactly one more is produced.
    ray.get(next(g))
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("a")) == 4,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    # Drain the rest, generator ends.
    _drain_all([g])

    # Fire again: the cap is reclaimed, the new task fills up to 3 again.
    g2 = a.gen.remote(reporter, "a")
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("a")) == 5 + 3,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )
    _drain_all([g2])


def test_actor_generator_backpressure_async_mt_actor(shutdown_only):
    """Two concurrent async generator tasks share an actor-wide cap of 6.

    Exercises the cross-task wakeup: when one stream's consumption frees shared
    budget, the other parked async generator must be woken to re-check.
    """
    ray.init(num_cpus=4)
    reporter = TagReporter.remote()
    A = _async_dual_stream_actor_class(actor_cap=6, method_cap=None)
    a = A.remote()

    g1 = a.gen.remote(reporter, "1")
    g2 = a.gen.remote(reporter, "2")

    # Together the two producers fill the shared cap of 6, then both park.
    wait_for_condition(
        lambda: ray.get(reporter.total_len.remote()) == 6,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )
    time.sleep(1)
    assert ray.get(reporter.total_len.remote()) == 6

    # Consume one ref from g1 — exactly one more report across the two streams.
    ray.get(next(g1))
    wait_for_condition(
        lambda: ray.get(reporter.total_len.remote()) == 7,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    _drain_all([g1])
    _drain_all([g2])
    assert ray.get(reporter.count_tag.remote("1")) == 5
    assert ray.get(reporter.count_tag.remote("2")) == 5
    assert ray.get(reporter.total_len.remote()) == 10


def test_actor_generator_backpressure_async_num_objects_per_yield(shutdown_only):
    """Actor-wide cap counts objects, not yields, for async generators too."""
    ray.init(num_cpus=2)
    reporter = TagReporter.remote()

    @ray.remote(_actor_generator_backpressure_num_objects=4)
    class A:
        @ray.method(_num_objects_per_yield=2)
        async def gen(self, rep, tag):
            for i in range(5):
                await rep.report.remote(tag, i)
                yield i, i

    a = A.remote()
    g = a.gen.remote(reporter, "a")

    # Two yields (= 4 objects) fill the cap, then the producer parks.
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("a")) == 2,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )
    time.sleep(1)
    assert ray.get(reporter.count_tag.remote("a")) == 2

    # Consuming a ref frees budget below the cap and admits one more yield.
    ray.get(next(g))
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("a")) == 3,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    _drain_all([g])
    assert ray.get(reporter.count_tag.remote("a")) == 5


def test_actor_generator_backpressure_async_mt_with_method_cap(shutdown_only):
    """Per-method cap 2 (tighter) under async actor-wide cap 6.

    Exercises both backpressure paths together: per-task (IsBackpressured) and
    actor-wide (TryReserveSlot), each awaiting the asyncio.Event.
    """
    ray.init(num_cpus=4)
    reporter = TagReporter.remote()
    A = _async_dual_stream_actor_class(actor_cap=6, method_cap=2)
    a = A.remote()

    g1 = a.gen.remote(reporter, "1")
    g2 = a.gen.remote(reporter, "2")

    # Each stream parks at its per-method cap of 2 (tighter than the actor cap).
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("1")) == 2,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("2")) == 2,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )
    time.sleep(1)
    assert ray.get(reporter.count_tag.remote("1")) == 2
    assert ray.get(reporter.count_tag.remote("2")) == 2

    # Consuming a ref from g1 admits exactly one more for that stream.
    ray.get(next(g1))
    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("1")) == 3,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    _drain_all([g1, g2])


def test_actor_generator_backpressure_async_reclaim_on_exception(shutdown_only):
    """Async yield-then-raise reclaims the cap for a follow-up task."""
    ray.init()

    @ray.remote(_actor_generator_backpressure_num_objects=1)
    class A:
        async def yields_then_raises(self):
            yield 0
            raise RuntimeError("boom")

        async def ok(self):
            for i in range(3):
                yield i

    a = A.remote()

    bad = a.yields_then_raises.remote()
    ray.get(next(bad))
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(next(bad))

    # If the exception did not reclaim the cap, this task could never start.
    good = a.ok.remote()
    seen = [ray.get(next(good)) for _ in range(3)]
    assert seen == [0, 1, 2]


def test_actor_generator_backpressure_async_reclaim_on_del_gen(shutdown_only):
    """Deleting a partially-read async stream frees BP budget for the other."""
    ray.init(num_cpus=4)
    reporter = TagReporter.remote()

    @ray.remote(_actor_generator_backpressure_num_objects=6)
    class A:
        def __init__(self):
            self._extend = asyncio.Event()

        async def enable_extend(self) -> None:
            self._extend.set()

        async def gen(self, rep, tag: str):
            if tag == "2":
                for i in range(5):
                    await rep.report.remote(tag, i)
                    yield i
                return
            for i in range(16):
                await rep.report.remote(tag, i)
                yield i
                if i == 5:
                    await self._extend.wait()

    a = A.remote()
    g1 = a.gen.remote(reporter, "1")
    g2 = a.gen.remote(reporter, "2")

    wait_for_condition(
        lambda: ray.get(reporter.total_len.remote()) == 6,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    n_take = 2
    for _ in range(n_take):
        ray.get(next(g2))

    n1_before = ray.get(reporter.count_tag.remote("1"))

    del g2
    gc.collect()

    ray.get(a.enable_extend.remote())

    wait_for_condition(
        lambda: ray.get(reporter.count_tag.remote("1")) >= n1_before + n_take,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )

    _drain_all([g1])
    assert ray.get(reporter.count_tag.remote("1")) == 16


def test_actor_generator_backpressure_mixed_sync_async(shutdown_only):
    """A sync and an async streaming generator on the same actor share the cap.

    Both methods reserve against the one actor-wide waiter; consumption from
    either stream frees shared budget for both (sync producers are woken via the
    condition variable, async producers via the asyncio.Event).
    """
    ray.init(num_cpus=4)
    reporter = TagReporter.remote()

    @ray.remote(_actor_generator_backpressure_num_objects=6)
    class A:
        def sync_gen(self, rep, tag):
            for i in range(5):
                ray.get(rep.report.remote(tag, i))
                yield i

        async def async_gen(self, rep, tag):
            for i in range(5):
                await rep.report.remote(tag, i)
                yield i

    a = A.remote()
    g_sync = a.sync_gen.remote(reporter, "sync")
    g_async = a.async_gen.remote(reporter, "async")

    # The two streams share one cap of 6 (not 6 each): collectively they park at
    # 6 produced. The split between the streams is nondeterministic -- one stream
    # may grab the whole budget -- so we only assert the shared total here.
    wait_for_condition(
        lambda: ray.get(reporter.total_len.remote()) == 6,
        timeout=_ACTOR_GEN_BP_WAIT_S,
    )
    time.sleep(1)
    assert ray.get(reporter.total_len.remote()) == 6

    # Drain both streams concurrently and assert each completes: consuming from
    # either stream frees shared budget for whichever needs it (sync producers
    # are woken via the condition variable, async via the asyncio.Event). They
    # must be drained concurrently because, under an uneven split, one stream can
    # hold the whole budget until the other is consumed.
    results = {}

    def drain(gen, tag):
        results[tag] = [ray.get(ref) for ref in gen]

    threads = [
        threading.Thread(target=drain, args=(g_sync, "sync"), daemon=True),
        threading.Thread(target=drain, args=(g_async, "async"), daemon=True),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=_ACTOR_GEN_BP_WAIT_S)
    assert all(not t.is_alive() for t in threads), "mixed generators deadlocked"
    assert results["sync"] == list(range(5))
    assert results["async"] == list(range(5))
    assert ray.get(reporter.total_len.remote()) == 10


def test_actor_generator_backpressure_async_owner_death_skips_between_yield_work(
    shutdown_only,
):
    """Async analog of the sync owner-death test: after the owner dies, the async
    executor must not run another iteration of between-yield user code before
    exiting. HandleOwnerDied marks the task canceled; the async loop bails on the
    IsTaskCanceled check before the next gen.asend (otherwise the reserve/wait
    completes for the now-dead task and one more gen body runs)."""
    namespace = "actor_generator_backpressure_async_owner_death_skip"
    actor_name = (
        f"actor_generator_backpressure_async_owner_death_skip_actor_{os.getpid()}"
    )
    reporter_name = (
        f"actor_generator_backpressure_async_owner_death_skip_reporter_{os.getpid()}"
    )
    address = ray.init(num_cpus=2, namespace=namespace).address_info["address"]

    driver = f"""
import os

import ray
from ray._common.test_utils import wait_for_condition

ray.init(address="{address}", namespace="{namespace}")


@ray.remote(name="{reporter_name}", lifetime="detached")
class Reporter:
    def __init__(self):
        self.between_yield_count = 0

    def between_yield(self):
        self.between_yield_count += 1

    def count(self):
        return self.between_yield_count


@ray.remote(
    name="{actor_name}",
    lifetime="detached",
    _actor_generator_backpressure_num_objects=10,
)
class A:
    @ray.method(_generator_backpressure_num_objects=1)
    async def gen(self, reporter):
        for i in range(10):
            await reporter.between_yield.remote()
            yield i

    async def ping(self):
        return "ok"


reporter = Reporter.remote()
a = A.remote()
g = a.gen.remote(reporter)
# After this returns, the executor has run the between-yield call exactly once
# and is parked awaiting consumption for yield 0.
wait_for_condition(lambda: ray.get(reporter.count.remote()) == 1, timeout=10)

os._exit(0)
"""

    run_string_as_driver(driver)
    a = ray.get_actor(actor_name, namespace=namespace)
    reporter = ray.get_actor(reporter_name, namespace=namespace)
    try:
        # The actor must accept new tasks (the gen task drained).
        assert ray.get(a.ping.remote(), timeout=_ACTOR_GEN_BP_WAIT_S) == "ok"
        # The between-yield code must not have run another iteration after owner
        # death — the count stays at the one call made before the driver exited.
        assert ray.get(reporter.count.remote()) == 1
    finally:
        ray.kill(a)
        ray.kill(reporter)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
