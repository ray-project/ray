import asyncio
import os
import signal
import sys
import time
from typing import List, Optional, Tuple, Type

import numpy as np
import pytest

import ray
from ray._common.test_utils import wait_for_condition
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


def test_backpressure_invalid(shutdown_only):
    """
    Verify invalid cases.
    1. Verify using backpressure + async actor raises an exception
    2. Verify _generator_backpressure_num_objects == 0 is not allowed.
    """
    ray.init(num_cpus=1)

    @ray.remote
    class A:
        async def f(self):
            for i in range(10):
                print("yield", i)
                yield i

    a = A.remote()
    gen = a.f.options(_generator_backpressure_num_objects=1).remote()
    with pytest.raises(ValueError):
        ray.get(next(gen))

    with pytest.raises(ValueError, match="backpressure_num_objects=0 is not allowed"):

        @ray.remote(_generator_backpressure_num_objects=0)
        def f():
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
                print("gen", tag)
                for i in range(5):
                    print(tag, " yield", i)
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
    wait_for_condition(lambda: ray.get(reporter.count_tag.remote("a")) == 3)

    # Consume one → exactly one more is produced.
    ray.get(next(g))
    wait_for_condition(lambda: ray.get(reporter.count_tag.remote("a")) == 4)

    # Drain the rest, generator ends.
    _drain_all([g])

    # Fire again: the cap is reclaimed, the new task fills up to 3 again.
    g2 = a.gen.remote(reporter, "a")
    wait_for_condition(lambda: ray.get(reporter.count_tag.remote("a")) == 5 + 3)
    _drain_all([g2])


def test_actor_generator_backpressure_mt_actor(shutdown_only):
    """Two concurrent sync generator tasks; actor-wide cap 6; reclaim on drain."""
    ray.init(num_cpus=4)
    reporter = TagReporter.remote()
    A = _dual_stream_actor_class(actor_cap=6, method_cap=None)
    a = A.remote()

    g1 = a.gen.remote(reporter, "1")
    g2 = a.gen.remote(reporter, "2")

    wait_for_condition(lambda: ray.get(reporter.total_len.remote()) == 6)

    # Consume one ref from g1 — exactly one more report.
    ray.get(next(g1))
    wait_for_condition(lambda: ray.get(reporter.total_len.remote()) == 7)

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

    wait_for_condition(lambda: ray.get(reporter.count_tag.remote("1")) == 2)
    wait_for_condition(lambda: ray.get(reporter.count_tag.remote("2")) == 2)

    ray.get(next(g1))
    wait_for_condition(lambda: ray.get(reporter.count_tag.remote("1")) == 3)

    _drain_all([g1, g2])


def test_actor_generator_backpressure_reclaim_on_cancel(shutdown_only):
    """``ray.cancel`` on one stream reclaims actor-wide slots for the other."""
    ray.init(num_cpus=4)
    reporter = TagReporter.remote()
    A = _dual_stream_actor_class(actor_cap=6, method_cap=None)
    a = A.remote()

    g1 = a.gen.remote(reporter, "1")
    g2 = a.gen.remote(reporter, "2")

    wait_for_condition(lambda: ray.get(reporter.total_len.remote()) == 6)

    ray.cancel(g1)
    _drain_cancelled(g1)

    _drain_all([g2])
    assert ray.get(reporter.count_tag.remote("2")) == 5


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
