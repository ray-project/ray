# coding: utf-8
import asyncio
import sys
import threading
import pytest
import ray
import time


# This tests the methods are executed in the correct eventloop.
def test_basic(ray_start_regular_shared):
    @ray.remote(concurrency_groups={"io": 2, "compute": 4})
    class AsyncActor:
        def __init__(self):
            self.eventloop_f1 = None
            self.eventloop_f2 = None
            self.eventloop_f3 = None
            self.eventloop_f4 = None
            self.default_eventloop = asyncio.get_event_loop()

        @ray.method(concurrency_group="io")
        async def f1(self):
            self.eventloop_f1 = asyncio.get_event_loop()
            return threading.current_thread().ident

        @ray.method(concurrency_group="io")
        def f2(self):
            self.eventloop_f2 = asyncio.get_event_loop()
            return threading.current_thread().ident

        @ray.method(concurrency_group="compute")
        def f3(self):
            self.eventloop_f3 = asyncio.get_event_loop()
            return threading.current_thread().ident

        @ray.method(concurrency_group="compute")
        def f4(self):
            self.eventloop_f4 = asyncio.get_event_loop()
            return threading.current_thread().ident

        def f5(self):
            # If this method is executed in default eventloop.
            assert asyncio.get_event_loop() == self.default_eventloop
            return threading.current_thread().ident

        @ray.method(concurrency_group="io")
        def do_assert(self):
            if self.eventloop_f1 != self.eventloop_f2:
                return False
            if self.eventloop_f3 != self.eventloop_f4:
                return False
            if self.eventloop_f1 == self.eventloop_f3:
                return False
            if self.eventloop_f1 == self.eventloop_f4:
                return False
            return True

    ###############################################
    a = AsyncActor.remote()
    f1_thread_id = ray.get(a.f1.remote())  # executed in the "io" group.
    f2_thread_id = ray.get(a.f2.remote())  # executed in the "io" group.
    f3_thread_id = ray.get(a.f3.remote())  # executed in the "compute" group.
    f4_thread_id = ray.get(a.f4.remote())  # executed in the "compute" group.

    assert f1_thread_id == f2_thread_id
    assert f3_thread_id == f4_thread_id
    assert f1_thread_id != f3_thread_id

    assert ray.get(a.do_assert.remote())

    assert ray.get(a.f5.remote())  # executed in the default group.

    # It also has the ability to specify it at runtime.
    # This task will be invoked in the `compute` thread pool.
    result = ray.get(a.f2.options(concurrency_group="compute").remote())
    assert result == f3_thread_id


# The case tests that the asyncio count down works well in one concurrency
# group.
def test_async_methods_in_concurrency_group(ray_start_regular_shared):
    @ray.remote(concurrency_groups={"async": 3})
    class AsyncBatcher:
        def __init__(self):
            self.batch = []
            self.event = None

        @ray.method(concurrency_group="async")
        def init_event(self):
            self.event = asyncio.Event()
            return True

        @ray.method(concurrency_group="async")
        async def add(self, x):
            self.batch.append(x)
            if len(self.batch) >= 3:
                self.event.set()
            else:
                await self.event.wait()
            return sorted(self.batch)

    a = AsyncBatcher.remote()
    ray.get(a.init_event.remote())

    x1 = a.add.remote(1)
    x2 = a.add.remote(2)
    x3 = a.add.remote(3)
    r1 = ray.get(x1)
    r2 = ray.get(x2)
    r3 = ray.get(x3)
    assert r1 == [1, 2, 3]
    assert r1 == r2 == r3


# This case tests that if blocking task in default group blocks
# tasks in other groups.
# See https://github.com/ray-project/ray/issues/20475
def test_default_concurrency_group_does_not_block_others(ray_start_regular_shared):
    @ray.remote(concurrency_groups={"my_group": 1})
    class AsyncActor:
        def __init__(self):
            pass

        async def f1(self):
            time.sleep(10000)
            return "never return"

        @ray.method(concurrency_group="my_group")
        def f2(self):
            return "ok"

    async_actor = AsyncActor.remote()
    async_actor.f1.remote()
    assert "ok" == ray.get(async_actor.f2.remote())


# This case tests that a blocking group doesn't blocks
# tasks in other groups.
# See https://github.com/ray-project/ray/issues/19593
def test_blocking_group_does_not_block_others(ray_start_regular_shared):
    @ray.remote(concurrency_groups={"group1": 1, "group2": 1})
    class AsyncActor:
        def __init__(self):
            pass

        @ray.method(concurrency_group="group1")
        async def f1(self):
            time.sleep(10000)
            return "never return"

        @ray.method(concurrency_group="group2")
        def f2(self):
            return "ok"

    async_actor = AsyncActor.remote()
    # Execute f1 twice for blocking the group1.
    obj_0 = async_actor.f1.remote()
    obj_1 = async_actor.f1.remote()
    # Wait a while to make sure f2 is scheduled after f1.
    ray.wait([obj_0, obj_1], timeout=5)
    # f2 should work well even if group1 is blocking.
    assert "ok" == ray.get(async_actor.f2.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
